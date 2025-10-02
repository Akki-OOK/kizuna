#include "cli/repl.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <string_view>

#include "sql/dml_parser.h"

namespace fs = std::filesystem;

namespace
{
    std::string trim_copy(std::string_view text)
    {
        auto begin = text.begin();
        auto end = text.end();
        while (begin != end && std::isspace(static_cast<unsigned char>(*begin)))
            ++begin;
        while (end != begin && std::isspace(static_cast<unsigned char>(*(end - 1))))
            --end;
        return std::string(begin, end);
    }

    std::string to_upper(std::string_view text)
    {
        std::string out;
        out.reserve(text.size());
        for (unsigned char c : text)
            out.push_back(static_cast<char>(std::toupper(c)));
        return out;
    }
}

namespace kizuna
{
    Repl::Repl()
    {
        init_handlers();
        db_path_ = std::string(config::DEFAULT_DB_DIR) + "demo" + config::DB_FILE_EXTENSION;
    }

    void Repl::init_handlers()
    {
        handlers_["help"] = [this](auto const &[[maybe_unused]] args)
        { print_help(); };
        handlers_["status"] = [this](auto const &args)
        { cmd_status(args); };
        handlers_["show"] = [this](auto const &args)
        { cmd_show_tables(args); };
        handlers_["schema"] = [this](auto const &args)
        { cmd_schema(args); };
        handlers_["open"] = [this](auto const &args)
        { cmd_open(args); };
        handlers_["newpage"] = [this](auto const &args)
        { cmd_newpage(args); };
        handlers_["write_demo"] = [this](auto const &args)
        { cmd_write_demo(args); };
        handlers_["read_demo"] = [this](auto const &args)
        { cmd_read_demo(args); };
        handlers_["loglevel"] = [this](auto const &args)
        { cmd_loglevel(args); };
        handlers_["freepage"] = [this](auto const &args)
        { cmd_freepage(args); };
    }

    void Repl::print_help() const
    {
        std::cout << "Commands:\n"
                  << "  help                      - show this help\n"
                  << "  open [path]               - open/create database file (default: " << config::DEFAULT_DB_DIR << "demo" << config::DB_FILE_EXTENSION << ")\n"
                  << "  status                    - show current status\n"
                  << "  show tables               - list tables in the current database\n"
                  << "  schema <table>            - show catalog info for a table\n"
                  << "  newpage [type]            - allocate new page (types: DATA, INDEX, METADATA)\n"
                  << "  write_demo <page_id>      - write a demo record to page\n"
                  << "  read_demo <page_id> <slot>- read and display a demo record\n"
                  << "  freepage <page_id>        - free a page (adds to free list)\n"
                  << "  loglevel <DEBUG|INFO|...> - set log verbosity\n"
                  << "  exit/quit                 - leave\n"
                  << "\nSQL DDL (V0.2):\n"
                  << "  CREATE TABLE <name>(...) [;]     - add a table to the catalog (INT, FLOAT, VARCHAR(n))\n"
                  << "  DROP TABLE [IF EXISTS] <name> [;]- drop table metadata and storage\n"
                  << "\nSQL DML (V0.3 baseline):\n"
                  << "  INSERT INTO <table> VALUES (...);                 - append rows\n"
                  << "  SELECT * FROM <table>;                            - scan entire table\n"
                  << "  DELETE FROM <table>;                              - delete all rows\n"
                  << "  TRUNCATE TABLE <table>;                            - wipe the table fast\n"
                  << "\nSQL DML (V0.4 additions):\n"
                  << "  INSERT INTO <table> [(col,...)] VALUES (...);      - column-targeted inserts\n"
                  << "  SELECT col[, ...] FROM <table> [WHERE ...] [LIMIT n]; - projection + filtering\n"
                  << "  UPDATE <table> SET col = expr[, ...] [WHERE ...];    - edit rows in place\n"
                  << "  DELETE FROM <table> [WHERE ...];                   - remove matching rows\n";
    }

    std::vector<std::string> Repl::tokenize(const std::string &line)
    {
        std::vector<std::string> tokens;
        std::istringstream iss(line);
        std::string tok;
        while (iss >> tok)
            tokens.push_back(tok);
        return tokens;
    }

    bool Repl::ensure_db_open() const
    {
        if (!fm_)
        {
            std::cout << "Open a DB first (use 'open')\n";
            return false;
        }
        return true;
    }

    bool Repl::ensure_valid_data_page(page_id_t id, bool must_exist) const
    {
        if (!ensure_db_open())
            return false;
        if (id == config::FIRST_PAGE_ID)
        {
            std::cout << "Page 1 is reserved for metadata; use a page >= 2\n";
            return false;
        }
        if (must_exist)
        {
            const auto count = fm_->page_count();
            if (id > count)
            {
                std::cout << "Page " << id << " does not exist (page count = " << count << "). Use 'newpage'.\n";
                return false;
            }
        }
        return true;
    }

    int Repl::run()
    {
        std::cout << "Kizuna REPL (V0.4) - type 'help'\n";
        Logger::instance().info("Starting REPL");

        try
        {
            fs::create_directories(config::DEFAULT_DB_DIR);
            fs::create_directories(config::TEMP_DIR);
            fs::create_directories(config::BACKUP_DIR);
        }
        catch (...)
        {
        }

        std::string line;
        while (true)
        {
            std::cout << "> " << std::flush;
            if (!std::getline(std::cin, line))
                break;
            auto tokens = tokenize(line);
            if (tokens.empty())
                continue;

            const std::string cmd = tokens[0];
            if (cmd == "exit" || cmd == "quit")
                break;

            auto it = handlers_.find(cmd);
            try
            {
                if (it != handlers_.end())
                {
                    it->second(tokens);
                }
                else if (looks_like_sql(line))
                {
                    dispatch_sql(line);
                }
                else
                {
                    std::cout << "Unknown command: " << cmd << " (try 'help')\n";
                }
            }
            catch (const DBException &e)
            {
                Logger::instance().error("Exception: ", e.what());
                std::cout << "Error: " << e.what() << "\n";
            }
            catch (const std::exception &e)
            {
                Logger::instance().error("Exception: ", e.what());
                std::cout << "Error: " << e.what() << "\n";
            }
            catch (...)
            {
                Logger::instance().error("Unknown exception");
                std::cout << "Unknown error\n";
            }
        }

        Logger::instance().info("Exiting REPL");
        return 0;
    }

    void Repl::cmd_open(const std::vector<std::string> &args)
    {
        if (args.size() >= 3)
        {
            std::cout << "Usage: open [path]\n";
            return;
        }
        if (args.size() == 2)
            db_path_ = args[1];
        std::cout << "Opening: " << db_path_ << "\n";

        fm_ = std::make_unique<FileManager>(db_path_, /*create_if_missing*/ true);
        fm_->open();
        pm_ = std::make_unique<PageManager>(*fm_, /*capacity*/ 64);
        catalog_ = std::make_unique<catalog::CatalogManager>(*pm_, *fm_);
        ddl_executor_ = std::make_unique<engine::DDLExecutor>(*catalog_, *pm_, *fm_);
        dml_executor_ = std::make_unique<engine::DMLExecutor>(*catalog_, *pm_, *fm_);
        Logger::instance().info("Opened DB ", db_path_);
    }

    void Repl::cmd_status(const std::vector<std::string> &[[maybe_unused]] args)
    {
        std::cout << "DB: " << (fm_ ? db_path_ : std::string("<not open>")) << "\n";
        if (!fm_)
            return;

        std::cout << "  size: " << fm_->size_bytes() << " bytes, pages: " << fm_->page_count();
        if (pm_)
        {
            std::cout << ", free pages: " << pm_->free_count();
        }
        if (catalog_)
        {
            auto tables = catalog_->list_tables();
            std::cout << ", tables: " << tables.size();
        }
        std::cout << "\n";
    }

    void Repl::cmd_schema(const std::vector<std::string> &args)
    {
        if (args.size() != 2)
        {
            std::cout << "Usage: schema <table>\n";
            return;
        }
        if (!ensure_db_open() || !catalog_)
            return;

        const std::string &table_name = args[1];
        auto table_opt = catalog_->get_table(table_name);
        if (!table_opt)
        {
            std::cout << "No table named '" << table_name << "'.\n";
            return;
        }

        const auto &table = *table_opt;
        auto columns = catalog_->get_columns(table.table_id);
        std::cout << "Table: " << table.name
                  << " (id=" << table.table_id
                  << ", root_page=" << table.root_page_id << ")\n";

        if (columns.empty())
        {
            std::cout << "  No columns recorded for this table.\n";
            if (!table.create_sql.empty())
                std::cout << "  CREATE SQL: " << table.create_sql << "\n";
            return;
        }

        std::cout << std::left;
        std::cout << "  #  " << std::setw(18) << "Name"
                  << std::setw(16) << "Type"
                  << "Constraints\n";
        std::cout << "  ------------------------------------------------------------\n";

        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            const auto &col_entry = columns[i];
            const auto &col = col_entry.column;

            std::string type_label;
            switch (col.type)
            {
            case DataType::INTEGER:
                type_label = "INTEGER";
                break;
            case DataType::BIGINT:
                type_label = "BIGINT";
                break;
            case DataType::FLOAT:
                type_label = "FLOAT";
                break;
            case DataType::DOUBLE:
                type_label = "DOUBLE";
                break;
            case DataType::BOOLEAN:
                type_label = "BOOLEAN";
                break;
            case DataType::VARCHAR:
                type_label = "VARCHAR(" + std::to_string(col.length) + ")";
                break;
            default:
                type_label = "TYPE#" + std::to_string(static_cast<int>(col.type));
                break;
            }

            std::string constraints = "";
            const auto &c = col.constraint;
            if (c.primary_key)
            {
                constraints = "PRIMARY KEY";
            }
            else
            {
                if (c.not_null)
                    constraints += constraints.empty() ? "NOT NULL" : ", NOT NULL";
                if (c.unique)
                    constraints += constraints.empty() ? "UNIQUE" : ", UNIQUE";
            }
            if (c.has_default)
            {
                constraints += constraints.empty() ? "DEFAULT " : ", DEFAULT ";
                constraints += c.default_value;
            }
            if (constraints.empty())
                constraints = "-";

            std::cout << "  " << std::setw(3) << (i + 1)
                      << std::setw(18) << col.name
                      << std::setw(16) << type_label
                      << constraints << "\n";
        }

        if (!table.create_sql.empty())
            std::cout << "  CREATE SQL: " << table.create_sql << "\n";
    }

    void Repl::cmd_show_tables(const std::vector<std::string> &args)
    {
        if (args.size() != 2 || to_upper(args[1]) != "TABLES")
        {
            std::cout << "Usage: show tables\n";
            return;
        }
        if (!ensure_db_open() || !catalog_)
            return;

        auto tables = catalog_->list_tables();
        if (tables.empty())
        {
            std::cout << "(no tables yet)\n";
            return;
        }

        std::cout << "Tables (" << tables.size() << "):\n";
        std::cout << "  #  " << std::setw(18) << "Name"
                  << std::setw(10) << "Table ID"
                  << std::setw(12) << "Root Page"
                  << "Columns\n";
        std::cout << "  -----------------------------------------------------------\n";

        for (std::size_t i = 0; i < tables.size(); ++i)
        {
            const auto &table = tables[i];
            auto cols = catalog_->get_columns(table.table_id);
            std::cout << "  " << std::setw(3) << (i + 1)
                      << std::setw(18) << table.name
                      << std::setw(10) << table.table_id
                      << std::setw(12) << table.root_page_id
                      << cols.size() << "\n";
        }
    }

    void Repl::cmd_newpage(const std::vector<std::string> &args)
    {
        if (!ensure_db_open())
            return;

        PageType t = PageType::DATA;
        if (args.size() == 2)
        {
            std::string type = to_upper(args[1]);
            if (type == "DATA")
                t = PageType::DATA;
            else if (type == "INDEX")
                t = PageType::INDEX;
            else if (type == "METADATA")
                t = PageType::METADATA;
            else
            {
                std::cout << "Unknown page type '" << args[1] << "' (use DATA/INDEX/METADATA)\n";
                return;
            }
        }

        page_id_t id = pm_->new_page(t);
        std::cout << "Allocated page " << id << " of type " << static_cast<int>(t) << "\n";
    }

    void Repl::cmd_write_demo(const std::vector<std::string> &args)
    {
        if (args.size() != 2)
        {
            std::cout << "Usage: write_demo <page_id>\n";
            return;
        }
        page_id_t id = static_cast<page_id_t>(std::stoul(args[1]));
        if (!ensure_valid_data_page(id, /*must_exist*/ true))
            return;
        auto &page = pm_->fetch(id, /*pin*/ true);
        auto pt = static_cast<PageType>(page.header().page_type);
        if (pt != PageType::DATA)
        {
            std::cout << "Page " << id << " is not a DATA page.\n";
            pm_->unpin(id, false);
            return;
        }
        std::vector<record::Field> fields;
        fields.push_back(record::from_int32(42));
        fields.push_back(record::from_string("hello world"));
        auto payload = record::encode(fields);
        slot_id_t slot{};
        if (!page.insert(payload.data(), static_cast<uint16_t>(payload.size()), slot))
        {
            std::cout << "Page full or not enough space (free=" << page.free_bytes()
                      << " bytes, need=" << (payload.size() + 2 + sizeof(uint16_t)) << ")\n";
        }
        else
        {
            std::cout << "Wrote record at slot " << slot << "\n";
        }
        pm_->unpin(id, /*dirty*/ true);
    }

    void Repl::cmd_read_demo(const std::vector<std::string> &args)
    {
        if (args.size() != 3)
        {
            std::cout << "Usage: read_demo <page_id> <slot>\n";
            return;
        }
        page_id_t id = static_cast<page_id_t>(std::stoul(args[1]));
        slot_id_t slot = static_cast<slot_id_t>(std::stoul(args[2]));
        if (!ensure_valid_data_page(id, /*must_exist*/ true))
            return;
        auto &page = pm_->fetch(id, /*pin*/ true);
        auto pt = static_cast<PageType>(page.header().page_type);
        if (pt != PageType::DATA)
        {
            std::cout << "Page " << id << " is not a DATA page.\n";
            pm_->unpin(id, false);
            return;
        }
        std::vector<uint8_t> out;
        if (!page.read(slot, out))
        {
            if (slot >= page.header().slot_count)
                std::cout << "No such slot (slot_count=" << page.header().slot_count << ")\n";
            else
                std::cout << "Empty/tombstoned or invalid record at that slot\n";
            pm_->unpin(id, false);
            return;
        }

        std::vector<record::Field> fields;
        if (!record::decode(out.data(), out.size(), fields))
        {
            std::cout << "Failed to decode record\n";
            pm_->unpin(id, false);
            return;
        }
        std::cout << "Record fields (" << fields.size() << "):\n";
        for (size_t i = 0; i < fields.size(); ++i)
        {
            std::cout << "  [" << i << "] ";
            switch (fields[i].type)
            {
            case DataType::INTEGER:
                if (fields[i].payload.size() == 4)
                {
                    int32_t v{};
                    std::memcpy(&v, fields[i].payload.data(), 4);
                    std::cout << "INTEGER=" << v;
                }
                break;
            case DataType::BIGINT:
                if (fields[i].payload.size() == 8)
                {
                    int64_t v{};
                    std::memcpy(&v, fields[i].payload.data(), 8);
                    std::cout << "BIGINT=" << v;
                }
                break;
            case DataType::DOUBLE:
                if (fields[i].payload.size() == 8)
                {
                    double v{};
                    std::memcpy(&v, fields[i].payload.data(), 8);
                    std::cout << "DOUBLE=" << v;
                }
                break;
            case DataType::BOOLEAN:
                if (!fields[i].payload.empty())
                    std::cout << "BOOLEAN=" << (fields[i].payload[0] ? "true" : "false");
                break;
            case DataType::VARCHAR:
                std::cout << "VARCHAR='" << std::string(fields[i].payload.begin(), fields[i].payload.end()) << "'";
                break;
            default:
                std::cout << "type=" << static_cast<int>(fields[i].type) << ", bytes=" << fields[i].payload.size();
                break;
            }
            std::cout << "\n";
        }
        pm_->unpin(id, false);
    }

    void Repl::cmd_loglevel(const std::vector<std::string> &args)
    {
        if (args.size() < 2)
        {
            std::cout << "Usage: loglevel <DEBUG|INFO|WARN|ERROR|FATAL>\n";
            return;
        }
        const std::string lv = to_upper(args[1]);
        LogLevel level = LogLevel::INFO;
        if (lv == "DEBUG")
            level = LogLevel::DEBUG;
        else if (lv == "INFO")
            level = LogLevel::INFO;
        else if (lv == "WARN")
            level = LogLevel::WARN;
        else if (lv == "ERROR")
            level = LogLevel::ERROR;
        else if (lv == "FATAL")
            level = LogLevel::FATAL;
        Logger::instance().set_level(level);
        std::cout << "Log level set to " << lv << "\n";
    }

    void Repl::cmd_freepage(const std::vector<std::string> &args)
    {
        if (args.size() != 2)
        {
            std::cout << "Usage: freepage <page_id>\n";
            return;
        }
        page_id_t id = static_cast<page_id_t>(std::stoul(args[1]));
        if (!ensure_valid_data_page(id, /*must_exist*/ true))
            return;
        pm_->free_page(id);
        std::cout << "Freed page " << id << " (added to free list)\n";
    }

    bool Repl::looks_like_sql(const std::string &line) const
    {
        auto trimmed = trim_copy(line);
        if (trimmed.empty())
            return false;
        if (trimmed.find(';') != std::string::npos)
            return true;

        std::istringstream iss(trimmed);
        std::string keyword;
        if (!(iss >> keyword))
            return false;
        std::string upper = to_upper(keyword);
        static const std::array<std::string, 7> sql_keywords = {"CREATE", "DROP", "ALTER", "TRUNCATE", "INSERT", "SELECT", "DELETE"};
        return std::find(sql_keywords.begin(), sql_keywords.end(), upper) != sql_keywords.end();
    }

    void Repl::dispatch_sql(const std::string &line)
    {
        if (!ensure_db_open() || !catalog_)
            return;
        auto trimmed = trim_copy(line);
        if (trimmed.empty())
            return;

        std::istringstream iss(trimmed);
        std::string keyword;
        if (!(iss >> keyword))
            return;
        std::string upper = to_upper(keyword);

        auto print_query_error = [&](const QueryException &e)
        {
            const char *code = status_code_to_string(e.code());
            std::cout << "SQL error [" << code << "] " << e.message();
            if (!e.context().empty())
                std::cout << " (" << e.context() << ")";
            std::cout << "\n";
        };
        auto print_engine_error = [&](const DBException &e)
        {
            const char *code = status_code_to_string(e.code());
            std::cout << "Engine error [" << code << "] " << e.message();
            if (!e.context().empty())
                std::cout << " (" << e.context() << ")";
            std::cout << "\n";
        };

        auto is_dml_keyword = [&](const std::string &kw)
        {
            return kw == "INSERT" || kw == "SELECT" || kw == "DELETE" || kw == "UPDATE" || kw == "TRUNCATE";
        };

        try
        {
            if (is_dml_keyword(upper))
            {
                if (!dml_executor_)
                {
                    std::cout << "DML executor not initialized (open a database first)\n";
                    return;
                }

                if (upper == "SELECT")
                {
                    auto stmt = sql::parse_select(trimmed);
                    auto result = dml_executor_->select(stmt);

                    if (result.column_names.empty())
                    {
                        std::cout << "(no columns)\n";
                    }
                    else
                    {
                        std::cout << "Columns:";
                        for (const auto &name : result.column_names)
                            std::cout << ' ' << name;
                        std::cout << "\n";
                    }

                    if (result.rows.empty())
                    {
                        std::cout << "(no rows)\n";
                    }
                    else
                    {
                        for (const auto &row : result.rows)
                        {
                            std::cout << "  ";
                            for (std::size_t i = 0; i < row.size(); ++i)
                            {
                                if (i)
                                    std::cout << " | ";
                                std::cout << row[i];
                            }
                            std::cout << "\n";
                        }
                    }

                    std::cout << "[rows=" << result.rows.size() << "]\n";
                }
                else if (upper == "DELETE")
                {
                    auto stmt = sql::parse_delete(trimmed);
                    auto result = dml_executor_->delete_all(stmt);
                    std::cout << "[rows=" << result.rows_deleted << "] deleted\n";
                }
                else if (upper == "UPDATE")
                {
                    auto stmt = sql::parse_update(trimmed);
                    auto result = dml_executor_->update_all(stmt);
                    std::cout << "[rows=" << result.rows_updated << "] updated\n";
                }
                else
                {
                    std::string message = dml_executor_->execute(trimmed);
                    std::cout << message << "\n";
                }
                return;
            }

            if (!ddl_executor_)
            {
                std::cout << "DDL executor not initialized (open a database first)\n";
                return;
            }

            if (upper == "CREATE" || upper == "DROP" || upper == "ALTER")
            {
                std::string message = ddl_executor_->execute(trimmed);
                std::cout << message << "\n";
                return;
            }

            std::cout << "Unknown SQL command (try 'help')\n";
        }
        catch (const QueryException &e)
        {
            print_query_error(e);
        }
        catch (const DBException &e)
        {
            print_engine_error(e);
        }
        catch (const std::exception &e)
        {
            std::cout << "Unhandled std::exception: " << e.what() << "\n";
        }
    }
}
