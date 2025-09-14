#include "cli/repl.h"

#include <cctype>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>

namespace fs = std::filesystem;

namespace kizuna
{
    Repl::Repl()
    {
        init_handlers();
        // Default DB path
        db_path_ = std::string(config::DEFAULT_DB_DIR) + "demo" + config::DB_FILE_EXTENSION;
    }

    void Repl::init_handlers()
    {
        handlers_["help"] = [this](auto const & [[maybe_unused]] args) { print_help(); };
        handlers_["status"] = [this](auto const &args) { cmd_status(args); };
        handlers_["open"] = [this](auto const &args) { cmd_open(args); };
        handlers_["newpage"] = [this](auto const &args) { cmd_newpage(args); };
        handlers_["write_demo"] = [this](auto const &args) { cmd_write_demo(args); };
        handlers_["read_demo"] = [this](auto const &args) { cmd_read_demo(args); };
        handlers_["loglevel"] = [this](auto const &args) { cmd_loglevel(args); };
        handlers_["freepage"] = [this](auto const &args) { cmd_freepage(args); };
        // Exit commands handled in run()
    }

    void Repl::print_help() const
    {
        std::cout << "Commands:\n"
                  << "  help                      - show this help\n"
                  << "  open [path]               - open/create database file (default: " << config::DEFAULT_DB_DIR << "demo" << config::DB_FILE_EXTENSION << ")\n"
                  << "  status                    - show current status\n"
                  << "  newpage [type]            - allocate new page (types: DATA, INDEX, METADATA)\n"
                  << "  write_demo <page_id>      - write a demo record to page\n"
                  << "  read_demo <page_id> <slot>- read and display a demo record\n"
                  << "  freepage <page_id>        - free a page (adds to free list)\n"
                  << "  loglevel <DEBUG|INFO|...> - set log verbosity\n"
                  << "  exit/quit                 - leave\n";
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
        if (!ensure_db_open()) return false;
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
        std::cout << "Kizuna REPL (V0.1) — type 'help'\n";
        Logger::instance().info("Starting REPL");

        // Ensure default dirs
        try
        {
            fs::create_directories(config::DEFAULT_DB_DIR);
            fs::create_directories(config::TEMP_DIR);
            fs::create_directories(config::BACKUP_DIR);
        }
        catch (...) {}

        std::string line;
        while (true)
        {
            std::cout << "> " << std::flush;
            if (!std::getline(std::cin, line)) break;
            auto tokens = tokenize(line);
            if (tokens.empty()) continue;

            const std::string cmd = tokens[0];
            if (cmd == "exit" || cmd == "quit") break;

            auto it = handlers_.find(cmd);
            try
            {
                if (it != handlers_.end())
                {
                    it->second(tokens);
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
            std::cout << "Usage: open [path]" << "\n";
            return;
        }
        if (args.size() == 2)
            db_path_ = args[1];
        std::cout << "Opening: " << db_path_ << "\n";

        fm_ = std::make_unique<FileManager>(db_path_, /*create_if_missing*/ true);
        fm_->open();
        pm_ = std::make_unique<PageManager>(*fm_, /*capacity*/ 8);
        Logger::instance().info("Opened DB ", db_path_);
    }

    void Repl::cmd_status(const std::vector<std::string> & [[maybe_unused]] args)
    {
        std::cout << "DB: " << (fm_ ? db_path_ : std::string("<not open>")) << "\n";
        if (fm_)
        {
            std::cout << "  size: " << fm_->size_bytes() << " bytes, pages: " << fm_->page_count();
            if (pm_)
            {
                std::cout << ", free pages: " << pm_->free_count();
            }
            std::cout << "\n";
        }
    }

    static PageType parse_page_type(const std::string &s)
    {
        if (s == "DATA") return PageType::DATA;
        if (s == "INDEX") return PageType::INDEX;
        if (s == "METADATA") return PageType::METADATA;
        return PageType::DATA;
    }

    void Repl::cmd_newpage(const std::vector<std::string> &args)
    {
        if (!pm_) { std::cout << "Open a DB first (use 'open')\n"; return; }
        if (args.size() > 2)
        {
            std::cout << "Usage: newpage [DATA|INDEX|METADATA]" << "\n";
            return;
        }
        PageType t = PageType::DATA;
        if (args.size() >= 2) t = parse_page_type(args[1]);
        page_id_t id = pm_->new_page(t);
        std::cout << "New page id: " << id << "\n";
        pm_->unpin(id, /*dirty*/ false);
    }

    void Repl::cmd_write_demo(const std::vector<std::string> &args)
    {
        if (args.size() != 2) { std::cout << "Usage: write_demo <page_id>\n"; return; }
        page_id_t id = static_cast<page_id_t>(std::stoul(args[1]));
        if (!ensure_valid_data_page(id, /*must_exist*/ true)) return;
        auto &page = pm_->fetch(id, /*pin*/ true);
        // Disallow writing into non-DATA pages
        auto pt = static_cast<PageType>(page.header().page_type);
        if (pt == PageType::FREE || pt == PageType::INVALID)
        {
            std::cout << "Page " << id << " is not allocated for data. Use 'newpage' to allocate.\n";
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
            const auto fb = page.free_bytes();
            std::cout << "Page full or not enough space (free=" << fb
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
        if (args.size() != 3) { std::cout << "Usage: read_demo <page_id> <slot>\n"; return; }
        page_id_t id = static_cast<page_id_t>(std::stoul(args[1]));
        slot_id_t slot = static_cast<slot_id_t>(std::stoul(args[2]));
        if (!ensure_valid_data_page(id, /*must_exist*/ true)) return;
        auto &page = pm_->fetch(id, /*pin*/ true);
        auto pt = static_cast<PageType>(page.header().page_type);
        if (pt == PageType::FREE || pt == PageType::INVALID)
        {
            std::cout << "Page " << id << " is not a data page.\n";
            pm_->unpin(id, false);
            return;
        }
        std::vector<uint8_t> out;
        if (!page.read(slot, out))
        {
            // Give a more precise hint: out-of-range vs tombstone/corruption
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
                    int32_t v{}; std::memcpy(&v, fields[i].payload.data(), 4);
                    std::cout << "INTEGER=" << v;
                }
                break;
            case DataType::BIGINT:
                if (fields[i].payload.size() == 8)
                {
                    int64_t v{}; std::memcpy(&v, fields[i].payload.data(), 8);
                    std::cout << "BIGINT=" << v;
                }
                break;
            case DataType::DOUBLE:
                if (fields[i].payload.size() == 8)
                {
                    double v{}; std::memcpy(&v, fields[i].payload.data(), 8);
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
        if (args.size() < 2) { std::cout << "Usage: loglevel <DEBUG|INFO|WARN|ERROR|FATAL>\n"; return; }
        const std::string lv = args[1];
        LogLevel level = LogLevel::INFO;
        if (lv == "DEBUG") level = LogLevel::DEBUG;
        else if (lv == "INFO") level = LogLevel::INFO;
        else if (lv == "WARN") level = LogLevel::WARN;
        else if (lv == "ERROR") level = LogLevel::ERROR;
        else if (lv == "FATAL") level = LogLevel::FATAL;
        Logger::instance().set_level(level);
        std::cout << "Log level set to " << lv << "\n";
    }

    void Repl::cmd_freepage(const std::vector<std::string> &args)
    {
        if (args.size() != 2) { std::cout << "Usage: freepage <page_id>\n"; return; }
        page_id_t id = static_cast<page_id_t>(std::stoul(args[1]));
        if (!ensure_valid_data_page(id, /*must_exist*/ true)) return;
        pm_->free_page(id);
        std::cout << "Freed page " << id << " (added to free list)\n";
    }
}
