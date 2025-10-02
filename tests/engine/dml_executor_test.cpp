#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "common/exception.h"
#include "engine/ddl_executor.h"
#include "engine/dml_executor.h"
#include "sql/dml_parser.h"
#include "storage/file_manager.h"
#include "storage/page_manager.h"

using namespace kizuna;
namespace fs = std::filesystem;

namespace
{
    struct TestContext
    {
        std::string db_path;
        FileManager fm;
        std::unique_ptr<PageManager> pm;
        std::unique_ptr<catalog::CatalogManager> catalog;

        explicit TestContext(const std::string &name)
            : db_path(std::string(config::TEMP_DIR) + name + config::DB_FILE_EXTENSION),
              fm(db_path, /*create_if_missing*/ true)
        {
            std::error_code ec;
            fs::create_directories(config::TEMP_DIR, ec);
            fs::remove(db_path, ec);
            fm.open();
            pm = std::make_unique<PageManager>(fm, /*capacity*/ 32);
            catalog = std::make_unique<catalog::CatalogManager>(*pm, fm);
        }

        ~TestContext()
        {
            catalog.reset();
            pm.reset();
            fm.close();
            std::error_code ec;
            fs::remove(db_path, ec);
        }
    };

    constexpr const char *kCreateEmployeesSql = "CREATE TABLE employees (id INTEGER PRIMARY KEY, name VARCHAR(32), active BOOLEAN, age INTEGER, joined DATE, nickname VARCHAR(32));";
    constexpr const char *kSeedEmployeesSql = "INSERT INTO employees (id, name, active, age, joined, nickname) VALUES (1, 'amy', TRUE, 25, '2023-05-01', 'ace'), (2, 'beth', TRUE, 34, '2022-04-15', NULL), (3, 'cora', FALSE, 31, '2020-01-01', 'cee'), (4, 'dina', TRUE, 41, '2019-12-12', NULL);";

    bool basic_flow()
    {
        TestContext ctx("dml_exec_v04");
        engine::DDLExecutor ddl(*ctx.catalog, *ctx.pm, ctx.fm);
        ddl.create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(32) NOT NULL, active BOOLEAN);");

        engine::DMLExecutor dml(*ctx.catalog, *ctx.pm, ctx.fm);

        auto insert_stmt = sql::parse_insert("INSERT INTO users (id, name, active) VALUES (1, 'alice', TRUE), (2, 'bob', FALSE), (3, 'cara', TRUE);");
        auto insert_result = dml.insert_into(insert_stmt);
        if (insert_result.rows_inserted != 3) return false;

        auto select_limit = dml.select(sql::parse_select("SELECT name FROM users WHERE active LIMIT 1;"));
        if (select_limit.column_names != std::vector<std::string>{"name"}) return false;
        if (select_limit.rows.size() != 1 || select_limit.rows[0][0] != "alice") return false;

        auto delete_result = dml.delete_all(sql::parse_delete("DELETE FROM users WHERE active = FALSE;"));
        if (delete_result.rows_deleted != 1) return false;

        auto update_result = dml.update_all(sql::parse_update("UPDATE users SET name = 'ally', active = FALSE WHERE id = 1;"));
        if (update_result.rows_updated != 1) return false;

        auto update_long = dml.update_all(sql::parse_update("UPDATE users SET name = 'this string is definitely longer' WHERE id = 3;"));
        if (update_long.rows_updated != 1) return false;

        auto check_rows = dml.select(sql::parse_select("SELECT id, name, active FROM users;"));
        if (check_rows.column_names != std::vector<std::string>{"id", "name", "active"}) return false;
        if (check_rows.rows.size() != 2) return false;

        bool found_ally = false;
        bool found_cara = false;
        for (const auto &row : check_rows.rows)
        {
            if (row.size() != 3) return false;
            if (row[0] == "1" && row[1] == "ally" && row[2] == "FALSE")
                found_ally = true;
            if (row[0] == "3" && row[1] == "this string is definitely longer" && row[2] == "TRUE")
                found_cara = true;
        }
        if (!found_ally || !found_cara) return false;

        bool caught_not_null = false;
        try
        {
            dml.update_all(sql::parse_update("UPDATE users SET name = NULL WHERE id = 1;"));
        }
        catch (const QueryException &)
        {
            caught_not_null = true;
        }
        if (!caught_not_null) return false;

        auto limit_zero = dml.select(sql::parse_select("SELECT name FROM users LIMIT 0;"));
        if (limit_zero.rows.size() != 0 || limit_zero.column_names != std::vector<std::string>{"name"}) return false;

        auto delete_all = dml.delete_all(sql::parse_delete("DELETE FROM users;"));
        if (delete_all.rows_deleted != 2) return false;

        auto empty = dml.select(sql::parse_select("SELECT * FROM users;"));
        if (empty.rows.size() != 0) return false;
        if (empty.column_names != std::vector<std::string>{"id", "name", "active"}) return false;

        dml.truncate(sql::parse_truncate("TRUNCATE TABLE users;"));
        auto after_truncate = dml.select(sql::parse_select("SELECT * FROM users;"));
        if (!after_truncate.rows.empty()) return false;

        return true;
    }

    bool projection_limit_tests()
    {
        TestContext ctx("dml_exec_projection");
        engine::DDLExecutor ddl(*ctx.catalog, *ctx.pm, ctx.fm);
        ddl.create_table(kCreateEmployeesSql);

        engine::DMLExecutor dml(*ctx.catalog, *ctx.pm, ctx.fm);
        dml.insert_into(sql::parse_insert(kSeedEmployeesSql));

        auto projection = dml.select(sql::parse_select("SELECT id, name, active, age, joined, nickname FROM employees WHERE active AND age >= 30 LIMIT 5;"));
        const std::vector<std::string> expected_columns = {"id", "name", "active", "age", "joined", "nickname"};
        if (projection.column_names != expected_columns) return false;

        const std::vector<std::vector<std::string>> expected_rows = {
            {"2", "beth", "TRUE", "34", "2022-04-15", "NULL"},
            {"4", "dina", "TRUE", "41", "2019-12-12", "NULL"}
        };
        if (projection.rows != expected_rows) return false;

        auto star_projection = dml.select(sql::parse_select("SELECT * FROM employees LIMIT 1;"));
        if (star_projection.column_names != expected_columns) return false;

        auto limit_two = dml.select(sql::parse_select("SELECT name FROM employees WHERE active LIMIT 2;"));
        if (limit_two.rows != std::vector<std::vector<std::string>>{{"amy"}, {"beth"}}) return false;

        auto limit_all = dml.select(sql::parse_select("SELECT name FROM employees WHERE active LIMIT 10;"));
        if (limit_all.rows.size() != 3) return false;
        if (limit_all.rows[2][0] != "dina") return false;

        return true;
    }

    bool predicate_null_tests()
    {
        TestContext ctx("dml_exec_predicates");
        engine::DDLExecutor ddl(*ctx.catalog, *ctx.pm, ctx.fm);
        ddl.create_table(kCreateEmployeesSql);

        engine::DMLExecutor dml(*ctx.catalog, *ctx.pm, ctx.fm);
        dml.insert_into(sql::parse_insert(kSeedEmployeesSql));

        auto null_ids = dml.select(sql::parse_select("SELECT id FROM employees WHERE nickname IS NULL;"));
        if (null_ids.rows != std::vector<std::vector<std::string>>{{"2"}, {"4"}}) return false;

        auto not_null = dml.select(sql::parse_select("SELECT id FROM employees WHERE nickname IS NOT NULL;"));
        if (not_null.rows != std::vector<std::vector<std::string>>{{"1"}, {"3"}}) return false;

        auto not_active = dml.select(sql::parse_select("SELECT id FROM employees WHERE NOT active;"));
        if (not_active.rows != std::vector<std::vector<std::string>>{{"3"}}) return false;

        auto or_pred = dml.select(sql::parse_select("SELECT id FROM employees WHERE nickname = 'ace' OR NOT active;"));
        if (or_pred.rows != std::vector<std::vector<std::string>>{{"1"}, {"3"}}) return false;

        auto delete_none = dml.delete_all(sql::parse_delete("DELETE FROM employees WHERE nickname = 'zzz';"));
        if (delete_none.rows_deleted != 0) return false;

        auto update_null = dml.update_all(sql::parse_update("UPDATE employees SET nickname = NULL WHERE id = 3;"));
        if (update_null.rows_updated != 1) return false;

        auto null_after = dml.select(sql::parse_select("SELECT id FROM employees WHERE nickname IS NULL;"));
        if (null_after.rows != std::vector<std::vector<std::string>>{{"2"}, {"3"}, {"4"}}) return false;

        auto delete_inactive = dml.delete_all(sql::parse_delete("DELETE FROM employees WHERE NOT active;"));
        if (delete_inactive.rows_deleted != 1) return false;

        auto remaining = dml.select(sql::parse_select("SELECT id FROM employees;"));
        if (remaining.rows.size() != 3) return false;
        if (remaining.rows[0][0] != "1" || remaining.rows[1][0] != "2" || remaining.rows[2][0] != "4") return false;

        return true;
    }
}

bool dml_executor_tests()
{
    return basic_flow() && projection_limit_tests() && predicate_null_tests();
}
