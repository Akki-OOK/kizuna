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
            pm = std::make_unique<PageManager>(fm, /*capacity*/ 16);
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

    bool basic_flow()
    {
        TestContext ctx("dml_exec_basic");
        engine::DDLExecutor ddl(*ctx.catalog, *ctx.pm, ctx.fm);
        ddl.create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(10) NOT NULL, active BOOLEAN);");

        engine::DMLExecutor dml(*ctx.catalog, *ctx.pm, ctx.fm);

        auto insert_stmt = sql::parse_insert("INSERT INTO users (id, name, active) VALUES (1, 'alice', TRUE), (2, 'bob', FALSE);");
        auto insert_result = dml.insert_into(insert_stmt);
        if (insert_result.rows_inserted != 2) return false;

        auto rows = dml.select_all(sql::parse_select("SELECT * FROM users;"));
        if (rows.size() != 2) return false;
        if (rows[0][0] != "1" || rows[0][1] != "alice" || rows[0][2] != "true") return false;
        if (rows[1][0] != "2" || rows[1][1] != "bob" || rows[1][2] != "false") return false;

        auto delete_result = dml.delete_all(sql::parse_delete("DELETE FROM users;"));
        if (delete_result.rows_deleted != 2) return false;

        rows = dml.select_all(sql::parse_select("SELECT * FROM users;"));
        if (!rows.empty()) return false;

        bool caught_not_null = false;
        try
        {
            auto bad_insert = sql::parse_insert("INSERT INTO users (id, name, active) VALUES (3, NULL, TRUE);");
            (void)dml.insert_into(bad_insert);
        }
        catch (const QueryException &)
        {
            caught_not_null = true;
        }
        if (!caught_not_null) return false;

        auto insert_again = sql::parse_insert("INSERT INTO users (id, name, active) VALUES (3, 'carol', TRUE);");
        dml.insert_into(insert_again);

        dml.truncate(sql::parse_truncate("TRUNCATE TABLE users;"));
        rows = dml.select_all(sql::parse_select("SELECT * FROM users;"));
        if (!rows.empty()) return false;

        return true;
    }
}

bool dml_executor_tests()
{
    return basic_flow();
}
