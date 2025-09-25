#include <cassert>
#include <filesystem>
#include <string>

#include "catalog/catalog_manager.h"
#include "engine/ddl_executor.h"
#include "common/config.h"
#include "common/exception.h"

using namespace kizuna;

namespace fs = std::filesystem;

bool catalog_manager_ddl_tests()
{
    const std::string db_path = std::string(config::TEMP_DIR) + "catalog_manager_test.kz";
    fs::create_directories(fs::path(db_path).parent_path());
    if (fs::exists(db_path))
        fs::remove(db_path);

    FileManager fm(db_path, true);
    fm.open();
    PageManager pm(fm, 32);
    catalog::CatalogManager catalog(pm, fm);
    engine::DDLExecutor executor(catalog, pm, fm);

    auto entry = executor.create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(32) NOT NULL, age INTEGER DEFAULT 0);");
    assert(entry.name == "users");
    assert(entry.table_id != 0);

    auto tables = catalog.list_tables();
    assert(tables.size() == 1);
    assert(tables.front().name == "users");

    auto columns = catalog.get_columns(entry.table_id);
    assert(columns.size() == 3);
    assert(columns[0].column.constraint.primary_key);
    assert(columns[1].column.constraint.not_null);
    assert(columns[2].column.constraint.has_default);

    bool duplicate_threw = false;
    try
    {
        executor.create_table("CREATE TABLE users (id INTEGER);");
    }
    catch (const DBException &ex)
    {
        duplicate_threw = (ex.code() == StatusCode::TABLE_EXISTS);
    }
    assert(duplicate_threw);

    executor.drop_table("DROP TABLE users;");
    tables = catalog.list_tables();
    assert(tables.empty());

    auto table_file = FileManager::table_path(entry.table_id);
    assert(!fs::exists(table_file));

    executor.drop_table("DROP TABLE IF EXISTS users;");

    pm.flush_all();
    fm.close();
    if (fs::exists(db_path))
        fs::remove(db_path);

    return true;
}

