#include <cassert>
#include <filesystem>
#include <string>

#include "storage/index/index_manager.h"
#include "catalog/catalog_manager.h"
#include "engine/ddl_executor.h"
#include "common/config.h"
#include "common/exception.h"

using namespace kizuna;

namespace fs = std::filesystem;

bool catalog_manager_ddl_tests()
{
    const std::string db_path = (config::temp_dir() / "catalog_manager_test.kz").string();
    fs::create_directories(fs::path(db_path).parent_path());
    if (fs::exists(db_path))
        fs::remove(db_path);

    FileManager fm(db_path, true);
    fm.open();
    PageManager pm(fm, 32);
    catalog::CatalogManager catalog(pm, fm);
    index::IndexManager index_manager;
    engine::DDLExecutor executor(catalog, pm, fm, index_manager);

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
    auto existing_indexes = catalog.get_indexes(entry.table_id);
    assert(existing_indexes.size() == 1);
    assert(existing_indexes.front().is_primary);
    catalog::IndexCatalogEntry index_entry;
    index_entry.table_id = entry.table_id;
    index_entry.name = "idx_users_name";
    index_entry.is_unique = true;
    index_entry.column_ids = {columns[1].column_id};
    index_entry.create_sql = "CREATE UNIQUE INDEX idx_users_name ON users(name);";
    index_entry.root_page_id = pm.new_page(PageType::INDEX);
    pm.unpin(index_entry.root_page_id, false);


    auto created_index = catalog.create_index(index_entry);
    assert(created_index.index_id != 0);
    assert(created_index.table_id == entry.table_id);
    assert(created_index.is_unique);
    assert(created_index.column_ids.size() == 1);
    assert(created_index.column_ids[0] == columns[1].column_id);


    assert(catalog.index_exists("idx_users_name"));
    auto fetched_index = catalog.get_index("idx_users_name");
    assert(fetched_index.has_value());
    assert(fetched_index->name == "idx_users_name");


    auto table_indexes = catalog.get_indexes(entry.table_id);
    assert(table_indexes.size() == 2);


    bool duplicate_index_threw = false;
    try
    {
        catalog.create_index(index_entry);
    }
    catch (const DBException &ex)
    {
        duplicate_index_threw = (ex.code() == StatusCode::DUPLICATE_KEY);
    }
    assert(duplicate_index_threw);


    bool dropped_index = catalog.drop_index("idx_users_name");
    assert(dropped_index);
    assert(catalog.list_indexes().size() == 1);


    // Recreate after drop to ensure catalog state resets
    index_entry.root_page_id = pm.new_page(PageType::INDEX);
    pm.unpin(index_entry.root_page_id, false);
    created_index = catalog.create_index(index_entry);
    assert(catalog.list_indexes().size() == 2);



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
    assert(catalog.list_indexes().empty());

    auto table_file = FileManager::table_path(entry.table_id);
    assert(!fs::exists(table_file));

    executor.drop_table("DROP TABLE IF EXISTS users;");

    pm.flush_all();
    fm.close();
    if (fs::exists(db_path))
        fs::remove(db_path);

    return true;
}


