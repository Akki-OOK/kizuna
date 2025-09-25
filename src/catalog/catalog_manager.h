#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/schema.h"
#include "storage/page.h"
#include "storage/page_manager.h"
#include "storage/file_manager.h"

namespace kizuna::catalog
{
    class CatalogManager
    {
    public:
        CatalogManager(PageManager &pm, FileManager &fm);

        bool table_exists(std::string_view name) const;
        std::optional<TableCatalogEntry> get_table(std::string_view name) const;
        std::optional<TableCatalogEntry> get_table(table_id_t id) const;
        std::vector<TableCatalogEntry> list_tables() const;

        std::vector<ColumnCatalogEntry> get_columns(table_id_t table_id) const;

        TableCatalogEntry create_table(TableDef def,
                                       page_id_t root_page_id,
                                       const std::string &create_sql);

        bool drop_table(std::string_view name, bool cascade);

    private:
        PageManager &pm_;
        FileManager &fm_;
        page_id_t tables_root_;
        page_id_t columns_root_;

        mutable bool tables_loaded_{false};
        mutable std::vector<TableCatalogEntry> tables_cache_;

        void ensure_catalog_pages();
        void load_tables_cache() const;
        void reload_tables_cache() const;

        void persist_table_entry(const TableCatalogEntry &entry);
        void persist_column_entry(const ColumnCatalogEntry &entry);

        std::vector<TableCatalogEntry> read_all_tables() const;
        std::vector<ColumnCatalogEntry> read_all_columns() const;
        std::vector<ColumnCatalogEntry> read_all_columns(table_id_t table_id) const;

        void rewrite_tables_page(const std::vector<TableCatalogEntry> &entries);
        void rewrite_columns_page(const std::vector<ColumnCatalogEntry> &entries);
    };
}
