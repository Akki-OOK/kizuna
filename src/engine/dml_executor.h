#pragma once

#include <string>
#include <vector>

#include "catalog/catalog_manager.h"
#include "sql/ast.h"
#include "sql/dml_parser.h"
#include "storage/table_heap.h"

namespace kizuna::engine
{
    struct InsertResult
    {
        std::size_t rows_inserted{0};
    };

    struct DeleteResult
    {
        std::size_t rows_deleted{0};
    };

    class DMLExecutor
    {
    public:
        DMLExecutor(catalog::CatalogManager &catalog,
                    PageManager &pm,
                    FileManager &fm);

        InsertResult insert_into(const sql::InsertStatement &stmt);
        std::vector<std::vector<std::string>> select_all(const sql::SelectStatement &stmt);
        DeleteResult delete_all(const sql::DeleteStatement &stmt);
        void truncate(const sql::TruncateStatement &stmt);

        std::string execute(std::string_view sql);

    private:
        catalog::CatalogManager &catalog_;
        PageManager &pm_;
        FileManager &fm_;

        std::vector<std::vector<std::string>> materialize_rows(const catalog::TableCatalogEntry &table_entry,
                                                               const std::vector<catalog::ColumnCatalogEntry> &columns);
        std::vector<std::string> materialize_row(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                                 const std::vector<uint8_t> &payload) const;
        std::vector<uint8_t> encode_row(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                        const sql::InsertRow &row,
                                        const std::vector<std::string> &column_names);
    };
}

