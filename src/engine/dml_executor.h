#pragma once

#include <string>
#include <vector>

#include "catalog/catalog_manager.h"
#include "common/value.h"
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

    struct UpdateResult
    {
        std::size_t rows_updated{0};
    };

    struct SelectResult
    {
        std::vector<std::string> column_names;
        std::vector<std::vector<std::string>> rows;
    };

    class DMLExecutor
    {
    public:
        DMLExecutor(catalog::CatalogManager &catalog,
                    PageManager &pm,
                    FileManager &fm);

        InsertResult insert_into(const sql::InsertStatement &stmt);
        SelectResult select(const sql::SelectStatement &stmt);
        DeleteResult delete_all(const sql::DeleteStatement &stmt);
        UpdateResult update_all(const sql::UpdateStatement &stmt);
        void truncate(const sql::TruncateStatement &stmt);

        std::string execute(std::string_view sql);

    private:
        catalog::CatalogManager &catalog_;
        PageManager &pm_;
        FileManager &fm_;

        std::vector<uint8_t> encode_row(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                        const sql::InsertRow &row,
                                        const std::vector<std::string> &column_names);

        std::vector<Value> decode_row_values(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                             const std::vector<uint8_t> &payload) const;
        std::vector<uint8_t> encode_values(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                           const std::vector<Value> &values) const;
        Value coerce_value_for_column(const catalog::ColumnCatalogEntry &column,
                                      const Value &value) const;
        std::vector<size_t> build_projection(const sql::SelectStatement &stmt,
                                             const std::vector<catalog::ColumnCatalogEntry> &columns,
                                             const std::string &table_name,
                                             std::vector<std::string> &out_names) const;
        std::size_t find_column_index(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                      const std::string &table_name,
                                      const sql::ColumnRef &ref) const;
    };
}
