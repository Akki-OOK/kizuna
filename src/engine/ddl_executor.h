#pragma once

#include <string>
#include <string_view>

#include "catalog/catalog_manager.h"
#include "sql/ddl_parser.h"
#include "storage/index/index_manager.h"

namespace kizuna::engine
{
    class DDLExecutor
    {
    public:
        DDLExecutor(catalog::CatalogManager &catalog,
                    PageManager &pm,
                    FileManager &fm,
                    index::IndexManager &index_manager);

        catalog::TableCatalogEntry create_table(std::string_view sql);
        void drop_table(std::string_view sql);
        std::string execute(std::string_view sql);

    private:
        catalog::CatalogManager &catalog_;
        PageManager &pm_;
        FileManager &fm_;
        index::IndexManager &index_manager_;

        catalog::TableCatalogEntry create_from_ast(const sql::CreateTableStatement &stmt,
                                                   std::string_view original_sql);
        bool drop_from_ast(const sql::DropTableStatement &stmt);
        std::string create_index_from_ast(const sql::CreateIndexStatement &stmt,
                                          std::string_view original_sql,
                                          bool is_primary = false);
        bool drop_index_from_ast(const sql::DropIndexStatement &stmt);

        static ColumnConstraint map_constraint(const sql::ColumnConstraintAST &constraint);
        static ColumnDef map_column(std::size_t index, const sql::ColumnDefAST &column_ast);
    };
}

