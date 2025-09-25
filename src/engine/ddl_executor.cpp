#include "engine/ddl_executor.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <unordered_set>

#include "common/exception.h"
#include "common/config.h"

namespace kizuna::engine
{
    namespace
    {
        std::string normalize_identifier(std::string_view name)
        {
            std::string upper(name);
            std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char c) {
                return static_cast<char>(std::toupper(c));
            });
            return upper;
        }
    }

    DDLExecutor::DDLExecutor(catalog::CatalogManager &catalog,
                             PageManager &pm,
                             FileManager &fm)
        : catalog_(catalog), pm_(pm), fm_(fm)
    {
    }

    catalog::TableCatalogEntry DDLExecutor::create_table(std::string_view sql)
    {
        auto stmt = sql::parse_create_table(sql);
        return create_from_ast(stmt, sql);
    }

    void DDLExecutor::drop_table(std::string_view sql)
    {
        auto stmt = sql::parse_drop_table(sql);
        drop_from_ast(stmt);
    }

    std::string DDLExecutor::execute(std::string_view sql)
    {
        auto ddl = sql::parse_ddl(sql);
        if (ddl.kind == sql::StatementKind::CREATE_TABLE)
        {
            auto entry = create_from_ast(ddl.create, sql);
            return "Table created: " + entry.name;
        }
        if (ddl.kind == sql::StatementKind::DROP_TABLE)
        {
            bool dropped = drop_from_ast(ddl.drop);
            if (dropped)
            {
                return "Table dropped: " + ddl.drop.table_name;
            }
            return "Table not found (no-op): " + ddl.drop.table_name;
        }
        throw DBException(StatusCode::NOT_IMPLEMENTED, "DDL not supported yet", std::string(sql));
    }

    catalog::TableCatalogEntry DDLExecutor::create_from_ast(const sql::CreateTableStatement &stmt,
                                                           std::string_view original_sql)
    {
        TableDef def;
        def.name = stmt.table_name;
        if (def.name.empty())
            throw QueryException::syntax_error(std::string(original_sql), 0, "table name");
        if (stmt.columns.empty())
            throw QueryException::syntax_error(std::string(original_sql), 0, "column list");
        if (stmt.columns.size() > config::MAX_COLUMNS_PER_TABLE)
            throw QueryException::invalid_constraint("too many columns");

        std::unordered_set<std::string> seen_names;
        bool primary_key_seen = false;
        def.columns.reserve(stmt.columns.size());
        for (std::size_t i = 0; i < stmt.columns.size(); ++i)
        {
            const auto &col_ast = stmt.columns[i];
            if (col_ast.name.empty())
                throw QueryException::syntax_error(std::string(original_sql), 0, "column name");
            std::string normalized = normalize_identifier(col_ast.name);
            if (!seen_names.insert(normalized).second)
                throw QueryException::duplicate_column(col_ast.name);

            ColumnDef column = map_column(i, col_ast);
            if (column.constraint.primary_key)
            {
                if (primary_key_seen)
                    throw QueryException::invalid_constraint("multiple PRIMARY KEY columns");
                primary_key_seen = true;
            }
            def.columns.push_back(std::move(column));
        }

        page_id_t root_page_id = pm_.new_page(PageType::DATA);
        pm_.unpin(root_page_id, false);

        catalog::TableCatalogEntry entry = catalog_.create_table(def, root_page_id, std::string(original_sql));

        auto table_file = FileManager::table_path(entry.table_id);
        std::error_code ec;
        auto parent = table_file.parent_path();
        if (!parent.empty()) std::filesystem::create_directories(parent, ec);
        std::ofstream ofs(table_file, std::ios::binary | std::ios::trunc);
        if (!ofs)
        {
            catalog_.drop_table(entry.name, true);
            pm_.free_page(entry.root_page_id);
            throw IOException::write_error(table_file.string(), 0);
        }
        ofs.close();
        return entry;
    }

    bool DDLExecutor::drop_from_ast(const sql::DropTableStatement &stmt)
    {
        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
        {
            if (stmt.if_exists)
            {
                return false;
            }
            throw QueryException::table_not_found(stmt.table_name);
        }
        const auto table_entry = table_opt.value();
        bool removed = catalog_.drop_table(stmt.table_name, stmt.cascade);
        if (!removed)
        {
            if (stmt.if_exists)
            {
                return false;
            }
            throw QueryException::table_not_found(stmt.table_name);
        }
        pm_.free_page(table_entry.root_page_id);
        auto table_file = FileManager::table_path(table_entry.table_id);
        if (FileManager::exists(table_file))
        {
            FileManager::remove_file(table_file);
        }
        return true;
    }

    ColumnConstraint DDLExecutor::map_constraint(const sql::ColumnConstraintAST &constraint)
    {
        ColumnConstraint result;
        result.not_null = constraint.not_null || constraint.primary_key;
        result.primary_key = constraint.primary_key;
        result.unique = constraint.unique || constraint.primary_key;
        if (constraint.default_literal.has_value())
        {
            result.has_default = true;
            result.default_value = constraint.default_literal.value();
        }
        return result;
    }

    ColumnDef DDLExecutor::map_column(std::size_t index, const sql::ColumnDefAST &column_ast)
    {
        ColumnDef column;
        column.id = static_cast<column_id_t>(index + 1);
        column.name = column_ast.name;
        column.type = column_ast.type;
        column.length = column_ast.length;
        column.constraint = map_constraint(column_ast.constraint);
        return column;
    }
}





