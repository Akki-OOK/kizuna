#include "engine/dml_executor.h"

#include <optional>
#include <unordered_map>
#include <limits>

#include "common/exception.h"
#include "storage/record.h"

namespace kizuna::engine
{
    namespace
    {
        using Row = std::vector<std::string>;

        DataType resolve_column_type(const catalog::ColumnCatalogEntry &entry)
        {
            return entry.column.type;
        }
    }

    DMLExecutor::DMLExecutor(catalog::CatalogManager &catalog,
                             PageManager &pm,
                             FileManager &fm)
        : catalog_(catalog), pm_(pm), fm_(fm)
    {
    }

    std::string DMLExecutor::execute(std::string_view sql)
    {
        auto parsed = sql::parse_dml(sql);
        switch (parsed.kind)
        {
        case sql::DMLStatementKind::INSERT:
        {
            auto result = insert_into(parsed.insert);
            return "Rows inserted: " + std::to_string(result.rows_inserted);
        }
        case sql::DMLStatementKind::SELECT:
        {
            auto rows = select_all(parsed.select);
            return "Rows returned: " + std::to_string(rows.size());
        }
        case sql::DMLStatementKind::DELETE:
        {
            auto result = delete_all(parsed.del);
            return "Rows deleted: " + std::to_string(result.rows_deleted);
        }
        case sql::DMLStatementKind::TRUNCATE:
        {
            truncate(parsed.truncate);
            return "Table truncated";
        }
        }
        throw DBException(StatusCode::NOT_IMPLEMENTED, "Unsupported DML statement", std::string(sql));
    }

    InsertResult DMLExecutor::insert_into(const sql::InsertStatement &stmt)
    {
        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
            throw QueryException::table_not_found(stmt.table_name);
        const auto table_entry = *table_opt;
        auto columns = catalog_.get_columns(table_entry.table_id);
        if (columns.empty())
            throw QueryException::invalid_constraint("table has no columns");

        std::vector<std::string> column_names = stmt.column_names;
        if (column_names.empty())
        {
            column_names.reserve(columns.size());
            for (const auto &c : columns)
                column_names.push_back(c.column.name);
        }
        if (column_names.size() != columns.size())
            throw QueryException::invalid_constraint("column count mismatch");

        TableHeap heap(pm_, table_entry.root_page_id);
        std::size_t inserted = 0;
        for (const auto &row : stmt.rows)
        {
            if (row.values.size() != column_names.size())
                throw QueryException::invalid_constraint("row value count mismatch");
            auto payload = encode_row(columns, row, column_names);
            heap.insert(payload);
            ++inserted;
        }
        return InsertResult{inserted};
    }

    std::vector<std::vector<std::string>> DMLExecutor::select_all(const sql::SelectStatement &stmt)
    {
        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
            throw QueryException::table_not_found(stmt.table_name);
        const auto table_entry = *table_opt;
        auto columns = catalog_.get_columns(table_entry.table_id);
        if (columns.empty())
            return {};
        return materialize_rows(table_entry, columns);
    }

    DeleteResult DMLExecutor::delete_all(const sql::DeleteStatement &stmt)
    {
        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
            throw QueryException::table_not_found(stmt.table_name);
        const auto table_entry = *table_opt;

        TableHeap heap(pm_, table_entry.root_page_id);
        std::size_t deleted = 0;
        for (auto it = heap.begin(); it != heap.end(); ++it)
        {
            auto loc = it.location();
            if (heap.erase(loc))
            {
                ++deleted;
            }
        }
        return DeleteResult{deleted};
    }

    void DMLExecutor::truncate(const sql::TruncateStatement &stmt)
    {
        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
            throw QueryException::table_not_found(stmt.table_name);
        const auto table_entry = *table_opt;

        TableHeap heap(pm_, table_entry.root_page_id);
        heap.truncate();
    }

    std::vector<std::vector<std::string>> DMLExecutor::materialize_rows(const catalog::TableCatalogEntry &table_entry,
                                                   const std::vector<catalog::ColumnCatalogEntry> &columns)
    {
        TableHeap heap(pm_, table_entry.root_page_id);
        std::vector<std::vector<std::string>> rows;
        for (auto it = heap.begin(); it != heap.end(); ++it)
        {
            rows.push_back(materialize_row(columns, *it));
        }
        return rows;
    }

    std::vector<std::string> DMLExecutor::materialize_row(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                                          const std::vector<uint8_t> &payload) const
    {
        std::vector<std::string> out;
        std::vector<record::Field> fields;
        if (!record::decode(payload.data(), payload.size(), fields))
        {
            throw DBException(StatusCode::INVALID_RECORD_FORMAT, "Failed to decode row", "table row");
        }
        if (fields.size() != columns.size())
        {
            throw DBException(StatusCode::INVALID_ARGUMENT, "Decoded field count mismatch", "table row");
        }
        out.reserve(fields.size());
        for (std::size_t i = 0; i < fields.size(); ++i)
        {
            if (fields[i].is_null)
            {
                out.emplace_back("NULL");
                continue;
            }
            const auto type = resolve_column_type(columns[i]);
            switch (type)
            {
            case DataType::BOOLEAN:
                out.emplace_back(fields[i].payload.empty() ? "false" : (fields[i].payload[0] ? "true" : "false"));
                break;
            case DataType::INTEGER:
            {
                if (fields[i].payload.size() != 4)
                    throw DBException(StatusCode::INVALID_ARGUMENT, "Bad INTEGER payload", "table row");
                int32_t value{};
                std::memcpy(&value, fields[i].payload.data(), 4);
                out.emplace_back(std::to_string(value));
                break;
            }
            case DataType::BIGINT:
            {
                if (fields[i].payload.size() != 8)
                    throw DBException(StatusCode::INVALID_ARGUMENT, "Bad BIGINT payload", "table row");
                int64_t value{};
                std::memcpy(&value, fields[i].payload.data(), 8);
                out.emplace_back(std::to_string(value));
                break;
            }
            case DataType::DOUBLE:
            {
                if (fields[i].payload.size() != 8)
                    throw DBException(StatusCode::INVALID_ARGUMENT, "Bad DOUBLE payload", "table row");
                double value{};
                std::memcpy(&value, fields[i].payload.data(), 8);
                out.emplace_back(std::to_string(value));
                break;
            }
            case DataType::VARCHAR:
            case DataType::TEXT:
            {
                out.emplace_back(reinterpret_cast<const char *>(fields[i].payload.data()), fields[i].payload.size());
                break;
            }
            default:
                out.emplace_back("<unsupported>");
                break;
            }
        }
        return out;
    }

    std::vector<uint8_t> DMLExecutor::encode_row(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                                 const sql::InsertRow &row,
                                                 const std::vector<std::string> &column_names)
    {
        std::unordered_map<std::string, const sql::LiteralValue *> value_lookup;
        value_lookup.reserve(column_names.size());
        for (std::size_t i = 0; i < column_names.size(); ++i)
        {
            value_lookup.emplace(column_names[i], &row.values[i]);
        }

        std::vector<record::Field> fields;
        fields.reserve(columns.size());

        for (const auto &entry : columns)
        {
            const auto &col = entry.column;
            auto it = value_lookup.find(col.name);
            if (it == value_lookup.end())
                throw QueryException::column_not_found(col.name, col.name);

            const sql::LiteralValue &literal = *(it->second);
            record::Field field;
            field.type = col.type;

            if (literal.kind == sql::LiteralKind::NULL_LITERAL)
            {
                if (col.constraint.not_null)
                    throw QueryException::invalid_constraint("column '" + col.name + "' is NOT NULL");
                field.is_null = true;
            }
            else
            {
                field.is_null = false;
                switch (col.type)
                {
                case DataType::BOOLEAN:
                    if (literal.kind != sql::LiteralKind::BOOLEAN)
                        throw QueryException::type_error("INSERT", "BOOLEAN", literal.text);
                    field = record::from_bool(literal.bool_value);
                    break;
                case DataType::INTEGER:
                case DataType::BIGINT:
                {
                    if (literal.kind != sql::LiteralKind::INTEGER)
                        throw QueryException::type_error("INSERT", "INTEGER", literal.text);
                    long long value = 0;
                    try
                    {
                        value = std::stoll(literal.text);
                    }
                    catch (const std::exception &)
                    {
                        throw QueryException::type_error("INSERT", "INTEGER", literal.text);
                    }
                    if (col.type == DataType::INTEGER)
                    {
                        if (value < std::numeric_limits<int32_t>::min() || value > std::numeric_limits<int32_t>::max())
                            throw QueryException::type_error("INSERT", "INTEGER", literal.text);
                        field = record::from_int32(static_cast<int32_t>(value));
                    }
                    else
                    {
                        field = record::from_int64(static_cast<int64_t>(value));
                    }
                    break;
                }
                case DataType::DOUBLE:
                {
                    if (literal.kind != sql::LiteralKind::DOUBLE && literal.kind != sql::LiteralKind::INTEGER)
                        throw QueryException::type_error("INSERT", "DOUBLE", literal.text);
                    double value = 0.0;
                    try
                    {
                        value = std::stod(literal.text);
                    }
                    catch (const std::exception &)
                    {
                        throw QueryException::type_error("INSERT", "DOUBLE", literal.text);
                    }
                    field = record::from_double(value);
                    break;
                }
                case DataType::VARCHAR:
                case DataType::TEXT:
                {
                    if (literal.kind != sql::LiteralKind::STRING)
                        throw QueryException::type_error("INSERT", "STRING", literal.text);
                    if (col.type == DataType::VARCHAR && col.length > 0 && literal.text.size() > col.length)
                        throw QueryException::invalid_constraint("value too long for column '" + col.name + "'");
                    field = record::from_string(literal.text);
                    break;
                }
                default:
                    throw QueryException::type_error("INSERT", "supported type", literal.text);
                }
            }

            fields.push_back(std::move(field));
        }

        return record::encode(fields);
    }


}







