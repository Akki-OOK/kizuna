#include "engine/dml_executor.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <optional>
#include <sstream>
#include <string_view>
#include <unordered_map>

#include "common/exception.h"
#include "common/logger.h"
#include "engine/expression_evaluator.h"
#include "storage/record.h"

namespace kizuna::engine
{

namespace
{
    std::string join_strings(const std::vector<std::string> &items, std::string_view delimiter)
    {
        std::ostringstream oss;
        for (std::size_t i = 0; i < items.size(); ++i)
        {
            if (i != 0)
                oss << delimiter;
            oss << items[i];
        }
        return oss.str();
    }

    std::string column_ref_to_string(const sql::ColumnRef &ref)
    {
        if (!ref.table.empty())
            return ref.table + "." + ref.column;
        return ref.column;
    }

    std::string literal_to_string(const sql::LiteralValue &literal)
    {
        switch (literal.kind)
        {
        case sql::LiteralKind::NULL_LITERAL:
            return "NULL";
        case sql::LiteralKind::BOOLEAN:
            return literal.bool_value ? "TRUE" : "FALSE";
        case sql::LiteralKind::STRING:
        case sql::LiteralKind::INTEGER:
        case sql::LiteralKind::DOUBLE:
            return literal.text;
        default:
            return "<literal>";
        }
    }

    std::string binary_operator_to_string(sql::BinaryOperator op)
    {
        switch (op)
        {
        case sql::BinaryOperator::EQUAL: return "=";
        case sql::BinaryOperator::NOT_EQUAL: return "!=";
        case sql::BinaryOperator::LESS: return "<";
        case sql::BinaryOperator::LESS_EQUAL: return "<=";
        case sql::BinaryOperator::GREATER: return ">";
        case sql::BinaryOperator::GREATER_EQUAL: return ">=";
        case sql::BinaryOperator::AND: return "AND";
        case sql::BinaryOperator::OR: return "OR";
        }
        return "?";
    }

    std::string describe_expression(const sql::Expression *expr)
    {
        if (expr == nullptr)
            return "<null>";

        switch (expr->kind)
        {
        case sql::ExpressionKind::LITERAL:
            return literal_to_string(expr->literal);
        case sql::ExpressionKind::COLUMN_REF:
            return column_ref_to_string(expr->column);
        case sql::ExpressionKind::UNARY:
            return "NOT (" + describe_expression(expr->left.get()) + ")";
        case sql::ExpressionKind::BINARY:
            return "(" + describe_expression(expr->left.get()) + " " +
                   binary_operator_to_string(expr->binary_op) + " " +
                   describe_expression(expr->right.get()) + ")";
        case sql::ExpressionKind::NULL_TEST:
            return describe_expression(expr->left.get()) +
                   (expr->is_not_null ? " IS NOT NULL" : " IS NULL");
        }
        return "<expr>";
    }

    std::string describe_assignments(const std::vector<sql::UpdateAssignment> &assignments)
    {
        std::vector<std::string> parts;
        parts.reserve(assignments.size());
        for (const auto &assign : assignments)
        {
            parts.push_back(assign.column_name + "=" + describe_expression(assign.value.get()));
        }
        return join_strings(parts, ", ");
    }

    bool is_true(TriBool value) noexcept
    {
        return value == TriBool::True;
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
            auto result = select(parsed.select);
            return "Rows returned: " + std::to_string(result.rows.size());
        }
        case sql::DMLStatementKind::DELETE:
        {
            auto result = delete_all(parsed.del);
            return "Rows deleted: " + std::to_string(result.rows_deleted);
        }
        case sql::DMLStatementKind::UPDATE:
        {
            auto result = update_all(parsed.update);
            return "Rows updated: " + std::to_string(result.rows_updated);
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

    SelectResult DMLExecutor::select(const sql::SelectStatement &stmt)
    {
        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
            throw QueryException::table_not_found(stmt.table_name);
        const auto table_entry = *table_opt;
        auto columns = catalog_.get_columns(table_entry.table_id);
        if (columns.empty())
            return SelectResult{};

        SelectResult result;
        result.column_names.clear();
        auto projection = build_projection(stmt, columns, table_entry.name, result.column_names);
        if (projection.empty())
        {
            projection.reserve(columns.size());
            for (std::size_t i = 0; i < columns.size(); ++i)
            {
                projection.push_back(i);
                result.column_names.push_back(columns[i].column.name);
            }
        }

        ExpressionEvaluator evaluator(columns, table_entry.name);
        const auto *predicate = stmt.where ? stmt.where.get() : nullptr;
        const std::size_t limit = stmt.limit.has_value() ? static_cast<std::size_t>(*stmt.limit) : std::numeric_limits<std::size_t>::max();
        std::string projection_desc = join_strings(result.column_names, ", ");
        if (projection_desc.empty()) projection_desc = "<none>";
        const std::string predicate_desc = predicate ? describe_expression(predicate) : "<none>";
        const std::string limit_desc = stmt.limit.has_value() ? std::to_string(static_cast<std::size_t>(*stmt.limit)) : "ALL";
        Logger::instance().debug("[SELECT] table=", table_entry.name, " projection=[", projection_desc, "] predicate=", predicate_desc, " limit=", limit_desc);
        if (limit == 0)
        {
            return result;
        }

        TableHeap heap(pm_, table_entry.root_page_id);
        std::size_t produced = 0;
        heap.scan([&](const TableHeap::RowLocation &, const std::vector<uint8_t> &payload) {
            if (produced >= limit)
                return;

            auto values = decode_row_values(columns, payload);
            if (predicate && !is_true(evaluator.evaluate_predicate(*predicate, values)))
                return;

            std::vector<std::string> row;
            row.reserve(projection.size());
            for (std::size_t idx : projection)
            {
                row.push_back(values[idx].to_string());
            }
            result.rows.push_back(std::move(row));
            ++produced;
        });

        return result;
    }

    DeleteResult DMLExecutor::delete_all(const sql::DeleteStatement &stmt)
    {
        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
            throw QueryException::table_not_found(stmt.table_name);
        const auto table_entry = *table_opt;
        auto columns = catalog_.get_columns(table_entry.table_id);

        TableHeap heap(pm_, table_entry.root_page_id);
        ExpressionEvaluator evaluator(columns, table_entry.name);
        const auto *predicate = stmt.where ? stmt.where.get() : nullptr;

        const std::string predicate_desc = predicate ? describe_expression(predicate) : "<none>";
        Logger::instance().debug("[DELETE] table=", table_entry.name, " predicate=", predicate_desc);

        std::size_t deleted = 0;
        heap.scan([&](const TableHeap::RowLocation &loc, const std::vector<uint8_t> &payload) {
            if (!predicate)
            {
                if (heap.erase(loc))
                    ++deleted;
                return;
            }

            auto values = decode_row_values(columns, payload);
            if (is_true(evaluator.evaluate_predicate(*predicate, values)))
            {
                if (heap.erase(loc))
                    ++deleted;
            }
        });

        return DeleteResult{deleted};
    }

    UpdateResult DMLExecutor::update_all(const sql::UpdateStatement &stmt)
    {
        if (stmt.assignments.empty())
            throw QueryException::invalid_constraint("UPDATE requires at least one assignment");

        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
            throw QueryException::table_not_found(stmt.table_name);
        const auto table_entry = *table_opt;
        auto columns = catalog_.get_columns(table_entry.table_id);

        std::unordered_map<std::string, std::size_t> column_index;
        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            column_index.emplace(columns[i].column.name, i);
        }

        TableHeap heap(pm_, table_entry.root_page_id);
        ExpressionEvaluator evaluator(columns, table_entry.name);
        const auto *predicate = stmt.where ? stmt.where.get() : nullptr;

        const std::string assignments_desc = describe_assignments(stmt.assignments);
        const std::string predicate_desc = predicate ? describe_expression(predicate) : "<none>";
        Logger::instance().debug("[UPDATE] table=", table_entry.name, " assignments=", (assignments_desc.empty() ? std::string("<none>") : assignments_desc), " predicate=", predicate_desc);

        struct UpdateTarget
        {
            TableHeap::RowLocation location;
            std::vector<Value> current_values;
        };

        std::vector<UpdateTarget> targets;

        heap.scan([&](const TableHeap::RowLocation &loc, const std::vector<uint8_t> &payload) {
            auto current_values = decode_row_values(columns, payload);
            if (predicate && !is_true(evaluator.evaluate_predicate(*predicate, current_values)))
                return;
            targets.push_back(UpdateTarget{loc, std::move(current_values)});
        });

        std::size_t updated = 0;
        for (auto &target : targets)
        {
            auto &current_values = target.current_values;
            std::vector<Value> new_values = current_values;
            for (const auto &assignment : stmt.assignments)
            {
                auto it = column_index.find(assignment.column_name);
                if (it == column_index.end())
                    throw QueryException::column_not_found(assignment.column_name, stmt.table_name);

                std::size_t idx = it->second;
                Value evaluated = evaluator.evaluate_scalar(*assignment.value, current_values);
                Value coerced = coerce_value_for_column(columns[idx], evaluated);
                new_values[idx] = coerced;
            }

            auto new_payload = encode_values(columns, new_values);
            heap.update(target.location, new_payload);
            ++updated;
        }

        return UpdateResult{updated};
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

    std::vector<Value> DMLExecutor::decode_row_values(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                                      const std::vector<uint8_t> &payload) const
    {
        std::vector<record::Field> fields;
        if (!record::decode(payload.data(), payload.size(), fields))
        {
            throw DBException(StatusCode::INVALID_RECORD_FORMAT, "Failed to decode row", "table row");
        }
        if (fields.size() != columns.size())
        {
            throw DBException(StatusCode::INVALID_ARGUMENT, "Decoded field count mismatch", "table row");
        }

        std::vector<Value> values;
        values.reserve(columns.size());
        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            const auto &col = columns[i].column;
            const auto &field = fields[i];
            if (field.is_null)
            {
                values.push_back(Value::null(col.type));
                continue;
            }

            switch (col.type)
            {
            case DataType::BOOLEAN:
            {
                bool v = !field.payload.empty() && field.payload[0] != 0;
                values.push_back(Value::boolean(v));
                break;
            }
            case DataType::INTEGER:
            {
                int32_t v = 0;
                std::memcpy(&v, field.payload.data(), sizeof(int32_t));
                values.push_back(Value::int32(v));
                break;
            }
            case DataType::BIGINT:
            {
                int64_t v = 0;
                std::memcpy(&v, field.payload.data(), sizeof(int64_t));
                values.push_back(Value::int64(v));
                break;
            }
            case DataType::DATE:
            {
                int64_t v = 0;
                std::memcpy(&v, field.payload.data(), sizeof(int64_t));
                values.push_back(Value::date(v));
                break;
            }
            case DataType::TIMESTAMP:
            {
                int64_t v = 0;
                std::memcpy(&v, field.payload.data(), sizeof(int64_t));
                values.push_back(Value::int64(v));
                break;
            }
            case DataType::FLOAT:
            {
                float v = 0.0f;
                std::memcpy(&v, field.payload.data(), sizeof(float));
                values.push_back(Value::floating(static_cast<double>(v)));
                break;
            }
            case DataType::DOUBLE:
            {
                double v = 0.0;
                std::memcpy(&v, field.payload.data(), sizeof(double));
                values.push_back(Value::floating(v));
                break;
            }
            case DataType::VARCHAR:
            case DataType::TEXT:
            {
                std::string text(reinterpret_cast<const char *>(field.payload.data()), field.payload.size());
                values.push_back(Value::string(std::move(text), col.type));
                break;
            }
            default:
                values.push_back(Value::string("<unsupported>"));
                break;
            }
        }
        return values;
    }

    std::vector<uint8_t> DMLExecutor::encode_values(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                                    const std::vector<Value> &values) const
    {
        std::vector<record::Field> fields;
        fields.reserve(columns.size());
        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            const auto &column = columns[i].column;
            const Value &value = values[i];
            if (value.is_null())
            {
                if (column.constraint.not_null)
                    throw QueryException::invalid_constraint("column '" + column.name + "' is NOT NULL");
                fields.push_back(record::from_null(column.type));
                continue;
            }

            switch (column.type)
            {
            case DataType::BOOLEAN:
                fields.push_back(record::from_bool(value.as_bool()));
                break;
            case DataType::INTEGER:
                fields.push_back(record::from_int32(value.as_int32()));
                break;
            case DataType::BIGINT:
                fields.push_back(record::from_int64(value.as_int64()));
                break;
            case DataType::FLOAT:
            case DataType::DOUBLE:
                fields.push_back(record::from_double(value.as_double()));
                break;
            case DataType::DATE:
                fields.push_back(record::from_date(value.as_int64()));
                break;
            case DataType::VARCHAR:
            case DataType::TEXT:
            {
                const std::string &text = value.as_string();
                if (column.type == DataType::VARCHAR && column.length > 0 && text.size() > column.length)
                    throw QueryException::invalid_constraint("value too long for column '" + column.name + "'");
                fields.push_back(record::from_string(text));
                break;
            }
            default:
                throw QueryException::unsupported_type("unsupported column type");
            }
        }
        return record::encode(fields);
    }

    Value DMLExecutor::coerce_value_for_column(const catalog::ColumnCatalogEntry &column,
                                               const Value &value) const
    {
        if (value.is_null())
        {
            if (column.column.constraint.not_null)
                throw QueryException::invalid_constraint("column '" + column.column.name + "' is NOT NULL");
            return Value::null(column.column.type);
        }

        switch (column.column.type)
        {
        case DataType::BOOLEAN:
            if (value.type() == DataType::BOOLEAN)
                return value;
            if (value.type() == DataType::INTEGER)
                return Value::boolean(value.as_int32() != 0);
            if (value.type() == DataType::BIGINT)
                return Value::boolean(value.as_int64() != 0);
            throw QueryException::type_error("UPDATE", "BOOLEAN", value.to_string());
        case DataType::INTEGER:
            if (value.type() == DataType::INTEGER)
                return value;
            if (value.type() == DataType::BIGINT)
            {
                auto v = value.as_int64();
                if (v < std::numeric_limits<int32_t>::min() || v > std::numeric_limits<int32_t>::max())
                    throw QueryException::type_error("UPDATE", "INTEGER", std::to_string(v));
                return Value::int32(static_cast<int32_t>(v));
            }
            throw QueryException::type_error("UPDATE", "INTEGER", value.to_string());
        case DataType::BIGINT:
            if (value.type() == DataType::BIGINT)
                return value;
            if (value.type() == DataType::INTEGER)
                return Value::int64(static_cast<int64_t>(value.as_int32()));
            throw QueryException::type_error("UPDATE", "BIGINT", value.to_string());
        case DataType::FLOAT:
        case DataType::DOUBLE:
            if (value.type() == DataType::DOUBLE || value.type() == DataType::FLOAT)
                return Value::floating(value.as_double());
            if (value.type() == DataType::INTEGER)
                return Value::floating(static_cast<double>(value.as_int32()));
            if (value.type() == DataType::BIGINT)
                return Value::floating(static_cast<double>(value.as_int64()));
            throw QueryException::type_error("UPDATE", "DOUBLE", value.to_string());
        case DataType::DATE:
            if (value.type() == DataType::DATE)
                return value;
            if (value.type() == DataType::VARCHAR || value.type() == DataType::TEXT)
            {
                auto parsed = parse_date(value.as_string());
                if (!parsed)
                    throw QueryException::type_error("UPDATE", "DATE", value.as_string());
                return Value::date(*parsed);
            }
            throw QueryException::type_error("UPDATE", "DATE", value.to_string());
        case DataType::VARCHAR:
        case DataType::TEXT:
            if (value.type() == DataType::VARCHAR || value.type() == DataType::TEXT)
                return Value::string(value.as_string(), column.column.type);
            throw QueryException::type_error("UPDATE", "STRING", value.to_string());
        default:
            throw QueryException::unsupported_type("unsupported column type");
        }
    }

    std::vector<size_t> DMLExecutor::build_projection(const sql::SelectStatement &stmt,
                                                      const std::vector<catalog::ColumnCatalogEntry> &columns,
                                                      const std::string &table_name,
                                                      std::vector<std::string> &out_names) const
    {
        std::vector<size_t> projection;
        out_names.clear();

        bool expanded_star = false;
        for (const auto &item : stmt.columns)
        {
            if (item.is_star)
            {
                if (!expanded_star)
                {
                    for (std::size_t i = 0; i < columns.size(); ++i)
                    {
                        projection.push_back(i);
                        out_names.push_back(columns[i].column.name);
                    }
                    expanded_star = true;
                }
                continue;
            }

            std::size_t idx = find_column_index(columns, table_name, item.column);
            projection.push_back(idx);
            out_names.push_back(item.column.column);
        }

        return projection;
    }

    std::size_t DMLExecutor::find_column_index(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                               const std::string &table_name,
                                               const sql::ColumnRef &ref) const
    {
        if (!ref.table.empty() && ref.table != table_name)
            throw QueryException::column_not_found(ref.column, ref.table);

        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            if (columns[i].column.name == ref.column)
                return i;
        }
        throw QueryException::column_not_found(ref.column, table_name);
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
                case DataType::DATE:
                {
                    if (literal.kind != sql::LiteralKind::STRING)
                        throw QueryException::type_error("INSERT", "DATE", literal.text);
                    auto parsed = parse_date(literal.text);
                    if (!parsed)
                        throw QueryException::type_error("INSERT", "DATE", literal.text);
                    field = record::from_date(*parsed);
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

} // namespace kizuna::engine

