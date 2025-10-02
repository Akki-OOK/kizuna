#include "engine/expression_evaluator.h"

#include <charconv>
#include <limits>

#include "common/exception.h"

namespace kizuna::engine
{
    namespace
    {
        TriBool value_to_tristate(const Value &value)
        {
            if (value.is_null())
                return TriBool::Unknown;
            switch (value.type())
            {
            case DataType::BOOLEAN:
                return value.as_bool() ? TriBool::True : TriBool::False;
            case DataType::INTEGER:
                return value.as_int32() == 0 ? TriBool::False : TriBool::True;
            case DataType::BIGINT:
            case DataType::DATE:
            case DataType::TIMESTAMP:
                return value.as_int64() == 0 ? TriBool::False : TriBool::True;
            case DataType::FLOAT:
            case DataType::DOUBLE:
                return value.as_double() == 0.0 ? TriBool::False : TriBool::True;
            default:
                throw QueryException::type_error("predicate", "BOOLEAN", data_type_to_string(value.type()));
            }
        }
    } // namespace

    ExpressionEvaluator::ExpressionEvaluator(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                             std::string table_name)
        : table_name_(std::move(table_name))
    {
        column_map_.reserve(columns.size() * 2);
        for (const auto &entry : columns)
        {
            ColumnBinding binding{entry.ordinal_position, entry.column.type};
            column_map_.emplace(entry.column.name, binding);
            if (!table_name_.empty())
            {
                column_map_.emplace(table_name_ + "." + entry.column.name, binding);
            }
        }
    }

    const ExpressionEvaluator::ColumnBinding *ExpressionEvaluator::lookup_column(const sql::ColumnRef &ref) const
    {
        if (!ref.table.empty())
        {
            auto it = column_map_.find(ref.table + "." + ref.column);
            if (it != column_map_.end())
                return &it->second;
        }
        auto it = column_map_.find(ref.column);
        if (it != column_map_.end())
            return &it->second;
        return nullptr;
    }

    Value ExpressionEvaluator::literal_to_value(const sql::LiteralValue &literal,
                                                std::optional<DataType> target_type) const
    {
        switch (literal.kind)
        {
        case sql::LiteralKind::NULL_LITERAL:
            return Value::null(target_type.value_or(DataType::NULL_TYPE));
        case sql::LiteralKind::BOOLEAN:
            return Value::boolean(literal.bool_value);
        case sql::LiteralKind::INTEGER:
        {
            long long parsed = 0;
            try
            {
                parsed = std::stoll(literal.text);
            }
            catch (const std::exception &)
            {
                throw QueryException::type_error("literal", "INTEGER", literal.text);
            }
            if (target_type)
            {
                switch (*target_type)
                {
                case DataType::BOOLEAN:
                    return Value::boolean(parsed != 0);
                case DataType::INTEGER:
                    if (parsed < std::numeric_limits<int32_t>::min() || parsed > std::numeric_limits<int32_t>::max())
                        throw QueryException::type_error("literal", "INTEGER", literal.text);
                    return Value::int32(static_cast<int32_t>(parsed));
                case DataType::BIGINT:
                case DataType::DATE:
                case DataType::TIMESTAMP:
                    return Value::int64(static_cast<int64_t>(parsed));
                default:
                    break;
                }
            }
            if (parsed >= std::numeric_limits<int32_t>::min() && parsed <= std::numeric_limits<int32_t>::max())
                return Value::int32(static_cast<int32_t>(parsed));
            return Value::int64(static_cast<int64_t>(parsed));
        }
        case sql::LiteralKind::DOUBLE:
        {
            double parsed = 0.0;
            try
            {
                parsed = std::stod(literal.text);
            }
            catch (const std::exception &)
            {
                throw QueryException::type_error("literal", "DOUBLE", literal.text);
            }
            return Value::floating(parsed);
        }
        case sql::LiteralKind::STRING:
        {
            if (target_type)
            {
                if (*target_type == DataType::DATE)
                {
                    auto parsed = parse_date(literal.text);
                    if (!parsed)
                        throw QueryException::type_error("literal", "DATE", literal.text);
                    return Value::date(*parsed);
                }
                if (*target_type == DataType::BOOLEAN)
                {
                    if (literal.text == "TRUE" || literal.text == "true")
                        return Value::boolean(true);
                    if (literal.text == "FALSE" || literal.text == "false")
                        return Value::boolean(false);
                    throw QueryException::type_error("literal", "BOOLEAN", literal.text);
                }
            }
            return Value::string(literal.text);
        }
        }
        throw QueryException::type_error("literal", "supported", literal.text);
    }

    Value ExpressionEvaluator::evaluate_value(const sql::Expression &expression,
                                              const std::vector<Value> &row_values,
                                              std::optional<DataType> target_hint) const
    {
        switch (expression.kind)
        {
        case sql::ExpressionKind::LITERAL:
            return literal_to_value(expression.literal, target_hint);
        case sql::ExpressionKind::COLUMN_REF:
        {
            const auto *binding = lookup_column(expression.column);
            if (!binding)
                throw QueryException::column_not_found(expression.column.column, expression.column.table);
            if (binding->index >= row_values.size())
                throw DBException(StatusCode::SCHEMA_MISMATCH, "Row does not contain column", expression.column.column);
            return row_values[binding->index];
        }
        case sql::ExpressionKind::UNARY:
        case sql::ExpressionKind::BINARY:
        case sql::ExpressionKind::NULL_TEST:
            throw QueryException::type_error("expression", "scalar", "predicate");
        }
        throw QueryException::type_error("expression", "scalar", "unknown");
    }

    Value ExpressionEvaluator::coerce_to_type(const Value &value, DataType target) const
    {
        if (value.is_null())
            return Value::null(target);
        if (value.type() == target)
            return value;
        switch (target)
        {
        case DataType::BOOLEAN:
            if (value.type() == DataType::INTEGER)
                return Value::boolean(value.as_int32() != 0);
            if (value.type() == DataType::BIGINT)
                return Value::boolean(value.as_int64() != 0);
            return value;
        case DataType::INTEGER:
            if (value.type() == DataType::BIGINT)
            {
                auto v = value.as_int64();
                if (v < std::numeric_limits<int32_t>::min() || v > std::numeric_limits<int32_t>::max())
                    throw QueryException::type_error("coercion", "INTEGER", std::to_string(v));
                return Value::int32(static_cast<int32_t>(v));
            }
            return value;
        case DataType::BIGINT:
            if (value.type() == DataType::INTEGER)
                return Value::int64(static_cast<int64_t>(value.as_int32()));
            return value;
        case DataType::DOUBLE:
        {
            double numeric = 0.0;
            if (value.type() == DataType::DOUBLE || value.type() == DataType::FLOAT)
                numeric = value.as_double();
            else if (value.type() == DataType::INTEGER)
                numeric = static_cast<double>(value.as_int32());
            else if (value.type() == DataType::BIGINT || value.type() == DataType::DATE || value.type() == DataType::TIMESTAMP)
                numeric = static_cast<double>(value.as_int64());
            else
                return value;
            return Value::floating(numeric);
        }
        default:
            return value;
        }
    }

    Value ExpressionEvaluator::evaluate_scalar(const sql::Expression &expression,
                                               const std::vector<Value> &row_values) const
    {
        return evaluate_value(expression, row_values, std::nullopt);
    }

    TriBool ExpressionEvaluator::evaluate_predicate_internal(const sql::Expression &expression,
                                                             const std::vector<Value> &row_values) const
    {
        switch (expression.kind)
        {
        case sql::ExpressionKind::LITERAL:
            return value_to_tristate(literal_to_value(expression.literal, std::nullopt));
        case sql::ExpressionKind::COLUMN_REF:
        {
            const auto *binding = lookup_column(expression.column);
            if (!binding)
                throw QueryException::column_not_found(expression.column.column, expression.column.table);
            if (binding->index >= row_values.size())
                throw DBException(StatusCode::SCHEMA_MISMATCH, "Row does not contain column", expression.column.column);
            return value_to_tristate(row_values[binding->index]);
        }
        case sql::ExpressionKind::UNARY:
        {
            auto operand = evaluate_predicate_internal(*expression.left, row_values);
            return logical_not(operand);
        }
        case sql::ExpressionKind::BINARY:
        {
            if (expression.binary_op == sql::BinaryOperator::AND)
            {
                auto lhs = evaluate_predicate_internal(*expression.left, row_values);
                auto rhs = evaluate_predicate_internal(*expression.right, row_values);
                return logical_and(lhs, rhs);
            }
            if (expression.binary_op == sql::BinaryOperator::OR)
            {
                auto lhs = evaluate_predicate_internal(*expression.left, row_values);
                auto rhs = evaluate_predicate_internal(*expression.right, row_values);
                return logical_or(lhs, rhs);
            }

            const auto *left_binding = expression.left && expression.left->kind == sql::ExpressionKind::COLUMN_REF
                                           ? lookup_column(expression.left->column)
                                           : nullptr;
            const auto *right_binding = expression.right && expression.right->kind == sql::ExpressionKind::COLUMN_REF
                                            ? lookup_column(expression.right->column)
                                            : nullptr;

            std::optional<DataType> left_hint;
            std::optional<DataType> right_hint;
            if (expression.left && expression.left->kind == sql::ExpressionKind::LITERAL && right_binding)
                left_hint = right_binding->type;
            if (expression.right && expression.right->kind == sql::ExpressionKind::LITERAL && left_binding)
                right_hint = left_binding->type;

            auto left_value = evaluate_value(*expression.left, row_values, left_hint);
            auto right_value = evaluate_value(*expression.right, row_values, right_hint);

            if (left_binding)
                left_value = coerce_to_type(left_value, left_binding->type);
            if (right_binding)
                right_value = coerce_to_type(right_value, right_binding->type);

            auto cmp = compare(left_value, right_value);
            if (cmp == CompareResult::Unknown)
                return TriBool::Unknown;

            bool result = false;
            switch (expression.binary_op)
            {
            case sql::BinaryOperator::EQUAL:
                result = (cmp == CompareResult::Equal);
                break;
            case sql::BinaryOperator::NOT_EQUAL:
                result = (cmp != CompareResult::Equal);
                break;
            case sql::BinaryOperator::LESS:
                result = (cmp == CompareResult::Less);
                break;
            case sql::BinaryOperator::LESS_EQUAL:
                result = (cmp == CompareResult::Less || cmp == CompareResult::Equal);
                break;
            case sql::BinaryOperator::GREATER:
                result = (cmp == CompareResult::Greater);
                break;
            case sql::BinaryOperator::GREATER_EQUAL:
                result = (cmp == CompareResult::Greater || cmp == CompareResult::Equal);
                break;
            case sql::BinaryOperator::AND:
            case sql::BinaryOperator::OR:
                break;
            }
            return result ? TriBool::True : TriBool::False;
        }
        case sql::ExpressionKind::NULL_TEST:
        {
            auto value = evaluate_value(*expression.left, row_values, std::nullopt);
            bool is_null = value.is_null();
            bool result = expression.is_not_null ? !is_null : is_null;
            return result ? TriBool::True : TriBool::False;
        }
        }
        throw QueryException::type_error("expression", "predicate", "unknown");
    }

    TriBool ExpressionEvaluator::evaluate_predicate(const sql::Expression &expression,
                                                   const std::vector<Value> &row_values) const
    {
        return evaluate_predicate_internal(expression, row_values);
    }
}
