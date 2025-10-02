﻿#include <cassert>
#include <vector>

#include "common/exception.h"
#include "engine/expression_evaluator.h"
#include "sql/ast.h"

using namespace kizuna;

namespace
{
    sql::LiteralValue int_literal(int value)
    {
        return sql::LiteralValue::integer(std::to_string(value));
    }

    std::unique_ptr<sql::Expression> column_expr(std::string column, std::string table = {})
    {
        sql::ColumnRef ref{std::move(table), std::move(column)};
        return sql::Expression::make_column(std::move(ref));
    }

    std::unique_ptr<sql::Expression> literal_expr(sql::LiteralValue literal)
    {
        return sql::Expression::make_literal(std::move(literal));
    }
}

bool expression_evaluator_tests()
{
    std::vector<catalog::ColumnCatalogEntry> columns;
    columns.push_back({0, 0, 0, ColumnDef{0, "id", DataType::INTEGER, 0, {}}});
    columns.push_back({0, 1, 1, ColumnDef{0, "name", DataType::VARCHAR, 0, {}}});
    columns.push_back({0, 2, 2, ColumnDef{0, "active", DataType::BOOLEAN, 0, {}}});
    columns.push_back({0, 3, 3, ColumnDef{0, "age", DataType::INTEGER, 0, {}}});
    columns.push_back({0, 4, 4, ColumnDef{0, "nickname", DataType::VARCHAR, 0, {}}});
    columns.push_back({0, 5, 5, ColumnDef{0, "joined", DataType::DATE, 0, {}}});

    auto joined_days = parse_date("2023-05-01");
    assert(joined_days.has_value());

    std::vector<Value> row = {
        Value::int32(1),
        Value::string("alice"),
        Value::boolean(true),
        Value::int32(20),
        Value::string("ally"),
        Value::date(*joined_days)
    };

    engine::ExpressionEvaluator evaluator(columns, "users");

    auto age_ge_18 = sql::Expression::make_binary(sql::BinaryOperator::GREATER_EQUAL,
                                                  column_expr("age"),
                                                  literal_expr(int_literal(18)));
    auto not_active = sql::Expression::make_unary(sql::UnaryOperator::NOT,
                                                  column_expr("active"));
    auto combined = sql::Expression::make_binary(sql::BinaryOperator::AND,
                                                 std::move(age_ge_18),
                                                 std::move(not_active));

    auto age_check = sql::Expression::make_binary(sql::BinaryOperator::GREATER_EQUAL,
                                                  column_expr("age", "users"),
                                                  literal_expr(int_literal(18)));
    auto name_not_bob = sql::Expression::make_binary(sql::BinaryOperator::NOT_EQUAL,
                                                     column_expr("name"),
                                                     literal_expr(sql::LiteralValue::string("bob")));
    auto name_equals_alice = sql::Expression::make_binary(sql::BinaryOperator::EQUAL,
                                                          column_expr("name"),
                                                          literal_expr(sql::LiteralValue::string("alice")));
    auto nickname_is_null = sql::Expression::make_null_check(column_expr("nickname"), false);
    auto nickname_is_not_null = sql::Expression::make_null_check(column_expr("nickname"), true);
    auto active_or_null = sql::Expression::make_binary(sql::BinaryOperator::OR,
                                                       column_expr("active"),
                                                       sql::Expression::make_null_check(column_expr("nickname"), false));
    auto recent_join = sql::Expression::make_binary(sql::BinaryOperator::GREATER,
                                                    column_expr("joined"),
                                                    literal_expr(sql::LiteralValue::string("2023-01-01")));

    assert(evaluator.evaluate_predicate(*age_check, row) == TriBool::True);
    assert(evaluator.evaluate_predicate(*combined, row) == TriBool::False);
    assert(evaluator.evaluate_predicate(*name_not_bob, row) == TriBool::True);
    assert(evaluator.evaluate_predicate(*nickname_is_null, row) == TriBool::False);
    assert(evaluator.evaluate_predicate(*nickname_is_not_null, row) == TriBool::True);
    assert(evaluator.evaluate_predicate(*active_or_null, row) == TriBool::True);
    assert(evaluator.evaluate_predicate(*recent_join, row) == TriBool::True);
    assert(evaluator.evaluate_predicate(*name_equals_alice, row) == TriBool::True);

    // Scalar evaluation
    auto name_expr = column_expr("name");
    auto lit_expr = literal_expr(sql::LiteralValue::string("bob"));
    auto name_value = evaluator.evaluate_scalar(*name_expr, row);
    auto bob_value = evaluator.evaluate_scalar(*lit_expr, row);
    assert(!name_value.is_null() && name_value.type() == DataType::VARCHAR);
    assert(bob_value.type() == DataType::VARCHAR && bob_value.to_string() == "bob");

    auto joined_expr = column_expr("joined");
    auto joined_value = evaluator.evaluate_scalar(*joined_expr, row);
    assert(joined_value.type() == DataType::DATE && joined_value.to_string() == "2023-05-01");

    // Unknown result when column value is NULL
    std::vector<Value> null_row = row;
    null_row[3] = Value::null(DataType::INTEGER);
    assert(evaluator.evaluate_predicate(*age_check, null_row) == TriBool::Unknown);

    std::vector<Value> null_combo = row;
    null_combo[2] = Value::null(DataType::BOOLEAN);
    null_combo[4] = Value::null(DataType::VARCHAR);
    assert(evaluator.evaluate_predicate(*active_or_null, null_combo) == TriBool::True);
    assert(evaluator.evaluate_predicate(*nickname_is_null, null_combo) == TriBool::True);

    std::vector<Value> null_name = row;
    null_name[1] = Value::null(DataType::VARCHAR);
    assert(evaluator.evaluate_predicate(*name_equals_alice, null_name) == TriBool::Unknown);

    bool exception_caught = false;
    try
    {
        auto bad_column = column_expr("missing");
        evaluator.evaluate_scalar(*bad_column, row);
    }
    catch (const DBException &ex)
    {
        exception_caught = (ex.code() == StatusCode::COLUMN_NOT_FOUND);
    }
    assert(exception_caught);

    return true;
}
