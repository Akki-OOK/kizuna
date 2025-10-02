#include <cassert>
#include <string>

#include "common/exception.h"
#include "sql/dml_parser.h"

using namespace kizuna;

static void check_select_with_where_limit()
{
    auto select = sql::parse_select("SELECT id, name FROM users WHERE age >= 18 AND NOT active LIMIT 5;");
    assert(select.table_name == "users");
    assert(select.columns.size() == 2);
    assert(!select.columns[0].is_star);
    assert(select.columns[0].column.column == "id");
    assert(select.columns[1].column.column == "name");
    assert(select.where != nullptr);
    assert(select.limit.has_value());
    assert(*select.limit == 5);
    assert(select.where->kind == sql::ExpressionKind::BINARY);
    assert(select.where->binary_op == sql::BinaryOperator::AND);
    assert(select.where->left != nullptr && select.where->right != nullptr);
}

static void check_select_star()
{
    auto select = sql::parse_select("SELECT * FROM logs;");
    assert(select.columns.size() == 1);
    assert(select.columns[0].is_star);
    assert(!select.where);
    assert(!select.limit.has_value());
}

static void check_select_star_mixed()
{
    auto select = sql::parse_select("SELECT id FROM employees WHERE nickname IS NULL OR NOT active;");
    assert(select.columns.size() == 1);
    assert(!select.columns[0].is_star);
    assert(select.columns[0].column.column == "id");
    assert(select.where != nullptr);
    assert(select.where->kind == sql::ExpressionKind::BINARY);
    assert(select.where->binary_op == sql::BinaryOperator::OR);
}

static void check_null_tests()
{
    auto select = sql::parse_select("SELECT id FROM employees WHERE nickname IS NOT NULL;");
    assert(select.where != nullptr);
    assert(select.where->kind == sql::ExpressionKind::NULL_TEST);
    assert(select.where->is_not_null);

    auto update = sql::parse_update("UPDATE employees SET nickname = NULL WHERE nickname IS NULL;");
    assert(update.where != nullptr);
    assert(update.where->kind == sql::ExpressionKind::NULL_TEST);
    assert(!update.where->is_not_null);
}

static void check_delete_where()
{
    auto del = sql::parse_delete("DELETE FROM users WHERE id = 10;");
    assert(del.table_name == "users");
    assert(del.where != nullptr);
    assert(del.where->kind == sql::ExpressionKind::BINARY);
    assert(del.where->binary_op == sql::BinaryOperator::EQUAL);
}

static void check_update_parse()
{
    auto update = sql::parse_update("UPDATE users SET name = 'bob', age = 30 WHERE id = 1;");
    assert(update.table_name == "users");
    assert(update.assignments.size() == 2);
    assert(update.assignments[0].column_name == "name");
    assert(update.assignments[0].value->kind == sql::ExpressionKind::LITERAL);
    assert(update.assignments[1].column_name == "age");
    assert(update.assignments[1].value->kind == sql::ExpressionKind::LITERAL);
    assert(update.where != nullptr);
    assert(update.where->kind == sql::ExpressionKind::BINARY);
}

static void check_insert_variants()
{
    auto insert = sql::parse_insert("INSERT INTO users (id, name, active) VALUES (1, 'alice', TRUE), (2, 'bob', FALSE);");
    assert(insert.table_name == "users");
    assert(insert.column_names.size() == 3);
    assert(insert.rows.size() == 2);
    assert(insert.rows[0].values[2].kind == sql::LiteralKind::BOOLEAN);

    auto insert2 = sql::parse_insert("INSERT INTO logs VALUES (-10, 3.14, NULL);");
    assert(insert2.rows.size() == 1);
    assert(insert2.rows[0].values[1].kind == sql::LiteralKind::DOUBLE);
    assert(insert2.rows[0].values[2].kind == sql::LiteralKind::NULL_LITERAL);
}

static void check_truncate()
{
    auto trunc = sql::parse_truncate("TRUNCATE TABLE users;");
    assert(trunc.table_name == "users");
}

static void check_parse_dml_switch()
{
    auto parsed = sql::parse_dml("UPDATE accounts SET balance = 100;");
    assert(parsed.kind == sql::DMLStatementKind::UPDATE);
    assert(parsed.update.assignments.size() == 1);
}

bool sql_dml_parser_tests()
{
    check_insert_variants();
    check_select_with_where_limit();
    check_select_star();
    check_select_star_mixed();
    check_null_tests();
    check_delete_where();
    check_update_parse();
    check_truncate();
    check_parse_dml_switch();

    bool caught = false;
    try
    {
        (void)sql::parse_select("SELECT users;");
    }
    catch (const DBException &ex)
    {
        caught = (ex.code() == StatusCode::SYNTAX_ERROR);
    }
    catch (...)
    {
        caught = true;
    }
    assert(caught);

    return true;
}
