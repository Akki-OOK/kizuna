#include <cassert>
#include <string>

#include "sql/dml_parser.h"
#include "common/exception.h"

using namespace kizuna;

bool sql_dml_parser_tests()
{
    try
    {
        auto insert = sql::parse_insert("INSERT INTO users (id, name, active) VALUES (1, 'alice', TRUE), (2, 'bob', FALSE);");
        assert(insert.table_name == "users");
        assert(insert.column_names.size() == 3);
        assert(insert.rows.size() == 2);
        assert(insert.rows[0].values.size() == 3);
        assert(insert.rows[0].values[0].kind == sql::LiteralKind::INTEGER);
        assert(insert.rows[0].values[2].kind == sql::LiteralKind::BOOLEAN);
        assert(insert.rows[1].values[2].kind == sql::LiteralKind::BOOLEAN);
    }
    catch (...)
    {
        return false;
    }

    try
    {
        auto insert = sql::parse_insert("INSERT INTO logs VALUES (-10, 3.14, NULL);");
        assert(insert.rows.size() == 1);
        assert(insert.rows[0].values[0].kind == sql::LiteralKind::INTEGER);
        assert(insert.rows[0].values[1].kind == sql::LiteralKind::DOUBLE);
        assert(insert.rows[0].values[2].kind == sql::LiteralKind::NULL_LITERAL);
    }
    catch (...)
    {
        return false;
    }

    try
    {
        auto select = sql::parse_select("SELECT * FROM users;");
        assert(select.table_name == "users");
    }
    catch (...)
    {
        return false;
    }

    try
    {
        auto del = sql::parse_delete("DELETE FROM users;");
        assert(del.table_name == "users");
    }
    catch (...)
    {
        return false;
    }

    try
    {
        auto trunc = sql::parse_truncate("TRUNCATE TABLE users;");
        assert(trunc.table_name == "users");
    }
    catch (...)
    {
        return false;
    }

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
