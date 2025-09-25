#include <cassert>
#include <string>

#include "sql/ddl_parser.h"
#include "common/exception.h"

using namespace kizuna;

bool sql_ddl_parser_tests()
{
    try
    {
        auto stmt = sql::parse_create_table("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(40) NOT NULL, age INTEGER);");
        assert(stmt.table_name == "users");
        assert(stmt.columns.size() == 3);
        assert(stmt.columns[0].constraint.primary_key);
        assert(stmt.columns[1].type == DataType::VARCHAR);
        assert(stmt.columns[1].length == 40);
        assert(stmt.columns[1].constraint.not_null);
    }
    catch (...)
    {
        return false;
    }

    try
    {
        auto stmt = sql::parse_drop_table("DROP TABLE IF EXISTS users CASCADE;");
        assert(stmt.table_name == "users");
        assert(stmt.if_exists);
        assert(stmt.cascade);
    }
    catch (...)
    {
        return false;
    }

    bool caught = false;
    try
    {
        (void)sql::parse_create_table("CREATE TABLE broken ();");
    }
    catch (const DBException &ex)
    {
        caught = (ex.code() == StatusCode::SYNTAX_ERROR || ex.code() == StatusCode::INVALID_ARGUMENT);
    }
    catch (...)
    {
        caught = true;
    }
    assert(caught);

    return true;
}
