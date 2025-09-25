#pragma once

#include <string>
#include <string_view>

#include "common/exception.h"
#include "sql/ast.h"

namespace kizuna::sql
{
    struct ParsedDDL
    {
        StatementKind kind{StatementKind::CREATE_TABLE};
        CreateTableStatement create;
        DropTableStatement drop;
    };

    CreateTableStatement parse_create_table(std::string_view sql);
    DropTableStatement parse_drop_table(std::string_view sql);
    ParsedDDL parse_ddl(std::string_view sql);
}
