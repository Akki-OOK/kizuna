#include "sql/ast.h"

#include <utility>

namespace kizuna::sql
{
    bool CreateTableStatement::has_primary_key() const
    {
        for (const auto &col : columns)
        {
            if (col.constraint.primary_key)
                return true;
        }
        return false;
    }

    LiteralValue LiteralValue::null()
    {
        LiteralValue lit;
        lit.kind = LiteralKind::NULL_LITERAL;
        lit.text = "NULL";
        lit.bool_value = false;
        return lit;
    }

    LiteralValue LiteralValue::boolean(bool value)
    {
        LiteralValue lit;
        lit.kind = LiteralKind::BOOLEAN;
        lit.bool_value = value;
        lit.text = value ? "TRUE" : "FALSE";
        return lit;
    }

    LiteralValue LiteralValue::integer(std::string value)
    {
        LiteralValue lit;
        lit.kind = LiteralKind::INTEGER;
        lit.text = std::move(value);
        return lit;
    }

    LiteralValue LiteralValue::floating(std::string value)
    {
        LiteralValue lit;
        lit.kind = LiteralKind::DOUBLE;
        lit.text = std::move(value);
        return lit;
    }

    LiteralValue LiteralValue::string(std::string value)
    {
        LiteralValue lit;
        lit.kind = LiteralKind::STRING;
        lit.text = std::move(value);
        return lit;
    }
}
