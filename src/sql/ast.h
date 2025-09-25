#pragma once

#include <optional>
#include <string>
#include <vector>

#include "common/types.h"

namespace kizuna::sql
{
    enum class StatementKind
    {
        CREATE_TABLE,
        DROP_TABLE
    };

    struct ColumnConstraintAST
    {
        bool not_null{false};
        bool primary_key{false};
        bool unique{false};
        std::optional<std::string> default_literal;
    };

    struct ColumnDefAST
    {
        std::string name;
        DataType type{DataType::NULL_TYPE};
        uint32_t length{0};
        ColumnConstraintAST constraint{};
    };

    struct CreateTableStatement
    {
        std::string table_name;
        std::vector<ColumnDefAST> columns;
        bool has_primary_key() const;
    };

    struct DropTableStatement
    {
        std::string table_name;
        bool if_exists{false};
        bool cascade{false};
    };

    enum class LiteralKind
    {
        NULL_LITERAL,
        INTEGER,
        DOUBLE,
        STRING,
        BOOLEAN
    };

    struct LiteralValue
    {
        LiteralKind kind{LiteralKind::NULL_LITERAL};
        std::string text;
        bool bool_value{false};

        static LiteralValue null();
        static LiteralValue boolean(bool value);
        static LiteralValue integer(std::string value);
        static LiteralValue floating(std::string value);
        static LiteralValue string(std::string value);
    };

    struct InsertRow
    {
        std::vector<LiteralValue> values;
    };

    struct InsertStatement
    {
        std::string table_name;
        std::vector<std::string> column_names;
        std::vector<InsertRow> rows;
    };

    struct SelectStatement
    {
        std::string table_name;
    };

    struct DeleteStatement
    {
        std::string table_name;
    };

    struct TruncateStatement
    {
        std::string table_name;
    };

    enum class DMLStatementKind
    {
        INSERT,
        SELECT,
        DELETE,
        TRUNCATE
    };

    struct ParsedDML
    {
        DMLStatementKind kind{DMLStatementKind::INSERT};
        InsertStatement insert;
        SelectStatement select;
        DeleteStatement del;
        TruncateStatement truncate;
    };
}
