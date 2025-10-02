#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/types.h"

namespace kizuna::sql
{
    // ------------------------------------------------------------------
    // DDL (existing definitions)
    // ------------------------------------------------------------------

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

    // ------------------------------------------------------------------
    // Literals shared between DDL & DML
    // ------------------------------------------------------------------

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

    // ------------------------------------------------------------------
    // DML expression tree
    // ------------------------------------------------------------------

    struct ColumnRef
    {
        std::string table;
        std::string column;

        bool has_table() const noexcept { return !table.empty(); }
    };

    enum class ExpressionKind
    {
        LITERAL,
        COLUMN_REF,
        UNARY,
        BINARY,
        NULL_TEST
    };

    enum class BinaryOperator
    {
        EQUAL,
        NOT_EQUAL,
        LESS,
        LESS_EQUAL,
        GREATER,
        GREATER_EQUAL,
        AND,
        OR
    };

    enum class UnaryOperator
    {
        NOT
    };

    struct Expression
    {
        ExpressionKind kind{ExpressionKind::LITERAL};
        LiteralValue literal{};
        ColumnRef column{};
        UnaryOperator unary_op{UnaryOperator::NOT};
        BinaryOperator binary_op{BinaryOperator::EQUAL};
        bool is_not_null{false};

        std::unique_ptr<Expression> left;
        std::unique_ptr<Expression> right;

        static std::unique_ptr<Expression> make_literal(LiteralValue literal);
        static std::unique_ptr<Expression> make_column(ColumnRef column);
        static std::unique_ptr<Expression> make_unary(UnaryOperator op, std::unique_ptr<Expression> operand);
        static std::unique_ptr<Expression> make_binary(BinaryOperator op,
                                                       std::unique_ptr<Expression> left,
                                                       std::unique_ptr<Expression> right);
        static std::unique_ptr<Expression> make_null_check(std::unique_ptr<Expression> operand, bool is_not);
    };

    struct SelectItem
    {
        bool is_star{false};
        ColumnRef column{};

        static SelectItem star();
        static SelectItem column_item(ColumnRef column);
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
        std::vector<SelectItem> columns; // empty -> treated as '*'
        std::unique_ptr<Expression> where;
        std::optional<std::int64_t> limit;
    };

    struct DeleteStatement
    {
        std::string table_name;
        std::unique_ptr<Expression> where;
    };

    struct TruncateStatement
    {
        std::string table_name;
    };

    struct UpdateAssignment
    {
        std::string column_name;
        std::unique_ptr<Expression> value;
    };

    struct UpdateStatement
    {
        std::string table_name;
        std::vector<UpdateAssignment> assignments;
        std::unique_ptr<Expression> where;
    };

    enum class DMLStatementKind
    {
        INSERT,
        SELECT,
        DELETE,
        UPDATE,
        TRUNCATE
    };

    struct ParsedDML
    {
        DMLStatementKind kind{DMLStatementKind::INSERT};
        InsertStatement insert;
        SelectStatement select;
        DeleteStatement del;
        UpdateStatement update;
        TruncateStatement truncate;
    };
}
