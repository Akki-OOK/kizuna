#pragma once

#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/schema.h"
#include "common/value.h"
#include "sql/ast.h"

namespace kizuna::engine
{
    class ExpressionEvaluator
    {
    public:
        ExpressionEvaluator(const std::vector<catalog::ColumnCatalogEntry> &columns,
                             std::string table_name = {});

        Value evaluate_scalar(const sql::Expression &expression,
                              const std::vector<Value> &row_values) const;

        TriBool evaluate_predicate(const sql::Expression &expression,
                                   const std::vector<Value> &row_values) const;

    private:
        struct ColumnBinding
        {
            std::size_t index{0};
            DataType type{DataType::NULL_TYPE};
        };

        std::string table_name_;
        std::unordered_map<std::string, ColumnBinding> column_map_;

        const ColumnBinding *lookup_column(const sql::ColumnRef &ref) const;
        Value literal_to_value(const sql::LiteralValue &literal, std::optional<DataType> target_type) const;
        Value evaluate_value(const sql::Expression &expression,
                             const std::vector<Value> &row_values,
                             std::optional<DataType> target_hint) const;
        TriBool evaluate_predicate_internal(const sql::Expression &expression,
                                            const std::vector<Value> &row_values) const;
        Value coerce_to_type(const Value &value, DataType target) const;
    };
}
