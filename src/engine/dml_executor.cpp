#include "engine/dml_executor.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <cstring>
#include <limits>
#include <optional>
#include <sstream>
#include <string_view>
#include <unordered_map>

#include "common/exception.h"
#include "common/logger.h"
#include "engine/expression_evaluator.h"
#include "storage/record.h"

namespace kizuna::engine
{

namespace
{
    std::string join_strings(const std::vector<std::string> &items, std::string_view delimiter)
    {
        std::ostringstream oss;
        for (std::size_t i = 0; i < items.size(); ++i)
        {
            if (i != 0)
                oss << delimiter;
            oss << items[i];
        }
        return oss.str();
    }

    std::string column_ref_to_string(const sql::ColumnRef &ref)
    {
        if (!ref.table.empty())
            return ref.table + "." + ref.column;
        return ref.column;
    }

    std::string literal_to_string(const sql::LiteralValue &literal)
    {
        switch (literal.kind)
        {
        case sql::LiteralKind::NULL_LITERAL:
            return "NULL";
        case sql::LiteralKind::BOOLEAN:
            return literal.bool_value ? "TRUE" : "FALSE";
        case sql::LiteralKind::STRING:
        case sql::LiteralKind::INTEGER:
        case sql::LiteralKind::DOUBLE:
            return literal.text;
        default:
            return "<literal>";
        }
    }

    std::string binary_operator_to_string(sql::BinaryOperator op)
    {
        switch (op)
        {
        case sql::BinaryOperator::EQUAL: return "=";
        case sql::BinaryOperator::NOT_EQUAL: return "!=";
        case sql::BinaryOperator::LESS: return "<";
        case sql::BinaryOperator::LESS_EQUAL: return "<=";
        case sql::BinaryOperator::GREATER: return ">";
        case sql::BinaryOperator::GREATER_EQUAL: return ">=";
        case sql::BinaryOperator::AND: return "AND";
        case sql::BinaryOperator::OR: return "OR";
        }
        return "?";
    }

    std::string describe_expression(const sql::Expression *expr)
    {
        if (expr == nullptr)
            return "<null>";

        switch (expr->kind)
        {
        case sql::ExpressionKind::LITERAL:
            return literal_to_string(expr->literal);
        case sql::ExpressionKind::COLUMN_REF:
            return column_ref_to_string(expr->column);
        case sql::ExpressionKind::UNARY:
            return "NOT (" + describe_expression(expr->left.get()) + ")";
        case sql::ExpressionKind::BINARY:
            return "(" + describe_expression(expr->left.get()) + " " +
                   binary_operator_to_string(expr->binary_op) + " " +
                   describe_expression(expr->right.get()) + ")";
        case sql::ExpressionKind::NULL_TEST:
            return describe_expression(expr->left.get()) +
                   (expr->is_not_null ? " IS NOT NULL" : " IS NULL");
        }
        return "<expr>";
    }

    std::string describe_assignments(const std::vector<sql::UpdateAssignment> &assignments)
    {
        std::vector<std::string> parts;
        parts.reserve(assignments.size());
        for (const auto &assign : assignments)
        {
            parts.push_back(assign.column_name + "=" + describe_expression(assign.value.get()));
        }
        return join_strings(parts, ", ");
    }

    bool is_true(TriBool value) noexcept
    {
        return value == TriBool::True;
    }
}

    struct DMLExecutor::ColumnPredicate
    {
        std::optional<Value> equality;
        std::optional<Value> lower;
        bool lower_inclusive{false};
        std::optional<Value> upper;
        bool upper_inclusive{false};
        bool contradiction{false};

        bool apply_equality(const Value &value);
        bool apply_lower(const Value &value, bool inclusive);
        bool apply_upper(const Value &value, bool inclusive);
        bool bounds_compatible() const;
    };

    struct DMLExecutor::PredicateExtraction
    {
        std::unordered_map<column_id_t, ColumnPredicate> predicates;
        bool contradiction{false};
    };

    struct DMLExecutor::IndexScanSpec
    {
        enum class Kind
        {
            Equality,
            Range
        };

        std::size_t context_index{0};
        Kind kind{Kind::Equality};
        std::vector<Value> equality_values;
        std::optional<Value> lower_value;
        bool lower_inclusive{true};
        std::optional<Value> upper_value;
        bool upper_inclusive{true};
    };

    DMLExecutor::DMLExecutor(catalog::CatalogManager &catalog,
                             PageManager &pm,
                             FileManager &fm,
                             index::IndexManager &index_manager)
        : catalog_(catalog), pm_(pm), fm_(fm), index_manager_(index_manager)
    {
    }

    std::string DMLExecutor::execute(std::string_view sql)
    {
        auto parsed = sql::parse_dml(sql);
        switch (parsed.kind)
        {
        case sql::DMLStatementKind::INSERT:
        {
            auto result = insert_into(parsed.insert);
            return "Rows inserted: " + std::to_string(result.rows_inserted);
        }
        case sql::DMLStatementKind::SELECT:
        {
            auto result = select(parsed.select);
            return "Rows returned: " + std::to_string(result.rows.size());
        }
        case sql::DMLStatementKind::DELETE:
        {
            auto result = delete_all(parsed.del);
            return "Rows deleted: " + std::to_string(result.rows_deleted);
        }
        case sql::DMLStatementKind::UPDATE:
        {
            auto result = update_all(parsed.update);
            return "Rows updated: " + std::to_string(result.rows_updated);
        }
        case sql::DMLStatementKind::TRUNCATE:
        {
            truncate(parsed.truncate);
            return "Table truncated";
        }
        }
        throw DBException(StatusCode::NOT_IMPLEMENTED, "Unsupported DML statement", std::string(sql));
    }

    InsertResult DMLExecutor::insert_into(const sql::InsertStatement &stmt)
    {
        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
            throw QueryException::table_not_found(stmt.table_name);
        const auto table_entry = *table_opt;
        auto columns = catalog_.get_columns(table_entry.table_id);
        if (columns.empty())
            throw QueryException::invalid_constraint("table has no columns");

        std::vector<std::string> column_names = stmt.column_names;
        if (column_names.empty())
        {
            column_names.reserve(columns.size());
            for (const auto &c : columns)
                column_names.push_back(c.column.name);
        }
        if (column_names.size() != columns.size())
            throw QueryException::invalid_constraint("column count mismatch");

        auto index_contexts = load_table_indexes(table_entry.table_id);
        std::vector<std::unique_ptr<index::IndexHandle>> index_handles;
        index_handles.reserve(index_contexts.size());
        for (auto &ctx : index_contexts)
        {
            index_handles.push_back(index_manager_.OpenIndex(ctx.catalog_entry));
        }
        auto column_lookup = build_column_lookup(columns);

        TableHeap heap(pm_, table_entry.root_page_id);
        std::size_t inserted = 0;
        for (const auto &row : stmt.rows)
        {
            if (row.values.size() != column_names.size())
                throw QueryException::invalid_constraint("row value count mismatch");
            auto payload = encode_row(columns, row, column_names);
            auto row_values = decode_row_values(columns, payload);
            auto location = heap.insert(payload);
            record_id_t record_id = make_record_id(location);

            for (std::size_t i = 0; i < index_contexts.size(); ++i)
            {
                auto key = build_index_key(index_contexts[i], columns, row_values, column_lookup);
                auto &tree = index_handles[i]->tree();
                tree.Insert(key, record_id);
                catalog_.set_index_root(index_contexts[i].catalog_entry.index_id, tree.root_page_id());
                index_contexts[i].catalog_entry.root_page_id = tree.root_page_id();
            }

            ++inserted;
        }
        return InsertResult{inserted};
    }

    SelectResult DMLExecutor::select(const sql::SelectStatement &stmt)
    {
        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
            throw QueryException::table_not_found(stmt.table_name);
        const auto table_entry = *table_opt;
        auto columns = catalog_.get_columns(table_entry.table_id);
        auto index_contexts = load_table_indexes(table_entry.table_id);
        auto column_lookup = build_column_lookup(columns);
        if (columns.empty())
            return SelectResult{};

        SelectResult result;
        result.column_names.clear();
        auto projection = build_projection(stmt, columns, table_entry.name, result.column_names);
        if (projection.empty())
        {
            projection.reserve(columns.size());
            for (std::size_t i = 0; i < columns.size(); ++i)
            {
                projection.push_back(i);
                result.column_names.push_back(columns[i].column.name);
            }
        }

        const sql::OrderByClause *order_clause = stmt.order_by ? &*stmt.order_by : nullptr;
        bool has_order = order_clause != nullptr;
        std::size_t order_column_index = 0;
        column_id_t order_column_id = 0;
        std::optional<std::size_t> order_index_context;
        if (has_order)
        {
            order_column_index = find_column_index(columns, table_entry.name, order_clause->column);
            order_column_id = columns[order_column_index].column_id;
            for (std::size_t i = 0; i < index_contexts.size(); ++i)
            {
                const auto &ctx = index_contexts[i];
                if (ctx.catalog_entry.column_ids.size() == 1 && ctx.catalog_entry.column_ids.front() == order_column_id)
                {
                    order_index_context = i;
                    break;
                }
            }
        }

        ExpressionEvaluator evaluator(columns, table_entry.name);
        const auto *predicate = stmt.where ? stmt.where.get() : nullptr;
        const std::size_t limit = stmt.limit.has_value() ? static_cast<std::size_t>(*stmt.limit) : std::numeric_limits<std::size_t>::max();
        std::string projection_desc = join_strings(result.column_names, ", ");
        if (projection_desc.empty()) projection_desc = "<none>";
        const std::string predicate_desc = predicate ? describe_expression(predicate) : "<none>";
        const std::string order_desc = has_order ? column_ref_to_string(order_clause->column) + (order_clause->ascending ? " ASC" : " DESC") : "<none>";
        const std::string limit_desc = stmt.limit.has_value() ? std::to_string(static_cast<std::size_t>(*stmt.limit)) : "ALL";
        Logger::instance().debug("[SELECT] table=", table_entry.name, " projection=[", projection_desc, "] predicate=", predicate_desc, " order_by=", order_desc, " limit=", limit_desc);
        if (limit == 0)
        {
            return result;
        }

        std::optional<PredicateExtraction> predicate_info;
        if (predicate)
            predicate_info = extract_column_predicates(predicate, columns, table_entry.name);
        if (predicate_info && predicate_info->contradiction)
        {
            return result;
        }

        std::optional<IndexScanSpec> index_spec;
        std::unique_ptr<index::IndexHandle> scan_handle;
        std::vector<record_id_t> candidate_ids;
        bool candidate_ids_populated = false;
        bool candidate_ids_in_final_order = false;
        if (predicate && predicate_info && !index_contexts.empty())
        {
            auto spec_opt = choose_index_scan(index_contexts, *predicate_info);
            if (spec_opt.has_value())
            {
                index_spec = std::move(*spec_opt);
                scan_handle = index_manager_.OpenIndex(index_contexts[index_spec->context_index].catalog_entry);
                candidate_ids = run_index_scan(*index_spec, index_contexts, *scan_handle, columns, column_lookup);
                candidate_ids_populated = true;
                if (has_order && order_index_context.has_value() && index_spec->context_index == *order_index_context)
                {
                    candidate_ids_in_final_order = true;
                    if (!order_clause->ascending)
                        std::reverse(candidate_ids.begin(), candidate_ids.end());
                }
            }
        }

        if (!candidate_ids_populated && has_order && order_index_context.has_value())
        {
            auto handle = index_manager_.OpenIndex(index_contexts[*order_index_context].catalog_entry);
            auto lookup_it = column_lookup.find(order_column_id);
            if (lookup_it == column_lookup.end())
            {
                KIZUNA_THROW_INDEX(StatusCode::INVALID_ARGUMENT, "Order column metadata missing", std::to_string(order_column_id));
            }
            std::vector<catalog::ColumnCatalogEntry> key_columns;
            key_columns.push_back(columns[lookup_it->second]);

            std::optional<std::vector<uint8_t>> lower_key;
            bool lower_inclusive = true;
            std::optional<std::vector<uint8_t>> upper_key;
            bool upper_inclusive = true;
            if (predicate_info)
            {
                auto it = predicate_info->predicates.find(order_column_id);
                if (it != predicate_info->predicates.end())
                {
                    const auto &pred = it->second;
                    if (pred.lower.has_value())
                    {
                        std::vector<Value> key_values{pred.lower.value()};
                        lower_key = encode_values(key_columns, key_values);
                        lower_inclusive = pred.lower_inclusive;
                    }
                    if (pred.upper.has_value())
                    {
                        std::vector<Value> key_values{pred.upper.value()};
                        upper_key = encode_values(key_columns, key_values);
                        upper_inclusive = pred.upper_inclusive;
                    }
                }
            }

            candidate_ids = handle->tree().ScanRange(lower_key, lower_inclusive, upper_key, upper_inclusive);
            candidate_ids_populated = true;
            candidate_ids_in_final_order = true;
            if (!order_clause->ascending)
                std::reverse(candidate_ids.begin(), candidate_ids.end());
        }

        TableHeap heap(pm_, table_entry.root_page_id);
        std::size_t produced = 0;

        struct OrderedRow
        {
            Value order_value;
            std::vector<std::string> row;
        };
        std::vector<OrderedRow> rows_for_sort;
        bool collect_for_sort = has_order && !candidate_ids_in_final_order;

        auto process_row = [&](const std::vector<Value> &values) {
            if (predicate && !is_true(evaluator.evaluate_predicate(*predicate, values)))
                return;

            std::vector<std::string> row;
            row.reserve(projection.size());
            for (std::size_t idx : projection)
            {
                row.push_back(values[idx].to_string());
            }

            if (has_order)
            {
                if (collect_for_sort)
                {
                    rows_for_sort.push_back(OrderedRow{values[order_column_index], std::move(row)});
                }
                else
                {
                    if (produced >= limit)
                        return;
                    result.rows.push_back(std::move(row));
                    ++produced;
                }
            }
            else
            {
                if (produced >= limit)
                    return;
                result.rows.push_back(std::move(row));
                ++produced;
            }
        };

        if (candidate_ids_populated)
        {
            for (record_id_t rid : candidate_ids)
            {
                if (!collect_for_sort && produced >= limit)
                    break;
                auto loc = decode_record_id(rid);
                std::vector<uint8_t> payload;
                if (!heap.read(loc, payload))
                    continue;
                auto values = decode_row_values(columns, payload);
                process_row(values);
            }
        }
        else
        {
            heap.scan([&](const TableHeap::RowLocation &, const std::vector<uint8_t> &payload) {
                if (!has_order && produced >= limit)
                    return;
                auto values = decode_row_values(columns, payload);
                process_row(values);
            });
        }

        if (has_order && collect_for_sort)
        {
            auto comparator = [&](const OrderedRow &lhs, const OrderedRow &rhs) {
                bool lhs_null = lhs.order_value.is_null();
                bool rhs_null = rhs.order_value.is_null();
                if (lhs_null != rhs_null)
                    return !lhs_null;
                auto cmp = compare(lhs.order_value, rhs.order_value);
                if (cmp == CompareResult::Less)
                    return order_clause->ascending;
                if (cmp == CompareResult::Greater)
                    return !order_clause->ascending;
                return false;
            };
            std::stable_sort(rows_for_sort.begin(), rows_for_sort.end(), comparator);
            for (std::size_t i = 0; i < rows_for_sort.size() && produced < limit; ++i)
            {
                result.rows.push_back(std::move(rows_for_sort[i].row));
                ++produced;
            }
        }

        return result;
    }

    DeleteResult DMLExecutor::delete_all(const sql::DeleteStatement &stmt)
    {
        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
            throw QueryException::table_not_found(stmt.table_name);
        const auto table_entry = *table_opt;
        auto index_contexts = load_table_indexes(table_entry.table_id);
        std::vector<std::unique_ptr<index::IndexHandle>> index_handles;
        index_handles.reserve(index_contexts.size());
        for (auto &ctx : index_contexts)
        {
            index_handles.push_back(index_manager_.OpenIndex(ctx.catalog_entry));
        }
        auto columns = catalog_.get_columns(table_entry.table_id);
        auto column_lookup = build_column_lookup(columns);

        TableHeap heap(pm_, table_entry.root_page_id);
        ExpressionEvaluator evaluator(columns, table_entry.name);
        const auto *predicate = stmt.where ? stmt.where.get() : nullptr;

        const std::string predicate_desc = predicate ? describe_expression(predicate) : "<none>";
        Logger::instance().debug("[DELETE] table=", table_entry.name, " predicate=", predicate_desc);

        std::optional<PredicateExtraction> predicate_info;
        if (predicate)
            predicate_info = extract_column_predicates(predicate, columns, table_entry.name);
        if (predicate_info && predicate_info->contradiction)
        {
            return DeleteResult{0};
        }

        std::optional<IndexScanSpec> index_spec;
        std::vector<record_id_t> candidate_ids;
        if (predicate && predicate_info && !index_contexts.empty())
        {
            auto spec_opt = choose_index_scan(index_contexts, *predicate_info);
            if (spec_opt.has_value())
            {
                index_spec = std::move(*spec_opt);
                candidate_ids = run_index_scan(*index_spec, index_contexts, *index_handles[index_spec->context_index], columns, column_lookup);
            }
        }

        std::size_t deleted = 0;
        auto remove_row = [&](const TableHeap::RowLocation &loc, const std::vector<Value> &values) {
            if (!heap.erase(loc))
                return;
            record_id_t record_id = make_record_id(loc);
            for (std::size_t i = 0; i < index_contexts.size(); ++i)
            {
                auto key = build_index_key(index_contexts[i], columns, values, column_lookup);
                auto &tree = index_handles[i]->tree();
                tree.Remove(key, record_id);
                catalog_.set_index_root(index_contexts[i].catalog_entry.index_id, tree.root_page_id());
                index_contexts[i].catalog_entry.root_page_id = tree.root_page_id();
            }
            ++deleted;
        };

        if (index_spec.has_value())
        {
            for (record_id_t rid : candidate_ids)
            {
                auto loc = decode_record_id(rid);
                std::vector<uint8_t> payload;
                if (!heap.read(loc, payload))
                    continue;
                auto values = decode_row_values(columns, payload);
                if (predicate && !is_true(evaluator.evaluate_predicate(*predicate, values)))
                    continue;
                remove_row(loc, values);
            }
        }
        else
        {
            heap.scan([&](const TableHeap::RowLocation &loc, const std::vector<uint8_t> &payload) {
                auto values = decode_row_values(columns, payload);
                if (predicate && !is_true(evaluator.evaluate_predicate(*predicate, values)))
                    return;
                remove_row(loc, values);
            });
        }

        return DeleteResult{deleted};
    }

    UpdateResult DMLExecutor::update_all(const sql::UpdateStatement &stmt)
    {
        if (stmt.assignments.empty())
            throw QueryException::invalid_constraint("UPDATE requires at least one assignment");

        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
            throw QueryException::table_not_found(stmt.table_name);
        const auto table_entry = *table_opt;
        auto index_contexts = load_table_indexes(table_entry.table_id);
        std::vector<std::unique_ptr<index::IndexHandle>> index_handles;
        index_handles.reserve(index_contexts.size());
        for (auto &ctx : index_contexts)
        {
            index_handles.push_back(index_manager_.OpenIndex(ctx.catalog_entry));
        }
        auto columns = catalog_.get_columns(table_entry.table_id);
        auto column_lookup = build_column_lookup(columns);

        std::unordered_map<std::string, std::size_t> column_index;
        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            column_index.emplace(columns[i].column.name, i);
        }

        TableHeap heap(pm_, table_entry.root_page_id);
        ExpressionEvaluator evaluator(columns, table_entry.name);
        const auto *predicate = stmt.where ? stmt.where.get() : nullptr;

        const std::string assignments_desc = describe_assignments(stmt.assignments);
        const std::string predicate_desc = predicate ? describe_expression(predicate) : "<none>";
        Logger::instance().debug("[UPDATE] table=", table_entry.name, " assignments=", (assignments_desc.empty() ? std::string("<none>") : assignments_desc), " predicate=", predicate_desc);

        struct UpdateTarget
        {
            TableHeap::RowLocation location;
            std::vector<Value> current_values;
        };

        std::vector<UpdateTarget> targets;
        std::optional<PredicateExtraction> predicate_info;
        if (predicate)
            predicate_info = extract_column_predicates(predicate, columns, table_entry.name);
        if (predicate_info && predicate_info->contradiction)
        {
            return UpdateResult{0};
        }

        std::optional<IndexScanSpec> index_spec;
        std::vector<record_id_t> candidate_ids;
        if (predicate && predicate_info && !index_contexts.empty())
        {
            auto spec_opt = choose_index_scan(index_contexts, *predicate_info);
            if (spec_opt.has_value())
            {
                index_spec = std::move(*spec_opt);
                candidate_ids = run_index_scan(*index_spec, index_contexts, *index_handles[index_spec->context_index], columns, column_lookup);
            }
        }

        auto collect_target = [&](const TableHeap::RowLocation &loc, const std::vector<uint8_t> &payload) {
            auto current_values = decode_row_values(columns, payload);
            if (predicate && !is_true(evaluator.evaluate_predicate(*predicate, current_values)))
                return;
            targets.push_back(UpdateTarget{loc, std::move(current_values)});
        };

        if (index_spec.has_value())
        {
            for (record_id_t rid : candidate_ids)
            {
                auto loc = decode_record_id(rid);
                std::vector<uint8_t> payload;
                if (!heap.read(loc, payload))
                    continue;
                collect_target(loc, payload);
            }
        }
        else
        {
            heap.scan([&](const TableHeap::RowLocation &loc, const std::vector<uint8_t> &payload) {
                collect_target(loc, payload);
            });
        }

        std::size_t updated = 0;
        for (auto &target : targets)
        {
            auto &current_values = target.current_values;
            std::vector<Value> new_values = current_values;
            for (const auto &assignment : stmt.assignments)
            {
                auto it = column_index.find(assignment.column_name);
                if (it == column_index.end())
                    throw QueryException::column_not_found(assignment.column_name, stmt.table_name);

                std::size_t idx = it->second;
                Value evaluated = evaluator.evaluate_scalar(*assignment.value, current_values);
                Value coerced = coerce_value_for_column(columns[idx], evaluated);
                new_values[idx] = coerced;
            }

            auto new_payload = encode_values(columns, new_values);
            record_id_t old_record_id = make_record_id(target.location);
            auto new_location = heap.update(target.location, new_payload);
            record_id_t new_record_id = make_record_id(new_location);

            for (std::size_t i = 0; i < index_contexts.size(); ++i)
            {
                auto old_key = build_index_key(index_contexts[i], columns, current_values, column_lookup);
                auto new_key = build_index_key(index_contexts[i], columns, new_values, column_lookup);
                if (old_record_id == new_record_id && old_key == new_key)
                    continue;
                auto &tree = index_handles[i]->tree();
                tree.Remove(old_key, old_record_id);
                tree.Insert(new_key, new_record_id);
                catalog_.set_index_root(index_contexts[i].catalog_entry.index_id, tree.root_page_id());
                index_contexts[i].catalog_entry.root_page_id = tree.root_page_id();
            }

            target.location = new_location;
            current_values = std::move(new_values);
            ++updated;
        }

        return UpdateResult{updated};
    }

    void DMLExecutor::truncate(const sql::TruncateStatement &stmt)
    {
        auto table_opt = catalog_.get_table(stmt.table_name);
        if (!table_opt)
            throw QueryException::table_not_found(stmt.table_name);
        const auto table_entry = *table_opt;

        TableHeap heap(pm_, table_entry.root_page_id);
        heap.truncate();
    }

    std::vector<Value> DMLExecutor::decode_row_values(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                                      const std::vector<uint8_t> &payload) const
    {
        std::vector<record::Field> fields;
        if (!record::decode(payload.data(), payload.size(), fields))
        {
            throw DBException(StatusCode::INVALID_RECORD_FORMAT, "Failed to decode row", "table row");
        }
        if (fields.size() != columns.size())
        {
            throw DBException(StatusCode::INVALID_ARGUMENT, "Decoded field count mismatch", "table row");
        }

        std::vector<Value> values;
        values.reserve(columns.size());
        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            const auto &col = columns[i].column;
            const auto &field = fields[i];
            if (field.is_null)
            {
                values.push_back(Value::null(col.type));
                continue;
            }

            switch (col.type)
            {
            case DataType::BOOLEAN:
            {
                bool v = !field.payload.empty() && field.payload[0] != 0;
                values.push_back(Value::boolean(v));
                break;
            }
            case DataType::INTEGER:
            {
                int32_t v = 0;
                std::memcpy(&v, field.payload.data(), sizeof(int32_t));
                values.push_back(Value::int32(v));
                break;
            }
            case DataType::BIGINT:
            {
                int64_t v = 0;
                std::memcpy(&v, field.payload.data(), sizeof(int64_t));
                values.push_back(Value::int64(v));
                break;
            }
            case DataType::DATE:
            {
                int64_t v = 0;
                std::memcpy(&v, field.payload.data(), sizeof(int64_t));
                values.push_back(Value::date(v));
                break;
            }
            case DataType::TIMESTAMP:
            {
                int64_t v = 0;
                std::memcpy(&v, field.payload.data(), sizeof(int64_t));
                values.push_back(Value::int64(v));
                break;
            }
            case DataType::FLOAT:
            {
                float v = 0.0f;
                std::memcpy(&v, field.payload.data(), sizeof(float));
                values.push_back(Value::floating(static_cast<double>(v)));
                break;
            }
            case DataType::DOUBLE:
            {
                double v = 0.0;
                std::memcpy(&v, field.payload.data(), sizeof(double));
                values.push_back(Value::floating(v));
                break;
            }
            case DataType::VARCHAR:
            case DataType::TEXT:
            {
                std::string text(reinterpret_cast<const char *>(field.payload.data()), field.payload.size());
                values.push_back(Value::string(std::move(text), col.type));
                break;
            }
            default:
                values.push_back(Value::string("<unsupported>"));
                break;
            }
        }
        return values;
    }

    std::vector<DMLExecutor::TableIndexContext> DMLExecutor::load_table_indexes(table_id_t table_id) const
    {
        std::vector<TableIndexContext> contexts;
        auto indexes = catalog_.get_indexes(table_id);
        contexts.reserve(indexes.size());
        for (auto &entry : indexes)
        {
            contexts.push_back(TableIndexContext{entry});
        }
        return contexts;
    }

    std::unordered_map<column_id_t, std::size_t> DMLExecutor::build_column_lookup(const std::vector<catalog::ColumnCatalogEntry> &columns) const
    {
        std::unordered_map<column_id_t, std::size_t> lookup;
        lookup.reserve(columns.size());
        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            lookup.emplace(columns[i].column_id, i);
        }
        return lookup;
    }

    std::vector<uint8_t> DMLExecutor::build_index_key(const TableIndexContext &ctx,
                                                     const std::vector<catalog::ColumnCatalogEntry> &columns,
                                                     const std::vector<Value> &row_values,
                                                     const std::unordered_map<column_id_t, std::size_t> &lookup) const
    {
        std::vector<catalog::ColumnCatalogEntry> key_columns;
        std::vector<Value> key_values;
        key_columns.reserve(ctx.catalog_entry.column_ids.size());
        key_values.reserve(ctx.catalog_entry.column_ids.size());
        for (auto column_id : ctx.catalog_entry.column_ids)
        {
            auto it = lookup.find(column_id);
            if (it == lookup.end())
            {
                KIZUNA_THROW_INDEX(StatusCode::INVALID_ARGUMENT, "Index column metadata missing", std::to_string(column_id));
            }
            key_columns.push_back(columns[it->second]);
            key_values.push_back(row_values[it->second]);
        }
        return encode_values(key_columns, key_values);
    }

    bool DMLExecutor::ColumnPredicate::bounds_compatible() const
    {
        if (contradiction)
            return false;
        if (lower && upper)
        {
            auto cmp = compare(*lower, *upper);
            if (cmp == CompareResult::Greater)
                return false;
            if (cmp == CompareResult::Equal && (!lower_inclusive || !upper_inclusive))
                return false;
        }
        return true;
    }

    bool DMLExecutor::ColumnPredicate::apply_lower(const Value &value, bool inclusive)
    {
        if (contradiction)
            return false;
        if (value.is_null())
        {
            contradiction = true;
            return false;
        }
        if (!lower.has_value())
        {
            lower = value;
            lower_inclusive = inclusive;
        }
        else
        {
            auto cmp = compare(value, *lower);
            if (cmp == CompareResult::Greater)
            {
                lower = value;
                lower_inclusive = inclusive;
            }
            else if (cmp == CompareResult::Equal)
            {
                lower_inclusive = lower_inclusive && inclusive;
            }
            else if (cmp == CompareResult::Unknown)
            {
                contradiction = true;
                return false;
            }
        }
        if (!bounds_compatible())
        {
            contradiction = true;
            return false;
        }
        return true;
    }

    bool DMLExecutor::ColumnPredicate::apply_upper(const Value &value, bool inclusive)
    {
        if (contradiction)
            return false;
        if (value.is_null())
        {
            contradiction = true;
            return false;
        }
        if (!upper.has_value())
        {
            upper = value;
            upper_inclusive = inclusive;
        }
        else
        {
            auto cmp = compare(value, *upper);
            if (cmp == CompareResult::Less)
            {
                upper = value;
                upper_inclusive = inclusive;
            }
            else if (cmp == CompareResult::Equal)
            {
                upper_inclusive = upper_inclusive && inclusive;
            }
            else if (cmp == CompareResult::Unknown)
            {
                contradiction = true;
                return false;
            }
        }
        if (!bounds_compatible())
        {
            contradiction = true;
            return false;
        }
        return true;
    }

    bool DMLExecutor::ColumnPredicate::apply_equality(const Value &value)
    {
        if (contradiction)
            return false;
        if (value.is_null())
        {
            contradiction = true;
            return false;
        }
        if (equality.has_value())
        {
            if (compare(*equality, value) != CompareResult::Equal)
            {
                contradiction = true;
                return false;
            }
        }
        equality = value;
        if (!apply_lower(value, true))
            return false;
        if (!apply_upper(value, true))
            return false;
        return true;
    }

    Value DMLExecutor::literal_to_value_for_column(const catalog::ColumnCatalogEntry &column,
                                                   const sql::LiteralValue &literal) const
    {
        const auto &col = column.column;
        switch (literal.kind)
        {
        case sql::LiteralKind::NULL_LITERAL:
            return Value::null(col.type);
        case sql::LiteralKind::BOOLEAN:
            if (col.type == DataType::BOOLEAN)
                return Value::boolean(literal.bool_value);
            if (col.type == DataType::INTEGER)
                return Value::int32(literal.bool_value ? 1 : 0);
            if (col.type == DataType::BIGINT)
                return Value::int64(literal.bool_value ? 1 : 0);
            break;
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
            switch (col.type)
            {
            case DataType::BOOLEAN:
                return Value::boolean(parsed != 0);
            case DataType::INTEGER:
                if (parsed < std::numeric_limits<int32_t>::min() || parsed > std::numeric_limits<int32_t>::max())
                    throw QueryException::type_error("literal", "INTEGER", literal.text);
                return Value::int32(static_cast<int32_t>(parsed));
            case DataType::BIGINT:
                return Value::int64(static_cast<int64_t>(parsed));
            case DataType::DOUBLE:
            case DataType::FLOAT:
                return Value::floating(static_cast<double>(parsed));
            default:
                break;
            }
            break;
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
            switch (col.type)
            {
            case DataType::DOUBLE:
            case DataType::FLOAT:
                return Value::floating(parsed);
            case DataType::INTEGER:
                if (parsed < std::numeric_limits<int32_t>::min() || parsed > std::numeric_limits<int32_t>::max())
                    throw QueryException::type_error("literal", "INTEGER", literal.text);
                return Value::int32(static_cast<int32_t>(parsed));
            case DataType::BIGINT:
                return Value::int64(static_cast<int64_t>(parsed));
            default:
                break;
            }
            break;
        }
        case sql::LiteralKind::STRING:
        {
            switch (col.type)
            {
            case DataType::DATE:
            {
                auto parsed = parse_date(literal.text);
                if (!parsed)
                    throw QueryException::type_error("literal", "DATE", literal.text);
                return Value::date(*parsed);
            }
            case DataType::VARCHAR:
            case DataType::TEXT:
            {
                if (col.type == DataType::VARCHAR && col.length > 0 && literal.text.size() > col.length)
                    throw QueryException::invalid_constraint("value too long for column '" + col.name + "'");
                return Value::string(literal.text, col.type);
            }
            default:
                break;
            }
            break;
        }
        }

        throw QueryException::type_error("literal comparison", data_type_to_string(col.type), literal.text);
    }

    std::optional<DMLExecutor::PredicateExtraction> DMLExecutor::extract_column_predicates(
        const sql::Expression *predicate,
        const std::vector<catalog::ColumnCatalogEntry> &columns,
        const std::string &table_name) const
    {
        PredicateExtraction extraction;
        if (!predicate)
            return extraction;

        std::function<bool(const sql::Expression *)> visit = [&](const sql::Expression *expr) -> bool {
            if (expr == nullptr)
                return true;

            if (expr->kind == sql::ExpressionKind::BINARY && expr->binary_op == sql::BinaryOperator::AND)
            {
                return visit(expr->left.get()) && visit(expr->right.get());
            }

            if (expr->kind == sql::ExpressionKind::BINARY)
            {
                sql::BinaryOperator op = expr->binary_op;
                switch (op)
                {
                case sql::BinaryOperator::EQUAL:
                case sql::BinaryOperator::LESS:
                case sql::BinaryOperator::LESS_EQUAL:
                case sql::BinaryOperator::GREATER:
                case sql::BinaryOperator::GREATER_EQUAL:
                    break;
                default:
                    return false;
                }

                const sql::Expression *column_expr = nullptr;
                const sql::Expression *literal_expr = nullptr;
                bool column_on_left = true;

                if (expr->left && expr->left->kind == sql::ExpressionKind::COLUMN_REF &&
                    expr->right && expr->right->kind == sql::ExpressionKind::LITERAL)
                {
                    column_expr = expr->left.get();
                    literal_expr = expr->right.get();
                    column_on_left = true;
                }
                else if (expr->right && expr->right->kind == sql::ExpressionKind::COLUMN_REF &&
                         expr->left && expr->left->kind == sql::ExpressionKind::LITERAL)
                {
                    column_expr = expr->right.get();
                    literal_expr = expr->left.get();
                    column_on_left = false;
                }
                else
                {
                    return false;
                }

                std::size_t column_index = find_column_index(columns, table_name, column_expr->column);
                const auto &column_entry = columns[column_index];
                Value literal_value = literal_to_value_for_column(column_entry, literal_expr->literal);
                if (literal_value.is_null())
                {
                    return false;
                }

                auto &column_predicate = extraction.predicates[column_entry.column_id];

                sql::BinaryOperator effective_op = op;
                if (!column_on_left)
                {
                    switch (op)
                    {
                    case sql::BinaryOperator::LESS:
                        effective_op = sql::BinaryOperator::GREATER;
                        break;
                    case sql::BinaryOperator::LESS_EQUAL:
                        effective_op = sql::BinaryOperator::GREATER_EQUAL;
                        break;
                    case sql::BinaryOperator::GREATER:
                        effective_op = sql::BinaryOperator::LESS;
                        break;
                    case sql::BinaryOperator::GREATER_EQUAL:
                        effective_op = sql::BinaryOperator::LESS_EQUAL;
                        break;
                    default:
                        break;
                    }
                }

                bool ok = true;
                switch (effective_op)
                {
                case sql::BinaryOperator::EQUAL:
                    ok = column_predicate.apply_equality(literal_value);
                    break;
                case sql::BinaryOperator::GREATER:
                    ok = column_predicate.apply_lower(literal_value, false);
                    break;
                case sql::BinaryOperator::GREATER_EQUAL:
                    ok = column_predicate.apply_lower(literal_value, true);
                    break;
                case sql::BinaryOperator::LESS:
                    ok = column_predicate.apply_upper(literal_value, false);
                    break;
                case sql::BinaryOperator::LESS_EQUAL:
                    ok = column_predicate.apply_upper(literal_value, true);
                    break;
                default:
                    ok = false;
                    break;
                }

                if (!ok)
                {
                    extraction.contradiction = column_predicate.contradiction;
                    if (!extraction.contradiction)
                        return false;
                }

                return true;
            }

            return false;
        };

        if (!visit(predicate))
            return std::nullopt;

        for (auto &entry : extraction.predicates)
        {
            if (entry.second.contradiction || !entry.second.bounds_compatible())
            {
                extraction.contradiction = true;
                break;
            }
        }

        return extraction;
    }

    std::optional<DMLExecutor::IndexScanSpec> DMLExecutor::choose_index_scan(
        const std::vector<TableIndexContext> &index_contexts,
        const PredicateExtraction &predicates) const
    {
        if (predicates.contradiction || predicates.predicates.empty())
            return std::nullopt;

        std::optional<IndexScanSpec> best_spec;
        std::size_t best_width = 0;

        for (std::size_t i = 0; i < index_contexts.size(); ++i)
        {
            const auto &ctx = index_contexts[i];
            if (ctx.catalog_entry.column_ids.empty())
                continue;

            bool matches_all = true;
            std::vector<Value> equality_values;
            equality_values.reserve(ctx.catalog_entry.column_ids.size());

            for (auto column_id : ctx.catalog_entry.column_ids)
            {
                auto pred_it = predicates.predicates.find(column_id);
                if (pred_it == predicates.predicates.end() || !pred_it->second.equality.has_value())
                {
                    matches_all = false;
                    break;
                }
                equality_values.push_back(pred_it->second.equality.value());
            }

            if (matches_all)
            {
                if (!best_spec.has_value() || ctx.catalog_entry.column_ids.size() > best_width)
                {
                    IndexScanSpec spec;
                    spec.context_index = i;
                    spec.kind = IndexScanSpec::Kind::Equality;
                    spec.equality_values = equality_values;
                    best_width = ctx.catalog_entry.column_ids.size();
                    best_spec = spec;
                }
            }
        }

        if (best_spec.has_value())
            return best_spec;

        for (std::size_t i = 0; i < index_contexts.size(); ++i)
        {
            const auto &ctx = index_contexts[i];
            if (ctx.catalog_entry.column_ids.size() != 1)
                continue;

            auto pred_it = predicates.predicates.find(ctx.catalog_entry.column_ids.front());
            if (pred_it == predicates.predicates.end())
                continue;

            const auto &column_pred = pred_it->second;
            if (column_pred.contradiction)
                return std::nullopt;

            if (column_pred.equality.has_value())
            {
                IndexScanSpec spec;
                spec.context_index = i;
                spec.kind = IndexScanSpec::Kind::Equality;
                spec.equality_values = {column_pred.equality.value()};
                return spec;
            }

            if (column_pred.lower.has_value() || column_pred.upper.has_value())
            {
                IndexScanSpec spec;
                spec.context_index = i;
                spec.kind = IndexScanSpec::Kind::Range;
                if (column_pred.lower.has_value())
                {
                    spec.lower_value = column_pred.lower;
                    spec.lower_inclusive = column_pred.lower_inclusive;
                }
                if (column_pred.upper.has_value())
                {
                    spec.upper_value = column_pred.upper;
                    spec.upper_inclusive = column_pred.upper_inclusive;
                }
                return spec;
            }
        }

        return std::nullopt;
    }

    std::vector<record_id_t> DMLExecutor::run_index_scan(
        const IndexScanSpec &spec,
        const std::vector<TableIndexContext> &index_contexts,
        index::IndexHandle &handle,
        const std::vector<catalog::ColumnCatalogEntry> &columns,
        const std::unordered_map<column_id_t, std::size_t> &column_lookup) const
    {
        const auto &ctx = index_contexts[spec.context_index];
        std::vector<catalog::ColumnCatalogEntry> key_columns;
        key_columns.reserve(ctx.catalog_entry.column_ids.size());
        for (auto column_id : ctx.catalog_entry.column_ids)
        {
            auto it = column_lookup.find(column_id);
            if (it == column_lookup.end())
            {
                KIZUNA_THROW_INDEX(StatusCode::INVALID_ARGUMENT, "Index column metadata missing", std::to_string(column_id));
            }
            key_columns.push_back(columns[it->second]);
        }

        auto &tree = handle.tree();
        switch (spec.kind)
        {
        case IndexScanSpec::Kind::Equality:
        {
            if (spec.equality_values.size() != key_columns.size())
                return {};
            auto key = encode_values(key_columns, spec.equality_values);
            return tree.ScanEqual(key);
        }
        case IndexScanSpec::Kind::Range:
        {
            std::optional<std::vector<uint8_t>> lower;
            std::optional<std::vector<uint8_t>> upper;
            if (spec.lower_value.has_value())
            {
                std::vector<Value> tmp{spec.lower_value.value()};
                lower = encode_values(key_columns, tmp);
            }
            if (spec.upper_value.has_value())
            {
                std::vector<Value> tmp{spec.upper_value.value()};
                upper = encode_values(key_columns, tmp);
            }
            return tree.ScanRange(lower, spec.lower_inclusive, upper, spec.upper_inclusive);
        }
        }
        return {};
    }

    TableHeap::RowLocation DMLExecutor::decode_record_id(record_id_t id)
    {
        TableHeap::RowLocation loc;
        loc.page_id = static_cast<page_id_t>(id >> 32);
        loc.slot = static_cast<slot_id_t>(id & 0xFFFFFFFFu);
        return loc;
    }
    record_id_t DMLExecutor::make_record_id(const TableHeap::RowLocation &loc)
    {
        return (static_cast<record_id_t>(loc.page_id) << 32) | static_cast<record_id_t>(loc.slot);
    }

    std::vector<uint8_t> DMLExecutor::encode_values(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                                    const std::vector<Value> &values) const
    {
        std::vector<record::Field> fields;
        fields.reserve(columns.size());
        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            const auto &column = columns[i].column;
            const Value &value = values[i];
            if (value.is_null())
            {
                if (column.constraint.not_null)
                    throw QueryException::invalid_constraint("column '" + column.name + "' is NOT NULL");
                fields.push_back(record::from_null(column.type));
                continue;
            }

            switch (column.type)
            {
            case DataType::BOOLEAN:
                fields.push_back(record::from_bool(value.as_bool()));
                break;
            case DataType::INTEGER:
                fields.push_back(record::from_int32(value.as_int32()));
                break;
            case DataType::BIGINT:
                fields.push_back(record::from_int64(value.as_int64()));
                break;
            case DataType::FLOAT:
            case DataType::DOUBLE:
                fields.push_back(record::from_double(value.as_double()));
                break;
            case DataType::DATE:
                fields.push_back(record::from_date(value.as_int64()));
                break;
            case DataType::VARCHAR:
            case DataType::TEXT:
            {
                const std::string &text = value.as_string();
                if (column.type == DataType::VARCHAR && column.length > 0 && text.size() > column.length)
                    throw QueryException::invalid_constraint("value too long for column '" + column.name + "'");
                fields.push_back(record::from_string(text));
                break;
            }
            default:
                throw QueryException::unsupported_type("unsupported column type");
            }
        }
        return record::encode(fields);
    }

    Value DMLExecutor::coerce_value_for_column(const catalog::ColumnCatalogEntry &column,
                                               const Value &value) const
    {
        if (value.is_null())
        {
            if (column.column.constraint.not_null)
                throw QueryException::invalid_constraint("column '" + column.column.name + "' is NOT NULL");
            return Value::null(column.column.type);
        }

        switch (column.column.type)
        {
        case DataType::BOOLEAN:
            if (value.type() == DataType::BOOLEAN)
                return value;
            if (value.type() == DataType::INTEGER)
                return Value::boolean(value.as_int32() != 0);
            if (value.type() == DataType::BIGINT)
                return Value::boolean(value.as_int64() != 0);
            throw QueryException::type_error("UPDATE", "BOOLEAN", value.to_string());
        case DataType::INTEGER:
            if (value.type() == DataType::INTEGER)
                return value;
            if (value.type() == DataType::BIGINT)
            {
                auto v = value.as_int64();
                if (v < std::numeric_limits<int32_t>::min() || v > std::numeric_limits<int32_t>::max())
                    throw QueryException::type_error("UPDATE", "INTEGER", std::to_string(v));
                return Value::int32(static_cast<int32_t>(v));
            }
            throw QueryException::type_error("UPDATE", "INTEGER", value.to_string());
        case DataType::BIGINT:
            if (value.type() == DataType::BIGINT)
                return value;
            if (value.type() == DataType::INTEGER)
                return Value::int64(static_cast<int64_t>(value.as_int32()));
            throw QueryException::type_error("UPDATE", "BIGINT", value.to_string());
        case DataType::FLOAT:
        case DataType::DOUBLE:
            if (value.type() == DataType::DOUBLE || value.type() == DataType::FLOAT)
                return Value::floating(value.as_double());
            if (value.type() == DataType::INTEGER)
                return Value::floating(static_cast<double>(value.as_int32()));
            if (value.type() == DataType::BIGINT)
                return Value::floating(static_cast<double>(value.as_int64()));
            throw QueryException::type_error("UPDATE", "DOUBLE", value.to_string());
        case DataType::DATE:
            if (value.type() == DataType::DATE)
                return value;
            if (value.type() == DataType::VARCHAR || value.type() == DataType::TEXT)
            {
                auto parsed = parse_date(value.as_string());
                if (!parsed)
                    throw QueryException::type_error("UPDATE", "DATE", value.as_string());
                return Value::date(*parsed);
            }
            throw QueryException::type_error("UPDATE", "DATE", value.to_string());
        case DataType::VARCHAR:
        case DataType::TEXT:
            if (value.type() == DataType::VARCHAR || value.type() == DataType::TEXT)
                return Value::string(value.as_string(), column.column.type);
            throw QueryException::type_error("UPDATE", "STRING", value.to_string());
        default:
            throw QueryException::unsupported_type("unsupported column type");
        }
    }

    std::vector<size_t> DMLExecutor::build_projection(const sql::SelectStatement &stmt,
                                                      const std::vector<catalog::ColumnCatalogEntry> &columns,
                                                      const std::string &table_name,
                                                      std::vector<std::string> &out_names) const
    {
        std::vector<size_t> projection;
        out_names.clear();

        bool expanded_star = false;
        for (const auto &item : stmt.columns)
        {
            if (item.is_star)
            {
                if (!expanded_star)
                {
                    for (std::size_t i = 0; i < columns.size(); ++i)
                    {
                        projection.push_back(i);
                        out_names.push_back(columns[i].column.name);
                    }
                    expanded_star = true;
                }
                continue;
            }

            std::size_t idx = find_column_index(columns, table_name, item.column);
            projection.push_back(idx);
            out_names.push_back(item.column.column);
        }

        return projection;
    }

    std::size_t DMLExecutor::find_column_index(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                               const std::string &table_name,
                                               const sql::ColumnRef &ref) const
    {
        if (!ref.table.empty() && ref.table != table_name)
            throw QueryException::column_not_found(ref.column, ref.table);

        for (std::size_t i = 0; i < columns.size(); ++i)
        {
            if (columns[i].column.name == ref.column)
                return i;
        }
        throw QueryException::column_not_found(ref.column, table_name);
    }

    std::vector<uint8_t> DMLExecutor::encode_row(const std::vector<catalog::ColumnCatalogEntry> &columns,
                                                 const sql::InsertRow &row,
                                                 const std::vector<std::string> &column_names)
    {
        std::unordered_map<std::string, const sql::LiteralValue *> value_lookup;
        value_lookup.reserve(column_names.size());
        for (std::size_t i = 0; i < column_names.size(); ++i)
        {
            value_lookup.emplace(column_names[i], &row.values[i]);
        }

        std::vector<record::Field> fields;
        fields.reserve(columns.size());

        for (const auto &entry : columns)
        {
            const auto &col = entry.column;
            auto it = value_lookup.find(col.name);
            if (it == value_lookup.end())
                throw QueryException::column_not_found(col.name, col.name);

            const sql::LiteralValue &literal = *(it->second);
            record::Field field;
            field.type = col.type;

            if (literal.kind == sql::LiteralKind::NULL_LITERAL)
            {
                if (col.constraint.not_null)
                    throw QueryException::invalid_constraint("column '" + col.name + "' is NOT NULL");
                field.is_null = true;
            }
            else
            {
                field.is_null = false;
                switch (col.type)
                {
                case DataType::BOOLEAN:
                    if (literal.kind != sql::LiteralKind::BOOLEAN)
                        throw QueryException::type_error("INSERT", "BOOLEAN", literal.text);
                    field = record::from_bool(literal.bool_value);
                    break;
                case DataType::INTEGER:
                case DataType::BIGINT:
                {
                    if (literal.kind != sql::LiteralKind::INTEGER)
                        throw QueryException::type_error("INSERT", "INTEGER", literal.text);
                    long long value = 0;
                    try
                    {
                        value = std::stoll(literal.text);
                    }
                    catch (const std::exception &)
                    {
                        throw QueryException::type_error("INSERT", "INTEGER", literal.text);
                    }
                    if (col.type == DataType::INTEGER)
                    {
                        if (value < std::numeric_limits<int32_t>::min() || value > std::numeric_limits<int32_t>::max())
                            throw QueryException::type_error("INSERT", "INTEGER", literal.text);
                        field = record::from_int32(static_cast<int32_t>(value));
                    }
                    else
                    {
                        field = record::from_int64(static_cast<int64_t>(value));
                    }
                    break;
                }
                case DataType::DOUBLE:
                {
                    if (literal.kind != sql::LiteralKind::DOUBLE && literal.kind != sql::LiteralKind::INTEGER)
                        throw QueryException::type_error("INSERT", "DOUBLE", literal.text);
                    double value = 0.0;
                    try
                    {
                        value = std::stod(literal.text);
                    }
                    catch (const std::exception &)
                    {
                        throw QueryException::type_error("INSERT", "DOUBLE", literal.text);
                    }
                    field = record::from_double(value);
                    break;
                }
                case DataType::DATE:
                {
                    if (literal.kind != sql::LiteralKind::STRING)
                        throw QueryException::type_error("INSERT", "DATE", literal.text);
                    auto parsed = parse_date(literal.text);
                    if (!parsed)
                        throw QueryException::type_error("INSERT", "DATE", literal.text);
                    field = record::from_date(*parsed);
                    break;
                }
                case DataType::VARCHAR:
                case DataType::TEXT:
                {
                    if (literal.kind != sql::LiteralKind::STRING)
                        throw QueryException::type_error("INSERT", "STRING", literal.text);
                    if (col.type == DataType::VARCHAR && col.length > 0 && literal.text.size() > col.length)
                        throw QueryException::invalid_constraint("value too long for column '" + col.name + "'");
                    field = record::from_string(literal.text);
                    break;
                }
                default:
                    throw QueryException::type_error("INSERT", "supported type", literal.text);
                }
            }

            fields.push_back(std::move(field));
        }

        return record::encode(fields);
    }

} // namespace kizuna::engine
