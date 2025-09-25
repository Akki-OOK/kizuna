#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "common/types.h"
#include "common/exception.h"

namespace kizuna::catalog
{
    enum class EntryType : uint8_t
    {
        TABLE = 1,
        COLUMN = 2,
        INDEX = 3
    };

    struct TableCatalogEntry
    {
        table_id_t table_id{0};
        page_id_t root_page_id{0};
        std::string name;          // user-visible table name
        std::string create_sql;    // raw CREATE TABLE statement

        TableDef to_table_def() const;
        static TableCatalogEntry from_table_def(const TableDef &def, page_id_t root_page, std::string create_sql = {});

        std::vector<uint8_t> serialize() const;
        static TableCatalogEntry deserialize(const uint8_t *data, size_t size, size_t &consumed);
    };

    struct ColumnCatalogEntry
    {
        table_id_t table_id{0};
        column_id_t column_id{0};
        uint32_t ordinal_position{0}; // position within CREATE TABLE list
        ColumnDef column;             // holds name/type/constraint metadata

        std::vector<uint8_t> serialize() const;
        static ColumnCatalogEntry deserialize(const uint8_t *data, size_t size, size_t &consumed);
    };

    uint8_t encode_constraints(const ColumnConstraint &constraint) noexcept;
    ColumnConstraint decode_constraints(uint8_t mask, std::string default_literal);
}

