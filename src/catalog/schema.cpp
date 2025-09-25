#include "catalog/schema.h"

#include <algorithm>
#include <cstring>
#include <limits>


namespace kizuna::catalog
{
    namespace
    {
        constexpr uint8_t kNotNullMask = 0x01;
        constexpr uint8_t kPrimaryKeyMask = 0x02;
        constexpr uint8_t kUniqueMask = 0x04;
        constexpr uint8_t kDefaultMask = 0x08;

        void write_u16(std::vector<uint8_t> &out, uint16_t value)
        {
            out.push_back(static_cast<uint8_t>(value & 0xFF));
            out.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
        }

        void write_u32(std::vector<uint8_t> &out, uint32_t value)
        {
            out.push_back(static_cast<uint8_t>(value & 0xFF));
            out.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
            out.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
            out.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
        }

        bool read_u16(const uint8_t *data, size_t size, size_t &offset, uint16_t &out)
        {
            if (offset + sizeof(uint16_t) > size)
                return false;
            out = static_cast<uint16_t>(data[offset]) |
                  static_cast<uint16_t>(data[offset + 1]) << 8;
            offset += sizeof(uint16_t);
            return true;
        }

        bool read_u32(const uint8_t *data, size_t size, size_t &offset, uint32_t &out)
        {
            if (offset + sizeof(uint32_t) > size)
                return false;
            out = static_cast<uint32_t>(data[offset]) |
                  (static_cast<uint32_t>(data[offset + 1]) << 8) |
                  (static_cast<uint32_t>(data[offset + 2]) << 16) |
                  (static_cast<uint32_t>(data[offset + 3]) << 24);
            offset += sizeof(uint32_t);
            return true;
        }

        bool read_bytes(const uint8_t *data, size_t size, size_t &offset, size_t len, std::string &out)
        {
            if (offset + len > size)
                return false;
            out.assign(reinterpret_cast<const char *>(data + offset), len);
            offset += len;
            return true;
        }
    } // namespace

    TableDef TableCatalogEntry::to_table_def() const
    {
        TableDef def;
        def.id = table_id;
        def.name = name;
        def.columns.clear();
        return def;
    }

    TableCatalogEntry TableCatalogEntry::from_table_def(const TableDef &def, page_id_t root_page, std::string create_sql_str)
    {
        TableCatalogEntry entry;
        entry.table_id = def.id;
        entry.name = def.name;
        entry.root_page_id = root_page;
        entry.create_sql = std::move(create_sql_str);
        return entry;
    }

    std::vector<uint8_t> TableCatalogEntry::serialize() const
    {
        if (name.size() > std::numeric_limits<uint16_t>::max())
        {
            KIZUNA_THROW_QUERY(StatusCode::INVALID_ARGUMENT, "table name too long", name);
        }
        std::vector<uint8_t> out;
        out.reserve(16 + name.size() + create_sql.size());
        write_u32(out, static_cast<uint32_t>(table_id));
        write_u32(out, static_cast<uint32_t>(root_page_id));
        write_u16(out, static_cast<uint16_t>(name.size()));
        out.insert(out.end(), name.begin(), name.end());
        write_u32(out, static_cast<uint32_t>(create_sql.size()));
        out.insert(out.end(), create_sql.begin(), create_sql.end());
        return out;
    }

    TableCatalogEntry TableCatalogEntry::deserialize(const uint8_t *data, size_t size, size_t &consumed)
{
    TableCatalogEntry entry;
    size_t offset = 0;
    uint32_t table_id_raw = 0;
    uint32_t root_page_raw = 0;
    if (!read_u32(data, size, offset, table_id_raw))
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "table catalog truncated", "table_id");
    if (!read_u32(data, size, offset, root_page_raw))
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "table catalog truncated", "root_page");
    uint16_t name_len = 0;
    if (!read_u16(data, size, offset, name_len))
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "table catalog truncated", "name_len");
    if (!read_bytes(data, size, offset, name_len, entry.name))
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "table catalog truncated", "name");
    uint32_t sql_len = 0;
    if (!read_u32(data, size, offset, sql_len))
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "table catalog truncated", "sql_len");
    if (!read_bytes(data, size, offset, sql_len, entry.create_sql))
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "table catalog truncated", "sql");

    entry.table_id = static_cast<table_id_t>(table_id_raw);
    entry.root_page_id = static_cast<page_id_t>(root_page_raw);
    consumed = offset;
    return entry;
}

    std::vector<uint8_t> ColumnCatalogEntry::serialize() const
    {
        if (column.name.size() > std::numeric_limits<uint16_t>::max())
        {
            KIZUNA_THROW_QUERY(StatusCode::INVALID_ARGUMENT, "column name too long", column.name);
        }
        if (column.constraint.has_default && column.constraint.default_value.size() > std::numeric_limits<uint16_t>::max())
        {
            KIZUNA_THROW_QUERY(StatusCode::INVALID_ARGUMENT, "default literal too long", column.name);
        }

        std::vector<uint8_t> out;
        out.reserve(32 + column.name.size() + column.constraint.default_value.size());
        write_u32(out, static_cast<uint32_t>(table_id));
        write_u32(out, static_cast<uint32_t>(column_id));
        write_u32(out, ordinal_position);
        out.push_back(static_cast<uint8_t>(column.type));
        write_u32(out, column.length);
        out.push_back(encode_constraints(column.constraint));
        write_u16(out, static_cast<uint16_t>(column.name.size()));
        out.insert(out.end(), column.name.begin(), column.name.end());
        const auto &def = column.constraint.default_value;
        uint16_t default_len = static_cast<uint16_t>(column.constraint.has_default ? def.size() : 0);
        write_u16(out, default_len);
        if (column.constraint.has_default)
        {
            out.insert(out.end(), def.begin(), def.end());
        }
        return out;
    }

    ColumnCatalogEntry ColumnCatalogEntry::deserialize(const uint8_t *data, size_t size, size_t &consumed)
{
    ColumnCatalogEntry entry;
    size_t offset = 0;
    uint32_t table_raw = 0;
    uint32_t column_raw = 0;
    uint32_t ordinal = 0;
    uint32_t length = 0;
    uint8_t type_byte = 0;
    uint8_t constraint_mask = 0;

    if (!read_u32(data, size, offset, table_raw))
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "column catalog truncated", "table_id");
    if (!read_u32(data, size, offset, column_raw))
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "column catalog truncated", "column_id");
    if (!read_u32(data, size, offset, ordinal))
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "column catalog truncated", "ordinal");
    if (offset >= size)
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "column catalog truncated", "type");
    type_byte = data[offset++];
    if (!read_u32(data, size, offset, length))
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "column catalog truncated", "length");
    if (offset >= size)
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "column catalog truncated", "constraint");
    constraint_mask = data[offset++];
    uint16_t name_len = 0;
    if (!read_u16(data, size, offset, name_len))
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "column catalog truncated", "name_len");
    if (!read_bytes(data, size, offset, name_len, entry.column.name))
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "column catalog truncated", "name");
    uint16_t default_len = 0;
    if (!read_u16(data, size, offset, default_len))
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "column catalog truncated", "default_len");
    std::string default_literal;
    if (!read_bytes(data, size, offset, default_len, default_literal))
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "column catalog truncated", "default_literal");

    if (type_byte > static_cast<uint8_t>(DataType::BLOB))
        KIZUNA_THROW_RECORD(StatusCode::INVALID_RECORD_FORMAT, "unknown column data type", std::to_string(type_byte));

    entry.table_id = static_cast<table_id_t>(table_raw);
    entry.column_id = static_cast<column_id_t>(column_raw);
    entry.ordinal_position = ordinal;
    entry.column.id = entry.column_id;
    entry.column.type = static_cast<DataType>(type_byte);
    entry.column.length = length;
    entry.column.constraint = decode_constraints(constraint_mask, std::move(default_literal));

    consumed = offset;
    return entry;
}

    uint8_t encode_constraints(const ColumnConstraint &constraint) noexcept
    {
        uint8_t mask = 0;
        if (constraint.not_null)
            mask |= kNotNullMask;
        if (constraint.primary_key)
            mask |= kPrimaryKeyMask;
        if (constraint.unique)
            mask |= kUniqueMask;
        if (constraint.has_default)
            mask |= kDefaultMask;
        return mask;
    }

    ColumnConstraint decode_constraints(uint8_t mask, std::string default_literal)
    {
        ColumnConstraint constraint;
        constraint.not_null = (mask & kNotNullMask) != 0;
        constraint.primary_key = (mask & kPrimaryKeyMask) != 0;
        constraint.unique = (mask & kUniqueMask) != 0;
        constraint.has_default = (mask & kDefaultMask) != 0;
        if (constraint.has_default)
        {
            constraint.default_value = std::move(default_literal);
        }
        else
        {
            constraint.default_value.clear();
        }
        return constraint;
    }
}




