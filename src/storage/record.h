#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "common/types.h"
#include "common/config.h"
#include "common/exception.h"

namespace kizuna::record
{
    // Field is a typed byte payload. For fixed-size types, payload stores the
    // little-endian bytes of the value (e.g., 4 bytes for INTEGER, 8 for BIGINT/DOUBLE).
    // For VARCHAR/TEXT/BLOB, payload stores raw bytes.
    struct Field
    {
        DataType type{DataType::NULL_TYPE};
        std::vector<std::uint8_t> payload{};
    };

    // Convenience builders for common types
    Field from_null();
    Field from_bool(bool v);
    Field from_int32(std::int32_t v);
    Field from_int64(std::int64_t v);
    Field from_double(double v);
    Field from_string(std::string_view s); // VARCHAR
    Field from_blob(const std::vector<std::uint8_t> &b);

    // Encode a record as a flat payload to be stored inside a Page.
    // Format (little-endian):
    //   uint16_t field_count
    //   repeated field_count times:
    //     uint8_t  type
    //     uint16_t length
    //     uint8_t[length] payload
    // Throws RecordException if total exceeds MAX_RECORD_SIZE.
    std::vector<std::uint8_t> encode(const std::vector<Field> &fields);

    // Decode a record payload back into fields. Returns false on malformed input.
    bool decode(const std::uint8_t *data, std::size_t len, std::vector<Field> &out_fields);
}

