#include "storage/record.h"

#include <cstring>

namespace kizuna::record
{
    namespace
    {
        inline void append_u16(std::vector<std::uint8_t> &buf, std::uint16_t v)
        {
            buf.push_back(static_cast<std::uint8_t>(v & 0xFF));
            buf.push_back(static_cast<std::uint8_t>((v >> 8) & 0xFF));
        }

        inline bool read_u16(const std::uint8_t *&p, const std::uint8_t *end, std::uint16_t &out)
        {
            if (p + 2 > end) return false;
            out = static_cast<std::uint16_t>(p[0]) | (static_cast<std::uint16_t>(p[1]) << 8);
            p += 2;
            return true;
        }

        template <typename T>
        inline void append_pod(std::vector<std::uint8_t> &buf, const T &v)
        {
            const std::uint8_t *raw = reinterpret_cast<const std::uint8_t *>(&v);
            buf.insert(buf.end(), raw, raw + sizeof(T));
        }

        template <typename T>
        inline bool read_pod(const std::uint8_t *&p, const std::uint8_t *end, T &out)
        {
            if (p + sizeof(T) > end) return false;
            std::memcpy(&out, p, sizeof(T));
            p += sizeof(T);
            return true;
        }
    } // namespace

    Field from_null() { return Field{DataType::NULL_TYPE, {}}; }

    Field from_bool(bool v)
    {
        Field f{DataType::BOOLEAN, {}};
        f.payload.push_back(static_cast<std::uint8_t>(v ? 1 : 0));
        return f;
    }

    Field from_int32(std::int32_t v)
    {
        Field f{DataType::INTEGER, {}};
        append_pod(f.payload, v);
        return f;
    }

    Field from_int64(std::int64_t v)
    {
        Field f{DataType::BIGINT, {}};
        append_pod(f.payload, v);
        return f;
    }

    Field from_double(double v)
    {
        Field f{DataType::DOUBLE, {}};
        append_pod(f.payload, v);
        return f;
    }

    Field from_string(std::string_view s)
    {
        Field f{DataType::VARCHAR, {}};
        f.payload.insert(f.payload.end(), s.begin(), s.end());
        return f;
    }

    Field from_blob(const std::vector<std::uint8_t> &b)
    {
        Field f{DataType::BLOB, {}};
        f.payload = b;
        return f;
    }

    std::vector<std::uint8_t> encode(const std::vector<Field> &fields)
    {
        std::vector<std::uint8_t> out;
        out.reserve(2 + fields.size() * 4); // rough

        if (fields.size() > 0xFFFF)
        {
            KIZUNA_THROW_RECORD(StatusCode::INVALID_ARGUMENT, "Too many fields", std::to_string(fields.size()));
        }
        append_u16(out, static_cast<std::uint16_t>(fields.size()));

        std::size_t total = 2;
        for (const auto &f : fields)
        {
            const auto t = static_cast<std::uint8_t>(f.type);
            if (f.payload.size() > 0xFFFF)
            {
                KIZUNA_THROW_RECORD(StatusCode::RECORD_TOO_LARGE, "Field too large", std::to_string(f.payload.size()));
            }
            const auto len = static_cast<std::uint16_t>(f.payload.size());

            out.push_back(t);
            append_u16(out, len);
            out.insert(out.end(), f.payload.begin(), f.payload.end());

            total += 1 + 2 + f.payload.size();
            if (total > config::MAX_RECORD_SIZE)
            {
                KIZUNA_THROW_RECORD(StatusCode::RECORD_TOO_LARGE, "Encoded record too large", std::to_string(total));
            }
        }
        return out;
    }

    bool decode(const std::uint8_t *data, std::size_t len, std::vector<Field> &out_fields)
    {
        out_fields.clear();
        const std::uint8_t *p = data;
        const std::uint8_t *end = data + len;

        std::uint16_t count = 0;
        if (!read_u16(p, end, count)) return false;
        out_fields.reserve(count);

        for (std::uint16_t i = 0; i < count; ++i)
        {
            if (p >= end) return false;
            DataType type = static_cast<DataType>(*p++);
            std::uint16_t flen = 0;
            if (!read_u16(p, end, flen)) return false;
            if (p + flen > end) return false;

            Field f;
            f.type = type;
            f.payload.assign(p, p + flen);
            out_fields.emplace_back(std::move(f));
            p += flen;
        }
        return p == end;
    }
}

