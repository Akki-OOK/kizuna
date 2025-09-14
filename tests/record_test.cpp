#include <string>
#include <vector>
#include <cstdint>

#include "storage/record.h"
#include "common/exception.h"

using namespace kizuna;

static bool check_encode_decode_basic()
{
    std::vector<record::Field> fields;
    fields.push_back(record::from_bool(true));
    fields.push_back(record::from_int32(123));
    fields.push_back(record::from_int64(4567890123LL));
    fields.push_back(record::from_double(3.14159));
    fields.push_back(record::from_string("alpha"));

    auto payload = record::encode(fields);

    std::vector<record::Field> out;
    if (!record::decode(payload.data(), payload.size(), out)) return false;
    if (out.size() != fields.size()) return false;
    if (out[4].type != DataType::VARCHAR) return false;
    return true;
}

static bool check_encode_too_large()
{
    // Construct a field that exceeds MAX_RECORD_SIZE on encode
    std::vector<record::Field> fields;
    std::string big(config::MAX_RECORD_SIZE + 100, 'x');
    fields.push_back(record::from_string(big));
    try
    {
        auto payload = record::encode(fields);
        (void)payload;
    }
    catch (const DBException &e)
    {
        return e.code() == StatusCode::RECORD_TOO_LARGE;
    }
    return false;
}

bool record_tests()
{
    return check_encode_decode_basic() && check_encode_too_large();
}

