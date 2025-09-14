#include <vector>
#include <string>
#include <filesystem>

#include "storage/file_manager.h"
#include "storage/page_manager.h"
#include "storage/record.h"

using namespace kizuna;
namespace fs = std::filesystem;

bool page_manager_tests()
{
    const std::string db_path = std::string(config::TEMP_DIR) + "pm_test" + config::DB_FILE_EXTENSION;
    std::error_code ec;
    fs::create_directories(config::TEMP_DIR, ec);
    fs::remove(db_path, ec);

    FileManager fm(db_path, true);
    try
    {
        fm.open();
        PageManager pm(fm, /*capacity*/ 2);

        // Create a new page and write a record
        page_id_t id = pm.new_page(PageType::DATA);
        auto &page = pm.fetch(id, true);

        std::vector<record::Field> fields;
        fields.push_back(record::from_int64(123456789));
        fields.push_back(record::from_string("pm"));
        auto payload = record::encode(fields);
        slot_id_t slot{};
        bool ok = page.insert(payload.data(), static_cast<uint16_t>(payload.size()), slot);
        if (!ok) return false;
        pm.unpin(id, /*dirty*/ true); // mark dirty so it flushes on eviction/flush

        // Evict by fetching more pages
        page_id_t id2 = pm.new_page(PageType::DATA);
        pm.unpin(id2, true);

        // Free id2 and ensure it gets reused
        pm.free_page(id2);
        page_id_t id3 = pm.new_page(PageType::DATA);
        if (id3 != id2) return false; // should reuse freed page id

        // Force flush all to disk
        pm.flush_all();

        // Reload and verify
        auto &page2 = pm.fetch(id, true);
        std::vector<uint8_t> out;
        if (!page2.read(slot, out)) return false;
        std::vector<record::Field> decoded;
        if (!record::decode(out.data(), out.size(), decoded)) return false;
        if (decoded.size() != 2) return false;
        pm.unpin(id, false);
    }
    catch (...)
    {
        return false;
    }
    return true;
}
