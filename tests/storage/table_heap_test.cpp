#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <cstring>

#include "storage/table_heap.h"
#include "storage/page_manager.h"
#include "storage/file_manager.h"
#include "storage/record.h"

using namespace kizuna;
namespace fs = std::filesystem;

namespace
{
    bool is_valid_page_id(page_id_t id)
    {
        return id >= config::FIRST_PAGE_ID;
    }

    struct TestContext
    {
        std::string db_path;
        FileManager fm;
        std::unique_ptr<PageManager> pm;

        TestContext(const std::string &name)
            : db_path(std::string(config::TEMP_DIR) + name + config::DB_FILE_EXTENSION),
              fm(db_path, true)
        {
            std::error_code ec;
            fs::create_directories(config::TEMP_DIR, ec);
            fs::remove(db_path, ec);
            fm.open();
            pm = std::make_unique<PageManager>(fm, 8);
        }

        ~TestContext()
        {
            pm.reset();
            fm.close();
            std::error_code ec;
            fs::remove(db_path, ec);
        }
    };

    std::vector<uint8_t> make_payload(int value)
    {
        std::vector<record::Field> fields;
        fields.push_back(record::from_int32(value));
        fields.push_back(record::from_string("val" + std::to_string(value)));
        return record::encode(fields);
    }

    bool decode_int(const std::vector<uint8_t> &payload, int &out)
    {
        std::vector<record::Field> fields;
        if (!record::decode(payload.data(), payload.size(), fields))
            return false;
        if (fields.empty() || fields[0].payload.size() != 4)
            return false;
        std::memcpy(&out, fields[0].payload.data(), 4);
        return true;
    }

    bool basic_insert_scan()
    {
        TestContext ctx("table_heap_basic");
        page_id_t root = ctx.pm->new_page(PageType::DATA);
        ctx.pm->unpin(root, false);

        TableHeap heap(*ctx.pm, root);
        const auto p1 = heap.insert(make_payload(1));
        const auto p2 = heap.insert(make_payload(2));
        const auto p3 = heap.insert(make_payload(3));

        std::vector<int> seen;
        for (auto it = heap.begin(); it != heap.end(); ++it)
        {
            int value = 0;
            if (!decode_int(*it, value)) return false;
            seen.push_back(value);
        }
        if (seen != std::vector<int>{1, 2, 3}) return false;
        if (p1.page_id != root || p2.page_id != root || p3.page_id != root) return false;
        return true;
    }

    bool overflow_insert()
    {
        TestContext ctx("table_heap_overflow");
        page_id_t root = ctx.pm->new_page(PageType::DATA);
        ctx.pm->unpin(root, false);

        TableHeap heap(*ctx.pm, root);
        std::vector<uint8_t> big_payload(1500, 0xAB);
        std::vector<TableHeap::RowLocation> locs;
        for (int i = 0; i < 8; ++i)
        {
            locs.push_back(heap.insert(big_payload));
        }
        bool moved_to_new_page = false;
        for (const auto &loc : locs)
        {
            if (loc.page_id != root)
            {
                moved_to_new_page = true;
                break;
            }
        }
        if (!moved_to_new_page) return false;

        auto &root_page = ctx.pm->fetch(root, true);
        page_id_t next = root_page.next_page_id();
        ctx.pm->unpin(root, false);
        if (!is_valid_page_id(next)) return false;

        auto &tail = ctx.pm->fetch(locs.back().page_id, true);
        if (tail.prev_page_id() == config::INVALID_PAGE_ID) return false;
        ctx.pm->unpin(locs.back().page_id, false);

        size_t count = 0;
        for (auto it = heap.begin(); it != heap.end(); ++it)
        {
            ++count;
        }
        if (count != locs.size()) return false;
        return true;
    }

    bool erase_skip()
    {
        TestContext ctx("table_heap_erase");
        page_id_t root = ctx.pm->new_page(PageType::DATA);
        ctx.pm->unpin(root, false);

        TableHeap heap(*ctx.pm, root);
        auto loc1 = heap.insert(make_payload(10));
        auto loc2 = heap.insert(make_payload(20));
        auto loc3 = heap.insert(make_payload(30));

        if (!heap.erase(loc2)) return false;
        std::vector<uint8_t> scratch;
        if (heap.read(loc2, scratch)) return false;

        std::vector<int> seen;
        for (auto it = heap.begin(); it != heap.end(); ++it)
        {
            int value = 0;
            if (!decode_int(*it, value)) return false;
            seen.push_back(value);
        }
        if (seen != std::vector<int>{10, 30}) return false;

        std::vector<uint8_t> out;
        if (!heap.read(loc1, out)) return false;
        if (!heap.read(loc3, out)) return false;
        return true;
    }

    bool truncate_resets()
    {
        TestContext ctx("table_heap_truncate");
        page_id_t root = ctx.pm->new_page(PageType::DATA);
        ctx.pm->unpin(root, false);

        TableHeap heap(*ctx.pm, root);
        for (int i = 0; i < 12; ++i)
        {
            heap.insert(make_payload(i));
        }
        heap.truncate();

        if (heap.begin() != heap.end()) return false;

        auto &root_page = ctx.pm->fetch(root, true);
        if (root_page.header().slot_count != 0) return false;
        if (root_page.next_page_id() != config::INVALID_PAGE_ID) return false;
        ctx.pm->unpin(root, false);

        auto loc = heap.insert(make_payload(99));
        if (loc.page_id != root) return false;
        return true;
    }
}

bool table_heap_tests()
{
    return basic_insert_scan() && overflow_insert() && erase_skip() && truncate_resets();
}
