#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "common/types.h"
#include "common/config.h"
#include "common/exception.h"

namespace kizuna
{
    // Page layout and basic slot-directory operations for V0.1
    //
    // Layout (PAGE_SIZE bytes):
    // [ Header (24B) ] [ ...free space grows upward... ] [ ...records grow downward... ] [ SlotDir (2B each) ]
    //
    // Record format used here is simple length-prefixed payload:
    //   uint16_t len; uint8_t data[len];
    // Slot directory stores 2-byte offsets pointing to the start of each record (the len field).

    struct PageHeader
    {
        uint32_t page_id;           // 4
        uint16_t record_count;      // 6
        uint16_t free_space_offset; // 8 (offset from start-of-page to first free byte in free region)
        uint16_t slot_count;        // 10
        uint8_t page_type;          // 11 (see kizuna::PageType)
        uint8_t reserved;           // 12
        uint32_t lsn;               // 16 (for future WAL use)
        uint64_t checksum;          // 24 (optional; may be 0)
    };

    class Page
    {
    public:
        static constexpr size_t kHeaderSize = config::PAGE_HEADER_SIZE;
        static_assert(kHeaderSize == sizeof(PageHeader), "config::PAGE_HEADER_SIZE must equal PageHeader size");

        Page()
        {
            std::memset(storage_.data(), 0, storage_.size());
            auto &h = header();
            h.page_id = 0;
            h.record_count = 0;
            h.slot_count = 0;
            h.page_type = static_cast<uint8_t>(PageType::INVALID);
            h.reserved = 0;
            h.lsn = 0;
            h.checksum = 0;
            h.free_space_offset = static_cast<uint16_t>(kHeaderSize);
        }

        void init(PageType type, page_id_t id)
        {
            auto &h = header();
            h.page_id = id;
            h.record_count = 0;
            h.slot_count = 0;
            h.page_type = static_cast<uint8_t>(type);
            h.reserved = 0;
            h.lsn = 0;
            h.checksum = 0;
            h.free_space_offset = static_cast<uint16_t>(kHeaderSize);
        }

        // Raw access
        uint8_t *data() { return storage_.data(); }
        const uint8_t *data() const { return storage_.data(); }

        // Header access
        PageHeader &header() { return *reinterpret_cast<PageHeader *>(storage_.data()); }
        const PageHeader &header() const { return *reinterpret_cast<const PageHeader *>(storage_.data()); }

        // Slot directory base (end of page) and helpers
        static constexpr size_t page_size() { return config::PAGE_SIZE; }
        static constexpr size_t slot_size() { return sizeof(uint16_t); }

        // Returns total free bytes available for inserting one more record,
        // accounting for the extra slot entry that will be appended.
        size_t free_bytes() const
        {
            const auto &h = header();
            const size_t records_limit = page_size() - (static_cast<size_t>(h.slot_count) + 1) * slot_size();
            if (h.free_space_offset > records_limit) return 0;
            return records_limit - h.free_space_offset;
        }

        // Insert a record (length-prefixed). Returns true on success, false if not enough space.
        bool insert(const uint8_t *payload, uint16_t len, slot_id_t &out_slot)
        {
            // Enforce correct page type
            if (static_cast<PageType>(header().page_type) != PageType::DATA)
            {
                KIZUNA_THROW_STORAGE(StatusCode::INVALID_PAGE_TYPE,
                                      "Insert on non-DATA page",
                                      std::to_string(header().page_id));
            }
            // Needed space: record (2 + len) + one slot entry (2)
            const size_t needed = static_cast<size_t>(len) + 2 + slot_size();
            if (needed > free_bytes())
                return false;

            auto &h = header();

            // Write new record at the current free space offset (grows upward)
            const size_t used_slots_after = static_cast<size_t>(h.slot_count + 1) * slot_size();
            const size_t records_limit = page_size() - used_slots_after;
            size_t record_start = h.free_space_offset;
            if (record_start + 2 + static_cast<size_t>(len) > records_limit)
                return false; // not enough space

            // Write length prefix and payload
            uint8_t *base = data();
            base[record_start + 0] = static_cast<uint8_t>(len & 0xFF);
            base[record_start + 1] = static_cast<uint8_t>((len >> 8) & 0xFF);
            std::memcpy(base + record_start + 2, payload, len);

            // Add slot entry (append at end, after accounting for new slot)
            uint16_t slot_offset = static_cast<uint16_t>(record_start);
            const size_t slot_pos = page_size() - ((static_cast<size_t>(h.slot_count) + 1) * slot_size());
            std::memcpy(base + slot_pos, &slot_offset, sizeof(slot_offset));

            h.slot_count += 1;
            h.record_count += 1;
            out_slot = static_cast<slot_id_t>(h.slot_count - 1);

            // Advance free space offset (records grow upward from header)
            h.free_space_offset = static_cast<uint16_t>(record_start + 2 + len);
            return true;
        }

        // Read record payload into out vector. Returns false if slot invalid.
        bool read(slot_id_t slot, std::vector<uint8_t> &out) const
        {
            if (static_cast<PageType>(header().page_type) != PageType::DATA)
            {
                KIZUNA_THROW_STORAGE(StatusCode::INVALID_PAGE_TYPE,
                                      "Read on non-DATA page",
                                      std::to_string(header().page_id));
            }
            if (slot >= header().slot_count) return false;
            const uint8_t *base = data();
            const size_t slot_pos = page_size() - ((static_cast<size_t>(slot) + 1) * slot_size());
            uint16_t record_off = 0;
            std::memcpy(&record_off, base + slot_pos, sizeof(record_off));
            if (record_off == 0xFFFF) return false; // tombstone
            // Validate bounds
            const auto &h = header();
            const size_t records_end = h.free_space_offset;
            if (record_off + 2 > records_end) return false;
            // Read length
            const uint16_t len = static_cast<uint16_t>(base[record_off]) | (static_cast<uint16_t>(base[record_off + 1]) << 8);
            if (record_off + 2 + len > records_end) return false;
            out.resize(len);
            std::memcpy(out.data(), base + record_off + 2, len);
            return true;
        }

        // Tombstone a record (does not compact). Returns false if slot invalid or already deleted.
        bool erase(slot_id_t slot)
        {
            if (static_cast<PageType>(header().page_type) != PageType::DATA)
            {
                KIZUNA_THROW_STORAGE(StatusCode::INVALID_PAGE_TYPE,
                                      "Erase on non-DATA page",
                                      std::to_string(header().page_id));
            }
            if (slot >= header().slot_count) return false;
            uint8_t *base = data();
            const size_t slot_pos = page_size() - ((static_cast<size_t>(slot) + 1) * slot_size());
            uint16_t record_off = 0;
            std::memcpy(&record_off, base + slot_pos, sizeof(record_off));
            if (record_off == 0xFFFF) return false; // already deleted

            // Tombstone the slot
            uint16_t tomb = 0xFFFF;
            std::memcpy(base + slot_pos, &tomb, sizeof(tomb));
            header().record_count -= 1;
            return true;
        }
    private:
        std::array<uint8_t, config::PAGE_SIZE> storage_{};
    };
}
