#include "storage/table_heap.h"

#include <cstring>
#include <limits>

namespace kizuna
{
    namespace
    {
        constexpr bool is_valid_page(page_id_t id)
        {
            return id >= config::FIRST_PAGE_ID;
        }
    }

    TableHeap::TableHeap(PageManager &pm, page_id_t root_page_id)
        : pm_(pm), root_page_id_(root_page_id), tail_page_id_(root_page_id)
    {
        if (!is_valid_page(root_page_id_))
        {
            KIZUNA_THROW_STORAGE(StatusCode::INVALID_ARGUMENT, "Invalid table root", std::to_string(root_page_id_));
        }
        auto &root = pm_.fetch(root_page_id_, true);
        const auto type = static_cast<PageType>(root.header().page_type);
        if (type != PageType::DATA)
        {
            pm_.unpin(root_page_id_, false);
            KIZUNA_THROW_STORAGE(StatusCode::INVALID_PAGE_TYPE, "Table root is not DATA", std::to_string(root_page_id_));
        }
        tail_page_id_ = find_tail(root_page_id_);
        pm_.unpin(root_page_id_, false);
    }

    TableHeap::RowLocation TableHeap::insert(const std::vector<uint8_t> &payload)
    {
        if (payload.size() > std::numeric_limits<uint16_t>::max())
        {
            KIZUNA_THROW_STORAGE(StatusCode::RECORD_TOO_LARGE,
                                 "Record payload too large",
                                 std::to_string(payload.size()));
        }

        page_id_t current = tail_page_id_;
        while (is_valid_page(current))
        {
            auto &page = pm_.fetch(current, true);
            slot_id_t slot{};
            if (page.insert(payload.data(), static_cast<uint16_t>(payload.size()), slot))
            {
                pm_.unpin(current, true);
                tail_page_id_ = current;
                return RowLocation{current, slot};
            }
            page_id_t next = page.next_page_id();
            pm_.unpin(current, false);
            if (is_valid_page(next))
            {
                current = next;
                continue;
            }
            return append_new_page(current, payload);
        }
        return append_new_page(root_page_id_, payload);
    }

    TableHeap::RowLocation TableHeap::update(const RowLocation &loc, const std::vector<uint8_t> &payload)
    {
        if (payload.size() > std::numeric_limits<uint16_t>::max())
        {
            KIZUNA_THROW_STORAGE(StatusCode::RECORD_TOO_LARGE,
                                 "Record payload too large",
                                 std::to_string(payload.size()));
        }
        if (!is_valid_page(loc.page_id))
        {
            KIZUNA_THROW_STORAGE(StatusCode::RECORD_NOT_FOUND, "Invalid page for update", std::to_string(loc.page_id));
        }

        auto &page = pm_.fetch(loc.page_id, true);
        bool updated = page.update(loc.slot, payload.data(), static_cast<uint16_t>(payload.size()));
        pm_.unpin(loc.page_id, updated);
        if (updated)
        {
            return loc;
        }

        if (!erase(loc))
        {
            KIZUNA_THROW_STORAGE(StatusCode::RECORD_NOT_FOUND, "Update erase failed", std::to_string(loc.page_id));
        }

        return insert(payload);
    }

    bool TableHeap::erase(const RowLocation &loc)
    {
        if (!is_valid_page(loc.page_id))
            return false;
        auto &page = pm_.fetch(loc.page_id, true);
        bool ok = page.erase(loc.slot);
        pm_.unpin(loc.page_id, ok);
        return ok;
    }

    bool TableHeap::read(const RowLocation &loc, std::vector<uint8_t> &out) const
    {
        if (!is_valid_page(loc.page_id))
            return false;
        auto &page = pm_.fetch(loc.page_id, true);
        bool ok = page.read(loc.slot, out);
        pm_.unpin(loc.page_id, false);
        return ok;
    }

    void TableHeap::truncate()
    {
        auto &root = pm_.fetch(root_page_id_, true);
        page_id_t next = root.next_page_id();
        root.set_next_page_id(config::INVALID_PAGE_ID);
        root.set_prev_page_id(config::INVALID_PAGE_ID);
        root.header().record_count = 0;
        root.header().slot_count = 0;
        root.header().free_space_offset = static_cast<uint16_t>(Page::kHeaderSize);
        std::memset(root.data() + Page::kHeaderSize, 0, Page::page_size() - Page::kHeaderSize);
        pm_.unpin(root_page_id_, true);

        page_id_t current = next;
        while (is_valid_page(current))
        {
            auto &page = pm_.fetch(current, true);
            page_id_t nxt = page.next_page_id();
            pm_.unpin(current, false);
            pm_.free_page(current);
            current = nxt;
        }
        tail_page_id_ = root_page_id_;
    }

    TableHeap::Iterator TableHeap::begin()
    {
        return Iterator(this, root_page_id_, 0, false);
    }

    TableHeap::Iterator TableHeap::end()
    {
        return Iterator();
    }

    page_id_t TableHeap::find_tail(page_id_t start) const
    {
        page_id_t current = start;
        while (is_valid_page(current))
        {
            auto &page = pm_.fetch(current, true);
            page_id_t next = page.next_page_id();
            pm_.unpin(current, false);
            if (!is_valid_page(next))
            {
                return current;
            }
            current = next;
        }
        return start;
    }

    TableHeap::RowLocation TableHeap::append_new_page(page_id_t previous_tail, const std::vector<uint8_t> &payload)
    {
        page_id_t new_page_id = pm_.new_page(PageType::DATA);
        auto &new_page = pm_.fetch(new_page_id, true);
        new_page.set_prev_page_id(previous_tail);
        new_page.set_next_page_id(config::INVALID_PAGE_ID);
        slot_id_t slot{};
        bool ok = new_page.insert(payload.data(), static_cast<uint16_t>(payload.size()), slot);
        if (!ok)
        {
            pm_.unpin(new_page_id, false);
            pm_.free_page(new_page_id);
            KIZUNA_THROW_STORAGE(StatusCode::PAGE_FULL, "Record does not fit in empty page", std::to_string(payload.size()));
        }
        pm_.unpin(new_page_id, true);

        auto &prev_page = pm_.fetch(previous_tail, true);
        prev_page.set_next_page_id(new_page_id);
        pm_.unpin(previous_tail, true);

        tail_page_id_ = new_page_id;
        return RowLocation{new_page_id, slot};
    }

    TableHeap::Iterator::Iterator(TableHeap *heap, page_id_t page, slot_id_t slot, bool end)
        : heap_(heap), page_(page), slot_(slot), end_(end)
    {
        if (end_ || heap_ == nullptr)
        {
            heap_ = nullptr;
            page_ = config::INVALID_PAGE_ID;
            slot_ = 0;
            loc_ = RowLocation{};
            payload_.clear();
            end_ = true;
        }
        else
        {
            advance();
        }
    }

    TableHeap::Iterator &TableHeap::Iterator::operator++()
    {
        advance();
        return *this;
    }

    TableHeap::Iterator TableHeap::Iterator::operator++(int)
    {
        Iterator tmp = *this;
        advance();
        return tmp;
    }

    bool TableHeap::Iterator::operator==(const Iterator &other) const
    {
        if (end_ && other.end_)
            return true;
        return heap_ == other.heap_ && end_ == other.end_ && loc_ == other.loc_;
    }

    void TableHeap::Iterator::advance()
    {
        if (heap_ == nullptr)
        {
            end_ = true;
            return;
        }

        while (is_valid_page(page_))
        {
            auto &page = heap_->pm_.fetch(page_, true);
            const auto slot_count = page.header().slot_count;
            while (slot_ < slot_count)
            {
                std::vector<uint8_t> data;
                if (page.read(slot_, data))
                {
                    loc_ = RowLocation{page_, slot_};
                    payload_ = std::move(data);
                    ++slot_;
                    heap_->pm_.unpin(page_, false);
                    end_ = false;
                    return;
                }
                ++slot_;
            }
            page_id_t next = page.next_page_id();
            heap_->pm_.unpin(page_, false);
            page_ = next;
            slot_ = 0;
        }

        heap_ = nullptr;
        page_ = config::INVALID_PAGE_ID;
        slot_ = 0;
        loc_ = RowLocation{};
        payload_.clear();
        end_ = true;
    }
}
