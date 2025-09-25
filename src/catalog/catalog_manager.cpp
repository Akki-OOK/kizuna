#include "catalog/catalog_manager.h"

#include <algorithm>
#include <cstring>
#include <limits>

#include "common/config.h"
#include "common/exception.h"

namespace kizuna::catalog
{
    namespace
    {
        template <typename F>
        void for_each_slot(PageManager &pm, page_id_t page_id, F &&fn)
        {
            auto &page = pm.fetch(page_id, true);
            try
            {
                const auto slot_count = page.header().slot_count;
                for (slot_id_t slot = 0; slot < slot_count; ++slot)
                {
                    std::vector<uint8_t> payload;
                    if (!page.read(slot, payload))
                        continue;
                    if (payload.empty())
                        continue;
                    fn(payload);
                }
                pm.unpin(page_id, false);
            }
            catch (...)
            {
                pm.unpin(page_id, false);
                throw;
            }
        }

        void refresh_cached_page(PageManager &pm, page_id_t page_id)
        {
            try
            {
                pm.fetch(page_id, true);
                pm.unpin(page_id, false);
            }
            catch (const DBException &)
            {
                // best-effort; ignore failures during refresh
            }
        }
    } // namespace

    CatalogManager::CatalogManager(PageManager &pm, FileManager &fm)
        : pm_(pm), fm_(fm)
    {
        ensure_catalog_pages();
    }

    void CatalogManager::ensure_catalog_pages()
    {
        tables_root_ = pm_.catalog_tables_root();
        columns_root_ = pm_.catalog_columns_root();

        if (tables_root_ < config::FIRST_PAGE_ID)
        {
            tables_root_ = pm_.new_page(PageType::DATA);
            pm_.set_catalog_tables_root(tables_root_);
            pm_.unpin(tables_root_, false);
        }
        if (columns_root_ < config::FIRST_PAGE_ID)
        {
            columns_root_ = pm_.new_page(PageType::DATA);
            pm_.set_catalog_columns_root(columns_root_);
            pm_.unpin(columns_root_, false);
        }
    }

    void CatalogManager::load_tables_cache() const
    {
        if (tables_loaded_)
            return;

        tables_cache_.clear();
        for_each_slot(pm_, tables_root_, [this](const std::vector<uint8_t> &payload) {
            size_t consumed = 0;
            TableCatalogEntry entry = TableCatalogEntry::deserialize(payload.data(), payload.size(), consumed);
            tables_cache_.push_back(std::move(entry));
        });
        tables_loaded_ = true;
    }

    void CatalogManager::reload_tables_cache() const
    {
        tables_loaded_ = false;
        load_tables_cache();
    }

    std::vector<TableCatalogEntry> CatalogManager::read_all_tables() const
    {
        load_tables_cache();
        return tables_cache_;
    }

    std::vector<ColumnCatalogEntry> CatalogManager::read_all_columns() const
    {
        std::vector<ColumnCatalogEntry> result;
        for_each_slot(pm_, columns_root_, [&result](const std::vector<uint8_t> &payload) {
            size_t consumed = 0;
            ColumnCatalogEntry entry = ColumnCatalogEntry::deserialize(payload.data(), payload.size(), consumed);
            result.push_back(std::move(entry));
        });
        std::sort(result.begin(), result.end(), [](const ColumnCatalogEntry &a, const ColumnCatalogEntry &b) {
            if (a.table_id == b.table_id)
                return a.ordinal_position < b.ordinal_position;
            return a.table_id < b.table_id;
        });
        return result;
    }

    std::vector<ColumnCatalogEntry> CatalogManager::read_all_columns(table_id_t table_id) const
    {
        std::vector<ColumnCatalogEntry> result;
        for_each_slot(pm_, columns_root_, [&result, table_id](const std::vector<uint8_t> &payload) {
            size_t consumed = 0;
            ColumnCatalogEntry entry = ColumnCatalogEntry::deserialize(payload.data(), payload.size(), consumed);
            if (entry.table_id == table_id)
            {
                result.push_back(std::move(entry));
            }
        });
        std::sort(result.begin(), result.end(), [](const ColumnCatalogEntry &a, const ColumnCatalogEntry &b) {
            return a.ordinal_position < b.ordinal_position;
        });
        return result;
    }

    bool CatalogManager::table_exists(std::string_view name) const
    {
        load_tables_cache();
        return std::any_of(tables_cache_.begin(), tables_cache_.end(), [&](const TableCatalogEntry &entry) {
            return entry.name == name;
        });
    }

    std::optional<TableCatalogEntry> CatalogManager::get_table(std::string_view name) const
    {
        load_tables_cache();
        auto it = std::find_if(tables_cache_.begin(), tables_cache_.end(), [&](const TableCatalogEntry &entry) {
            return entry.name == name;
        });
        if (it == tables_cache_.end())
            return std::nullopt;
        return *it;
    }

    std::optional<TableCatalogEntry> CatalogManager::get_table(table_id_t id) const
    {
        load_tables_cache();
        auto it = std::find_if(tables_cache_.begin(), tables_cache_.end(), [&](const TableCatalogEntry &entry) {
            return entry.table_id == id;
        });
        if (it == tables_cache_.end())
            return std::nullopt;
        return *it;
    }

    std::vector<TableCatalogEntry> CatalogManager::list_tables() const
    {
        return read_all_tables();
    }

    std::vector<ColumnCatalogEntry> CatalogManager::get_columns(table_id_t table_id) const
    {
        return read_all_columns(table_id);
    }

    void CatalogManager::persist_table_entry(const TableCatalogEntry &entry)
    {
        auto data = entry.serialize();
        auto &page = pm_.fetch(tables_root_, true);
        try
        {
            slot_id_t slot{};
            if (!page.insert(data.data(), static_cast<uint16_t>(data.size()), slot))
            {
                pm_.unpin(tables_root_, false);
                KIZUNA_THROW_STORAGE(StatusCode::PAGE_FULL, "Catalog table page full", std::to_string(tables_root_));
            }
            pm_.unpin(tables_root_, true);
        }
        catch (...)
        {
            pm_.unpin(tables_root_, false);
            throw;
        }
    }

    void CatalogManager::persist_column_entry(const ColumnCatalogEntry &entry)
    {
        auto data = entry.serialize();
        auto &page = pm_.fetch(columns_root_, true);
        try
        {
            slot_id_t slot{};
            if (!page.insert(data.data(), static_cast<uint16_t>(data.size()), slot))
            {
                pm_.unpin(columns_root_, false);
                KIZUNA_THROW_STORAGE(StatusCode::PAGE_FULL, "Catalog column page full", std::to_string(columns_root_));
            }
            pm_.unpin(columns_root_, true);
        }
        catch (...)
        {
            pm_.unpin(columns_root_, false);
            throw;
        }
    }

    void CatalogManager::rewrite_tables_page(const std::vector<TableCatalogEntry> &entries)
    {
        Page page;
        page.init(PageType::DATA, tables_root_);
        for (const auto &entry : entries)
        {
            auto data = entry.serialize();
            slot_id_t slot{};
            if (!page.insert(data.data(), static_cast<uint16_t>(data.size()), slot))
            {
                KIZUNA_THROW_STORAGE(StatusCode::PAGE_FULL, "Catalog table page full", std::to_string(tables_root_));
            }
        }
        fm_.write_page(tables_root_, page.data());
        refresh_cached_page(pm_, tables_root_);
    }

    void CatalogManager::rewrite_columns_page(const std::vector<ColumnCatalogEntry> &entries)
    {
        Page page;
        page.init(PageType::DATA, columns_root_);
        for (const auto &entry : entries)
        {
            auto data = entry.serialize();
            slot_id_t slot{};
            if (!page.insert(data.data(), static_cast<uint16_t>(data.size()), slot))
            {
                KIZUNA_THROW_STORAGE(StatusCode::PAGE_FULL, "Catalog column page full", std::to_string(columns_root_));
            }
        }
        fm_.write_page(columns_root_, page.data());
        refresh_cached_page(pm_, columns_root_);
    }

    TableCatalogEntry CatalogManager::create_table(TableDef def, page_id_t root_page_id, const std::string &create_sql)
    {
        ensure_catalog_pages();
        load_tables_cache();

        if (table_exists(def.name))
        {
            throw QueryException::table_exists(def.name);
        }

        table_id_t new_id = pm_.next_table_id();
        pm_.set_next_table_id(new_id + 1);

        def.id = new_id;
        TableCatalogEntry table_entry = TableCatalogEntry::from_table_def(def, root_page_id, create_sql);
        persist_table_entry(table_entry);
        if (tables_loaded_)
        {
            tables_cache_.push_back(table_entry);
        }

        for (std::size_t i = 0; i < def.columns.size(); ++i)
        {
            ColumnCatalogEntry col_entry;
            col_entry.table_id = new_id;
            col_entry.column_id = static_cast<column_id_t>(i + 1);
            col_entry.ordinal_position = static_cast<uint32_t>(i);
            col_entry.column = def.columns[i];
            col_entry.column.id = col_entry.column_id;
            persist_column_entry(col_entry);
        }

        return table_entry;
    }

    bool CatalogManager::drop_table(std::string_view name, bool cascade)
    {
        (void)cascade; // no dependent objects yet
        load_tables_cache();
        auto it = std::find_if(tables_cache_.begin(), tables_cache_.end(), [&](const TableCatalogEntry &entry) {
            return entry.name == name;
        });
        if (it == tables_cache_.end())
        {
            return false;
        }

        TableCatalogEntry removed = *it;
        tables_cache_.erase(it);
        rewrite_tables_page(tables_cache_);
        tables_loaded_ = true;

        auto all_columns = read_all_columns();
        std::vector<ColumnCatalogEntry> filtered;
        filtered.reserve(all_columns.size());
        for (auto &entry : all_columns)
        {
            if (entry.table_id != removed.table_id)
            {
                filtered.push_back(std::move(entry));
            }
        }
        rewrite_columns_page(filtered);
        return true;
    }
}







