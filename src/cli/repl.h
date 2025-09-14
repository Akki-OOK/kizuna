#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/types.h"
#include "common/config.h"
#include "common/logger.h"
#include "common/exception.h"
#include "storage/file_manager.h"
#include "storage/page_manager.h"
#include "storage/record.h"

namespace kizuna
{
    class Repl
    {
    public:
        Repl();
        int run();

    private:
        std::unique_ptr<FileManager> fm_;
        std::unique_ptr<PageManager> pm_;
        std::string db_path_;

        using Handler = std::function<void(const std::vector<std::string>&)>;
        std::unordered_map<std::string, Handler> handlers_;

        void init_handlers();
        void print_help() const;
        static std::vector<std::string> tokenize(const std::string &line);
        bool ensure_db_open() const;
        bool ensure_valid_data_page(page_id_t id, bool must_exist) const;

        // Command handlers
        void cmd_open(const std::vector<std::string> &args);
        void cmd_status(const std::vector<std::string> &args);
        void cmd_newpage(const std::vector<std::string> &args);
        void cmd_write_demo(const std::vector<std::string> &args);
        void cmd_read_demo(const std::vector<std::string> &args);
        void cmd_loglevel(const std::vector<std::string> &args);
        void cmd_freepage(const std::vector<std::string> &args);
    };
}
