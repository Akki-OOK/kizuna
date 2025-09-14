// src/main.cpp
#include <iostream>
#include "src/common/types.h"

int main()
{
    std::cout << "kizuna Starting..." << std::endl;
    std::cout << "Page size: " << kizuna::PAGE_SIZE << " bytes" << std::endl;
    std::cout << "Max cache size: " << kizuna::DEFAULT_CACHE_SIZE << " pages" << std::endl;

    // Test that our types work
    kizuna::page_id_t first_page = kizuna::FIRST_PAGE_ID;
    std::cout << "First page ID: " << first_page << std::endl;

    return 0;
}