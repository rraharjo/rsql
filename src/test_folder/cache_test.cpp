#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "cache.h"
BOOST_AUTO_TEST_CASE(LRU_cache_put_test)
{
    rsql::LRUCache<int, std::string> lru_cache(10);
    std::optional<std::string> evicted_1 = lru_cache.put(0, "Cache");
    std::optional<std::string> evicted_2 = lru_cache.put(15, "Baileys, the original Irish Cream");
    std::optional<std::string> evicted_3 = lru_cache.put(15, "Ignasius Djaynurdin");

    BOOST_CHECK(evicted_1 == std::nullopt);
    BOOST_CHECK(evicted_2 == std::nullopt);
    BOOST_CHECK(evicted_3.value() == "Baileys, the original Irish Cream");
    BOOST_CHECK(lru_cache.get_size() == 2);
    BOOST_CHECK(lru_cache.full() == false);
    BOOST_CHECK(lru_cache.empty() == false);
}


BOOST_AUTO_TEST_CASE(LRU_cache_full_put_test)
{
    rsql::LRUCache<int, std::string> lru_cache(3);
    std::optional<std::string> evicted_1 = lru_cache.put(0, "Cache");
    std::optional<std::string> evicted_2 = lru_cache.put(15, "Baileys the original Irish Cream");
    std::optional<std::string> evicted_3 = lru_cache.put(20, "Ignasius Djaynurdin");
    std::optional<std::string> evicted_4 = lru_cache.put(50, "Coca-Cola");

    BOOST_CHECK(evicted_1 == std::nullopt);
    BOOST_CHECK(evicted_2 == std::nullopt);
    BOOST_CHECK(evicted_3 == std::nullopt);
    BOOST_CHECK(evicted_4.value() == "Cache");
    BOOST_CHECK(lru_cache.get_size() == 3);
    BOOST_CHECK(lru_cache.full() == true);
    BOOST_CHECK(lru_cache.empty() == false);
}