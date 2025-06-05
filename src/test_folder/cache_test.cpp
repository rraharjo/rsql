#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "cache.h"
BOOST_AUTO_TEST_CASE(LRU_set_cache_put_test)
{
    rsql::LRUSetCache<int, std::string> lru_cache(10);
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

BOOST_AUTO_TEST_CASE(LRU_set_cache_full_put_test)
{
    rsql::LRUSetCache<int, std::string> lru_cache(3);
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

BOOST_AUTO_TEST_CASE(LRU_set_cache_put_test_same_value)
{
    rsql::LRUSetCache<int, std::string> lru_cache(3);
    std::optional<std::string> evicted_1 = lru_cache.put(0, "Cache");
    std::optional<std::string> evicted_2 = lru_cache.put(15, "Baileys the original Irish Cream");
    std::optional<std::string> evicted_3 = lru_cache.put(20, "Ignasius Djaynurdin");
    std::optional<std::string> evicted_4 = lru_cache.put(50, "Coca-Cola");
    std::optional<std::string> evicted_5 = lru_cache.put(15, "Baileys the original Irish Cream");

    BOOST_CHECK(evicted_1 == std::nullopt);
    BOOST_CHECK(evicted_2 == std::nullopt);
    BOOST_CHECK(evicted_3 == std::nullopt);
    BOOST_CHECK(evicted_4.value() == "Cache");
    BOOST_CHECK(evicted_5 == std::nullopt);
    BOOST_CHECK(lru_cache.get_size() == 3);
    BOOST_CHECK(lru_cache.full() == true);
    BOOST_CHECK(lru_cache.empty() == false);
}

BOOST_AUTO_TEST_CASE(LRU_set_cache_get_test)
{
    rsql::LRUSetCache<int, std::string> lru_cache(10);
    lru_cache.put(0, "Cache");
    lru_cache.put(15, "Baileys, the original Irish Cream");
    lru_cache.put(15, "Ignasius Djaynurdin");

    std::optional<std::string> val_1 = lru_cache.get(15);
    std::optional<std::string> val_2 = lru_cache.get(0);
    std::optional<std::string> val_3 = lru_cache.get(10);

    BOOST_CHECK(val_1.value() == "Ignasius Djaynurdin");
    BOOST_CHECK(val_2.value() == "Cache");
    BOOST_CHECK(val_3 == std::nullopt);
    BOOST_CHECK(lru_cache.get_size() == 2);
    BOOST_CHECK(lru_cache.full() == false);
    BOOST_CHECK(lru_cache.empty() == false);
}

BOOST_AUTO_TEST_CASE(LRU_set_cache_evict_test)
{
    rsql::LRUSetCache<int, std::string> lru_cache(10);
    lru_cache.put(0, "Cache");
    lru_cache.put(15, "Baileys, the original Irish Cream");
    lru_cache.put(25, "Ignasius Djaynurdin");

    std::string top_1 = lru_cache.evict();
    std::string top_2 = lru_cache.evict();

    BOOST_CHECK(top_1 == "Cache");
    BOOST_CHECK(top_2 == "Baileys, the original Irish Cream");
    BOOST_CHECK(lru_cache.get_size() == 1);
    BOOST_CHECK(lru_cache.full() == false);
    BOOST_CHECK(lru_cache.empty() == false);
}

BOOST_AUTO_TEST_CASE(LRU_set_cache_evict_put_order_test)
{
    rsql::LRUSetCache<int, std::string> lru_cache(10);
    lru_cache.put(0, "Cache");
    lru_cache.put(15, "Baileys, the original Irish Cream");
    lru_cache.put(25, "Ignasius Djaynurdin");
    lru_cache.put(0, "Cache");

    std::string top_1 = lru_cache.evict();
    std::string top_2 = lru_cache.evict();

    BOOST_CHECK(top_1 == "Baileys, the original Irish Cream");
    BOOST_CHECK(top_2 == "Ignasius Djaynurdin");
    BOOST_CHECK(lru_cache.get_size() == 1);
    BOOST_CHECK(lru_cache.full() == false);
    BOOST_CHECK(lru_cache.empty() == false);
}

BOOST_AUTO_TEST_CASE(LRU_set_cache_evict_get_order_test)
{
    rsql::LRUSetCache<int, std::string> lru_cache(10);
    lru_cache.put(0, "Cache");
    lru_cache.put(15, "Baileys, the original Irish Cream");
    lru_cache.put(25, "Ignasius Djaynurdin");
    lru_cache.get(0);

    std::string top_1 = lru_cache.evict();
    std::string top_2 = lru_cache.evict();

    BOOST_CHECK(top_1 == "Baileys, the original Irish Cream");
    BOOST_CHECK(top_2 == "Ignasius Djaynurdin");
    BOOST_CHECK(lru_cache.get_size() == 1);
    BOOST_CHECK(lru_cache.full() == false);
    BOOST_CHECK(lru_cache.empty() == false);
}

BOOST_AUTO_TEST_CASE(LRU_LL_cache_put_test)
{
    rsql::LRULinkedListCache<int, std::string> lru_cache(10);
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

BOOST_AUTO_TEST_CASE(LRU_LL_cache_full_put_test)
{
    rsql::LRULinkedListCache<int, std::string> lru_cache(3);
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

BOOST_AUTO_TEST_CASE(LRU_LL_cache_put_test_same_value)
{
    rsql::LRULinkedListCache<int, std::string> lru_cache(3);
    std::optional<std::string> evicted_1 = lru_cache.put(0, "Cache");
    std::optional<std::string> evicted_2 = lru_cache.put(15, "Baileys the original Irish Cream");
    std::optional<std::string> evicted_3 = lru_cache.put(20, "Ignasius Djaynurdin");
    std::optional<std::string> evicted_4 = lru_cache.put(50, "Coca-Cola");
    std::optional<std::string> evicted_5 = lru_cache.put(15, "Baileys the original Irish Cream");

    BOOST_CHECK(evicted_1 == std::nullopt);
    BOOST_CHECK(evicted_2 == std::nullopt);
    BOOST_CHECK(evicted_3 == std::nullopt);
    BOOST_CHECK(evicted_4.value() == "Cache");
    BOOST_CHECK(evicted_5 == std::nullopt);
    BOOST_CHECK(lru_cache.get_size() == 3);
    BOOST_CHECK(lru_cache.full() == true);
    BOOST_CHECK(lru_cache.empty() == false);
}

BOOST_AUTO_TEST_CASE(LRU_LL_cache_get_test)
{
    rsql::LRULinkedListCache<int, std::string> lru_cache(10);
    lru_cache.put(0, "Cache");
    lru_cache.put(15, "Baileys, the original Irish Cream");
    lru_cache.put(15, "Ignasius Djaynurdin");

    std::optional<std::string> val_1 = lru_cache.get(15);
    std::optional<std::string> val_2 = lru_cache.get(0);
    std::optional<std::string> val_3 = lru_cache.get(10);

    BOOST_CHECK(val_1.value() == "Ignasius Djaynurdin");
    BOOST_CHECK(val_2.value() == "Cache");
    BOOST_CHECK(val_3 == std::nullopt);
    BOOST_CHECK(lru_cache.get_size() == 2);
    BOOST_CHECK(lru_cache.full() == false);
    BOOST_CHECK(lru_cache.empty() == false);
}

BOOST_AUTO_TEST_CASE(LRU_LL_cache_evict_test)
{
    rsql::LRULinkedListCache<int, std::string> lru_cache(10);
    lru_cache.put(0, "Cache");
    lru_cache.put(15, "Baileys, the original Irish Cream");
    lru_cache.put(25, "Ignasius Djaynurdin");

    std::string top_1 = lru_cache.evict();
    std::string top_2 = lru_cache.evict();

    BOOST_CHECK(top_1 == "Cache");
    BOOST_CHECK(top_2 == "Baileys, the original Irish Cream");
    BOOST_CHECK(lru_cache.get_size() == 1);
    BOOST_CHECK(lru_cache.full() == false);
    BOOST_CHECK(lru_cache.empty() == false);
}

BOOST_AUTO_TEST_CASE(LRU_LL_cache_evict_put_order_test)
{
    rsql::LRULinkedListCache<int, std::string> lru_cache(10);
    lru_cache.put(0, "Cache");
    lru_cache.put(15, "Baileys, the original Irish Cream");
    lru_cache.put(25, "Ignasius Djaynurdin");
    lru_cache.put(0, "Cache");

    std::string top_1 = lru_cache.evict();
    std::string top_2 = lru_cache.evict();

    BOOST_CHECK(top_1 == "Baileys, the original Irish Cream");
    BOOST_CHECK(top_2 == "Ignasius Djaynurdin");
    BOOST_CHECK(lru_cache.get_size() == 1);
    BOOST_CHECK(lru_cache.full() == false);
    BOOST_CHECK(lru_cache.empty() == false);
}

BOOST_AUTO_TEST_CASE(LRU_LL_cache_evict_get_order_test)
{
    rsql::LRULinkedListCache<int, std::string> lru_cache(10);
    lru_cache.put(0, "Cache");
    lru_cache.put(15, "Baileys, the original Irish Cream");
    lru_cache.put(25, "Ignasius Djaynurdin");
    lru_cache.get(0);

    std::string top_1 = lru_cache.evict();
    std::string top_2 = lru_cache.evict();

    BOOST_CHECK(top_1 == "Baileys, the original Irish Cream");
    BOOST_CHECK(top_2 == "Ignasius Djaynurdin");
    BOOST_CHECK(lru_cache.get_size() == 1);
    BOOST_CHECK(lru_cache.full() == false);
    BOOST_CHECK(lru_cache.empty() == false);
}