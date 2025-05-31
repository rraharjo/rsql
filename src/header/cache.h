#ifndef RSQL_CACHE_H
#define RSQL_CACHE_H
#include <optional>
#include <unordered_map>
#include <chrono>
#include <stdexcept>
namespace rsql
{
    template <typename K, typename V>
    class Cache
    {
    protected:
        size_t cur_count = 0;
        size_t cur_size = 0;
        const size_t capacity;

    public:
        Cache(const size_t capacity);
        virtual ~Cache();
        virtual std::optional<V> get(K key) = 0;
        virtual std::optional<V> put(K key, V value) = 0;
        virtual V evict() = 0;

        inline bool empty() const { return this->cur_size == 0; }
        inline bool full() const { return this->cur_size == this->capacity; }
        inline size_t get_size() const { return this->cur_size; }
    };

    template <typename K, typename V>
    class LRUCache : public Cache<K, V>
    {
    private:
        struct LRUStruct
        {
            V val;
            size_t count;
        };
        std::unordered_map<K, LRUStruct> cache;

    public:
        LRUCache(const size_t capacity);
        ~LRUCache();
        std::optional<V> get(K key) override;
        std::optional<V> put(K key, V value) override;
        V evict() override;
    };

    // Implementation
    template <typename K, typename V>
    Cache<K, V>::Cache(const size_t capacity) : capacity(capacity)
    {
        if (this->capacity == 0)
            throw std::invalid_argument("Can't create a cache of size 0");
    }

    template <typename K, typename V>
    Cache<K, V>::~Cache()
    {
    }

    template <typename K, typename V>
    LRUCache<K, V>::LRUCache(const size_t capacity) : Cache<K, V>(capacity)
    {
    }

    template <typename K, typename V>
    LRUCache<K, V>::~LRUCache()
    {
    }

    template <typename K, typename V>
    std::optional<V> LRUCache<K, V>::get(K key)
    {
        auto val_it = this->cache.find(key);
        if (val_it == this->cache.end())
            return std::nullopt;
        this->cache[key].count = this->cur_count++;
        return std::optional<V>(val_it->second.val);
    }

    template <typename K, typename V>
    std::optional<V> LRUCache<K, V>::put(K key, V val)
    {
        auto val_it = this->cache.find(key);
        std::optional<V> to_ret = std::nullopt;
        if (val_it == this->cache.end())
        {
            if (this->full())
                to_ret = std::make_optional<V>(this->evict());
            this->cache.insert({key, {val, this->cur_count++}});
            this->cur_size++;
        }
        else
        {
            to_ret = std::make_optional<V>(this->cache[key].val);
            this->cache[key] = {val, this->cur_count++};
        }
        return to_ret;
    }
    template <typename K, typename V>
    V LRUCache<K, V>::evict()
    {
        if (this->cur_size == 0)
            throw std::runtime_error("Can't evict - cache is empty");
        size_t min_count = this->cur_count;
        std::optional<K> evict_key = std::nullopt;
        for (auto &pair : this->cache)
            if (pair.second.count < min_count)
            {
                min_count = pair.second.count;
                evict_key = std::make_optional<K>(pair.first);
            }
        V to_ret = this->cache[evict_key.value()].val;
        this->cache.erase(evict_key.value());
        this->cur_size--;
        return to_ret;
    }
}
#endif