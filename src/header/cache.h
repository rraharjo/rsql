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
    class LRUSetCache : public Cache<K, V>
    {
    private:
        struct LRUStruct
        {
            V val;
            size_t count;
        };
        std::unordered_map<K, LRUStruct> cache;

    public:
        LRUSetCache(const size_t capacity);
        ~LRUSetCache();
        std::optional<V> get(K key) override;
        std::optional<V> put(K key, V value) override;
        V evict() override;
    };

    template <typename K, typename V>
    class LRULinkedListCache : public Cache<K, V>
    {
    private:
        struct LRULinkedList
        {
            LRULinkedList *next;
            LRULinkedList *prev;
            K key;
            V val;
        };
        LRULinkedList *head;
        LRULinkedList *tail;
        std::unordered_map<K, LRULinkedList *> cache;

        void insert_front_ll(LRULinkedList *node);
        void remove_ll(LRULinkedList *node);

    public:
        LRULinkedListCache(const size_t capacity);
        ~LRULinkedListCache();
        std::optional<V> get(K key) override;
        std::optional<V> put(K key, V value) override;
        V evict() override;
    };

    // Cache (parent)
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

    // LRU Set Cache
    template <typename K, typename V>
    LRUSetCache<K, V>::LRUSetCache(const size_t capacity) : Cache<K, V>(capacity)
    {
    }

    template <typename K, typename V>
    LRUSetCache<K, V>::~LRUSetCache()
    {
    }

    template <typename K, typename V>
    std::optional<V> LRUSetCache<K, V>::get(K key)
    {
        auto val_it = this->cache.find(key);
        if (val_it == this->cache.end())
            return std::nullopt;
        this->cache[key].count = this->cur_count++;
        return std::optional<V>(val_it->second.val);
    }

    template <typename K, typename V>
    std::optional<V> LRUSetCache<K, V>::put(K key, V val)
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
    V LRUSetCache<K, V>::evict()
    {
        if (this->cur_size == 0)
            throw std::runtime_error("Can't evict - cache is empty");
        size_t min_count = this->cur_count;
        std::optional<K> evict_key = std::nullopt;
        for (const auto &pair : this->cache)
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

    // LinkedList LRU
    template <typename K, typename V>
    LRULinkedListCache<K, V>::LRULinkedListCache(const size_t capacity) : Cache<K, V>(capacity)
    {
        this->head = new LRULinkedList();
        this->tail = new LRULinkedList();
        head->next = tail;
        head->prev = nullptr;
        tail->next = nullptr;
        tail->prev = head;
    }

    template <typename K, typename V>
    LRULinkedListCache<K, V>::~LRULinkedListCache()
    {
        while (!this->empty())
            this->evict();
        delete this->head;
        delete this->tail;
    }

    template <typename K, typename V>
    void LRULinkedListCache<K, V>::insert_front_ll(LRULinkedListCache::LRULinkedList *node)
    {
        LRULinkedListCache::LRULinkedList *next_next = this->head->next;
        this->head->next = node;
        node->prev = this->head;
        next_next->prev = node;
        node->next = next_next;
    }

    template <typename K, typename V>
    void LRULinkedListCache<K, V>::remove_ll(LRULinkedListCache::LRULinkedList *node)
    {
        LRULinkedListCache::LRULinkedList *prev = node->prev;
        LRULinkedListCache::LRULinkedList *next = node->next;
        prev->next = next;
        next->prev = prev;
    }

    template <typename K, typename V>
    std::optional<V> LRULinkedListCache<K, V>::get(K key)
    {
        auto node_it = this->cache.find(key);
        if (node_it == this->cache.end())
            return std::nullopt;
        LRULinkedListCache::LRULinkedList *target_node = node_it->second;
        this->remove_ll(target_node);
        this->insert_front_ll(target_node);
        return std::make_optional<V>(target_node->val);
    }

    template <typename K, typename V>
    std::optional<V> LRULinkedListCache<K, V>::put(K key, V val)
    {
        auto node_it = this->cache.find(key);
        std::optional<V> to_ret = std::nullopt;
        if (node_it == this->cache.end())
        {
            if (this->full())
                to_ret = std::make_optional<V>(this->evict());
            LRULinkedListCache::LRULinkedList *new_node = new LRULinkedListCache::LRULinkedList();
            new_node->val = val;
            new_node->key = key;
            this->insert_front_ll(new_node);
            this->cache.insert({key, new_node});
            this->cur_size++;
        }
        else
        {
            LRULinkedListCache::LRULinkedList *old_node = node_it->second;
            to_ret = std::make_optional<V>(old_node->val);
            old_node->val = val;
            this->remove_ll(old_node);
            this->insert_front_ll(old_node);
        }
        return to_ret;
    }
    template <typename K, typename V>
    V LRULinkedListCache<K, V>::evict()
    {
        if (this->empty())
            throw std::runtime_error("Cache is empty");
        LRULinkedListCache::LRULinkedList *to_evict = this->tail->prev;
        this->remove_ll(to_evict);
        this->cache.erase(to_evict->key);
        V to_ret = to_evict->val;
        this->cur_size--;
        delete to_evict;
        return to_ret;
    }
}
#endif