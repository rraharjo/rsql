#ifndef BNODE_H
#define BNODE_H
#include <regex>
#include <vector>
#include <cstring>
#include <cstdint>
#include <fcntl.h>
#include <unistd.h>
#include <memory>
#include "column.h"
#include "comparison.h"

namespace rsql
{
    class BTree;
    class BNode
    {
    public:
        /**
         * @brief
         * Read file_name file and store all the data inside keys
         * @param cols column format
         * @param file_name
         * @return BNode*
         */
        static BNode *read_disk(BTree *tree, const std::string file_name);
        /**
         * @brief Get the file name of the node with the stated node number
         *
         * @param node_num
         * @return std::string
         */
        inline static const std::string get_file_name(const uint32_t node_num)
        {
            return "node_" + std::to_string(node_num) + ".rsql";
        }

    private:
        // written information
        uint32_t node_num;
        std::vector<Column> columns;
        uint32_t size;
        std::vector<char *> keys;
        std::vector<uint32_t> children;
        uint8_t leaf;

        // generated information
        bool changed;
        BTree *tree;

    private:
        /**
         * @brief return the first proper child idx that matches the symbol.
         *
         * @param k
         * @param symbol
         * @return int
         */
        int first_child_idx(const char *k, CompSymbol symbol) const;
        /**
         * @brief return the last proper child idx
         *
         * @param k
         * @param symbol
         * @return int
         */
        int last_child_idx(const char *k, CompSymbol symbol) const;
        /**
         * @brief Compare k byte, where k is the width of column at position col_idx
         *
         * @param k_1
         * @param k_2
         * @param col_idx
         * @param symbol
         * @return true if k_1 symbol k_2 is true
         */
        bool compare_key(const char *k_1, const char *k_2, size_t col_idx, CompSymbol symbol = CompSymbol::EQ) const;
        /**
         * @brief Split the children node c_i, which has to be this children at index idx
         * @warning newly created object is not inserted to the cache
         *
         * @param idx position of the target child
         * @param c_i a pointer to the child node (c_i has to be in index idx)
         * @return a pointer to the newly created node, at position idx + 1
         */
        BNode *split_children(const size_t idx, BNode *c_i);
        /**
         * @brief merge c_j to c_i, where c_i is the children number idx and c_j is children number (idx + 1).
         * BNode::destroy() will be called on c_j
         *
         * @param idx
         * @param c_i
         * @param c_j
         */
        void merge(const size_t idx, BNode *c_i, BNode *c_j);
        /**
         * @brief recuresively perform tree balancing on the left most children and
         * remove the left most key on the left most leaf node, rooted at current node
         *
         * @return char* the left most key of the left most leaf node, rooted at current node
         */
        char *delete_left();
        /**
         * @brief recuresively perform tree balancing on the right most children and
         * remove the right most key on the right most leaf node, rooted at current node
         *
         * @return char* the right most key of the right most leaf node, rooted at current node
         */
        char *delete_right();
        /**
         * @brief case 1 of deleting an item: item is in this node, and this node is a leaf
         *
         * @param key
         */
        char *delete_row_1(const size_t idx);
        /**
         * @brief case 2 of deleting an item: item is in this node, and this node is not a leaf
         *
         * @param key
         */
        char *delete_row_2(const char *key, const size_t idx, Comparison *comparison);
        /**
         * @brief case 3 of deleting an item: item is not in this node
         *
         * @param key
         * @param idx index of the child node
         */
        char *delete_row_3(const char *key, const size_t idx, Comparison *comparison);
        /**
         * @brief delete this node if this node is not a root node
         * @deprecated
         *
         */
        void del_if_not_in_cache();
        /**
         * @brief Delete this node along with the file
         * @warning this object will be removed from the cache and eviction_notice
         *
         */
        void destroy();
        /**
         * @brief Match the column structure of this node to the tree
         *
         */
        void match_columns();
        /**
         * @brief Check the eviction vector. If the eviction has the object, it's put on the cache.
         * Otherwise, it will ask the cache for the object.
         * @warning Calling this function will not guarantee ownership of this object.
         * this object may be placed in the cache or eviction_notice
         *
         * @param node_num
         * @return BNode*
         */
        BNode *get_node(const uint32_t node_num);
        /**
         * @brief Make sure that node is available on the cache, and remove from eviction vector
         *
         * @param node
         */
        void move_to_cache(BNode *node);
        /**
         * @brief remove the node object from the eviction_notice
         *
         * @param node
         * @return true
         * @return false
         */
        bool extract_eviction(BNode *node);
        /**
         * @brief delete all object inside the eviction_notice and clear the vector
         *
         */
        void clear_eviction();

    public:
        BNode(BTree *tree, const uint32_t node_num);
        ~BNode();
        inline size_t get_size() const { return this->size; }
        inline const std::vector<uint32_t> &get_children() const { return this->children; }
        inline const std::vector<char *> &get_keys() const { return this->keys; }
        inline bool is_leaf() const { return this->leaf; }
        inline bool full() const { return this->size == this->keys.capacity(); };
        /**
         * @brief find the first row that matches the key
         *
         * @param key only compares the first k bytes, where k is the width of the first column
         * @return n bytes of char dynamically allocated, where n is the width of the table. If not found it returns nullptr
         */
        char *find(const char *key);
        /**
         * @brief find all row where (key symbol k) is true. eg. if symbol is <, then find all row where key < k
         *
         * @param key the key that is searched for
         * @param alls found rows
         * @param symbol
         */
        void find_all_indexed(const char *k, std::vector<char *> &alls, CompSymbol symbol = CompSymbol::EQ);
        /**
         * @brief find all occurences that match the key by linear search. When a matching row is found, a copy is created
         *
         * @param key the key that is searched for
         * @param col_idx the column index that should match the key
         * @param preceding_size the sum of width of column 0 to (col_idx - 1)
         * @param alls found rows
         */
        void find_all_unindexed(const char *key, const size_t col_idx, const size_t preceding_size, std::vector<char *> &alls);
        void indexed_search(std::vector<char *> &result, const char *const key, const CompSymbol symbol = CompSymbol::EQ, Comparison *extra_condition = nullptr);
        void linear_search(std::vector<char *> &result, Comparison *condition);
        void insert(const char *row);
        char *delete_row(const char *key, Comparison *comp = nullptr);
        void write_disk();
        friend std::ostream &operator<<(std::ostream &stream, const BNode &obj);

        friend class BTree;
    };
}
#endif