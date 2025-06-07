#ifndef BTREE_H
#define BTREE_H
#include "node.h"
#include "comparison.h"
#include "cache.h"
#include <iostream>
#include <unistd.h>
#include <fcntl.h>
// Modifiable values

// Can be changed to any number >= 2
#define DEGREE 128
// can be changed to any number >= 1
#define NODE_CACHE_SIZE 32
#define BTREE_CACHE rsql::LRUSetCache<uint32_t, rsql::BNode *>
// #define BTREE_CACHE rsql::LRULinkedListCache<uint32_t, rsql::BNode *>
#define DISK_BUFFER_SZ 4096

// Can't be changed
#define TREE_FILE "tree.rsql"
namespace rsql
{
    typedef std::pair<BNode *, BNode *> NodePair;
    class Table;
    class BTree
    {
    public:
        /**
         * @brief Create a new tree object
         *
         * @param table the table of which the new tree belongs
         * @param tree_num the new tree number
         * @param unique_key Whether the key on the new tree would be unique
         * @return BTree*
         */
        static BTree *create_new_tree(Table *table = nullptr, const uint32_t tree_num = 0, bool unique_key = true);
        /**
         * @brief load a tree.rsql file to the disk
         *
         * @throw std::runtime_error if there's an error during reading file
         * @throw std::invalid_argument if file of the tree_num doesn't exist
         * @param table
         * @param tree_num
         * @return BTree*
         */
        static BTree *read_disk(Table *table = nullptr, const uint32_t tree_num = 0);

    private:
        // written information
        std::vector<Column> columns;
        uint32_t root_num;
        uint32_t max_node_num;
        uint32_t max_col_id;
        uint32_t tree_num;
        bool unique_key;
        // not written
        const Table *table;
        const size_t t;
        size_t width;
        Cache<uint32_t, BNode *> *node_cache;
        std::vector<BNode *> eviction_notice;

    private:
        BTree(bool unique, Table *table = nullptr);
        /**
         * @brief Get the node object either from cache or disk.
         * Either way, the new node is updated in the cache.
         * pair.first is the requested object while pair.second is the evicted object.
         * Evicted object is caller's responsibility
         *
         * @param node_num
         * @return NodePair - pair.first is the requested node, pair.second is the evicted node or nullptr
         */
        NodePair get_node(const uint32_t node_num);
        inline BNode *get_root()
        {
            NodePair pair = this->get_node(this->root_num);
            if (pair.second)
                delete pair.second;
            return pair.first;
        };

    public:
        ~BTree();
        inline const std::vector<Column> &get_columns() const { return this->columns; }
        inline const uint32_t get_root_num() const { return this->root_num; }
        inline const uint32_t get_max_node_num() const { return this->max_node_num; }
        inline const uint32_t get_max_col_id() const { return this->max_col_id; }
        inline const uint32_t get_tree_number() const { return this->tree_num; }
        inline const size_t get_degree() const { return this->t; }
        inline const size_t get_width() const { return this->width; }
        /**
         * @brief insert src to the tree. Only n bytes of src will be inserted, where n is the width of the tree
         *
         * @param src pointer that contains the row information
         */
        void insert_row(const char *src);
        /**
         * @deprecated
         * @brief find all the rows of which value at column col_idx equals to the argument key. If col_idx is not 0 (not indexed), a linear search will be performed
         *
         * @param key
         * @param col_idx column index
         * @return std::vector<char *> a vector constisting copies of the found elements, dynamically allocated
         */
        std::vector<char *> find_all_row(const char *search_key, const size_t col_idx);
        /**
         * @brief Search all rows that satisfy both key and comparison. if both arguments are null, then all rows will always satisfy the condition
         *
         * @param key null by default - if this is null, a linear search will be performed. This function does not take ownership of this pointer
         * @param symbol equal by default. Specify which key is valid. e.g. GEQ means takes all row of which key is greater or equal than the key argument
         * @param comparison null by default. This function does not take ownership of this pointer
         * @return std::vector<char *> each item is dynamically allocated. Ownership of pointers goes to the caller.
         */
        std::vector<char *> search_rows(const char *key = nullptr, CompSymbol symbol = CompSymbol::EQ, Comparison *comparison = nullptr);
        /**
         * @brief Delete the first occurence the row of which key equal to the argument key. Only compare the first column.
         *
         * @param key
         * @param comp
         * @return char * the deleted element, dynamically allocated
         */
        char *delete_row(const char *key, Comparison *comp = nullptr);
        /**
         * @brief Delete all rows with matching key and comparison. If key is null, a linear search will be performed
         *
         * @param key
         * @param comparison
         * @return std::vector<char *>
         */
        std::vector<char *> delete_all_row(const char *key = nullptr, CompSymbol symbol = CompSymbol::EQ, Comparison *comparison = nullptr);
        /**
         * @brief add a column at the end
         *
         * @param c
         */
        void add_column(const Column c);
        /**
         * @brief remove a column at index idx
         *
         * @param idx
         */
        void remove_column(const size_t idx);
        /**
         * @brief Return the path directory of which this tree is stored at (does not include the tree file, just the directory)
         *
         * @return std::string
         */
        std::string get_path() const;
        /**
         * @brief Write the content of this tree to the disk. File location can be obtained by calling get_path()
         *
         */
        void write_disk();
        /**
         * @brief Delete the file that represents this tree, as well as destroying all its node. Will also unload this object from memory (call delete on self)
         *
         */
        void destroy();
        friend class BNode;
        friend class Table;
    };
}
#endif