#ifndef BTREE_H
#define BTREE_H
#include "node.h"
#include "comparison.h"
#include <iostream>
#include <unistd.h>
#include <fcntl.h>
// Can be changed to any number >= 2
#define DEGREE 2
#define DISK_BUFFER_SZ 4096
#define TREE_FILE "tree.rsql"
namespace rsql
{
    class BNode;
    class Table;
    class BTree
    {
    public:
        std::vector<Column> columns;
        uint32_t root_num;
        uint32_t max_node_num;
        uint32_t max_col_id;
        uint32_t tree_num;
        bool unique_key;

        BNode *root;
        const Table *table;
        const size_t t;
        size_t width;
        BTree(Table *table = nullptr);
        void get_root_node();

    public:
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
        static BTree *create_new_tree(Table *table = nullptr, const uint32_t tree_num = 0, bool unique_key = true);
        ~BTree();

        /**
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
         * @param key null by default. This function does not take ownership of this pointer
         * @param symbol equal by default. Specify which key is valid. e.g. GEQ means takes all row of which key is greater or equal than the key argument
         * @param comparison null by default. This function does not take ownership of this pointer
         * @return std::vector<char *> each item is dynamically allocated. Ownership of pointers goes to the caller.
         */
        std::vector<char *> search_rows(const char *key = nullptr, CompSymbol symbol = CompSymbol::EQ, Comparison *comparison = nullptr);
        /**
         * @brief insert src to the tree. Only n bytes of src will be inserted, where n is the width of the tree
         *
         * @param src pointer that contains the row information
         */
        void insert_row(const char *src);
        /**
         * @brief Delete the first occurence the row of which key equal to the argument key. Only compare the first column.
         *
         * @param key
         * @return char * the deleted element, dynamically allocated
         */
        char *delete_row(const char *key);
        /**
         * @brief Delete all occurences of which value at column indexed col_idx equals to the argument key
         *
         * @param key
         * @param col_idx column index. If this value is 0 (indexed), a btree search is performed, otherwise linear search is performed
         * @return std::vector<char *> the deleted elements, each element is dynamically allocated
         */
        std::vector<char *> delete_all(const char *key, const size_t col_idx);
        /**
         * @brief Delete all occurences of all keys. Keys have to be the first column
         *
         * @param keys a collection of the keys
         * @return std::vector<char *> the deleted elements, each element is dynamically allocated
         */
        std::vector<char *> batch_delete(const std::vector<char *> &keys);
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
        std::string get_path() const;
        void write_disk();
        friend class BNode;
    };
}
#endif