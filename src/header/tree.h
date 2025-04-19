#ifndef BTREE_H
#define BTREE_H
#include "node.h"
#include <iostream>
#include <unistd.h>
#include <fcntl.h>
// Can be changed to any number >= 2
#define DEGREE 2
#define STARTING_BUFFER_SZ 2048
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

        BNode *root;
        Table *table;
        size_t t;
        size_t width;

        void get_root_node();

    public:
        static BTree *read_disk(Table *table = nullptr);
        BTree();
        ~BTree();

        /**
         * @brief find the first row of which key equals to the argument key. Will do comparison based on the first column
         *
         * @param key key of the row
         * @return char * if found, returns a copy of the element, dynamically allocated. Otherwise nullptr is returned.
         */
        char *find_row(const char *key);
        /**
         * @brief find all the rows of which value at column col_idx equals to the argument key. If col_idx is not 0 (not indexed), a linear search will be performed
         *
         * @param key
         * @param col_idx column index
         * @return std::vector<char *> a vector constisting copies of the found elements, dynamically allocated
         */
        std::vector<char *> find_all_row(const char *key, size_t col_idx);
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
        std::vector<char *> delete_all(const char *key, size_t col_idx);
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
        void set_table(Table *table);
        std::string get_path() const;
        void write_disk();
        friend class BNode;
    };
}
#endif