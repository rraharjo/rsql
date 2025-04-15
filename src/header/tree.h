#ifndef BTREE_H
#define BTREE_H
#include "node.h"
#include <iostream>
#define DEGREE 2
#define TREE_FILE "tree.rsql"
namespace rsql
{
    class BNode;
    class BTree
    {
    public:
        std::vector<Column> columns;
        uint32_t root_num;
        uint32_t max_node_num;
        uint32_t max_col_id;

        BNode *root;
        size_t t;
        size_t width;

        void get_root_node();

    public:
        static BTree *read_disk();
        BTree();
        ~BTree();

        /**
         * @brief find the first row of which key equals to the argument key
         *
         * @param key key of the row
         * @return char * if found, returns a copy of the element, dynamically allocated. Otherwise nullptr is returned.
         */
        char *find_row(const char *key);
        /**
         * @brief find all the rows of which key equals to the argument key
         *
         * @param key
         * @return std::vector<char *>  a vector constisting copies of the found elements, dynamically allocated
         */
        std::vector<char *> find_all_row(const char *key);

        /**
         * @brief insert src to the tree, length of the src must equal the width of the table
         *
         * @param src pointer that contains the row information
         */
        void insert_row(const char *src);
        /**
         * @brief Delete the first occurence the row of which key equal to the argument key
         *
         * @param key
         * @return char * the deleted element, dynamically allocated
         */
        char *delete_row(const char *key);
        /**
         * @brief Delete all occurences of which keys match the key argument
         * 
         * @param key 
         * @return std::vector<char *> the deleted elements, each element is dynamically allocated
         */
        std::vector<char *> delete_all(const char *key);
        /**
         * @brief Delete all occurences of all keys
         * 
         * @param keys 
         * @return std::vector<char *> the deleted elements, each element is dynamically allocated
         */
        std::vector<char *> batch_delete(const std::vector<const char *> &keys);
        void add_column(const Column c);
        void remove_column(const size_t idx);
        void write_disk();
        friend class BNode;
    };
}
#endif