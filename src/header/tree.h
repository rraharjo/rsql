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
    private:
        BNode *root;
        unsigned int root_num;
        unsigned int max_node_num;
        size_t t;
        size_t width;
        //First column is the index
        std::vector<Column> columns;

        void get_root_node();

    public:
        static BTree *read_disk();
        BTree(const std::vector<Column> columns);
        ~BTree();

        /**
         * @brief find the row of which key equals to the argument key
         * 
         * @param key key of the row
         * @return char* n bytes, where n is the width of the table
         */
        char *find_row(const char *key);

        /**
         * @brief insert src to the tree, length of the src must equal the width of the table
         * 
         * @param src pointer that contains the row information
         */
        void insert_row(const char *src);
        /**
         * @brief Delete the row of which key equal to the argument key
         * 
         * @param key 
         */
        void delete_row(const char *key);
        void write_disk();
        friend class BNode;
    };
}
#endif