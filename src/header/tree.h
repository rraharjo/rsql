#ifndef BTREE_H
#define BTREE_H
#include "node.h"
#include <iostream>
#define DEGREE 4
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

        char *find_row(const char *key);
        /**
         * @brief insert src to the tree, length of the src must equal the width of the table
         * 
         * @param src pointer that contains the row information
         */
        void insert_row(const char *src);

        void write_disk();
        friend class BNode;
    };
}
#endif