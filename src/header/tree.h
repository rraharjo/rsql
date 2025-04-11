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
        std::vector<Column> columns;
        uint32_t root_num;
        uint32_t max_node_num;
        uint32_t max_col_id;

        BNode *root;
        size_t t;
        size_t width;
        
        void get_root_node();
        BTree(const std::vector<Column>);
    public:
        static BTree *read_disk();
        BTree();
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
        void add_column(const Column c);
        void write_disk();
        friend class BNode;
    };
}
#endif