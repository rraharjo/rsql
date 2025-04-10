#ifndef BNODE_H
#define BNODE_H
#include <regex>
#include <vector>
#include <fstream>
#include <cstring>
#include "column.h"
namespace rsql
{
    class BTree;
}
namespace rsql
{
    class BNode
    {
    private:
        /**
         * @brief Compare the first k byte, where k is the size of the first column
         *
         * @param k_1
         * @param k_2
         * @return < 0 if k_1 is less than k_2 ; > 0 if k_1 is larger than k_2
         */
        int compare_key(const char *k_1, const char *k_2);
        /**
         * @brief Shift keys to the right by 1 unit, starting at idx (inclusive)
         *
         * @param idx
         */
        void merge(size_t idx, BNode *c_i, BNode *c_j);
        /**
         * @brief case 1 of deleting an item: item is in this node, and this node is a leaf
         *
         * @param key
         */
        void delete_row_1(size_t idx);
        /**
         * @brief case 2 of deleting an item: item is in this node, and this node is not a leaf
         *
         * @param key
         */
        void delete_row_2(const char *key, size_t idx);
        /**
         * @brief case 3 of deleting an item: item is not in this node
         *
         * @param key
         * @param idx index of the child node
         */
        void delete_row_3(const char *key, size_t idx);
        void del_if_not_root();
        /**
         * @brief Delete this node along with the file
         *
         */
        void destroy();
        /**
         * @brief Split the children node c_i, which has to be this children at index idx
         *
         * @param idx position of the target child
         * @param c_i a pointer to the child node (c_i has to be in index idx)
         * @return a pointer to the newly created node, at position idx + 1
         */
        BNode *split_children(size_t idx, BNode *c_i);

    public:
        BTree *tree;
        std::vector<Column> columns;
        // except the root, minimum of t-1 keys and a maximum of 2t - 1 keys
        std::vector<char *> keys;
        std::vector<unsigned int> children;
        unsigned int node_num;
        size_t size;
        bool changed, leaf;

    public:
        /**
         * @brief
         * Read file_name file and store all the data inside keys
         * @param cols column format
         * @param file_name
         * @return BNode*
         */
        static BNode *read_disk(BTree *tree, const std::string file_name);
        BNode(BTree *tree, unsigned int node_num);
        ~BNode();
        bool full();
        /**
         * @brief find the first row that matches the key
         *
         * @param key only compares the first k bytes, where k is the width of the first column
         * @return n bytes of char dynamically allocated, where n is the width of the table. If not found it returns nullptr
         */
        char *find(const char *key);
        void insert(const char *row);
        void delete_row(const char *key);
        void write_disk();

        friend class BTree;
    };
}
#endif