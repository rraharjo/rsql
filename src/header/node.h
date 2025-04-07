#ifndef BNODE_H
#define BNODE_H
#include <regex>
#include <vector>
#include <fstream>
#include <cstring>
#include "column.h"
namespace rsql{
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
        void shift_keys(size_t idx);
        void shift_children(size_t idx);
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
        // except the root, minimum of t-1 keys and a maximum of 2t - 1 keys
        std::vector<char *> keys;
        /*the number represent the file for the next node
        e.g. MYDB_int
        */
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
        static BNode *read_disk(BTree *tree, std::string file_name);
        BNode(BTree *tree, unsigned int node_num);
        ~BNode();
        bool full();
        void insert(const char *row);
        void write_disk();

        friend class BTree;
    };
}
#endif