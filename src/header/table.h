#ifndef TABLE_H
#define TABLE_H
#include <map>
#include <unordered_map>
#include "tree.h"
class Table{
    //Multiple trees
    private:
        std::string table_name;
        std::unordered_map<std::string, size_t> col_name_indexes;
        rsql::BTree *primary_tree;
    public:
        Table();
        ~Table();
};

#endif