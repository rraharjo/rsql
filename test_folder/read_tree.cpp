#include "tree.h"
int main(){
    std::vector<rsql::Column> sample_structure;
    sample_structure.push_back(rsql::Column::pkey_column());
    sample_structure.push_back(rsql::Column::int_column(4));
    sample_structure.push_back(rsql::Column::date_column());
    sample_structure.push_back(rsql::Column::char_column(10));
    rsql::BTree *tree = new rsql::BTree(sample_structure);
    tree->write_disk();
    delete tree;
    tree = rsql::BTree::read_disk();
    return 0;
}