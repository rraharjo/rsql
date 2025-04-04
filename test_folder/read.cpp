#include <iostream>
#include "node.h"
#define DEGREE 4
int main(){
    std::vector<rsql::Column> sample_structure;
    sample_structure.push_back(rsql::IntColumn(4));
    sample_structure.push_back(rsql::DateColumn());
    sample_structure.push_back(rsql::CharColumn(10));
    rsql::BNode *new_node = new rsql::BNode(sample_structure, DEGREE);
    std::string sample_insert = "fourdd-MM-yyyy10bytelong";
    new_node->insert(sample_insert);
    new_node->write_disk("sample.rsql");
    delete new_node;
    rsql::BNode *new_new_node = rsql::BNode::read_disk(sample_structure, DEGREE, "sample.rsql");
    std::cout << new_new_node << std::endl;
    return 0;
}