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
    char src[32 + 4 + 10 + 10 + 1] = "6bytes6bytes6bytes6bytes6bytes6bytes6bytes6bytes6bytes2b";
    for (int i = 0 ; i < 7 ; i++){
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    tree->insert_row(src);
    char *row = tree->find_row("6bytes6bytes6bytes6bytes6bytes6e");
    std::cout.write(row, 56);
    delete[] row;
    delete tree;
    return 0;
}