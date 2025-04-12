#include "tree.h"
int main(){
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    tree->write_disk();
    delete tree;
    tree = rsql::BTree::read_disk();
    char src[32 + 4 + 10 + 10 + 1] = "6bytes6bytes6bytes6bytes6bytes60ytes6bytes6bytes6bytes2b";
    for (int i = 0 ; i < 10 ; i++){
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    char *row = tree->find_row("6bytes6bytes6bytes6bytes6bytes69");
    std::cout.write(row, 56);
    tree->delete_row("6bytes6bytes6bytes6bytes6bytes69");
    tree->delete_row("6bytes6bytes6bytes6bytes6bytes68");
    tree->delete_row("6bytes6bytes6bytes6bytes6bytes61");
    tree->delete_row("6bytes6bytes6bytes6bytes6bytes63");
    delete[] row;
    delete tree;
    return 0;
}