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
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000ytes6bytes6bytes6bytes2b";
    for (int i = 0 ; i < 10 ; i++){
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    std::vector<char *>row = tree->find_all_row("00000000000000000000000000000009", 0);
    std::cout.write(row[0], 56);
    char *del_1 = tree->delete_row("00000000000000000000000000000009");
    char *del_2 = tree->delete_row("00000000000000000000000000000008");
    char *del_3 = tree->delete_row("00000000000000000000000000000001");
    char *del_4 = tree->delete_row("00000000000000000000000000000003");
    delete[] row[0];
    delete[] del_1;
    delete[] del_2;
    delete[] del_3;
    delete[] del_4;
    delete tree;
    return 0;
}