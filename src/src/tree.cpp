#include "tree.h"
namespace rsql
{
    BTree::BTree(const std::vector<Column> columns)
        : columns(columns), root_num(1), max_node_num(1), t(DEGREE), root(nullptr)
    {
        this->width = 0;
        for (Column &c : this->columns)
        {
            this->width += c.width;
        }
    }

    BTree::~BTree()
    {
        this->write_disk();
        delete this->root;
    }

    void BTree::get_root_node(){
        if (this->root == nullptr)
        {
            std::string root_file_name = "node_" + std::to_string(this->root_num) + ".rsql";
            try
            {
                this->root = BNode::read_disk(this, root_file_name);
            }
            catch (const std::invalid_argument &e)
            {
                this->root = new BNode(this, this->root_num);
                this->root->changed = true;
                this->root->leaf = true;
            }
        }
    }
    BTree *BTree::read_disk()
    {
        std::ifstream tree_file(TREE_FILE, std::ios::binary);
        if (!tree_file)
        {
            throw std::invalid_argument("Can't open read tree file...");
        }
        char s_pad[sizeof(int)];
        tree_file.read(s_pad, 4);
        int col_size = *reinterpret_cast<int *>(s_pad);
        std::vector<Column> cols;
        for (int i = 0; i < col_size; i++)
        {
            char c_type[DT_STR_LEN];
            tree_file.read(c_type, DT_STR_LEN);
            std::string c_type_str(c_type, DT_STR_LEN);
            char c_len[sizeof(int)];
            tree_file.read(c_len, 4);
            int c_width = *reinterpret_cast<int *>(c_len);
            cols.push_back(Column::get_column(str_to_dt(c_type_str), (size_t)c_width));
        }
        tree_file.read(s_pad, 4);
        unsigned int root_num = *reinterpret_cast<int *>(s_pad);
        tree_file.read(s_pad, 4);
        unsigned int max_node_num = *reinterpret_cast<int *>(s_pad);
        BTree *to_ret = new BTree(cols);
        to_ret->root_num = root_num;
        to_ret->max_node_num = max_node_num;
        tree_file.close();
        return to_ret;
    }

    char *BTree::find_row(const char *key){
        this->get_root_node();
        return this->root->find(key);
    }

    void BTree::insert_row(const char *src)
    {
        this->get_root_node();
        if (this->root->full()){
            BNode *new_root = new BNode(this, ++this->max_node_num);
            this->root_num = this->max_node_num;
            new_root->leaf = false;
            new_root->children[0] = this->root->node_num;
            BNode *new_children = new_root->split_children(0, this->root);
            delete new_children;
            delete this->root;
            this->root = new_root;
            this->root->write_disk();
            this->root->insert(src);
        }
        else{
            this->root->insert(src);
        }
        
    }

    void BTree::write_disk()
    {
        std::ofstream tree_file(TREE_FILE, std::ios::binary);
        if (!tree_file)
        {
            throw std::invalid_argument("Can't open write tree file...");
        }
        int col_size = this->columns.size();
        char *s_pad = reinterpret_cast<char *>(&col_size);
        tree_file.write(s_pad, 4);
        for (int i = 0; i < col_size; i++)
        {
            std::string type = dt_to_str(this->columns[i].type);
            tree_file.write(type.c_str(), DT_STR_LEN);
            int width = this->columns[i].width;
            char *w_pad = reinterpret_cast<char *>(&width);
            tree_file.write(w_pad, 4);
        }
        char *r_pad = reinterpret_cast<char *>(&this->root_num);
        tree_file.write(r_pad, 4);
        char *max_node_pad = reinterpret_cast<char *>(&this->max_node_num);
        tree_file.write(max_node_pad, 4);
        tree_file.close();
    }
}