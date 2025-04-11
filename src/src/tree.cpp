#include "tree.h"
namespace rsql
{
    BTree::BTree()
        : root_num(1), max_node_num(1), t(DEGREE), root(nullptr), max_col_id(0), width(0)
    {
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
        BTree *to_ret = new BTree();
        char s_pad[sizeof(uint32_t)];
        tree_file.read(s_pad, 4);
        uint32_t col_size = *reinterpret_cast<uint32_t *>(s_pad);
        for (uint32_t i = 0; i < col_size; i++)
        {
            char c_id[4];
            tree_file.read(c_id, 4);
            uint32_t col_id = *reinterpret_cast<uint32_t *>(c_id);

            char c_type[DT_STR_LEN];
            tree_file.read(c_type, DT_STR_LEN);
            std::string c_type_str(c_type, DT_STR_LEN);

            char c_len[sizeof(uint32_t)];
            tree_file.read(c_len, 4);
            uint32_t c_width = *reinterpret_cast<uint32_t *>(c_len);
            to_ret->add_column(Column::get_column(col_id, str_to_dt(c_type_str), (size_t)c_width));
        }
        tree_file.read(s_pad, 4);
        uint32_t root_num = *reinterpret_cast<uint32_t *>(s_pad);
        tree_file.read(s_pad, 4);
        uint32_t max_node_num = *reinterpret_cast<uint32_t *>(s_pad);
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
    void BTree::delete_row(const char *key){
        this->get_root_node();
        this->root->delete_row(key);
        if (this->root->size == 0){
            uint32_t new_root_num = this->root->children[0];
            std::string new_root_name = "node_" + std::to_string(new_root_num) + ".rsql";
            BNode *new_root = BNode::read_disk(this, new_root_name);
            this->root->destroy();
            this->root = new_root;
            this->root_num = this->root->node_num;
        }
    }
    void BTree::add_column(const Column c){
        this->columns.push_back(c);
        this->width += c.width;
        this->columns[this->columns.size() - 1].col_id = ++this->max_col_id;
    }
    void BTree::write_disk()
    {
        std::ofstream tree_file(TREE_FILE, std::ios::binary);
        if (!tree_file)
        {
            throw std::invalid_argument("Can't open write tree file...");
        }
        uint32_t col_size = this->columns.size();
        char *s_pad = reinterpret_cast<char *>(&col_size);
        tree_file.write(s_pad, 4);
        for (uint32_t i = 0; i < col_size; i++)
        {
            uint32_t cur_col_id = this->columns[i].col_id;
            char *col_pad = reinterpret_cast<char *>(&cur_col_id);
            tree_file.write(col_pad, 4);

            std::string type = dt_to_str(this->columns[i].type);
            tree_file.write(type.c_str(), DT_STR_LEN);

            uint32_t width = this->columns[i].width;
            char *w_pad = reinterpret_cast<char *>(&width);
            tree_file.write(w_pad, 4);
        }
        char *r_pad = reinterpret_cast<char *>(&this->root_num);
        tree_file.write(r_pad, 4);

        char *max_node_pad = reinterpret_cast<char *>(&this->max_node_num);
        tree_file.write(max_node_pad, 4);

        char *max_col_id = reinterpret_cast<char *>(&this->max_col_id);
        tree_file.write(max_col_id, 4);

        tree_file.close();
    }
}