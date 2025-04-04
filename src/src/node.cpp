#include "node.h"

namespace rsql
{
    BNode *BNode::read_disk(std::vector<Column> &cols, size_t t, std::string file_name)
    {
        BNode *new_node = new BNode(cols, t);
        size_t row_size = new_node->get_width();
        std::ifstream node_file(file_name, std::ios::binary);
        if (!node_file)
        {
            throw std::invalid_argument("Can't open " + file_name);
        }
        char pad[sizeof(int)];
        std::memset(pad, '\0', sizeof(int));
        node_file.read(pad, 4);
        int node_size = *reinterpret_cast<int *>(pad);
        new_node->size = node_size;
        for (int i = 0; i < node_size; i++)
        {
            new_node->keys[i] = new char[new_node->get_width()];
            node_file.read(new_node->keys[i], row_size);
        }
        for (int i = 0; i <= node_size; i++)
        {
            char idx_pad[sizeof(int)];
            std::memset(idx_pad, '\0', sizeof(int));
            node_file.read(idx_pad, 4);
            int idx = *reinterpret_cast<int *>(idx_pad);
            new_node->children[i] = idx;
        }
        node_file.close();
        return new_node;
    }
    BNode::BNode(std::vector<Column> &cols, size_t t) : cols(cols), t(t)
    {
        this->keys.reserve(2 * this->t - 1);
        this->keys.assign(this->keys.capacity(), nullptr);
        this->children.reserve(2 * this->t);
        this->children.assign(this->children.capacity(), -1);
    }

    BNode::~BNode()
    {
        for (int i = 0; i < this->get_size(); i++)
        {
            delete[] this->keys[i];
        }
    }

    void BNode::insert(const std::string row){
        if (row.length() > this->get_width()){
            throw std::invalid_argument("Row is too long");
        }
        this->keys[this->get_size()] = new char[this->get_width()];
        std::memcpy(this->keys[this->get_size()], row.c_str(), this->get_width());
        this->size++;
    }

    void BNode::write_disk(std::string file_name)
    {
        std::ofstream node_file(file_name, std::ios::binary);
        int this_size = (int) this->get_size();
        char *npad = reinterpret_cast<char *>(&this_size);
        node_file.write(npad, 4);
        for (int i = 0 ; i < this->get_size() ; i++){
            node_file.write(this->keys[i], this->get_width());
        }
        for (int i = 0 ; i <= this->get_size() ; i++){
            int cur_child = this->children[i];
            char *cpad = reinterpret_cast<char *>(&cur_child);
            node_file.write(cpad, 4);
        }
        node_file.close();
    }

    size_t BNode::get_size()
    {
        return this->size;
    }
    size_t BNode::get_t()
    {
        return this->t;
    }
    size_t BNode::get_width()
    {
        size_t to_ret = 0;
        for (Column &c : this->cols)
        {
            to_ret += c.get_width();
        }
        return to_ret;
    }
    std::ostream &operator<<(std::ostream &o, const BNode &node){
        for (int i = 0 ; i < node.size ; i++){
            o << std::string(node.keys[i], 24) << " ";
        }
        return o;
    }

}