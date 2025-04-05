#include "node.h"
#include "tree.h"

unsigned int get_node_no(const std::string& file_name){
    std::regex pattern(".*_([0-9]+)\\.rsql");
    std::smatch match;
    unsigned int node_no = 0;
    if (std::regex_match(file_name, match, pattern)) {
        // match[1] contains the first captured group, which is the number
        std::string extracted_number = match[1];
        node_no = std::stoi(extracted_number);
    } else {
        throw std::invalid_argument("File name is in the wrong format");
    }
    return node_no;
}

namespace rsql
{
    BNode *BNode::read_disk(BTree *tree, std::string file_name)
    {
        unsigned int node_no = get_node_no(file_name);
        BNode *new_node = new BNode(tree, node_no);
        size_t row_size = tree->width;
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
            new_node->keys[i] = new char[new_node->tree->width];
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
    BNode::BNode(BTree *tree, unsigned int node_num)
        : tree(tree), node_num(node_num), leaf(false), changed(false)
    {
        this->keys.reserve(2 * this->tree->t - 1);
        this->keys.assign(this->keys.capacity(), nullptr);
        this->children.reserve(2 * this->tree->t);
        this->children.assign(this->children.capacity(), 0);
    }

    BNode::~BNode()
    {
        for (int i = 0; i < this->size; i++)
        {
            delete[] this->keys[i];
        }
    }

    void BNode::insert(const char *row)
    {
        this->keys[this->size] = new char[this->tree->width];
        std::memcpy(this->keys[this->size], row, this->tree->width);
        this->size++;
    }

    void BNode::write_disk(std::string file_name)
    {
        std::ofstream node_file(file_name, std::ios::binary);
        int this_size = (int)this->size;
        char *npad = reinterpret_cast<char *>(&this_size);
        node_file.write(npad, 4);
        for (int i = 0; i < this->size; i++)
        {
            node_file.write(this->keys[i], this->tree->width);
        }
        for (int i = 0; i <= this->size; i++)
        {
            int cur_child = this->children[i];
            char *cpad = reinterpret_cast<char *>(&cur_child);
            node_file.write(cpad, 4);
        }
        node_file.close();
    }


}