#include "node.h"
#include "tree.h"

unsigned int get_node_no(const std::string &file_name)
{
    std::regex pattern(".*_([0-9]+)\\.rsql");
    std::smatch match;
    unsigned int node_no = 0;
    if (std::regex_match(file_name, match, pattern))
    {
        // match[1] contains the first captured group, which is the number
        std::string extracted_number = match[1];
        node_no = std::stoi(extracted_number);
    }
    else
    {
        throw std::invalid_argument("File name is in the wrong format");
    }
    return node_no;
}
std::string get_file_name(const size_t no)
{
    std::string to_ret = "node_" + std::to_string(no) + ".rsql";
    return to_ret;
}

namespace rsql
{
    BNode *BNode::read_disk(BTree *tree, const std::string file_name)
    {
        unsigned int node_no = get_node_no(file_name);
        size_t row_size = tree->width;
        std::ifstream node_file(file_name, std::ios::binary);
        if (!node_file)
        {
            throw std::invalid_argument("Can't open node.rsql file");
            return nullptr;
        }
        BNode *new_node = new BNode(tree, node_no);
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
        char l_pad;
        node_file.read(&l_pad, 1);
        new_node->leaf = (bool)l_pad;
        node_file.close();
        return new_node;
    }
    int BNode::compare_key(const char *k_1, const char *k_2)
    {
        return strncmp(k_1, k_2, this->tree->columns[0].width);
    }
    void BNode::shift_keys(size_t idx)
    {
        if (this->size < idx)
        {
            std::string err_msg =
                "Can't shift at index " + std::to_string(idx) + " when the key size is " + std::to_string(this->size);
            throw std::invalid_argument(err_msg);
            return;
        }
        if (this->full())
        {
            throw std::runtime_error("Can't shift key when node is full");
            return;
        }
        size_t j = this->size;
        while (j > idx)
        {
            this->keys[j] = this->keys[j - 1];
            j--;
        }
        this->keys[j] = nullptr;
        return;
    }
    void BNode::shift_children(size_t idx)
    {
        if (this->size < idx - 1)
        {
            std::string err_msg = "Can't shift children at index " + std::to_string(idx) + " when the key size is " + std::to_string(this->size);
            throw std::invalid_argument(err_msg);
            return;
        }
        if (this->full())
        {
            throw std::runtime_error("Can't shift children when the node is full");
            return;
        }
        size_t j = this->size + 1;
        while (j > idx)
        {
            this->children[j] = this->children[j - 1];
            j--;
        }
        this->children[j] = 0;
        return;
    }
    BNode *BNode::split_children(size_t idx, BNode *c_i)
    {
        if (!c_i->full())
        {
            throw std::runtime_error("Can't split a child if it's not full");
        }
        BNode *new_node = new BNode(this->tree, ++this->tree->max_node_num);
        new_node->leaf = c_i->leaf;
        this->shift_keys(idx);
        this->shift_children(idx + 1);
        this->keys[idx] = new char[this->tree->width];
        std::memcpy(this->keys[idx], c_i->keys[this->tree->t - 1], this->tree->width);
        this->children[idx + 1] = new_node->node_num;
        this->size++;
        for (size_t j = 0; j < this->tree->t - 1; j++)
        {
            new_node->keys[j] = new char[this->tree->width];
            std::memcpy(new_node->keys[j], c_i->keys[this->tree->t + j], this->tree->width);
            c_i->size--;
            new_node->size++;
            delete[] c_i->keys[this->tree->t + j];
        }
        if (!c_i->leaf)
        {
            size_t c_i_idx = this->tree->t;
            for (int j = 0; j < this->tree->t; j++)
            {
                new_node->children[j] = c_i->children[c_i_idx++];
            }
        }
        this->changed = true;
        c_i->changed = true;
        new_node->changed = true;
        return new_node;
    }

    BNode::BNode(BTree *tree, unsigned int node_num)
        : tree(tree), node_num(node_num), leaf(false), changed(true), size(0)
    {
        this->keys.reserve(2 * this->tree->t - 1);
        this->keys.assign(this->keys.capacity(), nullptr);
        this->children.reserve(2 * this->tree->t);
        this->children.assign(this->children.capacity(), 0);
    }
    BNode::~BNode()
    {
        if (this->changed){
            this->write_disk();
        }
        for (int i = 0; i < this->size; i++)
        {
            delete[] this->keys[i];
        }
    }

    bool BNode::full()
    {
        return this->size == this->keys.capacity();
    }
    char *BNode::find(const char *key)
    {
        size_t cur_idx = 0;
        while (cur_idx < this->size && this->compare_key(key, this->keys[cur_idx]) > 0)
        {
            cur_idx++;
        }
        if (cur_idx < this->size && this->compare_key(key, this->keys[cur_idx]) == 0)
        {
            char *to_ret = new char[this->tree->width];
            std::memcpy(to_ret, this->keys[cur_idx], this->tree->width);
            if (this->tree->root_num != this->node_num)
            {
                delete this;
            }
            return to_ret;
        }
        else if (!this->leaf)
        {
            std::string c_i_file = get_file_name(this->children[cur_idx]);
            BNode *c_i = BNode::read_disk(this->tree, c_i_file);
            if (this->tree->root_num != this->node_num)
            {
                delete this;
            }
            return c_i->find(key);
        }
        else
        {
            if (this->tree->root_num != this->node_num)
            {
                delete this;
            }
            return nullptr;
        }
    }
    void BNode::insert(const char *src)
    {
        if (this->leaf)
        {
            size_t cur_idx = 0;
            while (cur_idx < this->size && this->compare_key(src, this->keys[cur_idx]) > 0)
            {
                cur_idx++;
            }
            this->shift_keys(cur_idx);
            this->keys[cur_idx] = new char[this->tree->width];
            std::memcpy(this->keys[cur_idx], src, this->tree->width);
            this->size++;
            this->changed = true;
            if (this->node_num != this->tree->root_num){
                delete this;
            }
        }
        else
        {
            size_t cur_idx = 0;
            while (cur_idx < this->size && this->compare_key(src, this->keys[cur_idx]) > 0)
            {
                cur_idx++;
            }
            size_t c_num = this->children[cur_idx];
            std::string c_i_file = get_file_name(c_num);
            BNode *c_i = BNode::read_disk(this->tree, c_i_file);
            if (c_i->full())
            {
                BNode *new_node = this->split_children(cur_idx, c_i);
                if (this->compare_key(src, this->keys[cur_idx]) > 0)
                {
                    delete c_i;
                    c_i = new_node;
                }
                else
                {
                    delete new_node;
                }
            }
            if (this->node_num != this->tree->root_num){
                delete this;
            }
            c_i->insert(src);
        }
    }
    void BNode::write_disk()
    {
        if (!this->changed)
        {
            return;
        }
        std::string file_name = "node_" + std::to_string(this->node_num) + ".rsql";
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
        char *l_pad = reinterpret_cast<char *>(&this->leaf);
        node_file.write(l_pad, 1);
        node_file.close();
        this->changed = false;
    }
}