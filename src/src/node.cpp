#include "node.h"
#include "tree.h"

/**
 * @brief Shift all item starting at idx (inclusive) to the right by 1 unit
 *
 * @tparam T
 * @param v
 * @param idx
 */
template <typename T>
void shift_right(std::vector<T> &v, size_t idx, size_t size)
{
    for (size_t i = size ; i > idx; i--)
    {
        v[i] = v[i - 1];
    }
}
/**
 * @brief Shift all item starting at idx (inclusive) to the left by 1 unit
 *
 * @tparam T
 * @param v
 * @param idx
 */
template <typename T>
void shift_left(std::vector<T> &v, size_t idx, size_t size)
{
    if (idx == 0)
    {
        throw std::invalid_argument("Can't shift left at index 0");
    }
    for (size_t i = idx; i < size ; i++)
    {
        v[i - 1] = v[i];
    }
}
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
std::string get_file_name(const unsigned int no)
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
        new_node->columns.clear();
        char col_size_pad[sizeof(int)];
        node_file.read(col_size_pad, 4);
        int col_size = *reinterpret_cast<int *>(col_size_pad);
        for (int i = 0; i < col_size; i++)
        {
            char c_id[4];
            node_file.read(c_id, 4);
            unsigned int col_id = *reinterpret_cast<int *>(c_id);
            char c_type[DT_STR_LEN];
            node_file.read(c_type, DT_STR_LEN);
            std::string c_type_str(c_type, DT_STR_LEN);
            char c_len[sizeof(int)];
            node_file.read(c_len, 4);
            int c_width = *reinterpret_cast<int *>(c_len);
            new_node->columns.push_back(Column::get_column(col_id, str_to_dt(c_type_str), (size_t)c_width));
        }
        //TODO: if current node column is different than the tree columns, adjustment is needed
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
    BNode *BNode::split_children(size_t idx, BNode *c_i)
    {
        if (!c_i->full())
        {
            throw std::runtime_error("Can't split a child if it's not full");
        }
        BNode *new_node = new BNode(this->tree, ++this->tree->max_node_num);
        new_node->leaf = c_i->leaf;
        shift_right(this->keys, idx, this->size);
        shift_right(this->children, idx + 1, this->size + 1);
        this->keys[idx] = c_i->keys[this->tree->t - 1];
        c_i->keys[this->tree->t - 1] = nullptr;
        this->children[idx + 1] = new_node->node_num;
        c_i->size--;
        this->size++;
        for (size_t j = 0; j < this->tree->t - 1; j++)
        {
            new_node->keys[j] = c_i->keys[this->tree->t + j];
            c_i->keys[this->tree->t + j] = nullptr;
        }
        new_node->size += this->tree->t - 1;
        c_i->size -= this->tree->t - 1;
        if (!c_i->leaf)
        {
            size_t c_i_idx = this->tree->t;
            for (int j = 0; j < this->tree->t; j++)
            {
                new_node->children[j] = c_i->children[c_i_idx];
                c_i->children[c_i_idx++] = 0;
            }
        }
        this->changed = true;
        c_i->changed = true;
        new_node->changed = true;
        return new_node;
    }
    void BNode::merge(size_t idx, BNode *c_i, BNode *c_j)
    {
        size_t c_i_size = c_i->size;
        c_i->keys[c_i_size++] = this->keys[idx];
        for (int i = 0; i < c_j->size; i++)
        {
            c_i->keys[c_i_size++] = c_j->keys[i];
            c_j->keys[i] = nullptr;
        }
        if (!c_j->leaf)
        {
            c_i_size = c_i->size + 1;
            for (int i = 0; i < c_j->size + 1; i++)
            {
                c_i->children[c_i_size++] = c_j->children[i];
            }
        }
        c_i->size += 1 + c_j->size;
        shift_left(this->keys, idx + 1, this->size);
        shift_left(this->children, idx + 2, this->size + 1);
        this->keys[this->size - 1] = nullptr;
        this->children[this->size] = 0;
        this->size--;
        c_i->changed = true;
        this->changed = true;
        c_j->destroy();
    }
    void BNode::delete_row_1(size_t idx)
    {
        delete[] this->keys[idx];
        shift_left(this->keys, idx + 1, this->size);
        this->keys[this->size - 1] = nullptr;
        this->size--;
        this->changed = true;
        this->del_if_not_root();
    }
    void BNode::delete_row_2(const char *key, size_t idx)
    {
        unsigned int c_i_num = this->children[idx];
        std::string c_i_str = get_file_name(c_i_num);
        BNode *c_i = BNode::read_disk(this->tree, c_i_str);
        if (c_i->size >= this->tree->t)
        {
            char *temp = this->keys[idx];
            this->keys[idx] = c_i->keys[c_i->size - 1];
            c_i->keys[c_i->size - 1] = temp;
            c_i->changed = true;
            this->changed = true;
            this->del_if_not_root();
            return c_i->delete_row(key);
        }
        unsigned int c_j_num = this->children[idx + 1];
        std::string c_j_str = get_file_name(c_j_num);
        BNode *c_j = BNode::read_disk(this->tree, c_j_str);
        if (c_j->size >= this->tree->t)
        {
            char *temp = this->keys[idx];
            this->keys[idx] = c_j->keys[0];
            c_j->keys[0] = temp;
            c_j->changed = true;
            this->changed = true;
            this->del_if_not_root();
            return c_j->delete_row(key);
        }
        this->merge(idx, c_i, c_j);
        this->del_if_not_root();
        return c_i->delete_row(key);
    }
    void BNode::delete_row_3(const char *key, size_t idx)
    {
        unsigned int c_i_num = this->children[idx];
        std::string c_i_str = get_file_name(c_i_num);
        BNode *c_i = BNode::read_disk(this->tree, c_i_str);
        if (c_i->size <= this->tree->t - 1)
        {
            BNode *c_l = nullptr, *c_r = nullptr;
            if (idx > 0)
            {
                unsigned int c_l_num = this->children[idx - 1];
                std::string c_l_str = get_file_name(c_l_num);
                c_l = BNode::read_disk(this->tree, c_l_str);
                if (c_l->size >= this->tree->t)
                {
                    shift_right(c_i->keys, 0, c_i->size);
                    shift_right(c_i->children, 0, c_i->size + 1);
                    c_i->keys[0] = this->keys[idx - 1];
                    this->keys[idx - 1] = c_l->keys[c_l->size - 1];
                    c_i->children[0] = c_l->children[c_l->size];
                    c_l->keys[c_l->size - 1] = nullptr;
                    c_l->children[c_l->size] = 0;
                    c_l->size--;
                    c_i->size++;
                    c_l->changed = true;
                    c_i->changed = true;
                    this->changed = true;
                    delete c_l;
                    this->del_if_not_root();
                    return c_i->delete_row(key);
                }
            }
            if (idx < this->size)
            {
                unsigned int c_r_num = this->children[idx + 1];
                std::string c_r_str = get_file_name(c_r_num);
                c_r = BNode::read_disk(this->tree, c_r_str);
                if (c_r->size >= this->tree->t)
                {
                    c_i->keys[c_i->size - 1] = this->keys[idx];
                    this->keys[idx] = c_r->keys[0];
                    c_i->children[c_i->size] = c_r->children[0];
                    shift_left(c_r->keys, 1, c_r->size);
                    shift_left(c_r->children, 1, c_r->size + 1);
                    c_r->keys[c_r->size - 1] = nullptr;
                    c_r->children[c_r->size] = 0;
                    c_i->size++;
                    c_r->size--;
                    c_i->changed = true;
                    c_r->changed = true;
                    this->changed = true;
                    delete c_r;
                    this->del_if_not_root();
                    return c_i->delete_row(key);
                }
            }
            if (idx > 0)
            {
                this->merge(idx - 1, c_l, c_i);
                if (c_r != nullptr)
                {
                    delete c_r;
                }
                this->del_if_not_root();
                return c_l->delete_row(key);
            }
            this->merge(idx, c_i, c_r);
            // idx is definitely 0, c_l is null
            this->del_if_not_root();
            return c_i->delete_row(key);
        }
        this->del_if_not_root();
        return c_i->delete_row(key);
    }
    void BNode::del_if_not_root()
    {
        if (this->node_num != this->tree->root_num)
        {
            delete this;
        }
    }
    void BNode::destroy()
    {
        std::string file_name = get_file_name(this->node_num);
        if (std::remove(file_name.c_str()))
        {
            throw std::invalid_argument("Error deleting file");
        }
        this->changed = false;
        delete this;
    }
    BNode::BNode(BTree *tree, unsigned int node_num)
        : tree(tree), node_num(node_num), leaf(false), changed(true), size(0)
    {
        this->keys.reserve(2 * this->tree->t - 1);
        this->keys.assign(this->keys.capacity(), nullptr);
        this->children.reserve(2 * this->tree->t);
        this->children.assign(this->children.capacity(), 0);
        this->columns = tree->columns;
    }
    BNode::~BNode()
    {
        if (this->changed)
        {
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
            shift_right(this->keys, cur_idx, this->size);
            this->keys[cur_idx] = new char[this->tree->width];
            std::memcpy(this->keys[cur_idx], src, this->tree->width);
            this->size++;
            this->changed = true;
            this->del_if_not_root();
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
            this->del_if_not_root();
            c_i->insert(src);
        }
    }
    void BNode::delete_row(const char *key)
    {
        size_t idx = 0;
        while (idx < this->size && this->compare_key(key, this->keys[idx]) > 0)
        {
            idx++;
        }
        if (idx < this->size && this->compare_key(key, this->keys[idx]) == 0 && this->leaf)
        {
            this->delete_row_1(idx);
        }
        else if (idx < this->size && this->compare_key(key, this->keys[idx]) == 0 && !this->leaf)
        {
            this->delete_row_2(key, idx);
        }
        else if (!this->leaf)
        {
            this->delete_row_3(key, idx);
        }
        else
        {
            return;
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
        int col_num = this->columns.size();
        char *col_pad = reinterpret_cast<char *>(&col_num);
        node_file.write(col_pad, 4);
        for (int i = 0 ; i < col_num ; i++){
            unsigned int cur_col_id = this->columns[i].col_id;
            char *col_pad = reinterpret_cast<char *>(&cur_col_id);
            node_file.write(col_pad, 4);
            std::string type = dt_to_str(this->columns[i].type);
            node_file.write(type.c_str(), DT_STR_LEN);
            int width = this->columns[i].width;
            char *w_pad = reinterpret_cast<char *>(&width);
            node_file.write(w_pad, 4);
        }
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