#include "node.h"
#include "tree.h"
#include "table.h"
#include "database.h"

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
    for (size_t i = size; i > idx; i--)
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
    for (size_t i = idx; i < size; i++)
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
        char *read_buffer = new char[STARTING_BUFFER_SZ];
        size_t bytes_processed = 0, cur_read_bytes = 0;
        std::string where = std::filesystem::path(tree->get_path()) / file_name;
        int node_file_fd = open(where.c_str(), O_RDONLY);
        if (node_file_fd < 0)
        {
            delete[] read_buffer;
            throw std::invalid_argument("Fail to open node.rsql");
        }
        unsigned int node_no = get_node_no(file_name);
        size_t cur_row_width = 0;
        if ((cur_read_bytes = read(node_file_fd, read_buffer, STARTING_BUFFER_SZ)) < 0)
        {
            delete[] read_buffer;
            throw std::invalid_argument("Fail to read node.rsql");
        }
        BNode *new_node = new BNode(tree, node_no);
        new_node->columns.clear();
        char col_size_pad[sizeof(uint32_t)];
        std::memcpy(col_size_pad, read_buffer + bytes_processed, 4);
        bytes_processed += 4;
        uint32_t col_size = *reinterpret_cast<uint32_t *>(col_size_pad);
        for (uint32_t i = 0; i < col_size; i++)
        {
            if (cur_read_bytes - bytes_processed < 16)
            {
                std::memcpy(read_buffer, read_buffer + bytes_processed, cur_read_bytes - bytes_processed);
                bytes_processed = cur_read_bytes - bytes_processed;
                if ((cur_read_bytes = read(node_file_fd, read_buffer + bytes_processed, STARTING_BUFFER_SZ)) < 0)
                {
                    delete[] read_buffer;
                    throw std::invalid_argument("Fail to read.rsql");
                }
                bytes_processed = 0;
            }
            char col_id_pad[4];
            std::memcpy(col_id_pad, read_buffer + bytes_processed, 4);
            bytes_processed += 4;
            uint32_t col_id = *reinterpret_cast<uint32_t *>(col_id_pad);

            char col_type_pad[DT_STR_LEN];
            std::memcpy(col_type_pad, read_buffer + bytes_processed, 4);
            bytes_processed += 4;
            std::string c_type_str(col_type_pad, DT_STR_LEN);

            char col_width_pad[4];
            std::memcpy(col_width_pad, read_buffer + bytes_processed, 4);
            bytes_processed += 4;
            uint32_t col_width = *reinterpret_cast<uint32_t *>(col_width_pad);

            new_node->columns.push_back(Column::get_column(col_id, str_to_dt(c_type_str), (size_t)col_width));
            cur_row_width += col_width;
        }
        char node_size_pad[sizeof(uint32_t)];
        std::memcpy(node_size_pad, read_buffer + bytes_processed, 4);
        bytes_processed += 4;
        uint32_t node_size = *reinterpret_cast<uint32_t *>(node_size_pad);
        new_node->size = node_size;
        for (uint32_t i = 0; i < node_size; i++)
        {
            if (cur_read_bytes - bytes_processed < cur_row_width)
            {
                std::memcpy(read_buffer, read_buffer + bytes_processed, cur_read_bytes - bytes_processed);
                bytes_processed = cur_read_bytes - bytes_processed;
                if ((cur_read_bytes = read(node_file_fd, read_buffer + bytes_processed, STARTING_BUFFER_SZ)) < 0)
                {
                    delete[] read_buffer;
                    throw std::invalid_argument("Fail to read.rsql");
                }
                bytes_processed = 0;
            }
            new_node->keys[i] = new char[cur_row_width];
            std::memcpy(new_node->keys[i], read_buffer + bytes_processed, cur_row_width);
            bytes_processed += cur_row_width;
        }
        for (uint32_t i = 0; i <= node_size; i++)
        {
            if (cur_read_bytes - bytes_processed < 5)
            {
                std::memcpy(read_buffer, read_buffer + bytes_processed, cur_read_bytes - bytes_processed);
                bytes_processed = cur_read_bytes - bytes_processed;
                if ((cur_read_bytes = read(node_file_fd, read_buffer + bytes_processed, STARTING_BUFFER_SZ)) < 0)
                {
                    delete[] read_buffer;
                    throw std::invalid_argument("Fail to read.rsql");
                }
                bytes_processed = 0;
            }
            char idx_pad[sizeof(uint32_t)];
            std::memcpy(idx_pad, read_buffer + bytes_processed, 4);
            bytes_processed += 4;
            uint32_t idx = *reinterpret_cast<uint32_t *>(idx_pad);
            new_node->children[i] = idx;
        }
        char l_pad;
        std::memcpy(&l_pad, read_buffer + bytes_processed, 1);
        bytes_processed += 1;
        new_node->leaf = l_pad;
        close(node_file_fd);
        new_node->match_columns();
        delete[] read_buffer;
        return new_node;
    }
    int BNode::last_child_idx(const char *k)
    {
        int start = 0, end = this->size - 1;
        int mid;
        int to_ret = -1;
        while (start <= end)
        {
            mid = (start + end) / 2;
            int comp = this->compare_key(k, this->keys[mid], 0);
            if (comp < 0)
            {
                end = mid - 1;
            }
            else if (comp > 0)
            {
                to_ret = to_ret == -1 ? mid + 1 : std::min(to_ret, mid + 1);
                start = mid + 1;
            }
            else
            {
                to_ret = std::max(to_ret, mid + 1);
                start = mid + 1;
            }
        }
        return to_ret == -1 ? 0 : to_ret;
    }
    int BNode::first_child_idx(const char *k)
    {
        int start = 0, end = this->size - 1;
        int mid;
        int to_ret = -1;
        while (start <= end)
        {
            mid = (start + end) / 2;
            int comp = this->compare_key(k, this->keys[mid], 0);
            if (comp > 0)
            {
                start = mid + 1;
            }
            else if (comp < 0)
            {
                to_ret = std::max(to_ret, mid);
                end = mid - 1;
            }
            else
            {
                to_ret = to_ret == -1 ? mid : std::min(to_ret, mid);
                end = mid - 1;
            }
        }
        return to_ret == -1 ? this->size : to_ret;
    }

    int BNode::compare_key(const char *k_1, const char *k_2, size_t col_idx)
    {
        return strncmp(k_1, k_2, this->tree->columns[col_idx].width);
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
        for (uint32_t i = 0; i < c_j->size; i++)
        {
            c_i->keys[c_i_size++] = c_j->keys[i];
            c_j->keys[i] = nullptr;
        }
        if (!c_j->leaf)
        {
            c_i_size = c_i->size + 1;
            for (uint32_t i = 0; i < c_j->size + 1; i++)
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
    char *BNode::delete_left()
    {
        if (this->leaf)
        {
            char *to_ret = this->keys[0];
            shift_left(this->keys, 1, this->size);
            this->keys[this->size - 1] = nullptr;
            this->size--;
            this->del_if_not_root();
            return to_ret;
        }
        uint32_t c_zero_num = this->children[0];
        std::string c_zero_str = get_file_name(c_zero_num);
        BNode *c_zero = BNode::read_disk(this->tree, c_zero_str);
        if (c_zero->size <= this->tree->t - 1)
        {
            uint32_t c_r_num = this->children[1];
            std::string c_r_str = get_file_name(c_r_num);
            BNode *c_r = BNode::read_disk(this->tree, c_r_str);
            if (c_r->size >= this->tree->t)
            {
                c_zero->keys[c_zero->size] = this->keys[0];
                this->keys[0] = c_r->keys[0];
                c_zero->children[c_zero->size + 1] = c_r->children[0];
                shift_left(c_r->keys, 1, c_r->size);
                shift_left(c_r->children, 1, c_r->size + 1);
                c_r->keys[c_r->size - 1] = nullptr;
                c_r->children[c_r->size] = 0;
                c_zero->size++;
                c_r->size--;
                c_zero->changed = true;
                c_r->changed = true;
                this->changed = true;
                delete c_r;
                this->del_if_not_root();
                return c_zero->delete_left();
            }
            this->merge(0, c_zero, c_r);
            this->del_if_not_root();
            return c_zero->delete_left();
        }
        this->del_if_not_root();
        return c_zero->delete_left();
    }
    char *BNode::delete_right()
    {
        if (this->leaf)
        {
            char *to_ret = this->keys[this->size - 1];
            this->keys[this->size - 1] = nullptr;
            this->size--;
            this->del_if_not_root();
            return to_ret;
        }
        uint32_t c_i_num = this->children[this->size];
        std::string c_i_str = get_file_name(c_i_num);
        BNode *c_i = BNode::read_disk(this->tree, c_i_str);
        if (c_i->size <= this->tree->t - 1)
        {
            uint32_t c_l_num = this->children[this->size - 1];
            std::string c_l_str = get_file_name(c_l_num);
            BNode *c_l = BNode::read_disk(this->tree, c_l_str);
            if (c_l->size >= this->tree->t)
            {
                shift_right(c_i->keys, 0, c_i->size);
                shift_right(c_i->children, 0, c_i->size + 1);
                c_i->keys[0] = this->keys[this->size - 1];
                this->keys[this->size - 1] = c_l->keys[c_l->size - 1];
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
                return c_i->delete_right();
            }
            this->merge(this->size - 1, c_l, c_i);
            this->del_if_not_root();
            return c_l->delete_right();
        }
        this->del_if_not_root();
        return c_i->delete_right();
    }
    char *BNode::delete_row_1(size_t idx)
    {
        char *to_ret = this->keys[idx];
        shift_left(this->keys, idx + 1, this->size);
        this->keys[this->size - 1] = nullptr;
        this->size--;
        this->changed = true;
        this->del_if_not_root();
        return to_ret;
    }
    char *BNode::delete_row_2(const char *key, size_t idx)
    {
        uint32_t c_i_num = this->children[idx];
        std::string c_i_str = get_file_name(c_i_num);
        BNode *c_i = BNode::read_disk(this->tree, c_i_str);
        if (c_i->size >= this->tree->t)
        {
            char *predecessor = c_i->delete_right();
            char *to_ret = this->keys[idx];
            this->keys[idx] = predecessor;
            this->changed = true;
            this->del_if_not_root();
            return to_ret;
        }
        uint32_t c_j_num = this->children[idx + 1];
        std::string c_j_str = get_file_name(c_j_num);
        BNode *c_j = BNode::read_disk(this->tree, c_j_str);
        if (c_j->size >= this->tree->t)
        {
            delete c_i;
            char *successor = c_j->delete_left();
            char *to_ret = this->keys[idx];
            this->keys[idx] = successor;
            this->changed = true;
            this->del_if_not_root();
            return to_ret;
        }
        this->merge(idx, c_i, c_j);
        this->del_if_not_root();
        return c_i->delete_row(key);
    }
    char *BNode::delete_row_3(const char *key, size_t idx)
    {
        uint32_t c_i_num = this->children[idx];
        std::string c_i_str = get_file_name(c_i_num);
        BNode *c_i = BNode::read_disk(this->tree, c_i_str);
        if (c_i->size <= this->tree->t - 1)
        {
            BNode *c_l = nullptr, *c_r = nullptr;
            if (idx > 0)
            {
                uint32_t c_l_num = this->children[idx - 1];
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
                uint32_t c_r_num = this->children[idx + 1];
                std::string c_r_str = get_file_name(c_r_num);
                c_r = BNode::read_disk(this->tree, c_r_str);
                if (c_r->size >= this->tree->t)
                {
                    c_i->keys[c_i->size] = this->keys[idx];
                    this->keys[idx] = c_r->keys[0];
                    c_i->children[c_i->size + 1] = c_r->children[0];
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
        std::remove(file_name.c_str());
        this->changed = false;
        delete this;
    }
    BNode::BNode(BTree *tree, uint32_t node_num)
        : tree(tree), node_num(node_num), leaf(false), changed(true), size(0)
    {
        this->keys.reserve(2 * this->tree->t - 1);
        this->keys.assign(this->keys.capacity(), nullptr);
        this->children.reserve(2 * this->tree->t);
        this->children.assign(this->children.capacity(), 0);
        this->columns = tree->columns;
    }
    void BNode::match_columns()
    {
        if (this->columns == this->tree->columns)
        {
            return;
        }
        std::vector<size_t> removed_col_idx;
        size_t i = 0, j = 0;
        while (i < this->columns.size() && j < tree->columns.size())
        {
            if (this->columns[i] != tree->columns[j])
            {
                removed_col_idx.push_back(i++);
            }
            else
            {
                i++;
                j++;
            }
        }
        while (i < this->columns.size())
        {
            removed_col_idx.push_back(i++);
        }
        for (size_t k = 0; k < this->size; k++)
        {
            char *new_pointer = new char[this->tree->width];
            std::memset(new_pointer, 0, this->tree->width);
            char *new_moving_pointer = new_pointer;
            char *old_moving_pointer = this->keys[k];
            size_t removed_idx = 0;
            for (size_t l = 0; l < this->columns.size(); l++)
            {
                if (removed_idx < removed_col_idx.size() && removed_col_idx[removed_idx] == l)
                {
                    removed_idx++;
                    old_moving_pointer += this->columns[l].width;
                }
                else
                {
                    std::memcpy(new_moving_pointer, old_moving_pointer, this->columns[l].width);
                    old_moving_pointer += this->columns[l].width;
                    new_moving_pointer += this->columns[l].width;
                }
            }
            delete[] this->keys[k];
            this->keys[k] = new_pointer;
        }
        this->columns = tree->columns;
        this->changed = true;
    }
    BNode::~BNode()
    {
        if (this->changed)
        {
            this->write_disk();
        }
        for (uint32_t i = 0; i < this->size; i++)
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
        while (cur_idx < this->size && this->compare_key(key, this->keys[cur_idx], 0) > 0)
        {
            cur_idx++;
        }
        if (cur_idx < this->size && this->compare_key(key, this->keys[cur_idx], 0) == 0)
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
    void BNode::find_all_indexed(const char *key, std::vector<char *> &alls)
    {
        int low_proper = this->first_child_idx(key);
        int high_proper = this->last_child_idx(key);
        for (int i = low_proper; i < high_proper && this->compare_key(key, this->keys[i], 0) == 0; i++)
        {
            char *to_add = new char[this->tree->width];
            std::memcpy(to_add, this->keys[i], this->tree->width);
            alls.push_back(to_add);
        }
        if (this->leaf)
        {
            this->del_if_not_root();
            return;
        }
        std::vector<uint32_t> proper_children;
        proper_children.insert(proper_children.end(), this->children.begin() + low_proper, this->children.begin() + high_proper + 1);
        BTree *tree = this->tree;
        this->del_if_not_root();
        for (int i = 0; i < proper_children.size(); i++)
        {
            std::string c_i_file_name = get_file_name(proper_children[i]);
            BNode *c_i = BNode::read_disk(tree, c_i_file_name);
            c_i->find_all_indexed(key, alls);
        }
    }
    void BNode::find_all_unindexed(const char *k, size_t col_idx, size_t preceding_size, std::vector<char *> &alls)
    {
        for (int i = 0; i < this->size; i++)
        {
            if (this->compare_key(k, this->keys[i] + preceding_size, col_idx) == 0) // use compare_key
            {
                char *to_ret = new char[this->tree->width];
                std::memcpy(to_ret, this->keys[i], this->tree->width);
                alls.push_back(to_ret);
            }
        }
        if (this->leaf)
        {
            this->del_if_not_root();
            return;
        }
        std::vector<uint32_t> this_children;
        this_children.insert(this_children.end(), this->children.begin(), this->children.begin() + this->size + 1);
        BTree *tree = this->tree;
        this->del_if_not_root();
        for (int i = 0; i < this_children.size(); i++)
        {
            std::string c_i_str = get_file_name(this_children[i]);
            BNode *c_i = BNode::read_disk(tree, c_i_str);
            c_i->find_all_unindexed(k, col_idx, preceding_size, alls);
        }
    }
    void BNode::insert(const char *src)
    {
        if (this->leaf)
        {
            size_t cur_idx = 0;
            while (cur_idx < this->size && this->compare_key(src, this->keys[cur_idx], 0) > 0)
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
            while (cur_idx < this->size && this->compare_key(src, this->keys[cur_idx], 0) > 0)
            {
                cur_idx++;
            }
            size_t c_num = this->children[cur_idx];
            std::string c_i_file = get_file_name(c_num);
            BNode *c_i = BNode::read_disk(this->tree, c_i_file);
            if (c_i->full())
            {
                BNode *new_node = this->split_children(cur_idx, c_i);
                if (this->compare_key(src, this->keys[cur_idx], 0) > 0)
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
    char *BNode::delete_row(const char *key)
    {
        size_t idx = 0;
        while (idx < this->size && this->compare_key(key, this->keys[idx], 0) > 0)
        {
            idx++;
        }
        if (idx < this->size && this->compare_key(key, this->keys[idx], 0) == 0 && this->leaf)
        {
            return this->delete_row_1(idx);
        }
        else if (idx < this->size && this->compare_key(key, this->keys[idx], 0) == 0 && !this->leaf)
        {
            return this->delete_row_2(key, idx);
        }
        else if (!this->leaf)
        {
            return this->delete_row_3(key, idx);
        }
        else
        {
            this->del_if_not_root();
            return nullptr;
        }
    }
    void BNode::write_disk()
    {
        if (!this->changed)
        {
            return;
        }
        std::string where = std::filesystem::path(this->tree->get_path()) / get_file_name(this->node_num);
        char *write_buffer = new char[STARTING_BUFFER_SZ];
        size_t bytes_processed = 0;
        //std::string file_name = "node_" + std::to_string(this->node_num) + ".rsql";
        int node_file_fd = open(where.c_str(), O_APPEND | O_CREAT | O_TRUNC | O_WRONLY, 0644);
        uint32_t col_num = this->columns.size();
        char *col_pad = reinterpret_cast<char *>(&col_num);
        std::memcpy(write_buffer + bytes_processed, col_pad, 4);
        bytes_processed += 4;
        for (uint32_t i = 0; i < col_num; i++)
        {
            if (STARTING_BUFFER_SZ - bytes_processed < 16){
                write(node_file_fd, write_buffer, bytes_processed);
                bytes_processed = 0;
            }
            uint32_t cur_col_id = this->columns[i].col_id;
            char *col_num_pad = reinterpret_cast<char *>(&cur_col_id);
            std::memcpy(write_buffer + bytes_processed, col_num_pad, 4);
            bytes_processed += 4;

            std::string type = dt_to_str(this->columns[i].type);
            std::memcpy(write_buffer + bytes_processed, type.c_str(), 4);
            bytes_processed += 4;

            uint32_t width = this->columns[i].width;
            char *w_pad = reinterpret_cast<char *>(&width);
            std::memcpy(write_buffer + bytes_processed, w_pad, 4);
            bytes_processed += 4;

        }
        uint32_t this_size = this->size;
        char *npad = reinterpret_cast<char *>(&this_size);
        std::memcpy(write_buffer + bytes_processed, npad, 4);
        bytes_processed += 4;

        for (uint32_t i = 0; i < this->size; i++)
        {
            if (STARTING_BUFFER_SZ - bytes_processed < this->tree->width){
                write(node_file_fd, write_buffer, bytes_processed);
                bytes_processed = 0;
            }
            std::memcpy(write_buffer + bytes_processed, this->keys[i], this->tree->width);
            bytes_processed += this->tree->width;
        }
        for (uint32_t i = 0; i <= this->size; i++)
        {
            if (STARTING_BUFFER_SZ - bytes_processed < 5){
                write(node_file_fd, write_buffer, bytes_processed);
                bytes_processed = 0;
            }
            uint32_t cur_child = this->children[i];
            char *cpad = reinterpret_cast<char *>(&cur_child);
            std::memcpy(write_buffer + bytes_processed, cpad, 4);
            bytes_processed += 4;
        }
        char *l_pad = reinterpret_cast<char *>(&this->leaf);
        std::memcpy(write_buffer + bytes_processed, l_pad, 1);
        bytes_processed += 1;
        write(node_file_fd, write_buffer, bytes_processed);
        close(node_file_fd);
        delete[] write_buffer;
        this->changed = false;
    }
}