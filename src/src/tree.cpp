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
    void BTree::get_root_node()
    {
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
        char *starting_buffer = new char[STARTING_BUFFER_SZ];
        int tree_file_fd = open(TREE_FILE, O_RDONLY);
        if (tree_file_fd < 0)
        {
            delete[] starting_buffer;
            throw std::invalid_argument("Fail to open tree.rsql");
        }
        size_t bytes_processed = 0, cur_read_bytes = 0;
        if ((cur_read_bytes = read(tree_file_fd, starting_buffer, STARTING_BUFFER_SZ)) < 0)
        {
            delete[] starting_buffer;
            throw std::invalid_argument("Fail to read");
        }
        BTree *to_ret = new BTree();
        char s_pad[sizeof(uint32_t)];
        std::memcpy(s_pad, starting_buffer + bytes_processed, 4);
        bytes_processed += 4;
        uint32_t col_size = *reinterpret_cast<uint32_t *>(s_pad);
        for (uint32_t i = 0; i < col_size; i++)
        {
            if (cur_read_bytes - bytes_processed < 12)
            {
                std::memcpy(starting_buffer, starting_buffer + bytes_processed, cur_read_bytes - bytes_processed);
                bytes_processed = cur_read_bytes - bytes_processed;
                if ((cur_read_bytes = read(tree_file_fd, starting_buffer + bytes_processed, STARTING_BUFFER_SZ - bytes_processed)) < 0)
                {
                    delete[] starting_buffer;
                    throw std::invalid_argument("Fail to read");
                }
                bytes_processed = 0;
            }
            char c_id[4];
            std::memcpy(c_id, starting_buffer + bytes_processed, 4);
            bytes_processed += 4;
            uint32_t col_id = *reinterpret_cast<uint32_t *>(c_id);

            char c_type[DT_STR_LEN];
            std::memcpy(c_type, starting_buffer + bytes_processed, 4);
            bytes_processed += 4;
            std::string c_type_str(c_type, DT_STR_LEN);

            char c_len[sizeof(uint32_t)];
            std::memcpy(c_len, starting_buffer + bytes_processed, 4);
            bytes_processed += 4;
            uint32_t c_width = *reinterpret_cast<uint32_t *>(c_len);
            to_ret->add_column(Column::get_column(col_id, str_to_dt(c_type_str), (size_t)c_width));
        }
        if (cur_read_bytes - bytes_processed < 12)
        {
            std::memcpy(starting_buffer, starting_buffer + bytes_processed, cur_read_bytes - bytes_processed);
            bytes_processed = cur_read_bytes - bytes_processed;
            if ((cur_read_bytes = read(tree_file_fd, starting_buffer + bytes_processed, STARTING_BUFFER_SZ - bytes_processed)) < 0)
            {
                delete[] starting_buffer;
                throw std::invalid_argument("Fail to read");
            }
            bytes_processed = 0;
        }
        std::memcpy(s_pad, starting_buffer + bytes_processed, 4);
        bytes_processed += 4;
        uint32_t root_num = *reinterpret_cast<uint32_t *>(s_pad);
        std::memcpy(s_pad, starting_buffer + bytes_processed, 4);
        bytes_processed += 4;
        uint32_t max_node_num = *reinterpret_cast<uint32_t *>(s_pad);
        std::memcpy(s_pad, starting_buffer + bytes_processed, 4);
        uint32_t max_col_id = *reinterpret_cast<uint32_t *>(s_pad);
        delete[] starting_buffer;
        to_ret->root_num = root_num;
        to_ret->max_node_num = max_node_num;
        to_ret->max_col_id = max_col_id;
        close(tree_file_fd);
        return to_ret;
    }
    char *BTree::find_row(const char *key)
    {
        this->get_root_node();
        return this->root->find(key);
    }
    std::vector<char *> BTree::find_all_row(const char *key, size_t col_idx)
    {
        this->get_root_node();
        std::vector<char *> to_ret;
        if (col_idx == 0)
        {
            this->root->find_all_indexed(key, to_ret);
        }
        else
        {
            size_t preceding_size = 0;
            for (int i = 0; i < col_idx; i++)
            {
                preceding_size += this->columns[i].width;
            }
            this->root->find_all_unindexed(key, col_idx, preceding_size, to_ret);
        }
        return to_ret;
    }
    void BTree::insert_row(const char *src)
    {
        this->get_root_node();
        if (this->root->full())
        {
            BNode *new_root = new BNode(this, ++this->max_node_num);
            this->root_num = this->max_node_num;
            new_root->leaf = false;
            new_root->children[0] = this->root->node_num;
            BNode *new_children = new_root->split_children(0, this->root);
            delete new_children;
            delete this->root;
            this->root = new_root;
            this->root->insert(src);
        }
        else
        {
            this->root->insert(src);
        }
    }
    char *BTree::delete_row(const char *key)
    {
        this->get_root_node();
        char *to_ret = this->root->delete_row(key);
        if (this->root->size == 0)
        {
            uint32_t new_root_num = this->root->children[0];
            std::string new_root_name = "node_" + std::to_string(new_root_num) + ".rsql";
            BNode *new_root = BNode::read_disk(this, new_root_name);
            this->root->destroy();
            this->root = new_root;
            this->root_num = this->root->node_num;
        }
        return to_ret;
    }
    std::vector<char *> BTree::delete_all(const char *key, size_t idx)
    {
        std::vector<char *> to_ret;
        if (idx == 0)
        {
            char *to_add = nullptr;
            while ((to_add = this->delete_row(key)))
            {
                to_ret.push_back(to_add);
                to_add = nullptr;
            }
        }
        else
        {
            std::vector<char *> keys = this->find_all_row(key, idx);
            std::vector<char *> rows = this->batch_delete(keys);
            for (const char *key : keys)
            {
                delete[] key;
            }
            return rows;
        }
        return to_ret;
    }
    std::vector<char *> BTree::batch_delete(const std::vector<char *> &keys)
    {
        std::vector<char *> to_ret;
        for (int i = 0; i < keys.size(); i++)
        {
            std::vector<char *> cur_keys = this->delete_all(keys[i], 0);
            to_ret.insert(to_ret.end(), cur_keys.begin(), cur_keys.end());
        }
        return to_ret;
    }

    void BTree::add_column(const Column c)
    {
        this->get_root_node();
        this->columns.push_back(c);
        this->width += c.width;
        this->columns[this->columns.size() - 1].col_id = ++this->max_col_id;
        this->root->match_columns();
    }
    void BTree::remove_column(const size_t idx)
    {
        this->get_root_node();
        this->width -= this->columns[idx].width;
        this->columns.erase(this->columns.begin() + idx);
        this->root->match_columns();
    }
    void BTree::write_disk()
    {
        size_t num_of_col = this->columns.size();
        char *write_buffer = new char[4 * 4 + num_of_col * COLUMN_BYTES];
        size_t processed_bytes = 0;
        int tree_file_fd = open(TREE_FILE, O_APPEND | O_CREAT | O_TRUNC | O_WRONLY, 0644);
        if (tree_file_fd < 0)
        {
            throw std::invalid_argument("Can't open tree file");
        }
        uint32_t col_size = this->columns.size();
        char *s_pad = reinterpret_cast<char *>(&col_size);
        std::memcpy(write_buffer + processed_bytes, s_pad, 4);
        processed_bytes += 4;
        for (uint32_t i = 0; i < col_size; i++)
        {
            uint32_t cur_col_id = this->columns[i].col_id;
            char *col_pad = reinterpret_cast<char *>(&cur_col_id);
            std::memcpy(write_buffer + processed_bytes, col_pad, 4);
            processed_bytes += 4;

            std::string type = dt_to_str(this->columns[i].type);
            std::memcpy(write_buffer + processed_bytes, type.c_str(), 4);
            processed_bytes += 4;

            uint32_t width = this->columns[i].width;
            char *w_pad = reinterpret_cast<char *>(&width);
            std::memcpy(write_buffer + processed_bytes, w_pad, 4);
            processed_bytes += 4;
        }
        char *r_pad = reinterpret_cast<char *>(&this->root_num);
        std::memcpy(write_buffer + processed_bytes, r_pad, 4);
        processed_bytes += 4;

        char *max_node_pad = reinterpret_cast<char *>(&this->max_node_num);
        std::memcpy(write_buffer + processed_bytes, max_node_pad, 4);
        processed_bytes += 4;

        char *max_col_id = reinterpret_cast<char *>(&this->max_col_id);
        std::memcpy(write_buffer + processed_bytes, max_col_id, 4);
        processed_bytes += 4;

        write(tree_file_fd, write_buffer, processed_bytes);
        close(tree_file_fd);
        delete[] write_buffer;
    }
}