#include "tree.h"
#include "table.h"
#include "database.h"
namespace rsql
{
    BTree::BTree(Table *table)
        : root_num(1), max_node_num(1), t(DEGREE), root(nullptr), max_col_id(0), width(0), table(table), tree_num(0)
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
                this->root->leaf = 1;
            }
        }
    }
    BTree *BTree::read_disk(Table *table, const uint32_t tree_num)
    {
        static char starting_buffer[DISK_BUFFER_SZ];
        std::string where;
        if (!table)
        {
            where = TREE_FILE;
        }
        else
        {
            where = std::filesystem::path(table->get_path()) / std::to_string(tree_num) / TREE_FILE;
        }

        int tree_file_fd = open(where.c_str(), O_RDONLY);
        if (tree_file_fd < 0)
        {
            throw std::invalid_argument("Fail to open tree.rsql");
            return nullptr;
        }
        size_t bytes_processed = 0, cur_read_bytes = 0;
        if ((cur_read_bytes = read(tree_file_fd, starting_buffer, DISK_BUFFER_SZ)) < 0)
        {
            throw std::invalid_argument("Fail to read");
            return nullptr;
        }
        BTree *to_ret = new BTree(table);
        to_ret->table = table;
        uint32_t root_num;
        std::memcpy(&root_num, starting_buffer + bytes_processed, 4);
        bytes_processed += 4;

        uint32_t max_node_num;
        std::memcpy(&max_node_num, starting_buffer + bytes_processed, 4);
        bytes_processed += 4;

        uint32_t max_col_id;
        std::memcpy(&max_col_id, starting_buffer + bytes_processed, 4);
        bytes_processed += 4;

        uint32_t this_tree_num;
        std::memcpy(&this_tree_num, starting_buffer + bytes_processed, 4);
        bytes_processed += 4;

        uint8_t this_unique;
        std::memcpy(&this_unique, starting_buffer + bytes_processed, 1);
        bytes_processed += 1;

        uint32_t col_size;
        std::memcpy(&col_size, starting_buffer + bytes_processed, 4);
        bytes_processed += 4;

        to_ret->root_num = root_num;
        to_ret->max_node_num = max_node_num;
        to_ret->max_col_id = max_col_id;
        to_ret->tree_num = this_tree_num;
        to_ret->unique_key = (bool)this_unique;

        uint32_t col_id;
        std::string col_type_str;
        col_type_str.resize(4);
        uint32_t col_width;
        for (uint32_t i = 0; i < col_size; i++)
        {
            if (cur_read_bytes - bytes_processed < 12)
            {
                size_t remaining_bytes = cur_read_bytes - bytes_processed;
                std::memmove(starting_buffer, starting_buffer + bytes_processed, remaining_bytes);
                if ((cur_read_bytes = read(tree_file_fd, starting_buffer + remaining_bytes, DISK_BUFFER_SZ - remaining_bytes)) < 0)
                {
                    throw std::runtime_error("Error reading tree.rsql file");
                    return nullptr;
                };
                cur_read_bytes += remaining_bytes;
                bytes_processed = 0;
            }
            std::memcpy(&col_id, starting_buffer + bytes_processed, 4);
            bytes_processed += 4;

            std::memcpy(col_type_str.data(), starting_buffer + bytes_processed, 4);
            bytes_processed += 4;

            std::memcpy(&col_width, starting_buffer + bytes_processed, 4);
            bytes_processed += 4;

            to_ret->columns.push_back(Column::get_column(col_id, str_to_dt(col_type_str), (size_t)col_width));
            to_ret->width += (size_t)col_width;
        }
        close(tree_file_fd);
        to_ret->get_root_node();
        return to_ret;
    }
    BTree *BTree::create_new_tree(Table *table, const uint32_t tree_num, bool unique_key)
    {
        BTree *to_ret = new BTree(table);
        to_ret->tree_num = tree_num;
        to_ret->unique_key = unique_key;
        to_ret->get_root_node();
        return to_ret;
    }
    std::vector<char *> BTree::find_all_row(const char *key, const size_t col_idx)
    {
        std::vector<char *> to_ret;
        if (col_idx == 0)
        {
            if (this->unique_key)
            {
                char *res = this->root->find(key);
                if (res)
                {
                    to_ret.push_back(res);
                }
            }
            else
            {
                this->root->find_all_indexed(key, to_ret);
            }
        }
        else
        {
            size_t preceding_size = 0;
            for (size_t i = 0; i < col_idx; i++)
            {
                preceding_size += this->columns[i].width;
            }
            this->root->find_all_unindexed(key, col_idx, preceding_size, to_ret);
        }
        return to_ret;
    }
    std::vector<char *> BTree::search_rows(const char *key, CompSymbol symbol, Comparison *comparison)
    {
        std::vector<char *> result;
        if (key == nullptr)
        {
            if (comparison == nullptr){
                comparison = new ORComparisons();
                this->root->linear_search(result, comparison);
                delete comparison;
            }
            else{
                this->root->linear_search(result, comparison);
            }
        }
        else
        {
            this->root->indexed_search(result, key, symbol, comparison);
        }
        return result;
    }
    void BTree::insert_row(const char *src)
    {
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
    char *BTree::delete_row(const char *key, Comparison *comp)
    {
        char *to_ret = this->root->delete_row(key, comp);
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
    std::vector<char *> BTree::delete_all_row(const char *key, Comparison *comp){
        std::vector<char *> to_ret;
        if (key){
            char *to_add = nullptr;
            while ((to_add = this->delete_row(key, comp))){
                to_ret.push_back(to_add);
                to_add = nullptr;
            }
        }
        else{
            std::vector<char *> to_del = this->search_rows(key, rsql::CompSymbol::EQ, comp);
            for (const char *row : to_del){
                to_ret.push_back(this->delete_row(row, comp));
                delete[] row;
            }
        }
        return to_ret;
    }
    std::vector<char *> BTree::delete_all(const char *key, const size_t idx)
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
        for (size_t i = 0; i < keys.size(); i++)
        {
            std::vector<char *> cur_keys = this->delete_all(keys[i], 0);
            to_ret.insert(to_ret.end(), cur_keys.begin(), cur_keys.end());
        }
        return to_ret;
    }

    std::string BTree::get_path() const
    {
        if (this->table)
        {
            return std::filesystem::path(this->table->get_path()) / std::to_string(this->tree_num);
        }
        else
        {
            return "";
        }
    }
    void BTree::add_column(const Column c)
    {
        this->columns.push_back(c);
        this->width += c.width;
        this->columns[this->columns.size() - 1].col_id = ++this->max_col_id;
        this->root->match_columns();
    }
    void BTree::remove_column(const size_t idx)
    {
        if (idx == 0)
        {
            throw std::invalid_argument("Can't delete the first column");
            return;
        }
        this->width -= this->columns[idx].width;
        this->columns.erase(this->columns.begin() + idx);
        this->root->match_columns();
    }
    void BTree::destroy(){
        std::string rm_command = "rm -r " + this->get_path();
        delete this;
        std::system(rm_command.c_str());
    }
    void BTree::write_disk()
    {
        std::string where;
        if (this->table == nullptr)
        {
            where = TREE_FILE;
        }
        else
        {
            where = std::filesystem::path(table->get_path()) / std::to_string(this->tree_num) / TREE_FILE;
        }
        static char write_buffer[DISK_BUFFER_SZ];
        ssize_t bytes_processed = 0;
        int tree_file_fd = open(where.c_str(), O_APPEND | O_CREAT | O_TRUNC | O_WRONLY, 0644);
        if (tree_file_fd < 0)
        {
            throw std::invalid_argument("Can't open tree file");
        }
        std::memcpy(write_buffer + bytes_processed, &this->root_num, 4);
        bytes_processed += 4;

        std::memcpy(write_buffer + bytes_processed, &this->max_node_num, 4);
        bytes_processed += 4;

        std::memcpy(write_buffer + bytes_processed, &this->max_col_id, 4);
        bytes_processed += 4;

        std::memcpy(write_buffer + bytes_processed, &this->tree_num, 4);
        bytes_processed += 4;

        std::memcpy(write_buffer + bytes_processed, &this->unique_key, 1);
        bytes_processed += 1;

        uint32_t col_size = this->columns.size();
        std::memcpy(write_buffer + bytes_processed, &col_size, 4);
        bytes_processed += 4;

        uint32_t cur_col_id;
        std::string type;
        type.resize(4);
        uint32_t width;
        for (uint32_t i = 0; i < col_size; i++)
        {
            if (DISK_BUFFER_SZ - bytes_processed < 12)
            {
                if (write(tree_file_fd, write_buffer, bytes_processed) != bytes_processed)
                {
                    throw std::runtime_error("Failed to write to file");
                    return;
                };
                bytes_processed = 0;
            }
            cur_col_id = this->columns[i].col_id;
            std::memcpy(write_buffer + bytes_processed, &cur_col_id, 4);
            bytes_processed += 4;

            type = dt_to_str(this->columns[i].type);
            std::memcpy(write_buffer + bytes_processed, type.data(), 4);
            bytes_processed += 4;

            width = this->columns[i].width;
            std::memcpy(write_buffer + bytes_processed, &width, 4);
            bytes_processed += 4;
        }
        write(tree_file_fd, write_buffer, bytes_processed);
        close(tree_file_fd);
    }
}