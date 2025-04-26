#include "table.h"
#include "database.h"
namespace rsql
{
    Table *Table::create_new_table(Database *db, const std::string table_name)
    {
        std::string where = std::filesystem::path(db->get_path()) / table_name;
        if (std::filesystem::exists(where))
        {
            throw std::invalid_argument("Table already exists");
            return nullptr;
        }
        Table *new_table = new Table(db, table_name);
        new_table->db = db;
        std::filesystem::create_directories(where);
        std::string tree_folder = std::filesystem::path(new_table->get_path()) / std::to_string(new_table->primary_tree_num);
        std::filesystem::create_directory(tree_folder);
        new_table->primary_tree = BTree::create_new_tree(new_table, new_table->primary_tree_num);
        new_table->write_disk();
        return new_table;
    }
    Table *Table::load_table(Database *db, const std::string table_name)
    {
        std::string where = std::filesystem::path(ROOT_FOLDER) / db->db_name / table_name;
        if (!std::filesystem::exists(where))
        {
            throw std::invalid_argument("Table does not exist");
            return nullptr;
        }
        std::string file_name = std::filesystem::path(where) / TABLE_FILE_NAME;
        size_t bytes_processed = 0, cur_read_bytes = 0;
        int fd = open(file_name.c_str(), O_RDONLY);
        if (fd < 0)
        {
            throw std::runtime_error("Failed to open file");
            return nullptr;
        }
        static char read_buffer[DISK_BUFFER_SZ];
        if ((cur_read_bytes = read(fd, read_buffer, DISK_BUFFER_SZ)) < 0)
        {
            throw std::runtime_error("Failed to read file");
            return nullptr;
        }
        Table *new_table = new Table(db, table_name);
        bytes_processed += TABLE_NAME_SIZE;
        uint32_t col_num;
        std::memcpy(&col_num, read_buffer + bytes_processed, 4);
        bytes_processed += 4;
        char col_name[COL_NAME_SIZE];
        uint32_t col_idx;
        uint32_t tree_num;
        for (uint32_t i = 0; i < col_num; i++)
        {
            if (cur_read_bytes - bytes_processed < COL_NAME_SIZE + 8)
            {
                size_t remaining_bytes = cur_read_bytes - bytes_processed;
                std::memmove(read_buffer, read_buffer + bytes_processed, remaining_bytes);
                if ((cur_read_bytes = read(fd, read_buffer + remaining_bytes, DISK_BUFFER_SZ - remaining_bytes)) < 0)
                {
                    delete new_table;
                    throw std::runtime_error("Failed to read file");
                    return nullptr;
                }
                cur_read_bytes += remaining_bytes;
                bytes_processed = 0;
            }
            std::memset(col_name, 0, COL_NAME_SIZE);
            std::memcpy(col_name, read_buffer + bytes_processed, COL_NAME_SIZE);
            bytes_processed += COL_NAME_SIZE;
            col_name[COL_NAME_SIZE - 1] = 0;
            std::memcpy(&col_idx, read_buffer + bytes_processed, 4);
            bytes_processed += 4;
            std::memcpy(&tree_num, read_buffer + bytes_processed, 4);
            bytes_processed += 4;
            new_table->col_name_indexes[std::string(col_name)] = std::make_pair(col_idx, tree_num);
        }
        if (cur_read_bytes - bytes_processed < 8)
        {
            size_t remaining_bytes = cur_read_bytes - bytes_processed;
            std::memmove(read_buffer, read_buffer + bytes_processed, remaining_bytes);
            if ((cur_read_bytes = read(fd, read_buffer + remaining_bytes, DISK_BUFFER_SZ - remaining_bytes)) < 0)
            {
                delete new_table;
                throw std::runtime_error("Failed to read file");
                return nullptr;
            }
            cur_read_bytes += remaining_bytes;
            bytes_processed = 0;
        }
        uint32_t primary_tree_num;
        std::memcpy(&primary_tree_num, read_buffer + bytes_processed, 4);
        bytes_processed += 4;
        uint32_t max_tree_num;
        std::memcpy(&max_tree_num, read_buffer + bytes_processed, 4);
        bytes_processed += 4;
        new_table->db = db;
        new_table->changed = false;
        new_table->primary_tree_num = primary_tree_num;
        new_table->max_tree_num = max_tree_num;
        new_table->primary_tree = BTree::read_disk(new_table, new_table->primary_tree_num);
        return new_table;
    }

    Table::Table(const Database *db, const std::string table_name)
        : max_tree_num(1), primary_tree_num(1), changed(true), db(db), table_name(table_name)
    {
        // try
        // {
        //     this->primary_tree = BTree::read_disk(this, this->primary_tree_num);
        // }
        // catch (std::invalid_argument &e)
        // {
        //     this->primary_tree = BTree::create_new_tree(this, this->max_tree_num);
        // }
    }
    Table::~Table()
    {
        if (this->changed)
        {
            this->write_disk();
        }
        delete this->primary_tree;
    }
    size_t Table::get_width() const
    {
        return this->primary_tree->width;
    }
    size_t Table::get_col_width(const std::string name) const
    {
        auto it = this->col_name_indexes.find(name);
        if (it == this->col_name_indexes.end())
        {
            std::string err_msg = "Table " + this->table_name + " has no column named " + name;
            throw std::invalid_argument(err_msg);
            return 0;
        }
        return this->primary_tree->columns[it->second.first].width;
    }
    std::string Table::get_path() const
    {
        return std::filesystem::path(this->db->get_path()) / this->table_name;
    }
    void Table::add_column(const std::string name, const rsql::Column col)
    {
        if (name.length() >= COL_NAME_SIZE)
        {
            std::string err_msg = "Column name must be < " + std::to_string(COL_NAME_SIZE) + " in length";
            throw std::invalid_argument(err_msg);
            return;
        }
        if (this->col_name_indexes.find(name) != this->col_name_indexes.end())
        {
            throw std::runtime_error("Column already exists");
        }
        this->primary_tree->add_column(col);
        this->col_name_indexes[name] = std::make_pair(this->primary_tree->columns.size() - 1, 0);
        if (this->col_name_indexes.size() == 1)
        {
            this->col_name_indexes[name].second = 1;
        }
        this->changed = true;
    }
    void Table::remove_column(const std::string col_name)
    {
        auto it = this->col_name_indexes.find(col_name);
        if (it == this->col_name_indexes.end())
        {
            std::string err_msg = this->table_name + " does not have a column named " + col_name;
            throw std::invalid_argument(err_msg);
            return;
        }
        uint32_t col_idx = it->second.first;
        this->primary_tree->remove_column(col_idx);
        this->col_name_indexes.erase(it);
        for (auto &pair : this->col_name_indexes)
        {
            if (pair.second.first > col_idx)
            {
                pair.second.first--;
            }
        }
        this->changed = true;
    }
    void Table::insert_row_bin(const char *row)
    {
        this->primary_tree->insert_row(row);
    }
    void Table::insert_row_text(const std::vector<std::string> &row)
    {
        if (row.size() != this->col_name_indexes.size())
        {
            throw std::invalid_argument("Row size does not match table size");
        }
        char *row_bin = new char[this->get_width()];
        std::memset(row_bin, 0, this->get_width());
        size_t inserted_bytes = 0;
        for (size_t i = 0; i < row.size(); i++)
        {
            switch (this->primary_tree->columns[i].type)
            {
            case DataType::DATE:
                if (!valid_date(row[i]))
                {
                    std::string err_msg = row[i] + " is not a valid date";
                    throw std::invalid_argument(err_msg);
                }
                std::memset(row_bin + inserted_bytes, 0, this->primary_tree->columns[i].width);
                std::memcpy(row_bin + inserted_bytes, row[i].c_str(), row[i].size());
                inserted_bytes += this->primary_tree->columns[i].width;
                break;
            case DataType::CHAR:
            case DataType::PKEY:
                std::memset(row_bin + inserted_bytes, 0, this->primary_tree->columns[i].width);
                std::memcpy(row_bin + inserted_bytes, row[i].c_str(), row[i].size());
                inserted_bytes += this->primary_tree->columns[i].width;
                break;
            case DataType::INT:
                boost::multiprecision::cpp_int int_val(row[i]);
                std::vector<unsigned char> buff;
                // My machine is using little endian :)
                export_bits(int_val, std::back_inserter(buff), 8, false);
                std::memcpy(row_bin + inserted_bytes, buff.data(), std::min(this->primary_tree->columns[i].width, buff.size()));
                inserted_bytes += this->primary_tree->columns[i].width;
                break;
            }
        }
        this->insert_row_bin(row_bin);
        delete[] row_bin;
    }
    std::vector<char *> Table::find_row(const char *key, size_t col_idx, uint32_t tree_num)
    {
        if (tree_num != 0)
        {
            if (col_idx == 0)
            {
                char *found = this->primary_tree->find_row(key);
                std::vector<char *> to_ret;
                if (found)
                {
                    to_ret.push_back(found);
                }
                return to_ret;
            }
            else
            {
                // If this fails, program should terminate
                BTree *optional_tree = BTree::read_disk(this, tree_num);
                size_t optional_first_col_length = optional_tree->columns[0].width;
                std::vector<char *> optional_keys = optional_tree->find_all_row(key, 0);
                std::vector<char *> to_ret;
                delete optional_tree;
                for (char *op_key : optional_keys)
                {
                    to_ret.push_back(this->primary_tree->find_row(op_key + optional_first_col_length));
                    delete[] op_key;
                }
                return to_ret;
            }
        }
        else
        {
            return this->primary_tree->find_all_row(key, col_idx);
        }
    }
    std::vector<char *> Table::find_row_bin(const char *key, const std::string col_name)
    {
        auto it = this->col_name_indexes.find(col_name);
        if (it == this->col_name_indexes.end())
        {
            const std::string err_msg = this->table_name + " does not have a column named " + col_name;
            throw err_msg;
        }
        size_t col_idx = (size_t)it->second.first;
        uint32_t tree_num = it->second.second;
        return this->find_row(key, col_idx, tree_num);
    }
    std::vector<char *> Table::find_row_text(std::string key, const std::string col_name)
    {
        auto it = this->col_name_indexes.find(col_name);
        if (it == this->col_name_indexes.end())
        {
            const std::string err_msg = this->table_name + " does not have a column named " + col_name;
            throw err_msg;
        }
        size_t col_idx = (size_t)it->second.first;
        uint32_t tree_num = it->second.second;
        if (this->primary_tree->columns[col_idx].type == DataType::INT)
        {
            boost::multiprecision::cpp_int new_key(key);
            std::vector<char> buff;
            export_bits(new_key, std::back_inserter(buff), 8, false);
            buff.resize(this->primary_tree->columns[col_idx].width);
            return this->find_row(buff.data(), col_idx, tree_num);
        }
        key.resize(this->primary_tree->columns[col_idx].width);
        return this->find_row(key.data(), col_idx, tree_num);
    }
    void Table::index_column(const std::string col_name)
    {
        auto it = this->col_name_indexes.find(col_name);
        if (it == this->col_name_indexes.end())
        {
            throw std::invalid_argument("Can't index column that doesn't exist");
            return;
        }
        if (it->second.second != 0)
        {
            throw std::invalid_argument("Can't index indexed column");
            return;
        }
        BTree *new_tree = new BTree();
        size_t preceding_size = 0;
        size_t indexed_col_index = it->second.first;
        Column indexed_col = this->primary_tree->columns[indexed_col_index];
        Column primary_col = this->primary_tree->columns[0];
        new_tree->table = this;
        new_tree->tree_num = ++this->max_tree_num;
        this->col_name_indexes[col_name].second = new_tree->tree_num;
        std::string where = std::filesystem::path(this->get_path()) / std::to_string(new_tree->tree_num);
        std::filesystem::create_directories(where);
        new_tree->add_column(indexed_col);
        new_tree->add_column(primary_col);
        for (size_t i = 0; i < indexed_col_index; i++)
        {
            preceding_size += this->primary_tree->columns[i].width;
        }

        char *new_row = new char[new_tree->width];
        std::queue<uint32_t> children;
        for (size_t i = 0; i < this->primary_tree->root->size; i++)
        {
            std::memcpy(new_row, this->primary_tree->root->keys[i] + preceding_size, indexed_col.width);
            std::memcpy(new_row + indexed_col.width, this->primary_tree->root->keys[i], primary_col.width);
            new_tree->insert_row(new_row);
        }
        if (!this->primary_tree->root->leaf)
        {
            for (size_t i = 0; i <= this->primary_tree->root->size; i++)
            {
                children.push(this->primary_tree->root->children[i]);
            }
        }
        while (!children.empty())
        {
            uint32_t this_child = children.front();
            children.pop();
            std::string cur_node_name = BNode::get_file_name(this_child);
            BNode *cur_node = BNode::read_disk(this->primary_tree, cur_node_name);
            cur_node->match_columns();
            for (size_t i = 0; i < cur_node->size; i++)
            {
                std::memcpy(new_row, cur_node->keys[i] + preceding_size, indexed_col.width);
                std::memcpy(new_row + indexed_col.width, cur_node->keys[i], primary_col.width);
                new_tree->insert_row(new_row);
            }
            if (!cur_node->leaf)
            {
                for (size_t i = 0; i <= cur_node->size; i++)
                {
                    children.push(cur_node->children[i]);
                }
            }
            delete cur_node;
        }
        this->changed = true;
        delete[] new_row;
        delete new_tree;
    }
    void Table::write_disk()
    {
        if (!this->changed)
        {
            return;
        }
        static char write_buffer[DISK_BUFFER_SZ];
        size_t bytes_processed = 0;
        std::string where = std::filesystem::path(this->db->get_path()) / this->table_name;
        if (!std::filesystem::exists(where))
        {
            std::filesystem::create_directory(where);
        }
        std::string file_name = std::filesystem::path(where) / TABLE_FILE_NAME;
        int fd = open(file_name.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0644);
        if (fd < 0)
        {
            throw std::runtime_error("Failed to open file");
        }
        std::memset(write_buffer, 0, TABLE_NAME_SIZE);
        std::memcpy(write_buffer, this->table_name.c_str(), this->table_name.size());
        bytes_processed += TABLE_NAME_SIZE;
        uint32_t col_num = this->col_name_indexes.size();
        std::memcpy(write_buffer + bytes_processed, &col_num, 4);
        bytes_processed += 4;
        for (auto &col : this->col_name_indexes)
        {
            if (DISK_BUFFER_SZ - bytes_processed < COL_NAME_SIZE + 8)
            {
                if (write(fd, write_buffer, bytes_processed) < 0)
                {
                    close(fd);
                    throw std::runtime_error("Failed to write table");
                };
                bytes_processed = 0;
            }
            std::memset(write_buffer + bytes_processed, 0, COL_NAME_SIZE);
            std::memcpy(write_buffer + bytes_processed, col.first.c_str(), col.first.size());
            bytes_processed += COL_NAME_SIZE;
            std::memcpy(write_buffer + bytes_processed, &col.second.first, 4);
            bytes_processed += 4;
            std::memcpy(write_buffer + bytes_processed, &col.second.second, 4);
            bytes_processed += 4;
        }
        if (DISK_BUFFER_SZ - bytes_processed < 8)
        {
            if (write(fd, write_buffer, bytes_processed) < 0)
            {
                close(fd);
                throw std::runtime_error("Failed to write table");
            };
            bytes_processed = 0;
        }
        std::memcpy(write_buffer + bytes_processed, &this->primary_tree_num, 4);
        bytes_processed += 4;
        std::memcpy(write_buffer + bytes_processed, &this->max_tree_num, 4);
        bytes_processed += 4;
        if (write(fd, write_buffer, bytes_processed) < 0)
        {
            close(fd);
            throw std::runtime_error("Failed to write table");
        }
        close(fd);
        this->changed = false;
    }
}