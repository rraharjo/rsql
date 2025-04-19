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
        char *read_buffer = new char[STARTING_BUFFER_SZ];
        if (cur_read_bytes = read(fd, read_buffer, STARTING_BUFFER_SZ) < 0)
        {
            throw std::runtime_error("Failed to read file");
            return nullptr;
        }
        Table *new_table = new Table(db, table_name);
        bytes_processed += TABLE_NAME_SIZE;
        new_table->table_name = table_name;
        uint32_t col_num;
        std::memcpy(&col_num, read_buffer + bytes_processed, 4);
        bytes_processed += 4;
        for (uint32_t i = 0; i < col_num; i++)
        {
            if (cur_read_bytes - bytes_processed < COL_NAME_SIZE + 4)
            {
                std::memcpy(read_buffer, read_buffer + bytes_processed, cur_read_bytes - bytes_processed);
                bytes_processed = cur_read_bytes - bytes_processed;
                if (cur_read_bytes = read(fd, read_buffer + bytes_processed, STARTING_BUFFER_SZ - bytes_processed) < 0)
                {
                    delete[] read_buffer;
                    delete new_table;
                    throw std::runtime_error("Failed to read file");
                    return nullptr;
                }
                cur_read_bytes = read(fd, read_buffer, STARTING_BUFFER_SZ);
                bytes_processed = 0;
            }
            char col_name[COL_NAME_SIZE];
            uint32_t col_idx;
            std::memset(col_name, 0, COL_NAME_SIZE);
            std::memcpy(col_name, read_buffer + bytes_processed, COL_NAME_SIZE);
            bytes_processed += COL_NAME_SIZE;
            std::memcpy(&col_idx, read_buffer + bytes_processed, 4);
            bytes_processed += 4;
            new_table->col_name_indexes[std::string(col_name, COL_NAME_SIZE)] = col_idx;
        }
        new_table->db = db;
        new_table->changed = false;
        delete[] read_buffer;
        return new_table;
    }

    Table::Table(const Database *db, const std::string table_name) : changed(true), db(db)
    {
        this->table_name = table_name;
        try
        {
            this->primary_tree = BTree::read_disk(this);
        }
        catch (std::invalid_argument &e)
        {
            this->primary_tree = new BTree();
            primary_tree->table = this;
        }
        // this->add_column("_key", Column::get_column(0, DataType::PKEY, 0));
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
        return this->primary_tree->columns[it->second].width;
    }
    std::string Table::get_path() const
    {
        return std::filesystem::path(this->db->get_path()) / this->table_name;
    }
    void Table::add_column(const std::string name, const rsql::Column col)
    {
        if (this->col_name_indexes.find(name) != this->col_name_indexes.end())
        {
            throw std::runtime_error("Column already exists");
        }
        this->primary_tree->add_column(col);
        this->col_name_indexes[name] = this->primary_tree->columns.size() - 1;
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
        uint32_t col_idx = it->second;
        this->primary_tree->remove_column(col_idx);
        this->col_name_indexes.erase(it);
        for (auto &pair : this->col_name_indexes)
        {
            if (pair.second > col_idx)
            {
                pair.second--;
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
    std::vector<char *> Table::find_row(const char *key, const std::string col_name)
    {
        auto it = this->col_name_indexes.find(col_name);
        if (it == this->col_name_indexes.end())
        {
            const std::string err_msg = this->table_name + " does not have a column named " + col_name;
            throw err_msg;
        }
        size_t col_idx = (size_t)it->second;
        if (col_idx)
        {
            return this->primary_tree->find_all_row(key, col_idx);
        }
        else
        {
            char *found = this->primary_tree->find_row(key);
            std::vector<char *> to_ret;
            if (found)
            {
                to_ret.push_back(found);
            }
            return to_ret;
        }
    }

    void Table::write_disk()
    {
        if (!this->changed)
        {
            return;
        }
        char *write_buffer = new char[STARTING_BUFFER_SZ];
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
            delete[] write_buffer;
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
            if (STARTING_BUFFER_SZ - bytes_processed < COL_NAME_SIZE + 4)
            {
                if (write(fd, write_buffer, bytes_processed) < 0)
                {
                    close(fd);
                    delete[] write_buffer;
                    throw std::runtime_error("Failed to write table");
                };
                bytes_processed = 0;
            }
            std::memset(write_buffer + bytes_processed, 0, COL_NAME_SIZE);
            std::memcpy(write_buffer + bytes_processed, col.first.c_str(), col.first.size());
            bytes_processed += COL_NAME_SIZE;
            std::memcpy(write_buffer + bytes_processed, &col.second, 4);
            bytes_processed += 4;
        }
        if (write(fd, write_buffer, bytes_processed) < 0)
        {
            close(fd);
            delete[] write_buffer;
            throw std::runtime_error("Failed to write table");
        }
        close(fd);
        this->changed = false;
        delete[] write_buffer;
    }
}