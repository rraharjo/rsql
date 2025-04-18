#include "table.h"
#include "database.h"
namespace rsql
{
    Table *Table::create_new_table(Database *db, const std::string table_name)
    {
        std::string where = std::filesystem::path(ROOT_FOLDER) / db->db_name / table_name;
        if (std::filesystem::exists(where))
        {
            throw std::runtime_error("Table already exists");
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
            throw std::runtime_error("Table does not exist");
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
        this->add_column("_key", Column::get_column(0, DataType::PKEY, 0));
    }
    Table::~Table()
    {
        delete this->primary_tree;
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
            throw std::runtime_error("Column does not exist");
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
        delete[] write_buffer;
    }
}