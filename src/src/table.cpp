#include <memory>
#include "table.h"
#include "database.h"
static inline bool name_idx_comp(const std::pair<std::string, uint32_t> &l, const std::pair<std::string, uint32_t> &r)
{
    return l.second < r.second;
}
namespace rsql
{
    bool operator==(const uintuint32 &left, const uintuint32 &right)
    {
        bool first_exist = left.pair.first == right.pair.first || left.pair.first == right.pair.second;
        bool right_exist = left.pair.second == right.pair.first || left.pair.second == right.pair.second;
        return first_exist && right_exist;
    }
    std::size_t PairComp::operator()(const uintuint32 &p) const
    {
        auto h1 = std::hash<uint32_t>()(p.pair.first);
        auto h2 = std::hash<uint32_t>()(p.pair.second);
        return h1 ^ h2;
    }
    Table *Table::create_new_table(Database *db, const std::string table_name, std::vector<std::string> col_names, std::vector<Column> columns)
    {
        if (col_names.size() != columns.size())
        {
            throw std::invalid_argument("Column names and column vector are not the same size");
            return nullptr;
        }

        std::string where = std::filesystem::path(db->get_path()) / table_name;
        if (std::filesystem::exists(where))
        {
            throw std::invalid_argument("Table already exists");
            return nullptr;
        }

        Table *new_table = new Table(db, table_name);

        std::filesystem::create_directories(where);
        std::string tree_folder = std::filesystem::path(new_table->get_path()) / std::to_string(new_table->primary_tree_num);

        std::filesystem::create_directory(tree_folder);
        new_table->primary_tree = BTree::create_new_tree(new_table, new_table->primary_tree_num);
        new_table->primary_tree->add_column(Column::get_column(0, DataType::DEFAULT_KEY, 0));
        new_table->col_name_indexes[DEF_KEY_COL_NAME] = 0;
        for (size_t i = 0; i < col_names.size(); i++)
        {
            new_table->add_column(col_names[i], columns[i]);
        }
        std::memset(new_table->next_default_key, 0, DEFAULT_KEY_WIDTH);
        new_table->write_disk();
        return new_table;
    }
    Table *Table::load_table(Database *db, const std::string table_name)
    {
        std::string where = std::filesystem::path(DATABASE_FILE_ROOT_FOLDER) / db->db_name / table_name;
        if (!std::filesystem::exists(where) || table_name == "")
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
            new_table->col_name_indexes[std::string(col_name)] = col_idx;
            if (tree_num > 0)
            {
                new_table->optional_trees[col_idx] = BTree::read_disk(new_table, tree_num);
            }
        }
        if (cur_read_bytes - bytes_processed < 12 + DEFAULT_KEY_WIDTH)
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
        std::memcpy(new_table->next_default_key, read_buffer + bytes_processed, DEFAULT_KEY_WIDTH);
        bytes_processed += DEFAULT_KEY_WIDTH;
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
        : table_name(table_name), primary_tree_num(1), max_tree_num(1), db(db), changed(true)
    {
    }
    Table::~Table()
    {
        if (this->changed)
        {
            this->write_disk();
        }
        delete this->primary_tree;
        for (auto &pair : this->optional_trees)
        {
            delete pair.second;
        }
    }
    char *Table::convert_texts_to_char_stream(const std::vector<std::string> &values)
    {
        if (values.size() != this->col_name_indexes.size() - 1)
        {
            std::string err_msg = "Table " + this->table_name + " has " + std::to_string(this->col_name_indexes.size()) + " columns, but values only have " + std::to_string(values.size()) + " entries.";
            throw std::invalid_argument(err_msg);
            return nullptr;
        }
        char *buff = new char[this->get_width() - DEFAULT_KEY_WIDTH];
        size_t cur_position = 0;
        for (size_t i = 1; i < this->primary_tree->get_columns().size(); i++)
        {
            this->primary_tree->get_columns()[i].process_string(buff + cur_position, values[i - 1]);
            cur_position += this->primary_tree->get_columns()[i].width;
        }
        return buff;
    }
    std::vector<std::string> Table::convert_char_stream_to_texts(const char *const stream)
    {
        size_t cur_position = 0;
        std::vector<std::string> to_ret;
        for (size_t i = 0; i < this->primary_tree->get_columns().size(); i++)
        {
            std::string string_view = this->primary_tree->get_columns()[i].process_stream(stream + cur_position);
            to_ret.push_back(string_view);
            cur_position += this->primary_tree->get_columns()[i].width;
        }
        return to_ret;
    }
    Column Table::get_column(std::string col_name)
    {
        auto it = this->col_name_indexes.find(col_name);
        if (it == this->col_name_indexes.end())
        {
            std::string err_msg = "Table " + this->table_name + " has no column named " + col_name;
            throw std::invalid_argument(err_msg);
        }
        return this->primary_tree->get_columns()[it->second];
    }
    std::vector<std::pair<std::string, Column>> Table::get_columns()
    {
        std::vector<std::pair<std::string, uint32_t>> name_idx_vec(this->col_name_indexes.begin(), this->col_name_indexes.end());
        std::sort(name_idx_vec.begin(), name_idx_vec.end(), name_idx_comp);
        std::vector<std::pair<std::string, Column>> to_ret;
        for (const std::pair<std::string, uint32_t> &name_idx : name_idx_vec)
            to_ret.push_back(std::make_pair(name_idx.first, this->primary_tree->get_columns()[name_idx.second]));
        return to_ret;
    }
    size_t Table::get_preceding_length(const std::string col_name) const
    {
        auto it = this->col_name_indexes.find(col_name);
        if (it == this->col_name_indexes.end())
        {
            std::string err_msg = "Table " + this->table_name + " has no column named " + col_name;
            throw std::invalid_argument(err_msg);
        }
        size_t to_ret = 0;
        for (size_t i = 0; i < it->second; i++)
        {
            to_ret += this->primary_tree->get_columns()[i].width;
        }
        return to_ret;
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
        return this->primary_tree->get_columns()[it->second].width;
    }
    void Table::add_column(const std::string name, const rsql::Column col)
    {
        if (col.type == DataType::DEFAULT_KEY)
        {
            throw std::invalid_argument("Can't add default key column");
            return;
        }
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
        this->col_name_indexes[name] = this->primary_tree->get_columns().size() - 1;
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
        if (col_idx == 0)
        {
            throw std::invalid_argument("Can't remove default key column");
            return;
        }
        this->primary_tree->remove_column(col_idx);
        this->col_name_indexes.erase(it);
        auto opt_tree_it = this->optional_trees.find(col_idx);
        if (opt_tree_it != this->optional_trees.end())
        {
            opt_tree_it->second->destroy();
            this->optional_trees.erase(opt_tree_it);
        }
        for (auto &pair : this->col_name_indexes)
        {
            if (pair.second > col_idx)
            {
                pair.second--;
            }
        }
        std::unordered_map<uint32_t, BTree *> new_optional_tree;
        for (const auto &pair : this->optional_trees)
        {
            if (pair.first > col_idx)
            {
                new_optional_tree[pair.first - 1] = pair.second;
            }
            else
            {
                new_optional_tree[pair.first] = pair.second;
            }
        }
        this->optional_trees = new_optional_tree;
        this->changed = true;
    }
    void Table::insert_row_bin(const char *row)
    {
        char *const buffer = new char[this->get_width()];
        std::memcpy(buffer, this->next_default_key, DEFAULT_KEY_WIDTH);
        std::memcpy(buffer + DEFAULT_KEY_WIDTH, row, this->get_width() - DEFAULT_KEY_WIDTH);
        this->primary_tree->insert_row(buffer);
        for (const auto &it : this->optional_trees)
        {
            uint32_t col_index = it.first;
            size_t opt_key_size = this->primary_tree->get_columns()[col_index].width;
            size_t preceding_size = 0;
            for (size_t i = 0; i < (size_t)col_index; i++)
            {
                preceding_size += this->primary_tree->get_columns()[i].width;
            }
            char *new_key = new char[opt_key_size + DEFAULT_KEY_WIDTH];
            std::memcpy(new_key, buffer + preceding_size, opt_key_size);
            std::memcpy(new_key + opt_key_size, this->next_default_key, DEFAULT_KEY_WIDTH);
            it.second->insert_row(new_key);
            delete[] new_key;
        }
        increment_default_key(this->next_default_key);
        this->changed = true;
        delete[] buffer;
    }
    void Table::insert_row_text(const std::vector<std::string> &row)
    {
        if (row.size() != this->col_name_indexes.size() - 1) // Ignoring default key columns
        {
            throw std::invalid_argument("Row size does not match table size");
            return;
        }
        std::unique_ptr<char[]> row_bin = std::make_unique<char[]>(this->get_width() - DEFAULT_KEY_WIDTH);
        std::memset(row_bin.get(), 0, this->get_width() - DEFAULT_KEY_WIDTH);
        size_t inserted_bytes = 0;
        for (size_t i = 0; i < row.size(); i++)
        {
            this->primary_tree->get_columns()[i + 1].process_string(row_bin.get() + inserted_bytes, row[i]);
            inserted_bytes += this->primary_tree->get_columns()[i + 1].width;
        }
        this->insert_row_bin(row_bin.get());
    }
    std::vector<char *> Table::search_row_single_key(std::string key_col, const char *key_val, CompSymbol symbol, Comparison *comparison)
    {
        auto it = this->col_name_indexes.find(key_col);
        if (it == this->col_name_indexes.end())
        {
            std::string err_msg = "Table " + this->table_name + " does not have a column named " + key_col;
            throw std::invalid_argument(err_msg);
            return std::vector<char *>();
        }
        uint32_t col_idx = it->second;
        if (col_idx == 0)
        {
            return this->primary_tree->search_rows(key_val, symbol, comparison);
        }

        Column comparison_col = this->primary_tree->get_columns()[col_idx];
        size_t preceding_size = 0;
        for (uint32_t i = 0; i < col_idx; i++)
        {
            preceding_size += this->primary_tree->get_columns()[i].width;
        }

        auto optional_tree_it = this->optional_trees.find(col_idx);
        if (optional_tree_it == this->optional_trees.end())
        {
            MultiComparisons *new_comparison = new ANDComparisons();
            Comparison *key_comparison = new ConstantComparison(comparison_col.type, symbol, comparison_col.width, preceding_size, key_val);
            new_comparison->add_condition(key_comparison);
            if (comparison != nullptr)
                new_comparison->add_condition(comparison);
            delete key_comparison;
            std::vector<char *> to_ret = this->primary_tree->search_rows(nullptr, CompSymbol::EQ, new_comparison);
            delete new_comparison;
            return to_ret;
        }
        else
        {
            BTree *optional_tree = optional_tree_it->second;
            std::vector<char *> optional_result = optional_tree->search_rows(key_val, symbol);
            std::vector<char *> to_ret;
            for (size_t i = 0; i < optional_result.size(); i++)
            {
                std::vector<char *> res = this->primary_tree->search_rows(optional_result[i] + comparison_col.width, CompSymbol::EQ, comparison);
                to_ret.insert(to_ret.end(), res.begin(), res.end());
                delete[] optional_result[i];
            }
            return to_ret;
        }
    }
    std::vector<char *> Table::delete_row(std::string key_col, const char *key_val, CompSymbol symbol, Comparison *comparison)
    {
        auto key_col_it = this->col_name_indexes.find(key_col);
        if (key_col_it == this->col_name_indexes.end())
        {
            std::string err_msg = "Column " + key_col + " does not exist in this table";
            throw std::invalid_argument(err_msg);
            return std::vector<char *>();
        }
        uint32_t col_idx = key_col_it->second;
        std::vector<char *> to_ret;
        if (col_idx == 0)
        {
            to_ret = this->primary_tree->delete_all_row(key_val, symbol, comparison);
        }
        else
        {
            auto optional_tree_it = this->optional_trees.find(col_idx);
            Column key_column = this->primary_tree->get_columns()[col_idx];
            if (optional_tree_it == this->optional_trees.end())
            { // linear search has to be performed
                size_t preceding_size = 0;
                for (uint32_t i = 0; i < col_idx; i++)
                    preceding_size += this->primary_tree->get_columns()[i].width;
                Comparison *key_comparison = new ConstantComparison(key_column.type, symbol, key_column.width, preceding_size, key_val);
                MultiComparisons *and_comparison = new ANDComparisons();
                and_comparison->add_condition(key_comparison);
                if (comparison)
                    and_comparison->add_condition(comparison);
                to_ret = this->primary_tree->delete_all_row(nullptr, CompSymbol::EQ, and_comparison);
                delete key_comparison;
                delete and_comparison;
            }
            else
            {
                BTree *optional_tree = optional_tree_it->second;
                std::vector<char *> keys_from_optional_tree = optional_tree->search_rows(key_val, symbol, comparison);
                for (char *optional_key : keys_from_optional_tree)
                {
                    char *primary_key = optional_key + key_column.width;
                    to_ret.push_back(this->primary_tree->delete_row(primary_key));
                    delete[] optional_key;
                }
            }
        }
        for (auto pair : this->optional_trees)
        {
            size_t preceding_size = 0;
            for (uint32_t i = 0; i < pair.first; i++)
                preceding_size += this->primary_tree->get_columns()[i].width;
            size_t optional_preceding = this->primary_tree->get_columns()[pair.first].width;
            ConstantComparison *primary_key_comparison = new ConstantComparison(DataType::DEFAULT_KEY, CompSymbol::EQ, DEFAULT_KEY_WIDTH, optional_preceding, nullptr);
            for (const char *res : to_ret)
            {
                primary_key_comparison->change_right_val(res);
                std::vector<char *> opt_ret = pair.second->delete_all_row(res + preceding_size, CompSymbol::EQ, primary_key_comparison);
                for (char *opt_res : opt_ret)
                    delete[] opt_res;
            }
            delete primary_key_comparison;
        }
        return to_ret;
    }
    void Table::index_column(const std::string col_name)
    {
        auto it_index = this->col_name_indexes.find(col_name);
        if (it_index == this->col_name_indexes.end())
        {
            throw std::invalid_argument("Can't index column that doesn't exist");
            return;
        }
        if (it_index->second == 0)
        {
            throw std::invalid_argument("Can't index default key");
            return;
        }
        auto it_tree_num = this->optional_trees.find(it_index->second);
        if (it_tree_num != this->optional_trees.end())
        {
            throw std::invalid_argument("Can't index indexed column");
            return;
        }
        BTree *new_tree = rsql::BTree::create_new_tree(this, ++this->max_tree_num, false);
        size_t preceding_size = 0;
        size_t indexed_col_index = it_index->second;
        Column indexed_col = this->primary_tree->get_columns()[indexed_col_index];
        Column primary_col = this->primary_tree->get_columns()[0];
        this->optional_trees[it_index->second] = new_tree;
        std::string where = std::filesystem::path(this->get_path()) / std::to_string(new_tree->tree_num);
        std::filesystem::create_directories(where);
        new_tree->add_column(indexed_col);
        new_tree->add_column(primary_col);
        for (size_t i = 0; i < indexed_col_index; i++)
            preceding_size += this->primary_tree->get_columns()[i].width;

        char *new_row = new char[new_tree->width];
        std::queue<uint32_t> children;
        for (size_t i = 0; i < this->primary_tree->get_root()->get_size(); i++)
        {
            std::memcpy(new_row, this->primary_tree->get_root()->get_keys()[i] + preceding_size, indexed_col.width);
            std::memcpy(new_row + indexed_col.width, this->primary_tree->get_root()->get_keys()[i], primary_col.width);
            new_tree->insert_row(new_row);
        }
        if (!this->primary_tree->get_root()->is_leaf())
        {
            for (size_t i = 0; i <= this->primary_tree->get_root()->get_size(); i++)
                children.push(this->primary_tree->get_root()->get_children()[i]);
        }
        while (!children.empty())
        {
            uint32_t this_child = children.front();
            children.pop();
            NodePair from_primary_cache = this->primary_tree->get_node(this_child);
            BNode *cur_node = from_primary_cache.first;
            if (from_primary_cache.second)
                delete from_primary_cache.second;
            for (size_t i = 0; i < cur_node->get_size(); i++)
            {
                std::memcpy(new_row, cur_node->get_keys()[i] + preceding_size, indexed_col.width);
                std::memcpy(new_row + indexed_col.width, cur_node->get_keys()[i], primary_col.width);
                new_tree->insert_row(new_row);
            }
            if (!cur_node->is_leaf())
            {
                for (size_t i = 0; i <= cur_node->get_size(); i++)
                    children.push(cur_node->get_children()[i]);
            }
        }
        this->changed = true;
        delete[] new_row;
    }
    std::string Table::get_path() const
    {
        return std::filesystem::path(this->db->get_path()) / this->table_name;
    }
    void Table::write_disk()
    {
        if (!this->changed)
            return;
        static char write_buffer[DISK_BUFFER_SZ];
        size_t bytes_processed = 0;
        std::string where = std::filesystem::path(this->db->get_path()) / this->table_name;
        if (!std::filesystem::exists(where))
            std::filesystem::create_directory(where);
        std::string file_name = std::filesystem::path(where) / TABLE_FILE_NAME;
        int fd = open(file_name.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0644);
        if (fd < 0)
            throw std::runtime_error("Failed to open file");
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
            std::memcpy(write_buffer + bytes_processed, &col.second, 4);
            bytes_processed += 4;
            auto optional_it = this->optional_trees.find(col.second);
            if (optional_it == this->optional_trees.end())
            {
                std::memset(write_buffer + bytes_processed, 0, 4);
            }
            else
            {
                uint32_t col_tree_num = optional_it->second->tree_num;
                std::memcpy(write_buffer + bytes_processed, &col_tree_num, 4);
            }
            bytes_processed += 4;
        }
        if (DISK_BUFFER_SZ - bytes_processed < 12 + DEFAULT_KEY_WIDTH)
        {
            if (write(fd, write_buffer, bytes_processed) < 0)
            {
                close(fd);
                throw std::runtime_error("Failed to write table");
            };
            bytes_processed = 0;
        }
        std::memcpy(write_buffer + bytes_processed, this->next_default_key, DEFAULT_KEY_WIDTH);
        bytes_processed += DEFAULT_KEY_WIDTH;
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