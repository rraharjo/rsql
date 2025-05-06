#include "comparison.h"

typedef boost::multiprecision::cpp_int cpp_int;
bool compare_symbol(const int type_res, const rsql::CompSymbol symbol);
namespace rsql
{
    Comparison::Comparison()
    {
    }

    Comparison::Comparison(const Comparison &other)
    {
    }

    Comparison::~Comparison()
    {
    }

    SingleComparison::SingleComparison(DataType type, CompSymbol symbol, size_t len)
        : Comparison(), type(type), symbol(symbol), len(len)
    {
    }

    SingleComparison::SingleComparison(const SingleComparison &other)
        : Comparison(other), type(other.type), symbol(other.symbol), len(other.len)
    {
    }

    SingleComparison::~SingleComparison()
    {
    }

    ColumnComparison::ColumnComparison(DataType type, CompSymbol symbol, size_t left_len, const size_t left_preceding, const size_t right_preceding, const size_t right_len)
        : SingleComparison(type, symbol, left_len), left_preceding(left_preceding), right_preceding(right_preceding), right_len(right_len)
    {
    }

    ColumnComparison::ColumnComparison(const ColumnComparison &other)
        : SingleComparison(other), left_preceding(other.left_preceding), right_preceding(other.right_preceding), right_len(other.right_len)
    {
    }

    ColumnComparison::~ColumnComparison()
    {
    }

    ColumnComparison *ColumnComparison::clone()
    {
        return new ColumnComparison(*this);
    }

    bool ColumnComparison::compare(const char *const row)
    {
        int type_result;
        switch (this->type)
        {
        case DataType::DEFAULT_KEY:
        {
            int to_ret = std::memcmp(row + left_preceding, row + right_preceding, this->len);
            if (to_ret < 0)
            {
                type_result = -1;
            }
            else if (to_ret > 0)
            {
                type_result = 1;
            }
            else
            {
                // If the first n bytes are the same, the shorter one is smaller
                if (this->len < this->right_len)
                {
                    type_result = -1;
                }
                else if (this->len > this->right_len)
                {
                    type_result = 1;
                }
                else
                {
                    type_result = 0;
                }
            }
            break;
        }
        case DataType::CHAR:
        case DataType::DATE:
        {
            const size_t min_size = std::min(this->len, this->right_len);
            int to_ret = std::strncmp(row + left_preceding, row + right_preceding, min_size);
            if (to_ret < 0)
            {
                type_result = -1;
            }
            else if (to_ret > 0)
            {
                type_result = 1;
            }
            else
            {
                // If the first n bytes are the same, the shorter one is smaller
                if (this->len < this->right_len)
                {
                    type_result = -1;
                }
                else if (this->len > this->right_len)
                {
                    type_result = 1;
                }
                else
                {
                    type_result = 0;
                }
            }
            break;
        }
        case DataType::UINT:
        {
            cpp_int left_int, right_int;
            const char *const left_buff = row + this->left_preceding;
            const char *const right_buff = row + this->right_preceding;
            boost::multiprecision::import_bits(left_int, left_buff, left_buff + this->len, 8, false);
            boost::multiprecision::import_bits(right_int, right_buff, right_buff + this->right_len, 8, false);
            if (left_int < right_int)
            {
                type_result = -1;
            }
            else if (left_int > right_int)
            {
                type_result = 1;
            }
            else
            {
                type_result = 0;
            }
            break;
        }
        case DataType::SINT:
        {
            cpp_int left_int, right_int;
            const char *const left_buff = row + this->left_preceding;
            const char *const right_buff = row + this->right_preceding;
            int left_sign, right_sign;
            left_sign = static_cast<int>(*left_buff);
            right_sign = static_cast<int>(*right_buff);
            boost::multiprecision::import_bits(left_int, left_buff + 1, left_buff + this->len, 8, false);
            boost::multiprecision::import_bits(right_int, right_buff + 1, right_buff + this->right_len, 8, false);
            left_int *= left_sign;
            right_int *= right_sign;
            if (left_int < right_int)
            {
                type_result = -1;
            }
            else if (left_int > right_int)
            {
                type_result = 1;
            }
            else
            {
                type_result = 0;
            }
            break;
        }
        default:
            throw std::invalid_argument("Invalid type");
            return 0;
        }
        return compare_symbol(type_result, this->symbol);
    }

    ConstantComparison::ConstantComparison(DataType type, CompSymbol symbol, size_t len, const size_t preceding_size, const char *right)
        : SingleComparison(type, symbol, len), preceding_size(preceding_size)
    {
        this->constant_val = new char[len];
        std::memcpy(this->constant_val, right, len);
    }

    ConstantComparison::ConstantComparison(const ConstantComparison &other)
        : SingleComparison(other), preceding_size(other.preceding_size)
    {
    }

    ConstantComparison::~ConstantComparison()
    {
        delete[] this->constant_val;
    }

    ConstantComparison *ConstantComparison::clone()
    {
        return new ConstantComparison(*this);
    }

    bool ConstantComparison::compare(const char *const row)
    {
        int type_res;
        switch (this->type)
        {
        case DataType::DEFAULT_KEY:
        {
            int to_ret = std::memcmp(row + this->preceding_size, this->constant_val, this->len);
            if (to_ret < 0)
            {
                type_res = -1;
            }
            else if (to_ret > 0)
            {
                type_res = 1;
            }
            else
            {
                type_res = 0;
            }
            break;
        }
        case DataType::CHAR:
        case DataType::DATE:
        {
            int to_ret = std::strncmp(row + this->preceding_size, this->constant_val, this->len);
            if (to_ret < 0)
            {
                type_res = -1;
            }
            else if (to_ret > 0)
            {
                type_res = 1;
            }
            else
            {
                type_res = 0;
            }
            break;
        }
        case DataType::UINT:
        {
            cpp_int left_int, right_int;
            const char *const left_buff = row + this->preceding_size;
            boost::multiprecision::import_bits(left_int, left_buff, left_buff + this->len, 8, false);
            boost::multiprecision::import_bits(right_int, this->constant_val, this->constant_val + this->len, 8, false);
            if (left_int < right_int)
            {
                type_res = -1;
            }
            else if (left_int > right_int)
            {
                type_res = 1;
            }
            else
            {
                type_res = 0;
            }
            break;
        }
        case DataType::SINT:
        {
            cpp_int left_int, right_int;
            const char *const left_buff = row + this->preceding_size;
            int left_sign, right_sign;
            left_sign = static_cast<int>(*left_buff);
            right_sign = static_cast<int>(*(this->constant_val));
            boost::multiprecision::import_bits(left_int, left_buff + 1, left_buff + this->len, 8, false);
            boost::multiprecision::import_bits(right_int, this->constant_val + 1, this->constant_val + this->len, 8, false);
            left_int *= left_sign;
            right_int *= right_sign;
            if (left_int < right_int)
            {
                type_res = -1;
            }
            else if (left_int > right_int)
            {
                type_res = 1;
            }
            else
            {
                type_res = 0;
            }
            break;
        }
        default:
            throw std::invalid_argument("Invalid type");
            return 0;
        }
        return compare_symbol(type_res, this->symbol);
    }

    MultiComparisons::MultiComparisons()
    {
    }

    MultiComparisons::MultiComparisons(const MultiComparisons &other) : Comparison(other)
    {
        for (Comparison *const comp : other.conditions)
        {
            this->conditions.push_back(comp->clone());
        }
    }

    MultiComparisons::~MultiComparisons()
    {
        for (const Comparison *c : this->conditions)
        {
            delete c;
        }
    }

    void MultiComparisons::add_condition(Comparison *comp)
    {
        this->conditions.push_back(comp->clone());
    }

    ORComparisons::ORComparisons() : MultiComparisons()
    {
    }

    ORComparisons::ORComparisons(const ORComparisons &other) : MultiComparisons(other)
    {
    }

    ORComparisons::~ORComparisons()
    {
    }

    ORComparisons *ORComparisons::clone()
    {
        return new ORComparisons(*this);
    }

    bool ORComparisons::compare(const char *const row)
    {
        if (this->conditions.size() == 0)
        {
            return true;
        }
        for (Comparison *c : this->conditions)
        {
            if (c->compare(row))
            {
                return true;
            }
        }
        return false;
    }

    ANDComparisons::ANDComparisons() : MultiComparisons()
    {
    }

    ANDComparisons::ANDComparisons(const ANDComparisons &other) : MultiComparisons(other)
    {
    }

    ANDComparisons::~ANDComparisons()
    {
    }

    ANDComparisons *ANDComparisons::clone()
    {
        return new ANDComparisons(*this);
    }

    bool ANDComparisons::compare(const char *const row)
    {
        for (Comparison *c : this->conditions)
        {
            if (!c->compare(row))
            {
                return false;
            }
        }
        return true;
    }
}

bool compare_symbol(const int type_res, const rsql::CompSymbol symbol)
{
    switch (symbol)
    {
    case rsql::CompSymbol::EQ:
        return type_res == 0;
    case rsql::CompSymbol::GEQ:
        return type_res >= 0;
    case rsql::CompSymbol::GT:
        return type_res > 0;
    case rsql::CompSymbol::LEQ:
        return type_res <= 0;
    case rsql::CompSymbol::LT:
        return type_res < 0;
    default:
        throw std::invalid_argument("Unknown comparison symbol");
        return false;
    }
}