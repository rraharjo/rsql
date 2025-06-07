#include "comparison.h"

typedef boost::multiprecision::cpp_int cpp_int;
bool compare_symbol(const int type_res, const rsql::CompSymbol symbol);
namespace rsql
{
    CompSymbol get_symbol_from_string(const std::string src)
    {
        if (src == "<")
        {
            return CompSymbol::LT;
        }
        if (src == "<=")
        {
            return CompSymbol::LEQ;
        }
        if (src == "==")
        {
            return CompSymbol::EQ;
        }
        if (src == ">=")
        {
            return CompSymbol::GEQ;
        }
        if (src == ">")
        {
            return CompSymbol::GT;
        }
        throw std::invalid_argument("Unknown symbol " + src);
    }

    Comparison::Comparison()
    {
    }

    Comparison::Comparison(const Comparison &other)
    {
    }

    Comparison::~Comparison()
    {
    }

    SingleComparison::SingleComparison(const DataType type, const CompSymbol symbol, const size_t left_preceding, const size_t left_len)
        : Comparison(), type(type), symbol(symbol), left_preceding(left_preceding), left_len(left_len)
    {
        if (type == DataType::DEFAULT_KEY && left_len != DEFAULT_KEY_WIDTH)
        {
            throw std::invalid_argument("Can't compare default key. Default key width has to be " + std::to_string(DEFAULT_KEY_WIDTH));
            return;
        }
        if (type == DataType::DATE && left_len != DATE_TYPE_WIDTH)
        {
            throw std::invalid_argument("Can't compare date. Date width has to be " + std::to_string(DATE_TYPE_WIDTH));
            return;
        }
    }

    SingleComparison::SingleComparison(const SingleComparison &other)
        : Comparison(other), type(other.type), symbol(other.symbol), left_preceding(other.left_preceding), left_len(other.left_len)
    {
    }

    bool SingleComparison::operator==(const Comparison &other) const
    {
        if (this->get_comparison_type() != other.get_comparison_type())
            return false;
        const SingleComparison *other_ptr = static_cast<const SingleComparison *>(&other);
        return this->type == other_ptr->type &&
               this->symbol == other_ptr->symbol &&
               this->left_preceding == other_ptr->left_preceding &&
               this->left_len == other_ptr->left_len;
    }

    SingleComparison::~SingleComparison()
    {
    }

    ColumnComparison::ColumnComparison(DataType type, CompSymbol symbol, size_t left_len, const size_t left_preceding, const size_t right_len, const size_t right_preceding)
        : SingleComparison(type, symbol, left_preceding, left_len), right_len(right_len), right_preceding(right_preceding)
    {
        if (this->type == DataType::DEFAULT_KEY || this->type == DataType::DATE)
        {
            if (this->left_len != this->right_len)
            {
                throw std::invalid_argument("Can't compare date or default key if both columns do not have the same length");
                return;
            }
        }
    }

    ColumnComparison::ColumnComparison(const ColumnComparison &other)
        : SingleComparison(other), right_len(other.right_len), right_preceding(other.right_preceding)
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
            int to_ret = std::memcmp(row + left_preceding, row + right_preceding, this->left_len);
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
                if (this->left_len < this->right_len)
                {
                    type_result = -1;
                }
                else if (this->left_len > this->right_len)
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
            const size_t min_size = std::min(this->left_len, this->right_len);
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
                if (this->left_len < this->right_len)
                {
                    type_result = -1;
                }
                else if (this->left_len > this->right_len)
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
            boost::multiprecision::import_bits(left_int, left_buff, left_buff + this->left_len, 8, false);
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
            boost::multiprecision::import_bits(left_int, left_buff + 1, left_buff + this->left_len, 8, false);
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

    bool ColumnComparison::operator==(const Comparison &other) const 
    {
        if (this->get_comparison_type() != other.get_comparison_type())
            return false;
        const ColumnComparison *other_ptr = static_cast<const ColumnComparison *>(&other);
        return SingleComparison::operator==(other) &&
               this->right_len == other_ptr->right_len &&
               this->right_preceding == other_ptr->right_preceding;
    };

    ConstantComparison::ConstantComparison(DataType type, CompSymbol symbol, size_t len, const size_t left_preceding, const char *right_val)
        : SingleComparison(type, symbol, left_preceding, len)
    {
        this->constant_val = new char[this->left_len];
        if (right_val)
            std::memcpy(this->constant_val, right_val, this->left_len);
    }

    ConstantComparison::ConstantComparison(const ConstantComparison &other)
        : SingleComparison(other)
    {
        this->constant_val = new char[this->left_len];
        std::memcpy(this->constant_val, other.constant_val, this->left_len);
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
            int to_ret = std::memcmp(row + this->left_preceding, this->constant_val, this->left_len);
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
            int to_ret = std::strncmp(row + this->left_preceding, this->constant_val, this->left_len);
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
            const char *const left_buff = row + this->left_preceding;
            boost::multiprecision::import_bits(left_int, left_buff, left_buff + this->left_len, 8, false);
            boost::multiprecision::import_bits(right_int, this->constant_val, this->constant_val + this->left_len, 8, false);
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
            const char *const left_buff = row + this->left_preceding;
            int left_sign, right_sign;
            left_sign = static_cast<int>(*left_buff);
            right_sign = static_cast<int>(*(this->constant_val));
            boost::multiprecision::import_bits(left_int, left_buff + 1, left_buff + this->left_len, 8, false);
            boost::multiprecision::import_bits(right_int, this->constant_val + 1, this->constant_val + this->left_len, 8, false);
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

    void ConstantComparison::change_right_val(const char *new_right_val)
    {
        std::memcpy(this->constant_val, new_right_val, this->left_len);
    }

    bool ConstantComparison::operator==(const Comparison &other) const
    {
        if (this->get_comparison_type() != other.get_comparison_type())
            return false;
        const ConstantComparison *other_ptr = static_cast<const ConstantComparison *>(&other);
        bool to_ret = SingleComparison::operator==(other);
        bool cmp_ret = std::memcmp(this->constant_val, other_ptr->constant_val, this->left_len) == 0;
        return to_ret && cmp_ret;
    };

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

    bool MultiComparisons::operator==(const Comparison &other) const
    {
        if (other.get_comparison_type() != this->get_comparison_type())
            return false;
        const MultiComparisons *other_ptr = static_cast<const MultiComparisons *>(&other);
        const size_t this_size = this->conditions.size();
        if (this_size != other_ptr->conditions.size())
            return false;
        for (size_t i = 0; i < this_size; i++)
        {
            if (*(this->conditions[i]) != *(other_ptr->conditions[i]))
                return false;
        }
        return true;
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