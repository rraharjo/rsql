#include "data_type.h"

typedef boost::multiprecision::cpp_int cpp_int;

namespace rsql
{
    DataType str_to_dt(std::string string_repr)
    {
        if (string_repr.compare(DEF_KEY_STR) == 0)
        {
            return DataType::DEFAULT_KEY;
        }
        else if (string_repr.compare(UINT_STR) == 0)
        {
            return DataType::UINT;
        }
        else if (string_repr.compare(SINT_STR) == 0)
        {
            return DataType::SINT;
        }
        else if (string_repr.compare(CHAR_STR) == 0)
        {
            return DataType::CHAR;
        }
        else if (string_repr.compare(DATE_STR) == 0)
        {
            return DataType::DATE;
        }
        else
        {
            throw std::invalid_argument(string_repr + " does not represent any data type");
        }
    }
    std::string dt_to_str(DataType type)
    {
        switch (type)
        {
        case DataType::DEFAULT_KEY:
            return DEF_KEY_STR;
        case DataType::UINT:
            return UINT_STR;
        case DataType::SINT:
            return SINT_STR;
        case DataType::CHAR:
            return CHAR_STR;
        case DataType::DATE:
            return DATE_STR;
        default:
            throw std::invalid_argument("Unknown type");
        }
    }
    bool valid_date(const std::string &date)
    {
        if (date.size() != 10)
        {
            return false;
        }
        if (date[4] != '-' || date[7] != '-')
        {
            return false;
        }
        try
        {
            int year = std::stoi(date.substr(0, 4));
            int month = std::stoi(date.substr(5, 2));
            int day = std::stoi(date.substr(8, 2));
            if (month < 1 || month > 12)
            {
                return false;
            }
            if (day < 1 || day > 31)
            {
                return false;
            }
            if ((month == 4 || month == 6 || month == 9 || month == 11) && day == 31)
            {
                return false;
            }
            if (month == 2)
            {
                if (day > 29)
                {
                    return false;
                }
                if (day == 29 && (year % 4 != 0 || (year % 100 == 0 && year % 400 != 0)))
                {
                    return false;
                }
            }
        }
        catch (const std::invalid_argument &e)
        {
            return false;
        }
        catch (const std::out_of_range &e)
        {
            return false;
        }
        return true;
    }
    void increment_default_key(unsigned char *const key)
    {
        unsigned int cur_idx = DEFAULT_KEY_WIDTH;
        bool go_left = true;
        while (go_left)
        {
            cur_idx--;
            key[cur_idx]++;
            if (key[cur_idx] != 0)
            {
                go_left = false;
            }
        }
    }
    void ucpp_int_to_char(char *const dest, const size_t dest_len, cpp_int &ucpp_int)
    {
        size_t total_bits;
        if (ucpp_int.sign() == 0){
            total_bits = 1;
        }
        else{
            total_bits = boost::multiprecision::msb(ucpp_int) + 1;
        }
        size_t total_bytes = total_bits / 8;
        if (total_bits % 8)
        {
            total_bytes += 1;
        }
        if (total_bytes > dest_len){
            std::string err_msg = ucpp_int.str() + " need " + std::to_string(total_bytes) + " bytes of char. Destination only has " + std::to_string(dest_len);
            throw std::invalid_argument(err_msg);
            return;
        }
        std::memset(dest, 0, dest_len);
        boost::multiprecision::export_bits(ucpp_int, dest, 8, false);
    }
    void scpp_int_to_char(char *const dest, const size_t dest_len, cpp_int &scpp_int)
    {
        cpp_int absolute = boost::multiprecision::abs(scpp_int);
        size_t total_bits;
        if (scpp_int.sign() == 0){
            total_bits = 1;
        }
        else{
            total_bits = boost::multiprecision::msb(absolute) + 1;
        }
        size_t total_bytes = total_bits / 8;
        if (total_bits % 8)
        {
            total_bytes += 1;
        }
        total_bytes += 1; //Sign byte
        if (total_bytes > dest_len){
            std::string err_msg = scpp_int.str() + " need " + std::to_string(total_bytes) + " bytes of char. Destination only has " + std::to_string(dest_len);
            throw std::invalid_argument(err_msg);
            return;
        }
        std::memset(dest, 0, dest_len);
        int sign = scpp_int.sign();
        std::memcpy(dest, &sign, 1);
        boost::multiprecision::export_bits(scpp_int, dest + 1, 8, false);
    }
    void char_to_ucpp_int(char *const src, const size_t src_len, cpp_int &ucpp_int)
    {
        ucpp_int = 0;
        boost::multiprecision::import_bits(ucpp_int, src, src + src_len, 8, false);
    }
    void char_to_scpp_int(char *const src, const size_t src_len, cpp_int &scpp_int)
    {
        int sign = static_cast<int>(*src);
        if (sign != 0 && sign != 1 && sign != -1){
            throw std::invalid_argument("Invalid source");
            return;
        }
        scpp_int = 0;
        boost::multiprecision::import_bits(scpp_int, src + 1, src + src_len, 8, false);
        scpp_int *= sign;
    }
}