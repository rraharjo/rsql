#ifndef DATATYPE_H
#define DATATYPE_H

#include <string>
#include <stdexcept>

#define DT_STR_LEN 4
#define INT_STR "INTG"
#define CHAR_STR "CHAR"
#define DATE_STR "DATE"
#define PKEY_STR "PKEY"
namespace rsql
{
    enum class DataType
    {
        PKEY,
        INT,
        CHAR,
        DATE
    };
    DataType str_to_dt(std::string);
    std::string dt_to_str(DataType);
    bool valid_date(const std::string &);
}
#endif