#ifndef DATATYPE_H
#define DATATYPE_H

#include <string>
#include <stdexcept>
#include <cstring>
#include <boost/multiprecision/cpp_int.hpp>

#define DT_STR_LEN 4
#define UINT_STR "UINT"
#define SINT_STR "SINT"
#define CHAR_STR "CHAR"
#define DATE_STR "DATE"
#define PKEY_STR "PKEY"
namespace rsql
{
    enum class DataType
    {
        PKEY,
        UINT,
        SINT,
        CHAR,
        DATE
    };
    DataType str_to_dt(std::string);
    std::string dt_to_str(DataType);
    bool valid_date(const std::string &);

    class Cell
    {
    public:
        char *src = nullptr;
        const size_t len;
        const DataType type;
    protected: 
        Cell(const size_t len, const DataType type);
        // virtual ~Cell();
    public:

        bool operator==(const Cell &other) const;
        bool operator<(const Cell &other) const;
        bool operator<=(const Cell &other) const;
        bool operator>(const Cell &other) const;
        bool operator>=(const Cell &other) const;
        bool operator!=(const Cell &other) const;
    };

    class ConstCell : public Cell{
        public:
            ConstCell(const char *src, const size_t len, const DataType type);
    };
    class ColumnCell : public Cell{
        public:
            ColumnCell(char *src, const size_t len, const DataType type);
    };
}
#endif