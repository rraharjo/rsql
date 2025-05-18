#ifndef COLUMN_H
#define COLUMN_H
#include <string>
#include "data_type.h"
#include "comparison.h"

#define COLUMN_BYTES 12
#define MAX_COL_NAME 64
#define DATE_COL_W 10
namespace rsql
{
    class Column
    {
    public:
        DataType type;
        size_t width;
        unsigned int col_id;

        Column(unsigned int col_id, size_t width, DataType type);
        /**
         * @brief Process the src string (text format) into binary format and copy it to dest
         *
         * @throw std::invalid_argument if the length of src in binary exceed the column width
         * @param dest
         * @param src
         */
        void process_string(char *const dest, const std::string src);
        int compare_key(const char *const k1, const char *const k2, CompSymbol symbol = CompSymbol::EQ);
        bool operator==(const Column &other) const;
        bool operator!=(const Column &other) const;

        static Column pkey_column(unsigned int col_id);
        static Column unsigned_int_column(unsigned int col_id, const size_t width);
        /**
         * @brief return an unsigned column. The first byte is the sign byte (-1, 0, 1). The rest (n - 1) bytes are magnitude bytes
         *
         * @param col_id
         * @param width
         * @return Column
         */
        static Column signed_int_column(unsigned int col_id, const size_t width);
        static Column char_column(unsigned int col_id, const size_t width);
        static Column date_column(unsigned int col_id);

        static Column get_column(unsigned int col_id, DataType type, size_t width);
    };
}
#endif