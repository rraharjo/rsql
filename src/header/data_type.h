#ifndef DATATYPE_H
#define DATATYPE_H

#include <string>
#include <stdexcept>
#include <cstring>
#include <boost/multiprecision/cpp_int.hpp>

#define DT_STR_LEN 4
#define DEFAULT_KEY_WIDTH 32
#define DATE_TYPE_WIDTH 10
#define UINT_STR "UINT"
#define SINT_STR "SINT"
#define CHAR_STR "CHAR"
#define DATE_STR "DATE"
#define DEF_KEY_STR "DKEY"
namespace rsql
{
    enum class DataType
    {
        DEFAULT_KEY,
        UINT,
        SINT,
        CHAR,
        DATE
    };
    DataType str_to_dt(std::string);
    std::string dt_to_str(DataType);
    bool valid_date(const std::string &);
    void increment_default_key(unsigned char *const key);
    /**
     * @brief Copy the underlying byte of ucpp_int to dest. Will ignore sign (little endian format)
     * 
     * @throw std::invalid_argument if the destination size is less than the cpp_int total number of bytes
     * @param dest 
     * @param dest_len 
     * @param ucpp_int 
     */
    void ucpp_int_to_char(char *const dest, const size_t dest_len, boost::multiprecision::cpp_int &ucpp_int);
    /**
     * @brief @brief Copy the underlying byte of scpp_int to dest. the first byte is sign byte, the rest are magnitude bytes (little endian format)
     * 
     * @throw std::invalid_argument if the destination size is less than the cpp_int total number of bytes
     * @param dest 
     * @param dest_len 
     * @param scpp_int 
     */
    void scpp_int_to_char(char *const dest, const size_t dest_len, boost::multiprecision::cpp_int &scpp_int);
    /**
     * @brief Takes src_len bytes and convert it to unsigned cpp_int (little endian format)
     * 
     * @param src 
     * @param src_len 
     * @param ucpp_int 
     */
    void char_to_ucpp_int(char *const src, const size_t src_len, boost::multiprecision::cpp_int &ucpp_int);
    /**
     * @brief Takes src_len bytes and convert it to signed cpp_int (little endian format)
     * 
     * @throw std::invalid_argument if the first byte of src does not equal -1 or 0 or 1
     * @param src 
     * @param src_len 
     * @param scpp_int 
     */
    void char_to_scpp_int(char *const src, const size_t src_len, boost::multiprecision::cpp_int &scpp_int);
}
#endif