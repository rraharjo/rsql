#ifndef DATATYPE_H
#define DATATYPE_H
#include <string>
#include <stdexcept>
#define INT_STR "INT"
#define CHAR_STR "CHAR"
#define DATE_STR "DATE"
namespace rsql
{
    enum class DataType
    {
        INT,
        CHAR,
        DATE
    };
    DataType from_string(std::string);
    std::string to_string(DataType);
}

rsql::DataType rsql::from_string(std::string string_repr){
    if (string_repr.compare(INT_STR) == 0){
        return rsql::DataType::INT;
    }
    else if (string_repr.compare(CHAR_STR) == 0){
        return rsql::DataType::CHAR;
    }
    else if (string_repr.compare(DATE_STR) == 0){
        return rsql::DataType::DATE;
    }
    else{
        throw std::invalid_argument(string_repr + " does not specify any data type");
    }
}

std::string rsql::to_string(rsql::DataType type){
    if (type == rsql::DataType::INT){
        return INT_STR;
    }
    else if (type == rsql::DataType::CHAR){
        return CHAR_STR;
    }
    else if (type == rsql::DataType::DATE){
        return DATE_STR;
    }
    else{
        throw std::invalid_argument("Unknown type");
    }
}
#endif