#include "data_type.h"
rsql::DataType rsql::str_to_dt(std::string string_repr){
    if (string_repr.compare(PKEY_STR) == 0){
        return rsql::DataType::PKEY;
    }
    else if (string_repr.compare(INT_STR) == 0){
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

std::string rsql::dt_to_str(rsql::DataType type){
    if (type == rsql::DataType::PKEY){
        return PKEY_STR;
    }
    else if (type == rsql::DataType::INT){
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