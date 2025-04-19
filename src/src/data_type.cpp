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

bool rsql::valid_date(const std::string &date){
    if (date.size() != 10){
        return false;
    }
    if (date[2] != '-' || date[5] != '-'){
        return false;
    }
    try{
        int day = std::stoi(date.substr(0, 2));
        int month = std::stoi(date.substr(3, 2));
        int year = std::stoi(date.substr(6, 4));
        if (month < 1 || month > 12){
            return false;
        }
        if (day < 1 || day > 31){
            return false;
        }
        if ((month == 4 || month == 6 || month == 9 || month == 11) && day == 31){
            return false;
        }
        if (month == 2){
            if (day > 29){
                return false;
            }
            if (day == 29 && (year % 4 != 0 || (year % 100 == 0 && year % 400 != 0))){
                return false;
            }
        }
    } catch (const std::invalid_argument &e) {
        return false;
    } catch (const std::out_of_range &e) {
        return false;
    }
    return true;
}