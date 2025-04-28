#include <boost/multiprecision/cpp_int.hpp>
typedef boost::multiprecision::cpp_int cpp_int;
int main(int argc, char **argv){
    cpp_int some_number("100000000");
    std::cout << some_number << std::endl;
    int some_number_sign = -1;
    some_number *= some_number_sign;
    std::cout << some_number << std::endl;
    std::cout << (some_number == cpp_int("-100000000")) << std::endl;
    return 0;
}