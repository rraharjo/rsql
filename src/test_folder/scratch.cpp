#include <boost/multiprecision/cpp_int.hpp>
typedef boost::multiprecision::cpp_int cpp_int;
int main(int argc, char **argv)
{
    cpp_int a;
    std::string inp;
    std::cin >> inp;
    a = cpp_int(inp);
    size_t total_bits = boost::multiprecision::msb(a) + 1;
    size_t total_bytes = total_bits / 8;
    if (total_bits % 8)
    {
        total_bytes += 1;
    }
    std::cout << total_bytes << std::endl;
    return 0;
}