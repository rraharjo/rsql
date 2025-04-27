#include <iostream>
#include <cstring>

int main(int argc, char **argv){
    char s1[] = "abcdefg";
    char s2[] = "abccefg";
    std::cout << strncmp(s1, s2, 7) << std::endl;
    return 0;
}