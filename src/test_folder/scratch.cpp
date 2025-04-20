#include <iostream>
#include <cstring>

int main(int argc, char **argv){
    int j = 0;
    for (int i = 0 ; i < 100000 ; i++){
        j++;
    }
    std::cout << j << std::endl;
    return 0;
}