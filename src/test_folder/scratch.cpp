#include "database.h"
#include <chrono>

int main(int argc, char **argv){
    rsql::Database *db = rsql::Database::load_database("test_db");
    rsql::Table *table = db->get_table("test_table");
    std::string col_name, value;
    while (true){
        std::cout << "Enter column name: key, col_3, col_4" << std::endl;
        std::cin >> col_name;
        if (col_name == ""){
            break;
        }
        std::cout << "Enter value:" << std::endl;
        std::cin >> value;
        auto before = std::chrono::high_resolution_clock::now();
        std::vector<char *> res = table->find_row_text(value, col_name);
        auto after = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(after - before);
        std::cout << "Time taken: " << duration.count() << " microseconds" << std::endl;
        std::cout << res.size() << " items found: " <<std::endl;
        for (size_t i = 0 ; i < res.size() ; i++){
            uint32_t a, b;
            a = *reinterpret_cast<uint32_t*>(res[i] + 52);
            b = *reinterpret_cast<uint32_t*>(res[i] + 56);
            std::cout.write(res[i], 52);
            std::cout << " ";
            std::cout << a << " " << b << std::endl;
            delete[] res[i];
        }
    }
    delete table;
    delete db;
    return 0;
}