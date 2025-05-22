#include "driver.h"

int main()
{
    std::unique_ptr<rsql::Driver> driver = std::make_unique<rsql::Driver>();
    driver->routine();
    std::cout << "BYE!" << std::endl;
}