#include "driver.h"

int main()
{
    rsql::Driver *driver = new rsql::Driver();
    driver->routine();
    std::cout << "BYE!" << std::endl;
    delete driver;
}