#pragma once

#include <iostream>

void ConsoleLogDebug(std::string str)
{
#if 0
    printf(str.c_str());
#endif
}

void ConsoleLogAlways(std::string str)
{
#if 1
    printf(str.c_str());
#endif
}