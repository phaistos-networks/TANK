#pragma once
#include <switch.h>

#ifdef LEAN_SWITCH
#define RFLog(...) Print(__VA_ARGS__)
#endif
