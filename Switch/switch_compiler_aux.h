#pragma once

#define containerof(type, member_name, ptr) (type *)((char *)(ptr)-offsetof(type, member_name))
