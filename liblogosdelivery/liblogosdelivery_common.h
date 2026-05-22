#pragma once
#ifndef LOGOSDELIVERY_COMMON_DEFS
#define LOGOSDELIVERY_COMMON_DEFS

#include <stddef.h>
#include <stdint.h>

#define RET_OK 0
#define RET_ERR 1
#define RET_MISSING_CALLBACK 2
typedef void (*FFICallBack)(int callerRet, const char *msg, size_t len, void *userData);

#endif
