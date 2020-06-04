#pragma once

#include <stdbool.h>
#include <stdint.h>

typedef struct _file_handle{ 
    int fd;
} file_handle_t;

bool create_file(const char* file, file_handle_t** handle);

bool get_file_size(file_handle_t* handle, uint64_t* size);

bool close_file(file_handle_t** handle);

bool ensure_file_minimum_size(file_handle_t* handle, uint64_t minimum_size);

bool map_file(file_handle_t* handle, uint64_t minimum_size, void** address);

bool unmap_file(void* address, uint64_t size);

const char* _get_file_name(file_handle_t* handle);

