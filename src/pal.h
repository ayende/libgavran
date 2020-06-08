#pragma once

#include <stdbool.h>
#include <stdint.h>
#include "errors.h"

typedef struct pal_file_handle file_handle_t;

size_t get_file_handle_size(const char* path, const char* name);
MUST_CHECK bool create_file(const char* path, const char* name, file_handle_t* handle);
MUST_CHECK bool ensure_file_minimum_size(file_handle_t* handle, uint64_t minimum_size);
MUST_CHECK bool close_file(file_handle_t* handle);


MUST_CHECK bool get_file_size(file_handle_t* handle, uint64_t* size);

MUST_CHECK size_t get_file_handle_size(const char* path, const char* name);


MUST_CHECK bool map_file(file_handle_t* handle, uint64_t minimum_size, void** address);

MUST_CHECK bool unmap_file(void* address, uint64_t size);

const char* get_file_name(file_handle_t* handle);

