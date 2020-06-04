#pragma once


#include <stdint.h>

#define push_error(code, format, ...) push_error_internal(__FILE__, __LINE__, __func__, code, format, ##__VA_ARGS__)

__attribute__((__format__ (__printf__, 5, 6)))
void push_error_internal(const char* file, uint32_t line, const char *func, int32_t code, const char* format, ...);

typedef struct {
	int32_t len;
	uint32_t line;
 	int32_t code;
    char _padding[4];
	const char* msg;
	const char* file, *func; 
} error_entry;

typedef void(*error_callback)(error_entry* e, void * u);

void consume_errors(error_callback cb, void * u);

void print_all_errors(void);

void clear_errors(void);

error_entry* get_errors(uint32_t* number_of_errors);
