#pragma once

#include <stdint.h>

#define MUST_CHECK __attribute__((warn_unused_result))

#define mark_error() push_error_again(__FILE__, __LINE__, __func__)
#define push_error(code, format, ...) push_error_internal(\
	__FILE__, __LINE__, __func__, code, format, ##__VA_ARGS__)

__attribute__((__format__ (__printf__, 5, 6)))
void 	push_error_internal(const char* file, uint32_t line, const char *func, int32_t code, const char* format, ...);

void 	push_error_again(const char* file, uint32_t line, const char *func);

void 	print_all_errors(void);

void 	clear_errors(void);

const char** 
		get_errors_messages(size_t* number_of_errors);
		
int* 	get_errors_codes(size_t* number_of_errors);
