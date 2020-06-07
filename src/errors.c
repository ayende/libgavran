#include <stdio.h>
#include <string.h>
#include "errors.h" 

#define MAX_ERRORS 32
#define MAX_ERRORS_MSG_BUFFER 2048

_Thread_local static error_entry errors_buffer[32];
_Thread_local static char messages_buffer[MAX_ERRORS_MSG_BUFFER];

_Thread_local static uint32_t errors_count;
_Thread_local static uint32_t errors_buffer_len;
_Thread_local static uint32_t out_of_memory;

void print_error(error_entry* e, void *u);

__attribute__((__format__ (__printf__, 5, 6)))
void push_error_internal(const char* file, uint32_t line, const char *func, int32_t code, const char* format, ...) {
	error_entry* error;
	va_list ap; 

    if(errors_count + 1 >= MAX_ERRORS)
    {
        // we have no space any longer for errors, ignoring 
        out_of_memory |= 1;
        return;
    }

    error = &errors_buffer[errors_count++];
	
	error->code = code;
	error->file = file;
	error->line = line;
	error->func = func;

	va_start(ap, format);

    char* msg = (messages_buffer + errors_buffer_len);

    uint32_t avail = MAX_ERRORS_MSG_BUFFER - errors_buffer_len;
	error->len = vsnprintf(msg, avail, format, ap); 
    if ((uint32_t)error->len >= avail) {
		error->msg = NULL;
        error->msg = 0;
        out_of_memory |= 2;
    }
    else{
        errors_buffer_len += (uint32_t)error->len +1;
        error->msg = msg;
    }

	va_end(ap);
}

void consume_errors(error_callback cb, void * u) {
    for(uint32_t i = 0; i < errors_count; i ++){
		if (cb != NULL)
			cb(&errors_buffer[i], u);
    }

    if(out_of_memory){
        const char* msg = "Too many errors, additional errors were discarded";
        error_entry e = {
            .len = (int32_t)strlen(msg), 
            .line =  __LINE__,
            .code = -(int32_t)out_of_memory,
            .msg = msg,
            .file = __FILE__, 
            .func = __func__
        };
        cb(&e, u);
    }

    clear_errors();
}

error_entry* get_errors(uint32_t* number_of_errors){
    *number_of_errors = errors_count;
    return errors_buffer;
}

void clear_errors(void){
    out_of_memory = 0;
    memset(errors_buffer, 0, sizeof(error_entry) * errors_count);
    memset(messages_buffer, 0, errors_buffer_len);
    errors_buffer_len = 0;
    errors_count = 0;
}

void print_error(error_entry* e, void *u) {
    (void)u; // explicitly not needing this

	const char* file = strrchr(e->file, '/');
	if (file == NULL)
		file = strrchr(e->file, '\\');
	if (file == NULL)
		file = e->file;
	else
		file++;// move past the directory separator

    int chars = printf("%-12s - %6i - %s()", file, e->line, e->func);
	printf("%*i %s\n", 50 - chars, e->code, e->msg);
}


void print_all_errors(void) {
	consume_errors(print_error, NULL);
}
