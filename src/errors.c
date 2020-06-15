#include "errors.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

#define MAX_ERRORS 64
#define MAX_ERRORS_MSG_BUFFER 2048

_Thread_local static char _messages_buffer[MAX_ERRORS_MSG_BUFFER];
_Thread_local static const char* _errors_messages_buffer[MAX_ERRORS];
_Thread_local static int _errors_messages_codes[MAX_ERRORS];

_Thread_local static size_t _errors_count;
_Thread_local static size_t _errors_buffer_len;
_Thread_local static uint32_t _out_of_memory;

op_result_t* errors_append_message(const char* format, ...) {
  // should always be called with an error
  assert(_errors_count && _errors_buffer_len);
  char* msg = (_messages_buffer + _errors_buffer_len) - 1;
  size_t avail = MAX_ERRORS_MSG_BUFFER - _errors_buffer_len;

  va_list ap;
  va_start(ap, format);
  int chars = vsnprintf(msg, avail, format, ap);
  va_end(ap);
  if ((size_t)chars == avail) {
    *msg = 0;  // undo possible overwrite of null terminator
    _out_of_memory |= 2;
  }
  _errors_buffer_len += (size_t)chars;
  return 0;  // simply to allow it to be used in comma operator
}

__attribute__((__format__(__printf__, 5, 6))) op_result_t*
errors_push_new(const char* file, uint32_t line, const char* func,
                int32_t code, const char* format, ...) {
  if (_errors_count + 1 >= MAX_ERRORS) {
    // we have no space any longer for errors, ignoring
    _out_of_memory |= 1;
    return 0;
  }

  size_t index = _errors_count++;

  _errors_messages_codes[index] = code;

  char* msg = (_messages_buffer + _errors_buffer_len);

  size_t avail = MAX_ERRORS_MSG_BUFFER - _errors_buffer_len;
  int chars = snprintf(msg, avail, "%s()", func);
  chars += snprintf(msg + chars, avail - (size_t)chars,
                    "%-*c - %s:%i", 25 - chars, ' ', file, line);
  // safe to call immediately, if OOM, will write 0 bytes
  char stack_buffer[128];
  char* err_core_str = strerror_r(code, stack_buffer, 128);

  chars += snprintf(msg + chars, avail - (size_t)chars,
                    "%*c - %3i (%-20s) - ", 40 - chars, ' ', code,
                    err_core_str);
  if ((size_t)chars == avail) {
    goto oom;
  }

  va_list ap;
  va_start(ap, format);
  chars += vsnprintf(msg + chars, avail - (size_t)chars, format, ap);
  va_end(ap);

  if ((size_t)chars == avail) {
    goto oom;
  } else {
    _errors_buffer_len += (size_t)chars + 1;
    _errors_messages_buffer[index] = msg;
  }
  return 0;
oom:
  _out_of_memory |= 2;
  _errors_messages_buffer[index] = 0;
  return 0;
}

const char** get_errors_messages(size_t* number_of_errors) {
  *number_of_errors = _errors_count;
  return (const char**)_errors_messages_buffer;
}
int* get_errors_codes(size_t* number_of_errors) {
  *number_of_errors = _errors_count;
  return _errors_messages_codes;
}

void errors_print_all(void) {
  for (size_t i = 0; i < _errors_count; i++) {
    printf("%s\n", _errors_messages_buffer[i]);
  }

  if (_out_of_memory) {
    const char* msg =
        "Too many errors, "
        "additional errors were discarded";
    printf("%s (%d)\n", msg, -(int32_t)_out_of_memory);
  }
  errors_clear();
}

void errors_clear(void) {
  _out_of_memory = 0;
  memset(_errors_messages_codes, 0, sizeof(int32_t*) * _errors_count);
  memset(_errors_messages_buffer, 0, sizeof(char*) * _errors_count);
  memset(_messages_buffer, 0, _errors_buffer_len);
  _errors_buffer_len = 0;
  _errors_count = 0;
}

size_t errors_get_count() { return _errors_count; }
