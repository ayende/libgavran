// tag::declarations[]

#include "errors.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

#define MAX_ERRORS 64
#define MAX_ERRORS_MSG_BUFFER 2048

_Thread_local static char _messages_buffer[MAX_ERRORS_MSG_BUFFER];
_Thread_local static const char *_errors_messages_buffer[MAX_ERRORS];
_Thread_local static int _errors_messages_codes[MAX_ERRORS];

_Thread_local size_t _errors_count;
_Thread_local static size_t _errors_buffer_len;
_Thread_local static uint32_t _out_of_memory;

// end::declarations[]

// tag::try_sprintf[]
__attribute__((__format__(__printf__, 4, 0))) static bool
try_vsprintf(char **buffer, char *buffer_end, size_t *chars,
             const char *format, va_list ap) {
  size_t sz = (size_t)(buffer_end - *buffer);
  int rc = vsnprintf(*buffer, sz, format, ap);
  if (rc < 0 ||          // encoding
      (size_t)rc >= sz)  // space
    return false;

  *buffer += rc;
  *chars += (size_t)rc;
  return true;
}

__attribute__((__format__(__printf__, 4, 5))) static bool try_sprintf(
    char **buffer, char *buffer_end, size_t *chars,
    const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  bool ret = try_vsprintf(buffer, buffer_end, chars, format, ap);
  va_end(ap);
  return ret;
}
// end::try_sprintf[]

// tag::errors_push_new[]
op_result_t *errors_push_new(const char *file, uint32_t line,
                             const char *func, int32_t code) {
  // <1>
  if (_errors_count >= MAX_ERRORS) {
    // we have no space any longer for errors, ignoring
    _out_of_memory |= 1;
    return 0;
  }

  // <2>
  size_t index = _errors_count++;
  _errors_messages_codes[index] = code;

  char *msg = (_messages_buffer + _errors_buffer_len);
  char *end = _messages_buffer + MAX_ERRORS_MSG_BUFFER;
  char *start = msg;

  char stack_buffer[128];
  int rc = strerror_r(code, stack_buffer, 128);
  if (rc) strcpy(stack_buffer, "Unknown code");

  size_t chars_written = 0;
  // <3>
  if (!try_sprintf(&msg, end, &chars_written, "%s()", func) ||
      !try_sprintf(&msg, end, &chars_written, "%-*c - %s:%i",
                   (int)(30 - chars_written), ' ', file, line) ||
      !try_sprintf(&msg, end, &chars_written, "%*c - %3i %-20s |  ",
                   (int)(50 - chars_written), ' ', code,
                   stack_buffer))
    goto oom;

  _errors_buffer_len += (size_t)(msg - start);
  _errors_messages_buffer[index] = start;
  return 0;
oom:
  _out_of_memory |= 2;
  _errors_messages_buffer[index] = 0;
  return 0;
}
// end::errors_push_new[]

// tag::errors_append_message[]
op_result_t *errors_append_message(const char *format, ...) {
  if (!_errors_count && _errors_buffer_len) return 0;

  // <1>
  char *msg = (_messages_buffer + _errors_buffer_len) - 1;
  char *end = _messages_buffer + MAX_ERRORS_MSG_BUFFER;
  size_t chars_written = 0;

  va_list ap;
  va_start(ap, format);
  bool ret = try_vsprintf(&msg, end, &chars_written, format, ap);
  va_end(ap);
  if (!ret) {
    // <2>
    *msg = 0;  // undo possible overwrite of null terminator
    _out_of_memory |= 2;
    return 0;
  }

  _errors_buffer_len += chars_written;
  return 0;  // simply to allow it to be used in comma operator
}
// end::errors_append_message[]

// tag::rest[]
const char **errors_get_messages(size_t *number_of_errors) {
  *number_of_errors = _errors_count;
  return (const char **)_errors_messages_buffer;
}

int *errors_get_codes(size_t *number_of_errors) {
  *number_of_errors = _errors_count;
  return _errors_messages_codes;
}

void errors_print_all(void) {
  for (size_t i = 0; i < _errors_count; i++) {
    printf("%s\n", _errors_messages_buffer[i]);
  }

  if (_out_of_memory) {
    const char *msg =
        "Too many errors, "
        "additional errors were discarded";
    printf("%s (%d)\n", msg, -(int32_t)_out_of_memory);
  }
  errors_clear();
}

void errors_clear(void) {
  _out_of_memory = 0;
  memset(_errors_messages_codes, 0,
         sizeof(int32_t *) * _errors_count);
  memset(_errors_messages_buffer, 0, sizeof(char *) * _errors_count);
  memset(_messages_buffer, 0, _errors_buffer_len);
  _errors_buffer_len = 0;
  _errors_count = 0;
}

inline size_t errors_get_count() { return _errors_count; }

uint32_t errors_get_oom_flag() { return _out_of_memory; }
// end::rest[]
