#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gavran/infrastructure.h>
#include <gavran/test.h>

static result_t read_all(int fd, size_t size, void* buffer) {
  size_t read_bytes = 0;
  while (read_bytes < size) {
    ssize_t cur =
        read(fd, (char*)buffer + read_bytes, size - read_bytes);

    if (cur <= 0) {
      failed(errno, msg("Failed to read requested data"),
             with(read_bytes, "%zu"), with(cur, "%zd"),
             with(size, "%zu"));
    }
    read_bytes += (size_t)cur;
  }
  return success();
}

enable_defer_imp(close, -1, *(int*), "%d");

// tag::read_int_to_str_buffer[]
// <1>
result_t read_int_to_str_buffer(const char* path, char** buffer) {
  int fd = open(path, O_RDONLY, 0);
  if (fd == -1) {
    // <2>
    failed(errno, msg("Unable to open file"), with(path, "%s"));
  }
  // <3>
  defer(close, fd);
  // <4>
  void* tmp;
  ensure(mem_alloc(&tmp, 128));

  size_t cancel_defer = 0;
  // <5>
  try_defer(free, tmp, cancel_defer);

  int val;
  ensure(read_all(fd, sizeof(int), &val), with(path, "%s"));

  int chars = sprintf(tmp, "%d", val);
  ensure(realloc(&tmp, (size_t)chars + 1));  // decrease size

  *buffer = tmp;
  // <6>
  cancel_defer = 1;
  return success();
}
// end::read_int_to_str_buffer[]

// tag::tests_02[]
result_t mark(int* f) {
  *f = 1;
  return success();
}

enable_defer(mark);

describe(errors_and_resource_usage) {
  before_each() {
    errors_clear();  // reset the state
  }

  it("Can setup defer to happen automatically") {
    int a = 0;
    {
      defer(mark, a);
      // should be implicitly called here
    }
    assert(a == 1);
  }

  it("Can cancel deferral defer to happen automatically") {
    int a = 0;
    {
      size_t cancel_defer = 0;
      try_defer(mark, a, cancel_defer);
      cancel_defer = 1;
    }
    assert(a == 0);
  }

  it("No errors should return zero count") {
    assert(errors_get_count() == 0, "Should have no errors");
  }

  it("Can record error") {
    errors_push(EIO, msg("Testing errors"));

    assert(errors_get_count() == 1, "Expected error");
    size_t count;
    assert(errors_get_codes(&count)[0] == EIO, "Got wrong code back");
    assert(count == 1, "Count doesn't match");
  }

  it("Max 64 errors") {
    for (size_t i = 0; i < 100; i++) {
      errors_push(EIO, msg("Testing errors"));
    }

    assert(errors_get_count() == 64, "Too many errors");
  }

  it("Very large errors won't overflow") {
    char buffer[256] = {0};
    memset(buffer, 'a', 255);

    for (size_t i = 0; i < 100; i++) {
      errors_push(EIO, msg("Testing errors"), with(buffer, "%s"));
    }
    assert(errors_get_count() == 64);

    size_t count;
    int* codes = errors_get_codes(&count);
    assert(count == 64);
    const char** msgs = errors_get_messages(&count);
    assert(count == 64);

    for (size_t i = 0; i < 6; i++) {
      assert(codes[i] == EIO);
      assert(msgs[i] != 0);
    }

    for (size_t i = 6; i < 64; i++) {
      assert(codes[i] == EIO);  // retained the code
      assert(msgs[i] == 0);     // no space, ignored
    }
  }

  it("Will translate codes to strings") {
    errors_push(EINVAL, msg("Testing errors"));

    size_t count;
    const char** msgs = errors_get_messages(&count);
    assert(count == 1, "Where is my error?");

    assert(strstr(msgs[0], "Invalid argument") != 0,
           "Couldn't find the error string");
  }

  it("Can call function and get error back") {
    char cwd[PATH_MAX];
    getcwd(cwd, sizeof(cwd));
    char* buffer = 0;
    // we pass the working directory, this expects a file, error
    op_result_t* res = read_int_to_str_buffer(cwd, &buffer);
    free(buffer);
    assert(res == 0, "Operation should have failed");
    size_t count;
    int* codes = errors_get_codes(&count);
    assert(count == 2, "Expected to get erors");
    assert(codes[0] == EISDIR, "Bad error code?");
    assert(codes[1] == EINVAL, "Bad error code?");
  }
}
// end::tests_02[]

snow_main();
