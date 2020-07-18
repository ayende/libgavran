#include <errno.h>
#include <string.h>

#include <gavran/infrastructure.h>
#include <gavran/pal.h>
#include <gavran/test.h>

// tag::create_and_set_file[]
static result_t create_and_set_file(const char* file) {
  file_handle_t* h;
  ensure(pal_create_file(file, &h, pal_file_creation_flags_none));
  defer(pal_close_file, h);
  ensure(h->size == 0);

  ensure(pal_set_file_size(h, 1024 * 128, 1024 * 128));
  ensure(h->size == 1024 * 128);

  return success();
}
// end::create_and_set_file[]

// tag::read_write_io[]
static result_t read_write_io(const char* file) {
  file_handle_t* h;
  ensure(pal_create_file(file, &h, pal_file_creation_flags_none));
  defer(pal_close_file, h);
  ensure(pal_set_file_size(h, 1024 * 128, 1024 * 128));

  span_t range = {.size = h->size};
  ensure(pal_mmap(h, 0, &range));
  defer(pal_unmap, range);
  ensure(range.size == 1024 * 128);

  const char* msg = "Hello from Gavran";
  ensure(pal_write_file(h, 0, msg, strlen(msg)));

  ensure(strcmp(msg, range.address) == 0);

  return success();
}
// end::read_write_io[]

// tag::tests[]
describe(pal_tests) {
  const char file[] = "/tmp/files/try";

  before_each() {
    errors_clear();
    system("mkdir -p /tmp/files");
    system("rm -f /tmp/files/*");
  }

  it("can work with files") { assert(create_and_set_file(file)); }

  it("can read and write") { assert(read_write_io(file)); }

  it("can get file name") {
    file_handle_t* h;
    assert(pal_create_file(file, &h, pal_file_creation_flags_none));
    defer(pal_close_file, h);
    assert(strcmp(file, h->filename) == 0);
  }

  it("can open and close file") {
    file_handle_t* h;
    assert(pal_create_file(file, &h, pal_file_creation_flags_none));
    assert(pal_close_file(h));
  }

  it("will create empty file") {
    file_handle_t* h;
    assert(pal_create_file(file, &h, pal_file_creation_flags_none));
    defer(pal_close_file, h);
    assert(h->size == 0);
  }

  it("will error on opening directory") {
    file_handle_t* h;
    assert(!pal_create_file("/tmp/files", &h,
                            pal_file_creation_flags_none));
  }
}
// end::tests[]
