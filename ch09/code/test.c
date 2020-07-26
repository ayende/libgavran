#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <gavran/db.h>
#include <gavran/test.h>

// tag::tests09[]
describe(wal_tests) {
  before_each() {
    errors_clear();
    system("mkdir -p /tmp/db");
    system("rm -f /tmp/db/*");
  }
}
// end::tests09[]
