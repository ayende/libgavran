
#include "defer.h"
#include "errors.h"
#include <errno.h>
#include <unistd.h>

void
defer_close(struct cancel_defer* cd)
{
  if (cd->cancelled && *cd->cancelled)
    return;
  int fd = *(int*)cd->target;
  if (close(fd) == -1) {
    push_error(errno, "Failed to close file");
    with(fd, "%d");
  }
}
