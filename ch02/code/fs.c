#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <linux/limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "defer.h"
#include "errors.h"
#include "platform.fs.h"

// tag::handle_impl[]
struct pal_file_handle {
  int fd;
  char filename[]; // memory past end of buffer
};

result_t palfs_compute_handle_size(const char *path, size_t *required_size) {
  errors_assert_empty();
  size_t len = path ? strlen(path) : 0;
  if (!len) {
    failed(EINVAL, msg("The provided path was null or empty"));
  }

  if (*path != '/') {
    failed(EINVAL, msg("The path is not an absolute path"), with(path, "%s"));
  }

  *required_size = sizeof(file_handle_t) + len + 1 /* null termination*/;
  success();
}

const char *palfs_get_filename(file_handle_t *handle) {
  return handle->filename;
}
// end::handle_impl[]

void defer_close(struct cancel_defer *cd) {
  if (cd->cancelled && *cd->cancelled)
    return;
  int *fdp = cd->target;
  if (close(*fdp) == -1) {
    errors_push(errno, msg("Failed to close file"), with(*fdp, "%i"));
  }
}

// tag::fsync_parent_directory[]
static result_t fsync_parent_directory(char *file) {
  char *last = strrchr(file, '/');
  int fd;
  if (!last) {
    fd = open(".", O_RDONLY);
  } else {
    // <1>
    *last = 0;
    fd = open(file, O_RDONLY);
    *last = '/';
  }
  // <2>
  if (fd == -1) {
    failed(errno, msg("Unable to open parent directory"), with(file, "%s"));
  }
  // <3>
  defer(close, &fd);
  op_result_t *res = (void *)1;
  if (fsync(fd)) {
    failed(errno, msg("Failed to fsync parent directory"), with(file, "%s"));
  }
  success();
}
// end::fsync_parent_directory[]

static void defer_restore_slash(struct cancel_defer *cd) {
  char *p = cd->target;
  *p = '/';
}

static result_t ensure_file_path(char *file) {
  // already exists?
  struct stat st;
  if (!stat(file, &st)) {
    if (S_ISDIR(st.st_mode)) {
      failed(EISDIR, msg("The path is a directory, expected a file"),
             with(file, "%s"));
    }
    success(); // file exists, so we are good
  }

  char *cur = file;
  if (*cur == '/') // rooted path
    cur++;

  while (*cur) {
    char *next_sep = strchr(cur, '/');
    if (!next_sep) {
      success();
    }
    *next_sep = 0; // add null sep to cut the string
    defer(restore_slash, next_sep);

    if (!stat(file, &st)) { // now we are checking the directory!
      if (!S_ISDIR(st.st_mode)) {
        failed(ENOTDIR, msg("The path is a file, but expected a directory"),
               with(file, "%s"));
      }
    } else { // probably does not exists
      if (mkdir(file, S_IRWXU) == -1 && errno != EEXIST) {
        failed(errno, msg("Unable to create directory"), with(file, "%s"));
      }
      ensure(fsync_parent_directory(file));
    }

    cur = next_sep + 1;
  }
  failed(EINVAL, msg("The last char in the path is '/', which is not allowed"),
         with(file, "%s"));
}

result_t palfs_create_file(const char *path, file_handle_t *handle) {
  errors_assert_empty();

  memcpy(handle->filename, path, strlen(path) + 1);
  struct stat st;
  int isNew = false;
  if (stat(handle->filename, &st) == -1) {
    if (errno != ENOENT) {
      failed(errno, msg("Unable to stat "), with(handle->filename, "%s"));
    }
    isNew = true;
    ensure(ensure_file_path(handle->filename));
  } else {
    if (S_ISDIR(st.st_mode)) {
      failed(EISDIR, msg("The path is a directory, expected a file "),
             with(handle->filename, "%s"));
    }
  }

  int fd =
      open(handle->filename, O_CLOEXEC | O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  if (fd == -1) {
    failed(errno, msg("Unable to open file "), with(handle->filename, "%s"));
  }
  size_t cancel_defer = 0;
  try_defer(close, &fd, cancel_defer);
  if (isNew) {
    ensure(fsync_parent_directory(handle->filename));
  }
  cancel_defer = 1;
  handle->fd = fd;
  success();
}

result_t palfs_get_filesize(file_handle_t *handle, uint64_t *size) {
  errors_assert_empty();
  struct stat st;
  int res = fstat(handle->fd, &st);
  if (res != -1) {
    *size = (uint64_t)st.st_size;
    success();
  }
  failed(errno, msg("Unable to stat"), palfs_get_filename(handle));
}

result_t palfs_mmap(file_handle_t *handle, uint64_t offset, uint64_t size,
                    void **address) {
  errors_assert_empty();
  void *addr = mmap(0, size, PROT_READ, MAP_SHARED, handle->fd, (off_t)offset);
  if (addr == MAP_FAILED) {
    failed(errno, msg("Unable to map file"),
           with(palfs_get_filename(handle), "%s"), with(size, "%lu"));
  }
  *address = addr;
  success();
}

result_t palfs_unmap(void *address, uint64_t size) {
  if (munmap(address, size) == -1) {
    failed(EINVAL, msg("Unable to unmap"), with(address, "%p"));
  }
  success();
}

result_t palfs_close_file(file_handle_t *handle) {
  if (close(handle->fd) == -1) {
    failed(errno, msg("Failed to close file"),
           with(palfs_get_filename(handle), "%s"), with(handle->fd, "%i"));
  }
  success();
}

void defer_palfs_close_file(struct cancel_defer *cd) {
  if (cd->cancelled && *cd->cancelled)
    return;
  if (!palfs_close_file(cd->target)) {
    errors_push(EINVAL, msg("Failure to close file during defer"));
  }
}

void defer_palfs_unmap(struct cancel_defer *cd) {
  if (cd->cancelled && *cd->cancelled)
    return;
  struct unmap_defer_ctx *ctx = cd->target;
  if (!palfs_unmap(ctx->addr, ctx->size)) {
    errors_push(EINVAL, msg("Failure to close file during defer"));
  }
}

result_t palfs_set_file_minsize(file_handle_t *handle, uint64_t minimum_size) {
  errors_assert_empty();

  const char *filename = palfs_get_filename(handle);

  int rc = posix_fallocate(handle->fd, 0, (off_t)minimum_size);

  if (rc) {
    failed(rc, msg("Unable to extend file to size"), with(filename, "%s"),
           with(minimum_size, "%lu"));
  }
  char filename_mutable[PATH_MAX];
  size_t name_len = strlen(filename);
  if (name_len + 1 >= PATH_MAX) {
    failed(ENAMETOOLONG, msg("The provided name is too long"),
           with(name_len, "%zu"));
  }
  memcpy(filename_mutable, filename, name_len + 1);

  ensure(fsync_parent_directory(filename_mutable));

  success();
}

static result_t create_and_set_file() {
  size_t size;
  ensure(palfs_compute_handle_size(
      "/home/ayende/projects/libgavran/ch02/code/test", &size));

  file_handle_t *handle = malloc(size);
  ensure(handle);
  defer(free, handle);

  ensure(palfs_create_file("/home/ayende/projects/libgavran/ch02/code/test",
                           handle));
  defer(palfs_close_file, handle);

  ensure(palfs_set_file_minsize(handle, 128 * 1024));

  success();
}

int main() {
  if (!create_and_set_file()) {
    errors_print_all();
  }
  return 1;
}
