#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <linux/limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <gavran/infrastructure.h>
#include <gavran/pal.h>

enable_defer_imp(close, -1, *(int *), "%d");

// tag::fsync_parent_directory[]
static result_t fsync_parent_directory(char *file) {
  char *last = strrchr(file, '/');
  int fd;
  if (!last) {
    // <1>
    fd = open(".", O_RDONLY);
  } else {
    // <2>
    *last = 0;
    fd = open(file, O_RDONLY);
    *last = '/';
  }
  if (fd == -1) {
    failed(errno, msg("Unable to open parent directory"),
           with(file, "%s"));
  }
  defer(close, fd);
  if (fsync(fd)) {
    failed(errno, msg("Failed to fsync parent directory"),
           with(file, "%s"));
  }
  return success();
}
// end::fsync_parent_directory[]

// tag::pal_ensure_full_path[]
static result_t pal_ensure_full_path(char *file) {
  // already exists?
  struct stat st;
  if (!stat(file, &st)) {
    if (S_ISDIR(st.st_mode)) {
      failed(EISDIR, msg("The path is a directory, expected a file"),
             with(file, "%s"));
    }
    return success();  // file exists, so we are good
  }

  char *cur = file;
  if (*cur == '/')  // rooted path
    cur++;

  while (*cur) {
    char *next_sep = strchr(cur, '/');
    if (!next_sep) {
      return success();  // no more directories in path
    }
    *next_sep = 0;  // add null sep to cut the string

    if (!stat(file, &st)) {  // now we are checking the directory!
      if (!S_ISDIR(st.st_mode)) {
        failed(ENOTDIR,
               msg("The path is a file, but expected a directory"),
               with(file, "%s"));
      }
    } else {  // probably does not exists
      if (mkdir(file, S_IRWXU) == -1 && errno != EEXIST) {
        failed(errno, msg("Unable to create directory"),
               with(file, "%s"));
      }
      ensure(fsync_parent_directory(file));
    }
    *next_sep = '/';
    cur = next_sep + 1;
  }
  failed(
      EINVAL,
      msg("The last char in the path is '/', which is not allowed"),
      with(file, "%s"));
}
// end::pal_ensure_full_path[]

// tag::pal_ensure_path[]
static result_t pal_ensure_path(char *filename, uint64_t *size) {
  struct stat st;
  if (stat(filename, &st) == -1) {
    if (errno != ENOENT) {
      failed(errno, msg("Unable to stat "), with(filename, "%s"));
    }
    *size = 0;
    ensure(pal_ensure_full_path(filename));
    int fd = open(filename, O_CLOEXEC | O_CREAT | O_RDWR,
                  S_IRUSR | S_IWUSR);
    if (fd == -1) {
      failed(errno, msg("Unable to create file "),
             with(filename, "%s"));
    }
    if (close(fd) == -1) {
      failed(errno, msg("Unable to close file after creating it "),
             with(filename, "%s"));
    }
  } else {
    *size = (uint64_t)st.st_size;
    if (S_ISDIR(st.st_mode)) {
      failed(EISDIR, msg("The path is a directory, expected a file "),
             with(filename, "%s"));
    }
  }
  return success();
}
// end::pal_ensure_path[]

// tag::pal_create_file[]
result_t pal_create_file(const char *path, file_handle_t **handle_out,
                         enum pal_file_creation_flags flags) {
  errors_assert_empty();
  size_t cancel_defer = 0;

  // <1>
  file_handle_t *handle;
  ensure(mem_alloc((void *)&handle, sizeof(file_handle_t)));
  try_defer(free, handle, cancel_defer);

  // <2>
  char *mutable;
  ensure(mem_duplicate_string(&mutable, path));
  defer(free, mutable);
  ensure(pal_ensure_path(mutable, &handle->size));

  // <3>
  handle->filename = realpath(path, 0);
  if (!handle->filename) {
    failed(errno, msg("Failed to resolve realpath() of file"),
           with(path, "%s"));
  }
  try_defer(free, handle->filename, cancel_defer);

  // <4>
  int open_flags = O_CLOEXEC | O_CREAT | O_RDWR;
  if (flags & pal_file_creation_flags_durable) {
    open_flags |= O_DIRECT | O_DSYNC;
  }
  handle->fd = open(handle->filename, open_flags, S_IRUSR | S_IWUSR);
  if (handle->fd == -1) {
    failed(errno, msg("Unable to open file "),
           with(handle->filename, "%s"));
  }
  // <5>
  if (handle->size == 0) {  // new db
    if (!fsync_parent_directory(handle->filename)) {
      failed(EIO,
             msg("Failed to fsync parent dir on new file creation"),
             with(handle->filename, "%s"));
    }
  }

  *handle_out = handle;
  cancel_defer = 1;
  return success();
}
// end::pal_create_file[]

// tag::pal_mmap[]
result_t pal_mmap(file_handle_t *handle, uint64_t offset, span_t *m) {
  errors_assert_empty();
  m->address = mmap(0, m->size, PROT_READ, MAP_SHARED, handle->fd,
                    (off_t)offset);
  if (m->address == MAP_FAILED) {
    m->address = 0;
    failed(errno, msg("Unable to map file"),
           with(handle->filename, "%s"), with(m->size, "%lu"));
  }
  return success();
}

result_t pal_unmap(span_t *m) {
  if (!m->address) return success();
  if (munmap(m->address, m->size) == -1) {
    failed(EINVAL, msg("Unable to unmap"), with(m->address, "%p"));
  }
  m->address = 0;
  return success();
}
// end::pal_mmap[]

// tag::pal_close_file[]
result_t pal_close_file(file_handle_t *handle) {
  if (!handle) return success();
  defer(free, handle);
  defer(free, handle->filename);
  if (close(handle->fd) == -1) {
    failed(errno, msg("Failed to close file"),
           with(handle->filename, "%s"), with(handle->fd, "%i"));
  }
  return success();
}
// end::pal_close_file[]

// tag::pal_enable_writes[]
result_t pal_enable_writes(span_t *s) {
  if (mprotect(s->address, s->size, PROT_READ | PROT_WRITE) == -1) {
    failed(errno,
           msg("Unable to modify the memory protection flags"));
  }
  return success();
}

void defer_pal_disable_writes(cancel_defer_t *cd) {
  if (cd->cancelled && *cd->cancelled) return;
  span_t *s = *cd->target;
  if (mprotect(s->address, s->size, PROT_READ) == -1) {
    errors_push(errno,
                msg("Unable to modify the memory protection flags"));
  }
}
// end::pal_enable_writes[]

// tag::pal_fsync[]
result_t pal_fsync(file_handle_t *handle) {
  if (fdatasync(handle->fd) == -1) {
    failed(errno, msg("Failed to sync file"),
           with(handle->filename, "%s"), with(handle->fd, "%i"));
  }
  return success();
}
// end::pal_fsync[]

// tag::pal_map_defer[]
void defer_pal_close_file(cancel_defer_t *cd) {
  if (cd->cancelled && *cd->cancelled) return;
  if (!verify(pal_close_file(*cd->target))) {
    errors_push(EINVAL, msg("Failure to close file during defer"));
  }
}

void defer_pal_unmap(cancel_defer_t *cd) {
  if (cd->cancelled && *cd->cancelled) return;
  span_t *ctx = *cd->target;
  if (!verify(pal_unmap(ctx))) {
    errors_push(EINVAL, msg("Failure to close file during defer"),
                with(ctx->address, "%p"));
  }
}
// end::pal_map_defer[]

// tag::pal_set_file_size[]
result_t pal_set_file_size(file_handle_t *handle,
                           uint64_t minimum_size,
                           uint64_t maximum_size) {
  errors_assert_empty();

  struct stat st;
  if (fstat(handle->fd, &st)) {
    failed(errno, msg("Unable to stat file"),
           with(handle->filename, "%s"), with(minimum_size, "%lu"));
  }
  uint64_t new_size = 0;

  if (minimum_size > (uint64_t)st.st_size) {
    new_size = minimum_size;
  } else if (maximum_size < (uint64_t)st.st_size) {
    new_size = maximum_size;
  }

  if (!new_size) return success();

  if (ftruncate(handle->fd, (off_t)new_size) == -1) {
    failed(errno, msg("Unable to change file to size"),
           with(handle->filename, "%s"), with(new_size, "%lu"));
  }
  handle->size = new_size;

  char *mutable;
  ensure(mem_duplicate_string(&mutable, handle->filename));
  defer(free, mutable);

  ensure(fsync_parent_directory(mutable));

  return success();
}
// end::pal_set_file_size[]

// tag::pal_write_file[]
result_t pal_write_file(file_handle_t *handle, uint64_t offset,
                        const char *buffer, size_t size) {
  errors_assert_empty();
  while (size) {
    ssize_t result = pwrite(handle->fd, buffer, size, (off_t)offset);
    if (result == -1) {
      if (errno == EINTR) continue;  // repeat on signal

      failed(errno, msg("Unable to write bytes to file"),
             with(size, "%lu"), with(handle->filename, "%s"));
    }
    size -= (size_t)result;
    buffer += result;
    offset += (size_t)result;
  }
  return success();
}
result_t pal_read_file(file_handle_t *handle, uint64_t offset,
                       void *buffer, size_t size) {
  errors_assert_empty();
  while (size) {
    ssize_t result = pread(handle->fd, buffer, size, (off_t)offset);
    if (result == 0) {
      failed(EINVAL, msg("File EOF before we read entire buffer"),
             with(size, "%lu"), with(handle->filename, "%s"));
    }
    if (result == -1) {
      if (errno == EINTR) continue;  // repeat on signal

      failed(errno, msg("Unable to read bytes from file"),
             with(size, "%lu"), with(handle->filename, "%s"));
    }
    size -= (size_t)result;
    buffer += result;
    offset += (size_t)result;
  }
  return success();
}
// end::pal_write_file[]
