#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <linux/limits.h>

#include "errors.h"
#include "pal.h"

struct pal_file_handle {
  int fd;
  char filename[];  // memory past end of buffer
};

const char *get_file_name(file_handle_t *handle) {
  return handle->filename;
}

MUST_CHECK bool get_file_handle_size(const char *path,
                                     size_t *required_size) {
  assert_no_existing_errors();
  size_t len = path ? strlen(path) : 0;
  if (!len) {
    fault(EINVAL, "The provided path was null or empty");
  }

  if (*path != '/') {
    fault(EINVAL, "The provided file: '%s' is not an absolute path",
          path);
  }

  *required_size =
      sizeof(file_handle_t) + len + 1 /* null terminating*/;
  return true;
}

static MUST_CHECK bool fsync_parent_directory(char *file) {
  char *last = strrchr(file, '/');
  int fd;
  if (!last) {
    fd = open(".", O_RDONLY);
  } else {
    *last = 0;
    fd = open(file, O_RDONLY);
    *last = '/';
  }
  if (fd == -1) {
    fault(errno, "Unable to open parent directory of: %s", file);
  }
  bool res = true;
  if (fsync(fd)) {
    push_error(errno, "Failed to fsync parent directory of: %s",
               file);
    res = false;
  }
  if (close(fd)) {
    push_error(
        errno,
        "Failed to close (after fsync) parent directory of: %s",
        file);
    res = false;
  }
  return res;
}

static void restore_slash(void **p) { **(char **)p = '/'; }

static MUST_CHECK bool ensure_file_path(char *file) {
  // already exists?
  struct stat st;
  if (!stat(file, &st)) {
    if (S_ISDIR(st.st_mode)) {
      fault(EISDIR, "The path '%s' is a directory, expected a file",
            file);
    }
    return true;  // file exists, so we are good
  }

  char *cur = file;
  if (*cur == '/')  // rooted path
    cur++;

  while (*cur) {
    char *next_sep = strchr(cur, '/');
    if (!next_sep) {
      return true;  // no more directories
    }
    *next_sep = 0;  // add null sep to cut the string
    defer(restore_slash, next_sep);

    if (!stat(file, &st)) {  // now we are checking the directory!
      if (!S_ISDIR(st.st_mode)) {
        fault(ENOTDIR,
              "The path '%s' is a file, but expected a directory",
              file);
      }
    } else {  // probably does not exists
      if (mkdir(file, S_IRWXU) == -1 && errno != EEXIST) {
        fault(errno, "Unable to create directory: %s", file);
      }
      ensure(fsync_parent_directory(file));
    }

    cur = next_sep + 1;
  }
  fault(EINVAL, "The last char in '%s' is '/', which is not allowed",
        file);
}

bool create_file(const char *path, file_handle_t *handle) {
  assert_no_existing_errors();

  memcpy(handle->filename, path, strlen(path) + 1);
  struct stat st;
  int isNew = false;
  if (stat(handle->filename, &st) == -1) {
    if (errno != ENOENT) {
      fault(errno, "Unable to stat(%s)", handle->filename);
    }
    isNew = true;
    ensure(ensure_file_path(handle->filename));
  } else {
    if (S_ISDIR(st.st_mode)) {
      fault(EISDIR, "The path '%s' is a directory, expected a file",
            handle->filename);
    }
  }

  int fd = open(handle->filename, O_CLOEXEC | O_CREAT | O_RDWR,
                S_IRUSR | S_IWUSR);
  if (fd == -1) {
    fault(errno, "Unable to open file %s", handle->filename);
  }
  if (isNew) {
    if (!fsync_parent_directory(handle->filename)) {
      push_error(EIO,
                 "Unable to fsync parent directory after creating "
                 "new file: %s",
                 handle->filename);
      if (!close(fd)) {
        push_error(errno, "Unable to close file (%i) %s", fd,
                   handle->filename);
      }
      return false;
    }
  }
  handle->fd = fd;
  return true;
}

bool get_file_size(file_handle_t *handle, uint64_t *size) {
  assert_no_existing_errors();
  struct stat st;
  int res = fstat(handle->fd, &st);
  if (res != -1) {
    *size = (uint64_t)st.st_size;
    return true;
  }
  fault(errno, "Unable to stat(%s)", get_file_name(handle));
}

bool map_file(file_handle_t *handle, uint64_t offset, uint64_t size,
              void **address) {
  assert_no_existing_errors();
  void *addr =
      mmap(0, size, PROT_READ, MAP_SHARED, handle->fd, (off_t)offset);
  if (addr == MAP_FAILED) {
    *address = 0;
    fault(errno, "Unable to map file %s with size %lu",
          get_file_name(handle), size);
  }
  *address = addr;
  return true;
}

bool unmap_file(void *address, uint64_t size) {
  if (munmap(address, size) == -1) {
    fault(EINVAL, "Unable to unmap %p!", address);
  }
  return true;
}

bool close_file(file_handle_t *handle) {
  if (!handle) return true;

  if (close(handle->fd) == -1) {
    fault(errno, "Failed to close file %s (%i)",
          get_file_name(handle), handle->fd);
  }

  return true;
}

bool ensure_file_minimum_size(file_handle_t *handle,
                              uint64_t minimum_size) {
  assert_no_existing_errors();
  int rc = posix_fallocate(handle->fd, 0, (off_t)minimum_size);
  const char *filename = get_file_name(handle);
  if (rc) {
    fault(rc, "Unable to extend file to size %s to %lu", filename,
          minimum_size);
  }
  char filename_mutable[PATH_MAX];
  size_t name_len = strlen(filename);
  if (name_len >= PATH_MAX) {
    fault(ENAMETOOLONG,
          "The provided name is %zu characters, which is too long",
          name_len);
  }
  memcpy(filename_mutable, filename, name_len + 1);

  ensure(fsync_parent_directory(filename_mutable));

  return true;
}

MUST_CHECK bool write_file(file_handle_t *handle, uint64_t offset,
                           const char *buffer, size_t len_to_write) {
  assert_no_existing_errors();
  while (len_to_write) {
    ssize_t result =
        pwrite(handle->fd, buffer, len_to_write, (off_t)offset);
    if (result == -1) {
      if (errno == EINTR) continue;

      fault(errno, "Unable to write %zu bytes to file %s",
            len_to_write, get_file_name(handle));
    }
    len_to_write -= (size_t)result;
    buffer += result;
    offset += (size_t)result;
  }
  return true;
}
