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

#include "defer.h"
#include "errors.h"
#include "platform.fs.h"

enable_defer_imp(close, -1, *(int *), "%d");

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
  return success();
}

const char *palfs_get_filename(file_handle_t *handle) {
  return handle->filename;
}
// end::handle_impl[]

static result_t closedir_null_safe(void *p) {
  if (!p)
    return success();
  ensure(closedir(p) != -1, msg("Failed to close director"));
  return success();
}
enable_defer(closedir_null_safe);

static bool try_parse_number(const char *src, char **end, int64_t *value) {
  errno = 0;
  *value = strtol(src, end, 10);
  if (*value) // if the value is not zero, we're good
    return true;
  if (src == *end)
    return false; // no digits
  if (errno)
    return false;
  return true; // really read 0
}

static int compare_wal_file_number(const void *a, const void *b) {
  const struct numbered_file *x = a;
  const struct numbered_file *y = b;
  if (x->number == y->number)
    return 0;
  return (x->number > y->number) ? 1 : -1;
}

result_t palfs_release_wal_files(struct files_list *list) {
  if (!list)
    return success();
  for (size_t i = 0; i < list->size; i++) {
    free(list->files[i]);
  }
  free(list);
  return success();
}
result_t palfs_get_wal_files(file_handle_t *datbase_file,
                             struct files_list **list) {
  DIR *dir = 0;
  char *mutable_file = 0;
  defer(free, mutable_file);
  defer(closedir_null_safe, dir);

  size_t file_name_len = strlen(datbase_file->filename);
  mutable_file = malloc(MAX(2, file_name_len + 1));
  if (!mutable_file) {
    goto no_mem;
  }
  memcpy(mutable_file, datbase_file->filename, file_name_len + 1);
  char *last = strrchr(mutable_file, '/');
  size_t directory_size_to_copy;
  if (!last) {
    dir = opendir(".");
    directory_size_to_copy = 0;
  } else {
    *last = 0;
    dir = opendir(mutable_file);
    // leave just the file name here
    directory_size_to_copy = (size_t)(last - mutable_file);
    file_name_len = file_name_len - directory_size_to_copy - 1;
    memmove(mutable_file, last + 1, file_name_len);
    mutable_file[file_name_len] = 0;
  }
  if (!dir) {
    failed(errno, msg("Unable to open directory"),
           with(datbase_file->filename, "%s"));
  }

  *list = calloc(sizeof(struct files_list), 1);
  if (!list) {
    goto no_mem;
  }
  while (true) {
    struct dirent *file = readdir(dir);
    if (!file)
      break;
    size_t cur_file_len = strlen(file->d_name);
    if (cur_file_len < file_name_len + 8) // using prefix "-000.wal"
      continue;
    if (strncmp(file->d_name, mutable_file, file_name_len))
      continue; // not a match
    char *num_end;
    int64_t wal_num;
    if (!try_parse_number(file->d_name + file_name_len, &num_end, &wal_num) ||
        !*num_end || wal_num >= 0) // we check for <0 for the - separator
      continue;
    if (strncmp(num_end, ".wal", 4))
      continue;
    // we found a file whose name is [dbname]-[num].wal
    struct numbered_file *wf =
        malloc(sizeof(struct files_list) + directory_size_to_copy + 1 +
               cur_file_len + 1);
    if (!wf)
      goto no_mem;
    wf->number = (uint64_t)(wal_num * -1);
    memcpy(wf->name, datbase_file->filename, directory_size_to_copy + 1);
    strcpy(wf->name + directory_size_to_copy + 1, file->d_name);
    struct files_list *n = realloc(*list, sizeof(struct files_list) +
                                              sizeof(struct numbered_file *) *
                                                  ((*list)->size + 1));
    if (!n) {
      free(wf);
      goto no_mem;
    }
    *list = n;
    (*list)->files[(*list)->size++] = wf;
  }
  // return in sorted order, we can't assume sorting from the file system.
  qsort((*list)->files, (*list)->size, sizeof(struct wal_file *),
        compare_wal_file_number);
  return success();

no_mem:
  if (*list) {
    for (size_t i = 0; i < (*list)->size; i++) {
      free((*list)->files[i]);
    }
  }
  free(*list);
  *list = 0;
  failed(ENOMEM, msg("Unable to allocate memory for wal_files"));
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
  if (fsync(fd)) {
    failed(errno, msg("Failed to fsync parent directory"), with(file, "%s"));
  }
  return success();
}
// end::fsync_parent_directory[]

// tag::ensure_file_path[]
static result_t ensure_file_path(char *file) {
  // already exists?
  struct stat st;
  if (!stat(file, &st)) {
    if (S_ISDIR(st.st_mode)) {
      failed(EISDIR, msg("The path is a directory, expected a file"),
             with(file, "%s"));
    }
    return success(); // file exists, so we are good
  }

  char *cur = file;
  if (*cur == '/') // rooted path
    cur++;

  while (*cur) {
    char *next_sep = strchr(cur, '/');
    if (!next_sep) {
      return success(); // no more directories in path
    }
    *next_sep = 0; // add null sep to cut the string

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
    *next_sep = '/';
    cur = next_sep + 1;
  }
  failed(EINVAL, msg("The last char in the path is '/', which is not allowed"),
         with(file, "%s"));
}
// end::ensure_file_path[]

// tag::palfs_create_file[]
result_t palfs_create_file(const char *path, file_handle_t *handle,
                           enum palfs_file_creation_flags flags) {
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
  // tag::file_creation[]
  int open_flags = O_CLOEXEC | O_CREAT | O_RDWR;
  if (flags & palfs_file_creation_flags_durable) {
    open_flags |= O_DIRECT | O_DSYNC;
  }
  int fd = open(handle->filename, open_flags, S_IRUSR | S_IWUSR);
  // end::file_creation[]
  if (fd == -1) {
    failed(errno, msg("Unable to open file "), with(handle->filename, "%s"));
  }
  if (isNew) {
    if (!fsync_parent_directory(handle->filename)) {
      failed(EIO, msg("Failed to fsync parent dir on new file creation"),
             with(handle->filename, "%s"));
    }
  }
  handle->fd = fd;
  return success();
}
// end::palfs_create_file[]

result_t palfs_get_filesize(file_handle_t *handle, uint64_t *size) {
  errors_assert_empty();
  struct stat st;
  int res = fstat(handle->fd, &st);
  if (res != -1) {
    *size = (uint64_t)st.st_size;
    return success();
  }
  failed(errno, msg("Unable to stat"), palfs_get_filename(handle));
}

result_t palfs_enable_writes(void *address, size_t size) {
  if (mprotect(address, size, PROT_READ | PROT_WRITE) == -1) {
    failed(errno, msg("Unable to modify the memory protection flags"));
  }
  return success();
}

result_t palfs_disable_writes(void *address, size_t size) {
  if (mprotect(address, size, PROT_READ) == -1) {
    failed(errno, msg("Unable to modify the memory protection flags"));
  }
  return success();
}

// tag::palfs_mmap[]

result_t palfs_mmap(file_handle_t *handle, uint64_t offset,
                    struct mmap_args *m) {
  errors_assert_empty();
  m->address =
      mmap(0, m->size, PROT_READ, MAP_SHARED, handle->fd, (off_t)offset);
  if (m->address == MAP_FAILED) {
    m->address = 0;
    failed(errno, msg("Unable to map file"),
           with(palfs_get_filename(handle), "%s"), with(m->size, "%lu"));
  }
  return success();
}

result_t palfs_unmap(struct mmap_args *m) {
  if (!m->address)
    return success();
  if (munmap(m->address, m->size) == -1) {
    failed(EINVAL, msg("Unable to unmap"), with(m->address, "%p"));
  }
  m->address = 0;
  return success();
}
// end::palfs_mmap[]

// tag::palfs_close_file[]
result_t palfs_close_file(file_handle_t *handle) {
  if (!handle)
    return success();
  if (close(handle->fd) == -1) {
    failed(errno, msg("Failed to close file"),
           with(palfs_get_filename(handle), "%s"), with(handle->fd, "%i"));
  }
  return success();
}
// end::palfs_close_file[]

// tag::palfs_fsync_file[]
result_t palfs_fsync_file(file_handle_t *handle) {
  if (fdatasync(handle->fd) == -1) {
    failed(errno, msg("Failed to sync file"),
           with(palfs_get_filename(handle), "%s"), with(handle->fd, "%i"));
  }
  return success();
}
// end::palfs_fsync_file[]

// tag::defer_palfs_close_file[]

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
  struct mmap_args *ctx = cd->target;
  if (!palfs_unmap(ctx)) {
    errors_push(EINVAL, msg("Failure to close file during defer"),
                with(ctx->address, "%p"));
  }
}
// end::defer_palfs_close_file[]

// tag::palfs_set_file_minsize[]
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

  return success();
}
// end::palfs_set_file_minsize[]

// tag::palfs_write_file[]
result_t palfs_write_file(file_handle_t *handle, uint64_t offset,
                          const char *buffer, size_t len_to_write) {
  errors_assert_empty();
  while (len_to_write) {
    ssize_t result = pwrite(handle->fd, buffer, len_to_write, (off_t)offset);
    if (result == -1) {
      if (errno == EINTR)
        continue; // repeat on signal

      failed(errno, msg("Unable to write bytes to file"),
             with(len_to_write, "%lu"), with(palfs_get_filename(handle), "%s"));
    }
    len_to_write -= (size_t)result;
    buffer += result;
    offset += (size_t)result;
  }
  return success();
}
// end::palfs_write_file[]
