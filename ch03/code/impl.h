#include "db.h"
#include "platform.fs.h"

struct database_state {
  database_options_t options;
  struct mmap_args mmap;
  file_handle_t *handle;
};
