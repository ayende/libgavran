#pragma once

#include "db.h"
#include "errors.h"

result_t palmem_allocate_pages(void **p, uint64_t pages);

result_t palmem_free_page(void **p);

enable_defer(palmem_free_page);
