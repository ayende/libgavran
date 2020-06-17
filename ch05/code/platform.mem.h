#pragma once

#include "db.h"
#include "errors.h"

result_t palmem_allocate_pages(page_t* p);

result_t palmem_free_page(page_t* p);

enable_defer(palmem_free_page);