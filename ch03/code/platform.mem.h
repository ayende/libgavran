#pragma once

#include "db.h"
#include "errors.h"
#include "paging.h"

result_t palmem_allocate_pages(size_t num_of_pages, void** buffer);