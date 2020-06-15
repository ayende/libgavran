#pragma once

#include "db.h"
#include "errors.h"
#include "paging.h"

result_t txn_allocate_page(txn_t* tx, page_t* p);