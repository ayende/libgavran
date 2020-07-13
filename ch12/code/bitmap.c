#include <limits.h>
#include <string.h>

#include "impl.h"

// tag::init_search[]
void init_search(bitmap_search_state_t *search, void *bitmap, uint64_t size,
                 uint64_t required) {
  memset(search, 0, sizeof(bitmap_search_state_t));
  search->bitmap = bitmap;
  search->bitmap_size = size;
  search->space_required = required;
  search->current_word = search->bitmap[0];
  search->previous_set_bit = ULONG_MAX;
  // <1>
  if ((search->space_required & ~PAGES_IN_METADATA_MASK) == 0) {
    // we must use one more in this cases, so the first page
    // would "poke" into an existing range that has metadata
    // pages
    search->space_required++;
  }
}
// end::init_search[]

static bool handle_final_word(bitmap_search_state_t *search) {
  if (search->current_set_bit >
      search->previous_set_bit + search->space_required) {
    search->found_position =
        search->previous_set_bit + 1; // intentionally overflowing here
    search->space_available_at_position =
        (search->current_set_bit - search->found_position);
    return true;
  }
  return false;
}

static bool search_word(bitmap_search_state_t *search) {
  uint64_t bitset = search->current_word;

  if (bitset == ULONG_MAX) {
    // all bits are set, can skip whole thing
    handle_final_word(search);
    search->previous_set_bit = (search->index + 1) * 64 - 1;
    return false;
  }

  if (bitset == 0) {
    search->current_set_bit = (search->index + 1) * 64;
    return handle_final_word(search);
  }

  while (bitset != 0) {
    int r = __builtin_ctzl(bitset);
    search->current_set_bit = (search->index) * 64 + (uint64_t)r;
    if (search->current_set_bit >
        search->previous_set_bit + search->space_required) {
      // intentionally overflowing here
      search->found_position = search->previous_set_bit + 1;
      search->space_available_at_position =
          (search->current_set_bit - search->found_position);
      search->previous_set_bit = search->current_set_bit;
      return true;
    }
    search->previous_set_bit = search->current_set_bit;
    bitset ^= (bitset & -bitset);
  }

  search->current_set_bit = (search->index + 1) * 64;
  return handle_final_word(search);
}

// tag::filter_unacceptable_ranges[]
static bool filter_small_unacceptable_range(bitmap_search_state_t *search) {
  if ((search->found_position & ~PAGES_IN_METADATA_MASK) == 0) {
    // <2>
    // cannot use, falls on metadata page, try to shift it
    search->found_position++;
    search->space_available_at_position--;
    // may fail if there isn't enough room now
    return (search->space_required > search->space_available_at_position);
  }

  // <3>
  uint64_t start = search->found_position & PAGES_IN_METADATA_MASK;
  uint64_t end = (search->found_position + search->space_required - 1) &
                 PAGES_IN_METADATA_MASK;

  if (start == end) // on the same MB, nothing to do
    return true;

  // <4>
  uint64_t new_start =
      start + PAGES_IN_METADATA + 1; // past the next metadata page

  if (new_start + search->space_required >
      search->found_position + search->space_available_at_position) {
    // not enough space to shift things
    return false;
  }
  search->space_available_at_position -= (new_start - search->found_position);
  search->found_position = new_start;
  return true;
}

static bool filter_unacceptable_ranges(bitmap_search_state_t *search) {
  if (search->space_required > search->space_available_at_position)
    return false;
  // <5>
  // handle small allocations
  if (search->space_required < PAGES_IN_METADATA) {
    return filter_small_unacceptable_range(search);
  }

  // large values here, size is guranteed to *not* be a multiple of 128
  if (((search->found_position + search->space_required + 1) %
       PAGES_IN_METADATA) == 0) {
    // nothing to do, already ends just before a metadata page
    return true;
  }

  // <6>
  uint64_t new_end = ((search->found_position + search->space_required) &
                      PAGES_IN_METADATA_MASK) +
                     PAGES_IN_METADATA;
  if (new_end > search->found_position + search->space_available_at_position) {
    return false; // not enough room to shift things
  }
  search->space_available_at_position -=
      new_end - search->found_position - search->space_required;
  search->found_position = new_end - search->space_required;
  return true;
}
// end::filter_unacceptable_ranges[]

// tag::search_bitmap[]
static bool search_bitmap(bitmap_search_state_t *search) {
  uint64_t original_pos = search->found_position;
  do {
    if (search_word(search)) {
      if (search->current_set_bit % 64) {
        // mask the already found item
        uint64_t mask = ~(ULONG_MAX << (search->current_set_bit % 64));
        search->current_word |= mask;
      } else {
        // run out in the current word, but maybe we
        // have more in the next?
        if (search->index + 1 < search->bitmap_size &&
            (search->bitmap[search->index + 1] & 1) == 0) {
          search->index++;
          search->current_word = search->bitmap[search->index];
          continue;
        } else {
          search->current_word = ULONG_MAX;
        }
      }
      if (!filter_unacceptable_ranges(search))
        continue;
      return true;
    }
    if (original_pos != search->found_position &&
        filter_unacceptable_ranges(search))
      return true;
    search->index++;
    if (search->index >= search->bitmap_size)
      return false;
    search->current_word = search->bitmap[search->index];
  } while (true);
}
// end::search_bitmap[]

// tag::search_for_smallest_nearby[]
#define MAX_DISTANCE_TO_SEARCH_BEST_MATCH 64

static bool search_for_smallest_nearby(bitmap_search_state_t *search) {
  uint64_t current_pos = 0;
  uint64_t current_size = ULONG_MAX;

  size_t boundary =
      MAX_DISTANCE_TO_SEARCH_BEST_MATCH +
      // the bigger the request range, the less we care about locality
      +search->space_required;

  while (search_bitmap(search)) {
    if (search->space_required == search->space_available_at_position)
      return true; // perfect match!
    if (current_size > search->space_available_at_position) {
      current_size = search->space_available_at_position;
      current_pos = search->found_position;
    }
    if (search->near_position && search->found_position > boundary) {
      // We have gone too far? Stop being choosy
      if (current_size < search->space_available_at_position) {
        search->space_available_at_position = current_size;
        search->found_position = current_pos;
      }
      return true;
    }
  }

  search->space_available_at_position = current_size;
  search->found_position = current_pos;

  return current_size != ULONG_MAX;
}
// end::search_for_smallest_nearby[]

// tag::search_free_range_in_bitmap[]
bool search_free_range_in_bitmap(bitmap_search_state_t *search) {
  if (!search->space_required ||
      search->near_position / 64 >= search->bitmap_size)
    return false;

  size_t high = search->near_position / 64;

  void *old_bitmap = search->bitmap;
  uint64_t old_size = search->bitmap_size;

  search->bitmap += high;
  search->bitmap_size -= high;

  if (search_for_smallest_nearby(search)) {
    search->found_position += high * 64;
    return true;
  }
  if (!high) {
    return false; // already scanned it all
  }

  search->bitmap = old_bitmap;
  search->bitmap_size = old_size;

  // we search _high_, couldn't find anything, maybe lower?
  if (search_bitmap(search)) {
    return true;
  }

  return false;
}

// end::search_free_range_in_bitmap[]
