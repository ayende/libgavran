#include <limits.h>
#include <string.h>

#include "impl.h"

// tag::search_word[]
void init_search(bitmap_search_state_t *search, void *bitmap,
                 uint64_t size, uint64_t required) {
  memset(search, 0, sizeof(bitmap_search_state_t));
  search->bitmap = bitmap;
  search->bitmap_size = size;
  search->space_required = required;
  search->current_word = search->bitmap[0];
  search->previous_set_bit = ULONG_MAX;
}

static bool handle_zero_word(bitmap_search_state_t *search) {
  search->current_set_bit = (search->index + 1) * 64;
  if (search->current_set_bit >
      search->previous_set_bit + search->space_required) {
    search->found_position = search->previous_set_bit +
                             1;  // intentionally overflowing here
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
    search->previous_set_bit = (search->index + 1) * 64 - 1;
    return false;
  }

  if (bitset == 0) {
    return handle_zero_word(search);
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

  return handle_zero_word(search);
}
// end::search_word[]

// tag::search_bitmap[]
static bool search_bitmap(bitmap_search_state_t *search) {
  do {
    if (search_word(search)) {
      if (search->current_set_bit % 64) {
        // mask the already found item
        uint64_t mask =
            ~(ULONG_MAX << (search->current_set_bit % 64));
        search->current_word |= mask;
      } else {
        // run out in the current word, but maybe we
        // have more in the next?
        if (search->index + 1 < search->bitmap_size) {
          search->index++;
          search->current_word = search->bitmap[search->index];
          continue;
        } else {
          search->current_word = ULONG_MAX;
        }
      }
      return true;
    }
    search->index++;
    if (search->index >= search->bitmap_size) return false;
    search->current_word = search->bitmap[search->index];
  } while (true);
}
// end::search_bitmap[]

// tag::search_for_smallest_nearby[]
#define MAX_DISTANCE_TO_SEARCH_BEST_MATCH 64

static bool search_for_smallest_nearby(
    bitmap_search_state_t *search) {
  uint64_t current_pos = 0;
  uint64_t current_size = ULONG_MAX;

  size_t boundary =
      MAX_DISTANCE_TO_SEARCH_BEST_MATCH +
      // the bigger the request range, the less we care about locality
      +search->space_required;

  while (search_bitmap(search)) {
    if (search->space_required == search->space_available_at_position)
      return true;  // perfect match!
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
    return false;  // already scanned it all
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
