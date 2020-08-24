#include <assert.h>
#include <string.h>

#include <gavran/db.h>
#include <gavran/internal.h>

result_t btree_stack_push(
    btree_stack_t* s, uint64_t page_num, int16_t pos) {
  if (s->index == s->size) {
    s->size = (uint32_t)next_power_of_two(s->size + 1);
    ensure(mem_realloc(
        (void*)&s->pages, (size_t)s->size * sizeof(uint64_t)));
    ensure(mem_realloc(
        (void*)&s->positions, (size_t)s->size * sizeof(int16_t)));
  }
  s->pages[s->index]     = page_num;
  s->positions[s->index] = pos;

  s->index++;
  return success();
}

result_t btree_stack_pop(
    btree_stack_t* s, uint64_t* page_num, int16_t* pos) {
  if (s->index == 0) {
    failed(EINVAL, msg("The stack is empty, cannot pop"));
  }
  s->index--;
  *page_num = s->pages[s->index];
  *pos      = s->positions[s->index];

  return success();
}

result_t btree_stack_peek(
    btree_stack_t* s, uint64_t* page_num, int16_t* pos) {
  if (s->index == 0) {
    failed(EINVAL, msg("The stack is empty, cannot pop"));
  }
  *page_num = s->pages[s->index - 1];
  *pos      = s->positions[s->index - 1];

  return success();
}

result_t btree_stack_free(btree_stack_t* s) {
  free(s->positions);
  free(s->pages);
  return success();
}

void btree_stack_clear(btree_stack_t* s) {
  if (s->index == 0) return;
  memset(s->positions, 0, s->index * sizeof(int16_t));
  memset(s->pages, 0, s->index * sizeof(uint64_t));
  s->index = 0;
}