#include <assert.h>
#include <string.h>

#include <gavran/db.h>
#include <gavran/internal.h>

// tag::btree_stack[]
static result_t btree_increase_size(btree_stack_t* s) {
  size_t size = next_power_of_two(s->size + 1);
  ensure(mem_realloc((void*)&s->pages, size * sizeof(uint64_t)));
  ensure(mem_realloc((void*)&s->positions, size * sizeof(uint16_t)));
  s->size = size;
  return success();
}
result_t btree_stack_push(
    btree_stack_t* s, uint64_t page_num, int16_t pos) {
  if (s->index == s->size) {
    ensure(btree_increase_size(s));
  }
  s->pages[s->index]     = page_num;
  s->positions[s->index] = pos;
  s->index++;
  return success();
}
result_t btree_stack_pop(
    btree_stack_t* s, uint64_t* page_num, int16_t* pos) {
  ensure(s->index != 0, msg("The stack is empty, cannot pop"));
  s->index--;
  *page_num = s->pages[s->index];
  *pos      = s->positions[s->index];
  return success();
}
// end::btree_stack[]

// tag::btree_stack_utils[]
result_t btree_stack_peek(
    btree_stack_t* s, uint64_t* page_num, int16_t* pos) {
  ensure(s->index != 0, msg("The stack is empty, cannot pop"));
  *page_num = s->pages[s->index - 1];
  *pos      = s->positions[s->index - 1];
  return success();
}
result_t btree_stack_free(btree_stack_t* s) {
  free(s->positions);
  free(s->pages);
  memset(s, 0, sizeof(btree_stack_t));
  return success();
}
void btree_stack_clear(btree_stack_t* s) {
  if (s->index == 0) return;
  memset(s->positions, 0, s->index * sizeof(int16_t));
  memset(s->pages, 0, s->index * sizeof(uint64_t));
  s->index = 0;
}
// end::btree_stack_utils[]