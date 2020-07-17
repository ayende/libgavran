
=== Extending the size of the WAL and db

result_t palmem_free_page(page_t *p) in chapter 4!


* Add hash validation to the pages
* Handle very large free space bitmap
* replace multi parameter signatures with structs
* explain code structure
* Backup

Thread safety:

* db->last_write_tx

* Update default_read_tx global_state

* allocate page, release it, try to allocate again with bigger size

* Discuss error handling, when a write transaction errors, what does that means, etc?

Exercise:
* Set free space with uint64_t, not each bit
* On move, do the same there
* Use mremap to try avoiding mapping from scratch
 

 *** RUN ALICE***