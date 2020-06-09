#include <stdio.h>
#include <stdlib.h>

#include "database.h"
#include "errors.h"
#include "pal.h"

#include <errno.h>

#define defer(func, var) void* \
   __defer ## __LINE__ __attribute__ \
   ((__cleanup__(func))) = var; \
   (void)__defer ## __LINE__ 

int main () {

   size_t size = get_file_handle_size("db", "phones");
   file_handle_t* handle = malloc(size);
   if(!handle)
      return ENOMEM;
   int i = 4;
   defer(freef, handle);
    if(!create_file("db", "phones", handle) || 
   	  !ensure_file_minimum_size(handle, 128 * 1024) || 
   	  !close_file(handle)
   	  ) {
	      print_all_errors();
	      return EIO;
   }
   return 0;

   // database_options_t options = {
   //    .path = "db",
   //    .name = "orev"
   // };

   // size_t req_size = get_database_handle_size(&options);
   // database_handle_t* db = malloc(req_size);
   // if(!db){
   //    return -2;
   // }

   // if(!create_database(&options, db)){
   //    print_all_errors();
   //    free(db);
   //    return -1;
   // }

   // close_database(db);
   
   // free(db);

   // printf("Done\n");


}
