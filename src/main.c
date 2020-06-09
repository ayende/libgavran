#include <stdio.h>
#include <stdlib.h>

#include "database.h"
#include "errors.h"
#include "pal.h"

#include <errno.h>

#define DB_SIZE (128*1024)

static void close_handle_p(void** h){
   if(!close_file(*h)){
      print_all_errors();
   }
}

static void unmap_mem_p(void** m){
   if(!unmap_file(*(void**)m, DB_SIZE)){
      print_all_errors();
   }
}
int main () {

   file_handle_t* handle = malloc(128);
   if(!handle)
      return ENOMEM;
   if(!create_file("db/orev", handle)){
      print_all_errors();
      return EIO;
   }
   defer(close_handle_p, handle);

   if(!ensure_file_minimum_size(handle, DB_SIZE)){
      print_all_errors();
      return EIO;
   }
   
   void* addr;
   if(!map_file(handle, 0, DB_SIZE, &addr)){
      print_all_errors();
      return EIO;
   }
   defer(unmap_mem_p, addr);

   const char msg[] = "Hello Gavran";
   if(!write_file(handle, 0, msg, sizeof(msg))){
      print_all_errors();
      return EIO;
   }

   printf("%s\n", addr);


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
