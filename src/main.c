#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include "transactions.h"
#include "errors.h"
#include "pal.h"

#include <errno.h>

#define DB_SIZE (128*1024)

static void close_handle_p(void** h){
   if(!close_file(*h)){
      print_all_errors();
   }
}

static void close_transction_p(void**h){
   if(!close_transaction(*h)){
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
   
   txn_t tx;
   if(!create_transaction(handle, 0, &tx)){
      print_all_errors();
      return EIO;
   }
   defer(close_transction_p, &tx);

   page_t page;
   page.page_num = 0;
   if(!modify_page(&tx, &page)){
      print_all_errors();
      return EIO;
   }

   void* modified = page.address;

   const char msg[] = "Hello Gavran";
   memcpy(modified, msg, sizeof(msg));

   page.page_num = 0;
   if(!modify_page(&tx, &page)){
      print_all_errors();
      return EIO;
   }
   printf("%p - %p\n", modified, page.address);

   if(!commit_transaction(&tx)){
      print_all_errors();
      return EIO;
   }

   if(!close_transaction(&tx)){
      print_all_errors();
      return EIO;
   }

   if(!create_transaction(handle, 0, &tx)){
      print_all_errors();
      return EIO;
   }

   if(!get_page(&tx,&page)){
      print_all_errors();
      return EIO;
   }
   
   printf("%s\n%p vs. %p", page.address, page.address, modified);

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
