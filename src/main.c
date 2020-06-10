#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include "transactions.h"
#include "errors.h"
#include "pal.h"

#include <errno.h>


int main () {

   database_options_t options = {0};
   database_t db;
   if(!open_database("db/orev", &options, &db)){
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
