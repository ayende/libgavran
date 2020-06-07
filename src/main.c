#include <stdio.h>
#include <stdlib.h>

#include "database.h"
#include "errors.h"

void hi(void) {
   printf("hi");
}

int main () {

   database_options_t options = {
      .path = "db",
      .name = "orev"
   };

   size_t req_size = get_database_handle_size(&options);
   void* mem = malloc(req_size);
   if(!mem){
      return -2;
   }

   database_handle_t* db;
   if(!create_database(&options, &db, mem)){
      print_all_errors();
      free(mem);
      return -1;
   }

   close_database(db);
   
   free(mem);

   printf("Done\n");


   return(0);
}
