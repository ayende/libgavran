#include <stdio.h>
#include "database.h"
#include "errors.h"

int main () {
  
   database_options_t options = {
      .path = "db",
      .name = "orev"
   };
   database_handle_t* db;
   if(!create_database(&options, &db)){
      print_all_errors();
      return -1;
   }

   close_database(&db);

   printf("Done\n");


   return(0);
}
