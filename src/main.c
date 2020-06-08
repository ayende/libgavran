#include <stdio.h>
#include <stdlib.h>

#include "database.h"
#include "errors.h"

#include <errno.h>

static  MUST_CHECK  bool dance() {
   return true;
}

static MUST_CHECK bool sing(const char* song){
   push_error(ENOTSUP, "Can't sing '%s', can't recall the lyrics", song);
   return false;
}

static MUST_CHECK bool action(){
   if(!sing("Fernando")){
      mark_error();
      return false;
   }
   if(!dance()){
      mark_error();
      return false;
   }
   return true;
}

int main () {

   (void)action();

   print_all_errors();

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


   return(0);
}
