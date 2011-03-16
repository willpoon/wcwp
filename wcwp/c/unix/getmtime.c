#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
time_t get_mtime(const char *path)
{
   struct stat statbuf;
   if (stat(path, &statbuf) == -1) {
                perror(path);
                exit(1);
   }
   return statbuf.st_mtime;
}

int main(int argc,char *argv[]){
        get_mtime("/bassapp/bass2/panzw2/.profile");
        return 0;
}
