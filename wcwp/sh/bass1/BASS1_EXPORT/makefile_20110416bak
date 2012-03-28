#For 64 bit executables use:
EXTRA_CFLAGS=-m64

DB2PATH=/db2home/db2inst1/sqllib
CFLAGS=$(EXTRA_CFLAGS) -I/opt/IBM/db2/V9.1/include
MTFLAGS=-I$(DB2PATH)/include -mt -D_POSIX_PTHREAD_SEMANTICS $(EXTRA_CFLAGS)
MTLIBS=-L$(DB2PATH)/lib -R$(DB2PATH)/lib -ldb2 -lpthread
CC=/usr/bin/CC

DB=BASSDB
UID=app
PWD=app



COPY=cp
ERASE=rm -f

bass1_export:bass1_export.o 
	$(CC) -o bass1_export bass1_export.o  $(MTFLAGS) $(MTLIBS)
bass1_export.o: bass1_export.c
	$(CC) -c -g bass1_export.c $(MTFLAGS) 
bass1_export.c: bass1_export.sqc
	embprep bass1_export $(DB) $(UID) $(PWD)
clear: 
	$(ERASE)  *.o 
	$(ERASE)  *.c 
	$(ERASE)  *.bnd
install:
	cp ./bass1_export.ini       /bassapp/backapp/bin/bass1_export
	cp ./bass1_export           /bassapp/backapp/bin/bass1_export
	cp ./ftp_bass1_export.sh    /bassapp/backapp/bin/bass1_export
