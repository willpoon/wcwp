CC = CC

DBINC = -I$(ORACLE_HOME)/rdbms/demo -I$(ORACLE_HOME)/rdbms/public -I$(ORACLE_HOME)/network/public

INC = -I.  -I../__public -I../__public_db $(DBINC) -m64

CPPLAG = -w -g -D__ORACLECLASS
LDFLAG = -DA800

ORALIB = -L$(ORACLE_HOME)/lib32/ -L$(ORACLE_HOME)/rdbms/lib32/

SUNV = -ldl -lw -lgen  -lnsl -lclntsh -lrt -lintl -mt

.SUFFIXES:.cpp .c

PUBLIC_OBJS = ../__public/pub_string.o \
			../__public/c_app_error_log.o \
			../__public/c_app_log.o \
			../__public/c_application.o \
			../__public/c_exception.o \
			../__public/c_file_stream.o \
			../__public/c_foxdbf.o \
			../__public/c_ini_file.o \
			../__public/c_list.o \
			../__public/c_list_file.o \
			../__public/c_string.o \
			../__public/c_string_list.o \
			../__public/c_system.o \
			../__public/c_time.o \
			../__public/pub_dir_file.o \
			../__public/app_config.o

PUBLIC_DB_OBJS = ../__public_db/c_query.o \
				../__public_db/c_string_grid.o \
				../__public_db/c_database.o
            
BOSS_CRM_INTERFACS_OBJS = multithread.o \
						c_thread_pool_run.o \
						c_thread_pool.o \
						c_thread_run.o \
						c_thread.o \
						c_handletask.o \
						c_base_func.o \
						c_critical_section.o

all:../bin/__boss_crm_interface.bin

.cpp.o:
	$(CC) $(INC) $(SUNV) $(CPPLAG) -c $< -o $@

../bin/__boss_crm_interface.bin:$(PUBLIC_OBJS) $(PUBLIC_DB_OBJS) $(BOSS_CRM_INTERFACS_OBJS)
	$(CC) $(INC) $(PUBLIC_OBJS) $(PUBLIC_DB_OBJS) $(BOSS_CRM_INTERFACS_OBJS) \
	$(LDFLAG) $(SUNV) $(ORALIB) -o $@
clean:
	rm -f $(PUBLIC_OBJS)
	rm -f $(PUBLIC_DB_OBJS)
	rm -f $(BOSS_CRM_INTERFACS_OBJS)
	rm -f ../bin/__boss_crm_interface.bin
