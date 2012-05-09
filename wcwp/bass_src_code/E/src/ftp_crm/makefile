CC = CC

INC = -I.  -I../__public_sock -I../__public

CPPLAG = -w 
LDFLAG = 

SUNV = -ldl -lw -lgen  -lnsl -lsocket

.SUFFIXES:.cpp .c

PUBLIC_SOCK_OBJS = ../__public_sock/c_comm_packet.o \
            ../__public_sock/c_ftp_client.o \
            ../__public_sock/c_monitor_item_analyze.o \
            ../__public_sock/c_power_socket.o \
            ../__public_sock/c_server_socket.o \
            ../__public_sock/c_socket_thread.o \
            ../__public_sock/c_thread.o

PUBLIC_OBJS = ../__public/app_config.o \
            ../__public/c_app_error_log.o \
            ../__public/c_app_log.o \
            ../__public/c_application.o \
            ../__public/c_db_index.o \
            ../__public/c_exception.o \
            ../__public/c_file_stream.o \
            ../__public/c_foxdbf.o \
            ../__public/c_indexed_dbf.o \
            ../__public/c_ini_file.o \
            ../__public/c_list.o \
            ../__public/c_list_file.o \
            ../__public/c_mzcompress.o \
            ../__public/c_mzcompress_bitio.o \
            ../__public/c_mzcompress_lzw15v.o \
            ../__public/c_string.o \
            ../__public/c_string_list.o \
            ../__public/c_system.o \
            ../__public/c_time.o \
            ../__public/pub_dir_file.o \
            ../__public/pub_string.o

FTP_CRM_OBJS = ftp_crm.o \
            c_file_trans.o

all:ftp_crm

.cpp.o:
	$(CC) $(INC) $(CPPLAG) -c $< -o $@

ftp_crm:$(PUBLIC_SOCK_OBJS) $(PUBLIC_OBJS) $(FTP_CRM_OBJS)
	$(CC) $(INC) $(PUBLIC_SOCK_OBJS) $(PUBLIC_OBJS) $(FTP_CRM_OBJS) \
	$(LDFLAG) $(SUNV) -o ../bin/__ftp_crm.bin

clean:
	rm -f $(PUBLIC_SOCK_OBJS)
	rm -f $(PUBLIC_OBJS)
	rm -f $(FTP_CRM_OBJS)
	rm -f ../bin/__ftp_crm.bin
