
######################################################################################################		
#�ӿ�����: "��ȷ��������������"                                                               
#�ӿڱ��룺22058                                                                                          
#�ӿ�˵����"��¼���������������������е�������������������ȷ��Ϊ����������������ݡ�"
#��������: G_S_22058_MONTH.tcl                                                                            
#��������: ����22058������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110727
#�����¼��
#�޸���ʷ: 
##~   1. panzw 20110727	1.7.4 newly added
##~   2. panzw 20120328	1.7.9 add column : YK_MODE
##~   �޸Ľӿ�22058����ȷ������������������
##~   1��	�����ֶΡ�����ģʽ����
##~   2��	���������������·ݡ�����ʵ��������ʶ����������ģʽ����
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      

  #1.���������������·ݡ�����ʵ��������ʶ����
	set tabname "G_S_22058_MONTH"
	set pk 			"OP_TIME||CHANNEL_ID||YK_MODE"
	chkpkunique ${tabname} ${pk} ${op_month}
	
return 0
}


##~   rename bass1.G_S_22058_MONTH to G_S_22058_MONTH_old;
##~   CREATE TABLE "BASS1   "."G_S_22058_MONTH"  (
                  ##~   "TIME_ID" INTEGER , 
                  ##~   "OP_TIME" CHAR(6) , 
                  ##~   "CHANNEL_ID" CHAR(40) , 
                  ##~   "YK_MODE" CHAR(2) , 
                  ##~   "YK_CNT" CHAR(12) , 
                  ##~   "YK_AMT" CHAR(12) )   
                 ##~   DISTRIBUTE BY HASH("OP_TIME","CHANNEL_ID","YK_MODE")   
                   ##~   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 
				   
##~   insert into G_S_22058_MONTH
##~   (
         ##~   TIME_ID
        ##~   ,OP_TIME
        ##~   ,CHANNEL_ID
		##~   ,YK_MODE
        ##~   ,YK_CNT
        ##~   ,YK_AMT
##~   )
##~   select 
         ##~   TIME_ID
        ##~   ,OP_TIME
        ##~   ,CHANNEL_ID
		##~   ,'' YK_MODE
        ##~   ,YK_CNT
        ##~   ,YK_AMT
##~   from G_S_22058_MONTH_old
