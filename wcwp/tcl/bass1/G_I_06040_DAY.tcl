
######################################################################################################		
#�ӿ�����: ���г�ֵ�������Ϣ                                                               
#�ӿڱ��룺06040                                                                                          
#�ӿ�˵������¼�߱����г�ֵ���ܵ����������Ϣ
#��������: G_I_06040_DAY.tcl                                                                            
#��������: ����06040������
#��������: DAY
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20120328
#�����¼��
#�޸���ʷ: 1. panzw 20120328	�й��ƶ�һ����Ӫ����ϵͳʡ�����ݽӿڹ淶 (V1.7.9) 
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
		global app_name
		set app_name "G_I_06040_DAY.tcl"        


  #ɾ����������
	set sql_buff "delete from bass1.g_i_06040_day where time_id=$timestamp"
	exec_sql $sql_buff

	set sql_buff "
	insert into bass1.g_i_06040_day
	select  distinct
			$timestamp time_id
			,a.OTHER_INFO  CHRG_NBR
			,b.REGION_CODE CMCC_ID
			,char(a.CHANNEL_ID)   CHNL_ID
			,case when PARTNER_CODE in ('00','01','02','03') then '2' else '1' end CHNL_TYPE
	from bass2.Dw_channel_dealer_$timestamp a
		,bass2.dim_channel_info b 
	where a.CHANNEL_ID = b.CHANNEL_ID
			and a.DEALER_STATE = 1
	with ur 
"
	exec_sql $sql_buff
	
  #���н�����ݼ��
  #���chkpkunique
	set tabname "g_i_06040_day"
	set pk 			"CHRG_NBR"
	chkpkunique ${tabname} ${pk} ${timestamp}

	return 0
}


##~   00		��¼�к�
##~   01		���г�ֵר���ֻ���
##~   02		����CMCC��Ӫ��˾��ʶ
##~   03		ʵ��������ʶ
##~   04		���г�ֵ������

##~   ������λ���룺������������̼���Ϊ��ȫ��������ŷ�ΧΪ00-20���ֱ�Ϊ��00��������01��������02������ͨ��03�������ģ����Ϊ��ʡ������
##~   ��ŷ�Χ��21��50�����ֹ�˾�Զ���Ĭ��Ϊ21���ޣ����Ϊ�����м�������ŷ�ΧΪ��51��99����Ĭ��Ϊ51:�ޡ�


##~   CREATE TABLE G_I_06040_DAY (
        ##~   TIME_ID                INTEGER             --  ��¼�к�        
        ##~   ,CHRG_NBR               CHAR(15)            --  ���г�ֵר���ֻ���
        ##~   ,CMCC_ID                CHAR(5)             --  ����CMCC��Ӫ��˾��ʶ
        ##~   ,CHNL_ID                CHAR(40)            --  ʵ��������ʶ    
        ##~   ,CHNL_TYPE              CHAR(1)             --  ���г�ֵ������  
##~   ) DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX PARTITIONING KEY( TIME_ID,CHRG_NBR ) USING HASHING;



