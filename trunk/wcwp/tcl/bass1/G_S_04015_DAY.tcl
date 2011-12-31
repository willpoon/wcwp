######################################################################################################
#�ӿ����ƣ�����ҵ�񻰵�
#�ӿڱ��룺04015
#�ӿ�˵��������ҵ�񻰵�
#��������: G_S_04015_DAY.tcl
#��������: ����04015������
#��������: ��
#Դ    ��1.
#          
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�xhx
#��дʱ�䣺2007-08-12
#�����¼��bill_type���ּ�¼У�鲻ͨ��
#         ���⣺bill_typeԴ���ֶζ�Ӧ����ȷ��Ӧ��Ϊ,char(bill_flag)   ������ismg_id
#�޸���ʷ: 1.20070831 export����ڳ���������OPP_NUMBER�ֶ�ΪNULL��ԭ�򣬹��޸ĳ���:@20070831 BY TYM.
#          2.20071120 �Ľӿ��޸�������Դ.
#######################################################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        


  #ɾ����������
  #--------------------------------------------------------------------------------------------------------------
  set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_04015_day where time_id=$timestamp"
	
  exec_sql $sql_buff
  #--------------------------------------------------------------------------------------------------------------

       

  #���뱾������
  #--------------------------------------------------------------------------------------------------------------
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_04015_DAY
                           (
                             TIME_ID           ,
                             BILL_TYPE         ,
                             PRODUCT_NO        ,
                             PRTY_CODE         ,
                             SP_ID             ,
                             SERV_CODE         ,                             
                             CUSTOM_CHNL       ,                             
                             MEET_NUM          ,                             
                             PREZENT_BEGIN_TIME,
                             PREZENT_END_TIME  ,                             
                             FEE_AMT           ,                             
                             BUSI_TYPE         ,
                             DEAL_TIME         ,
                             CUST_CONTE         
                           )
                         select
                           ${timestamp}
                           ,char(FEETYPE_ID)  
                           ,ltrim(rtrim(PRODUCT_NO))
                           ,value(ltrim(rtrim(REV_PRODUCT_NO)),' ')
                           ,value(SP_CODE, ' ')
                           ,value(SP_SVC_CODE,' ')
                           ,case when customize_way_code in ('1','2','3','4','5') then customize_way_code
                            else '6' end customize_way_code
                           ,value(IN_PRODUCT_NO,' ')
                           ,value(BEGIN_TIME,' ')
                           ,value(END_TIME,' ')
                           ,value(char(FEE),'0')
                           ,value(char(BUSITYPE_ID),' ')
                           ,value(substr(replace(replace(char(CDR_TIMES),'-',''),'.',''),1,14),' ')
                           ,value(RING_ID,' ')                        
                        from 
                          bass2.DWD_MR_OPER_CDR_${timestamp}
where product_no is not null
" 
  puts $sql_buff
  exec_sql $sql_buff
  #--------------------------------------------------------------------------------------------------------------


	return 0
}



	
	
#�ڲ���������	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}
#--------------------------------------------------------------------------------------------------------------

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}
#--------------------------------------------------------------------------------------------------------------

