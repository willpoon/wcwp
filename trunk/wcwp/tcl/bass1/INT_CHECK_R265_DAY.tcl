######################################################################################################
#�������ƣ�	INT_CHECK_R265_DAY.tcl
#У��ӿڣ�	
#��������: MONTH
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2012-06-09 
#�����¼��
#�޸���ʷ:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month				
        #��Ȼ��
				puts $op_time 
        set curr_month 201206
        ##~   set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        set last_month 201205		
        ##~   set last_month [GetLastMonth [string range $op_month 0 5]]
		
        #������
        global app_name
        set app_name "INT_CHECK_R265_DAY.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$last_month and rule_code in (
					 'R265'
					)
			"

		exec_sql $sql_buff





##~   --~   R265	��	03_������־	��ҵ���ض��Ż����еġ�������롱���ڼ��ſͻ��˿���Դʹ������ӿڵġ���ҵӦ�ô���ȫ�롱	"04016 ��ҵ���ض��Ż���
##~   --~   22036 ���ſͻ��˿���Դʹ�����"	��ҵ���ض��Ż����еġ�������롱���ڼ��ſͻ��˿���Դʹ������ӿڵġ���ҵӦ�ô���ȫ�롱��	0.05	

##~   "Step1.04016����ҵ���ض��Ż������ӿ��з���״̬Ϊ0���ɹ����ġ�������롱���ϣ�
##~   Step2.22036�����ſͻ��˿���Դʹ��������ӿ��н�ֹ��ͳ������ĩҵ������=1����ҵ���ض��š��ġ���ҵӦ�ô���ȫ�롱���ϣ�
##~   Step3.����Step1�Ƿ���ڼ���Step2�С�"



set sql_buff "


select count(0)
from table(

select distinct  SERV_CODE from G_S_04016_DAY where time_id / 100 = $curr_month and SEND_STATUS = '0'
except
                        select distinct APP_LENCODE from 
                        (
                                        select t.*
                                        ,row_number()over(partition by BILL_MONTH,EC_CODE,APP_LENCODE,APNCODE,BUSI_NAME order by time_id desc ) rn 
                                        from 
                                        G_A_22036_DAY  t
										where  time_id / 100 <= $curr_month
                          ) a
                        where rn = 1    and OPERATE_TYPE = '1'
										And bigint(OPEN_DATE) <= $curr_month
                        
) a
with ur
"

chkzero2 $sql_buff "R265 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R265',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff
  


	return 0
}
