######################################################################################################
#�ӿڵ�Ԫ���ƣ�ͳһ��ѯ�˶��ջ���
#�ӿڵ�Ԫ���룺22080
#�ӿڵ�Ԫ˵�����ɼ���ֵҵ��0000ͳһ��ѯ���˶�������ձ����ݣ��������ͻ���ѯ�����˶�����ҵ���˶�ʧ�������ͻ�Ͷ�����������˶�ҵ����������;�ɼ������ҵ���˶��ʡ��˶�ʧ���ʡ��ͻ�ƽ��ÿ���˶�ҵ����������ȡ�
#��������: G_S_22080_DAY.tcl
#��������: ����22080������
#��������: ��
#Դ    ����
#1.bass2.DW_THREE_ITEM_STAT_DM_$op_month 
#2.	BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_$op_month
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzhiwei
#��дʱ�䣺2011-04-26
#�����¼��1.
#�޸���ʷ: 1. 1.7.2 �淶
# 2011-11-08 panzhiwei
#������zxq  11:56:49
#���ݼ���ͨ����8�·���ֵҵ��0000ͳһ��ѯ���˶������ʵʩ����ҹ�˾�˶��ʸߴ�72%��ԶԶ��������ʡ��˾�����Ҳ���ʵ�����ҹ�˾����ҵ���˶��ʼ��㷽ʽΪ�����о���0000����ѯ���˶�ҵ���������û����͡�0000����ѯ�ܴ���������һ���㷽ʽ�����û�����һ�ζ���ͬʱ�˶����ҵ���������Ӷ�����Ŀǰ�ҹ�˾�˶��ʹ��ߡ�
#
#Ϊ�����ܲ����ҹ�˾����ͨ��������󲿽��ҹ�˾����ҵ��0000�˶��ʼ��㷽ʽ��������ʡ��˾���㷽ʽ�����µ�����
#
#1��    �û�ͨ�� ��0000�����У���ѯ���˶�һ�ε���������ҵ���˶�����Ϊһ�Σ�
#
#2��    �û����͡�0000����ѯ�ܴ�����������
# note: 
##	1. ȡ BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM ��Ϊ�˶���
##	2. �˶���Ӧ�����˶�ʧ����
##	3.ҵ���˶���	��ָ�����˶��Ķ�����ϵ���� �� Ӧ��С���˶�����
# 	2011-11-11 ���������췴�����˶�������ƫ�����Ըĳɣ�  count(distinct PHONE_ID||char(SP_ID)||SP_CODE) ->  count(distinct PHONE_ID) 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
      set op_month [string range $timestamp 0 5]

      set curr_month [string range $op_time 0 3][string range $op_time 5 6]


  #ɾ����������
	set sql_buff "delete from bass1.G_S_22080_DAY where time_id=$timestamp"
	exec_sql $sql_buff
	
set sql_buff "
	select count(distinct PHONE_ID) cnt3 
from     BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_$op_month
	where replace(char(CREATE_DATE),'-','') = '$timestamp'
with ur
"

set RESULT_VAL [get_single $sql_buff]

  #ֱ����Դ�ڶ����û������ݣ��µĽӿڱ�
  #����ҵ���˶���CANCEL_BUSI_CNT�����ܲ��Ǻ�׼���淶ָ����ÿ�β�ѯ�󣬷������˶���ҵ����?��
  #CANCEL_CNT�ھ�ҲҪ����ȷ�ϡ�
	set sql_buff "
	insert into bass1.G_S_22080_DAY
		  (
         TIME_ID
        ,OP_TIME
        ,QRY_CNT
        ,CANCEL_CNT
        ,CANCEL_FAIL_CNT
        ,COMPLAINT_CNT
        ,CANCEL_BUSI_TYPE_CNT
		  )
 select      $timestamp TIME_ID
             ,replace(char(date(a.create_date)),'-','') op_time
             ,char(a.TYCX_QUERY)             qry_cnt
             ,'${RESULT_VAL}'                cancel_cnt
             ,char(a.TYCX_TUIDING_FAIL)      cancel_fail_cnt
             ,char(a.TYCX_TOUSU_LIANG)       complaint_cnt
             ,char( case when (${RESULT_VAL} - a.TYCX_TUIDING_FAIL) < 0 
			then 0 else (${RESULT_VAL} - a.TYCX_TUIDING_FAIL) 
		   end 
		  ) CANCEL_BUSI_CNT
        from  bass2.DW_THREE_ITEM_STAT_DM_$op_month a ,
              (select  replace(char(date(a.create_date)),'-','') op_time
              					,count(0) CANCEL_BUSI_CNT
                       from   
                       	BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_$op_month a
                        where replace(char(date(a.create_date)),'-','') =  '$timestamp'  
                        group by replace(char(date(a.create_date)),'-','')
                    ) b 
        where replace(char(date(a.create_date)),'-','') = '$timestamp' 
and    replace(char(date(a.create_date)),'-','') = b.op_time
with ur
  "
	exec_sql $sql_buff


  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_S_22080_DAY"
	set pk 			"OP_TIME"
	chkpkunique ${tabname} ${pk} ${timestamp}
	#
  aidb_runstats bass1.$tabname 3
  
	return 0
}