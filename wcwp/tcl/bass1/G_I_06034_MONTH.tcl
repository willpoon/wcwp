
######################################################################################################		
#�ӿ�����: ��Ӫ��ӪҵԱ��Ϣ                                                               
#�ӿڱ��룺06034                                                                                          
#�ӿ�˵���������ƶ���˾��Ӫ���ڲ��е�ҵ������ӪҵԱԱ����Ӫ����Ա/�ͷ���Ա�������Ϣ
#��������: G_I_06034_MONTH.tcl                                                                            
#��������: ����06034������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110922
#�����¼��
#�޸���ʷ: 1. panzw 20110922	1.7.5 newly added
#######################################################################################################   


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#set optime_month 2011-06
   #���� yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

    #���� yyyy-mm-dd
    set optime $op_time

    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
		set last_month [GetLastMonth [string range $op_month 0 5]]
    #���µ�һ�� yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day

    #�������һ�� yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day

    #ɾ����ʽ��������
    set sql_buff "delete from bass1.G_I_06034_MONTH where time_id=$op_month"
    exec_sql $sql_buff

##		, CASE WHEN A.CHANNEL_TYPE_CLASS=90105 THEN '1'
##			 		WHEN A.CHANNEL_TYPE_CLASS=90102 AND A.CHANNEL_TYPE IN (90175,90186,90740,90741,90881) THEN '2'
##			 		ELSE '3'
##		  END  CHANNEL_TYPE

#2011-04-15  �޸���γ����У��ͨ����������ͨ������������ɵġ�
    #����ʽ����뱾������
    set sql_buff "
    insert into BASS1.G_I_06034_MONTH
	select 
	$op_month
	,char(b.OP_ID)
	,char(b.org_id)
	,b.OP_NAME
	,case when state = 0 then '1' else  '2' end
	from    bass2.dw_channel_info_$op_month a
	, bass2.DIM_BOSS_STAFF b
	where a.channel_type_class=90105 and a.channel_type in (90196,90153,90154,90155,90156,90157,90158,90940,90941,90942,90943) 
	and a.channel_id = b.ORG_ID
	and (b.op_name not like '%����%' 
	and  b.op_name not like '%����%' 
	)
	with ur
 "


  exec_sql $sql_buff


	return 0
}
