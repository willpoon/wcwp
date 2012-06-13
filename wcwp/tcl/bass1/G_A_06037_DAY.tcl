
######################################################################################################		
#�ӿ�����: ʵ��������Դ������Ϣ(������)                                                               
#�ӿڱ��룺06037                                                                                          
#�ӿ�˵������¼ʵ��������Դ������Ϣ, �漰��Ӫ����ί�о�Ӫ����24Сʱ����Ӫҵ��������������
#��������: G_A_06037_DAY.tcl                                                                            
#��������: ����06037������
#��������: DAY
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110922
#�����¼��
#�޸���ʷ: 1. panzw 20110922	1.7.5 newly added
##~   �޸�Ϊleft join ����֤ R280	��	09_������Ӫ	ʵ��������Դ������Ϣ���¹�ϵ ͨ����
#######################################################################################################   


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

   #���� yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

    #���� yyyy-mm-dd
    set optime $op_time

    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month

    #���µ�һ�� yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day

    #�������һ�� yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day

  	set sql_buff "delete from bass1.G_A_06037_DAY where time_id < 20111001"
	exec_sql $sql_buff
	
	
    #ɾ����ʽ��������
    set sql_buff "delete from bass1.G_A_06037_DAY where time_id=$timestamp"
    puts $sql_buff
    exec_sql $sql_buff



	set sql_buff "ALTER TABLE BASS1.G_A_06037_DAY_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

    #����ʽ����뱾������
    set sql_buff "
    insert into BASS1.G_A_06037_DAY_1
	SELECT  distinct 
		trim(char(a.channel_id))
		,value(char(b.BUILD_AREA/100),'20') BUILD_AREA
		,' ' USE_AREA
		,value(char(b.STORE_AREA/100),'20') STORE_AREA
		,value(char(b.SEAT_NUM),'1') SEAT_NUM
		,value(char(b.EMPLOYEE_NUM),'1') STORE_EMPLOYE
		,value(char(b.ENSURE_NUM),'0') GUARD_EMPLOYE
		,value(char(b.CLEAN_NUM),'1') CLEAR_EMPLOYE
		,value(char(b.HAVE_QUEUE),'0') IF_WAIT_MARK
		,value(char(b.HAVE_POS),'0') IF_POS_MARK
		,value(char(b.HAVE_VIPLINE),'0') IF_VIP_SEAT
		,value(char(b.HAVE_VIPROOM),'0') IF_VIP_ROOM
		,value(char(b.PRINTER_NUM),'0') PRINT_NUM
		,value(char(b.GENERALATM_NUM),'0') TERM_NUM
		,value(char(b.TASTE_AREA),'0') G3_AREA
		,value(char(b.TVSCREEN_NUM),'0') TV_NUM
		,value(char(b.NEWBUSI_PLATFORM_NUM),'0') NEW_BUSITERM_NUM
		,value(char(b.HEART_PLATFORM_NUM),'0') HEART_TERM_NUM
		,value(char(b.ONLINE_NUM),'0') NET_TERM_NUM
		,'0' AREA
		,'0' ACCEPT_AREA
		,'0' MAIN_NET_TYPE
		,'0' IF_CZ
	FROM BASS2.Dim_CHANNEL_INFO A
	left join bass2.Dwd_channel_selfsite_info_$timestamp b on a.channel_id = b.channel_id
	where A.CHANNEL_TYPE_CLASS IN (90105,90102)
with ur
"
    exec_sql $sql_buff



    set sql_buff "
        insert into BASS1.G_A_06037_DAY
		select 
		$timestamp time_id
		,t.*
		from table(
		select * from g_a_06037_day_1
		except
		select 
			CHANNEL_ID
			,BUILD_AREA
			,USE_AREA
			,BUSI_AREA
			,COUNTER_CNT
			,STAFF_CNT
			,SECURE_CNT
			,CLEANER_CNT
			,WAITING_MACHINE
			,POS_MACHINE
			,VIP_SERVANT
			,IF_VIP_SCHOOL
			,PRINTER_CNT
			,SELFTERM_CNT
			,G3_AREA
			,SCREEN_CNT
			,EXP_PLAT_TERM_CNT
			,XINJI_TERM_CNT
			,WEB_TERM_CNT
			,HALL_AREA
			,CMCC_BUSI_AREA
			,ACCESS_WAY
			,IF_KONGCONG
		from  (
		select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
		from G_A_06037_DAY a 
		) o where o.rn = 1 
		) t 
		with ur
"
    exec_sql $sql_buff
    
	return 0
}