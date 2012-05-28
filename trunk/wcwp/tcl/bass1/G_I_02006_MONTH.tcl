######################################################################################################
#�ӿ����ƣ��û����ֱ�����
#�ӿڱ��룺02006
#�ӿ�˵������¼�û����ֱ仯�����ĩ����
#��������: g_i_02006_month.tcl
#��������: ����02006������
#��������: ��
#Դ    ��1.bass2.ods_product_sc_scorelist_yyyymm(����_��ϸ��)
#          2.bass2.dw_product_$op_month
#          3.
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuqf
#��дʱ�䣺2010-06-08
#�����¼��1.ע�⣬ֻ�������ת���û�ID
#�޸���ʷ: 1.�Ϸ����������½ű�/�µ�ҵ��ץȡ�ھ���
#          2.�޸�������������ֿھ� liuqf 20110105
#          3.1.7.1�淶�޸� liuqf 20110127
#          3.1.7.8�淶�޸� panzw 20120117
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        #set op_time 2008-10-01
        set optime_month 2012-01
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        
        set last_month [GetLastMonth [string range $op_month 0 5]]
        
        puts $op_time
        puts $op_month
        puts $last_month


      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay


  #ɾ����������
	set sql_buff "delete from bass1.g_i_02006_month where time_id=$op_month"
	exec_sql $sql_buff
	
	set sql_buff "alter table bass1.g_i_02006_month_1 activate not logged initially with empty table"
	exec_sql $sql_buff


##~   1.��ȫ��ͨƷ�ƵĿͻ�-����Ʒ�ƽ������� 
##~   2-3��ȫ��ͨ�����еش�2Ʒ�ơ�ר��ת�ƻ��֡����������ݣ�
##~   1-3��������Ʒ�ơ�ר��ת�ƻ��֡����������ݣ�
	set sql_buff "
		insert into G_I_02006_MONTH_1
		select
			product_instance_id  as user_id
			,sum( case when  count_cycle_id=$op_month and scrtype=1 then orgscr+adjscr else 0 end )   as month_points
			,sum( case when  count_cycle_id=$op_month and scrtype=24 and b.brand_id = '1' then orgscr+adjscr else 0 end )   as month_qqt_points
			,sum( case when  count_cycle_id=$op_month and scrtype=25 then orgscr+adjscr else 0 end )   as month_age_points
			,sum( case when  count_cycle_id<=$op_month and $op_month <= 201201 and scrtype=5  and b.brand_id in ('1','3') then orgscr+adjscr else 0 end )   as trans_points --ֻ��ȫ/������ת�ƻ��� ���� Ӧ�÷ŵ�һ�·��У������·�û��ת�ƻ��֡�
			,sum(  CURSCR )   as convertible_points
			,sum( orgscr+adjscr )   as all_points
			,sum( case when  scrtype=1 then orgscr+adjscr else 0 end )   as all_consume_points
			,sum( USRSCR )   as all_converted_points
			,0 LEAVE_CLEAR_POINTS
			,0 OTHER_CLEAR_POINTS
		from bass2.dwd_product_sc_scorelist_$op_month  a ,  bass1.INT_02004_02008_MONTH_$op_month  b
		where   actflag='1' and count_cycle_id <= $op_month
		and a.PRODUCT_INSTANCE_ID = b.user_id
		group by product_instance_id
		with ur
	"
	exec_sql $sql_buff


	set sql_buff "
	insert into G_I_02006_MONTH_1
			(
				USER_ID
				,LEAVE_CLEAR_POINTS
			)
select 
		b.user_id
		,sum(case when b.month_off_mark = 1 then c.end_month_score else 0 end) LEAVE_CLEAR_POINTS
		from bass2.dw_product_$op_month b,
		     bass2.ods_product_sc_month_backup_$last_month c
		where b.user_id=c.product_instance_id
		  and b.month_off_mark = 1
		group by b.user_id
with ur
"
	exec_sql $sql_buff
	
	
#OTHER_CLEAR_POINTS

set sql_buff "
	insert into G_I_02006_MONTH_1
	  (
		user_id
		,OTHER_CLEAR_POINTS
	  )
	select 
		user_id
		,sum(case when chgbrand_mark = 1 then change_score else 0 end) OTHER_CLEAR_POINTS
	from bass2.dw_product_$op_month
	where chgbrand_mark = 1
	group by user_id 
	having sum(case when chgbrand_mark = 1 then change_score else 0 end) > 0
	with ur
       "
	exec_sql $sql_buff

# ��׻������㣬ÿ�¶���¼T-3��ģ�����	
#		set sql_buff "
#		insert into G_I_02006_MONTH_1
#				(
#					USER_ID
#					,OTHER_CLEAR_POINTS
#				)
#	select 
#			b.user_id
#			,sum(case when b.month_off_mark = 1 then c.END_SCORE else 0 end) LEAVE_CLEAR_POINTS
#			from bass2.dw_product_$op_month b,
#				bass2.ods_product_sc_month_backup_$op_month c
#			where b.user_id=c.product_instance_id
#			group by b.user_id
#			having sum(case when b.month_off_mark = 1 then c.END_SCORE else 0 end)
#	with ur
#	"
#		exec_sql $sql_buff
#		

##~   20120524       Ϊ����У�飬�޸Ŀھ���  ,char(sum(value(case when b.brand_id = 1 then a.MONTH_QQT_POINTS else 0 end ,0)))
##~   R282	��	10_���ּƻ�	��ȫ��ͨƷ�ƵĿͻ���Ӧ�ϴ�Ʒ�ƽ�������	02006 �û��������	��ȫ��ͨƷ�ƵĿͻ���Ӧ�ϴ�Ʒ�ƽ�������	0.05	



set sql_buff "
	insert into G_I_02006_MONTH
	  (
         TIME_ID
        ,USER_ID
        ,MONTH_POINTS
        ,MONTH_QQT_POINTS
        ,MONTH_AGE_POINTS
        ,TRANS_POINTS
        ,CONVERTIBLE_POINTS
        ,ALL_POINTS
        ,ALL_CONSUME_POINTS
        ,ALL_CONVERTED_POINTS
        ,LEAVE_CLEAR_POINTS
        ,OTHER_CLEAR_POINTS
	  )
	select 
        $op_month TIME_ID
        ,a.USER_ID
        ,char(sum(value(a.MONTH_POINTS,0)))
        ,char(sum(value(case when b.brand_id = 1 then a.MONTH_QQT_POINTS else 0 end ,0)))
        ,char(sum(value(a.MONTH_AGE_POINTS,0)))
        ,char(sum(value(a.TRANS_POINTS,0)))
        ,char(sum(value(a.CONVERTIBLE_POINTS,0)))
        ,char(sum(value(a.ALL_POINTS,0)))
        ,char(sum(value(a.ALL_CONSUME_POINTS,0)))
        ,char(sum(value(a.ALL_CONVERTED_POINTS,0)))
        ,char(sum(value(a.LEAVE_CLEAR_POINTS,0)))
        ,char(sum(value(a.OTHER_CLEAR_POINTS,0)))
	from G_I_02006_MONTH_1 a
	,bass2.dw_product_$op_month b
	   where a.user_id = b.user_id 
		 and b.usertype_id in (1,2,9)
		 and b.test_mark = 0
		 and (b.userstatus_id in (1,2,3,6,8) or b.month_off_mark = 1)
   group by a.user_id
	with ur
       "
	exec_sql $sql_buff

##~   20120524
##~   --~   R283	��	10_���ּƻ�	��ǰ�ɶһ����ֲ���Ϊ��ֵ	02006 �û��������	��ǰ�ɶһ����ֲ���Ϊ��ֵ	0.05	


##~   2.��ǰ�ɶһ�����Ϊ��ֵ
set sql_buff "
	delete from ( select * from G_I_02006_MONTH where time_id = $op_month and  bigint(CONVERTIBLE_POINTS) < 0 ) t 
       "
	exec_sql $sql_buff





#	
#	
#	  #���ڿɶһ�����<0���ڿɶһ�����=0
#	  set handle [aidb_open $conn]
#		set sql_buff "update bass1.g_i_02006_month set canuse_point = '0' where time_id = $op_month and bigint(canuse_point)<0 "
#	    
#	    puts $sql_buff
#		if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#			WriteTrace "$errmsg" 2020
#			puts $errmsg
#			aidb_close $handle
#			return -1
#		}
#		aidb_commit $conn
#		aidb_close $handle
#	
#	
#		#����ۼ��Ѷ��ֻ���<0��Ϊ0
#	  set handle [aidb_open $conn]
#		set sql_buff "update bass1.g_i_02006_month set cash_pointlj = '0' where time_id = $op_month and bigint(cash_pointlj) < 0 "
#	    
#	    puts $sql_buff
#		if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#			WriteTrace "$errmsg" 2020
#			puts $errmsg
#			aidb_close $handle
#			return -1
#		}
#		aidb_commit $conn
#		aidb_close $handle
#	
#	
#	  #R048 ���ֱ��в����������û������������û�����Ϊ��
#	  set handle [aidb_open $conn]
#		set sql_buff "delete from g_i_02006_month where time_id = $op_month and user_id in
#	                      (select user_id from  bass1.month_02006_mid2) with ur"
#	    
#	    puts $sql_buff
#		if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#			WriteTrace "$errmsg" 2020
#			puts $errmsg
#			aidb_close $handle
#			return -1
#		}
#		aidb_commit $conn
#		aidb_close $handle
#	


set sql_buff "
	select count(0)
	from G_I_02006_MONTH
	where bigint(ALL_POINTS) <> bigint(ALL_CONVERTED_POINTS) + bigint(CONVERTIBLE_POINTS) 
	and time_id = $op_month
	with ur
"

chkzero2 $sql_buff "�ܻ���<>�Ѷһ�+�ɶһ�"




  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "g_i_02006_month"
        set pk                  "USER_ID"
        chkpkunique ${tabname} ${pk} ${op_month}
        #
                
				
	return 0
}


##~   20120524:
##~   rules:
##~   CONVERTIBLE_POINTS+ALL_CONVERTED_POINTS= ALL_POINTS
