######################################################################################################
#�������ƣ�INT_CHECK_02004_02008_DAY.tcl
#У��ӿڣ�G_A_02004_DAY.tcl G_A_02008_DAY.tcl
#��������: ��
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhanght
#��дʱ�䣺2009-05-30
#�����¼��
#�޸���ʷ: liuqf 090731 ����idԭ����R01109 �޸�ΪR001
#          20100125 �����û��ھ��䶯 usertype_id not in ('2010','2020','2030','9000') ���ų����ݿ� sim_code<>'1'

#          20110526 R001 -> R003 ���ݣ����� һ����Ӫ����ϵͳ����׼ȷ�Կ��˹���2011��.xls

#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#        #���� yyyymm
#        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#            
#        #����  yyyymm
#        set last_month [GetLastMonth [string range $op_month 0 5]]
#        
#        #��Ȼ�µ�һ�� yyyymmdd
#        set timestamp [string range $op_time 0 3][string range $op_time 5 6]01
#        
#        #���µ�һ�� yyyymmdd
#        set l_timestamp [string range $optime_month 0 3][string range $optime_month 5 6]01
        
        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]

 set curr_month [string range $op_time 0 3][string range $op_time 5 6]
 
        #������
        set app_name "INT_CHECK_02004_02008_DAY.tcl"

	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $timestamp 
	                    and rule_code in ('R003','R010')"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


	set handle [aidb_open $conn]
	set sql_buff "ALTER TABLE CHECK_0200402008_DAY_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


	set handle [aidb_open $conn]
	set sql_buff "insert into CHECK_0200402008_DAY_1
select user_id,product_no,usertype_id,sim_code from
 (
 select user_id,product_no,usertype_id,sim_code,row_number()over(partition by user_id order by time_id desc) row_id from G_A_02004_DAY
 where time_id<=$timestamp
 ) k
 where k.row_id=1 with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


	set handle [aidb_open $conn]
	set sql_buff "ALTER TABLE CHECK_0200402008_DAY_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	set handle [aidb_open $conn]
	set sql_buff "insert into CHECK_0200402008_DAY_2
   select user_id,usertype_id from
   (
   select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id from G_A_02008_DAY
   where time_id<=$timestamp
   ) k
   where k.row_id=1 with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set handle [aidb_open $conn]
	set sql_buff "ALTER TABLE CHECK_0200402008_DAY_3 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
	
	
	set handle [aidb_open $conn]
	set sql_buff "insert into CHECK_0200402008_DAY_3
select a.*,b.user_id user_no,b.usertype_id usertype_no from 
                CHECK_0200402008_DAY_1 a,
                CHECK_0200402008_DAY_2 b
               where a.user_id = b.user_id and a.usertype_id <> '3' 
                 and b.usertype_id not in ('2010','2020','2030','9000')
with ur"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	


  set handle [aidb_open $conn]
	set sql_buff "select count(*) from
(
select product_no,count(*) cnt from CHECK_0200402008_DAY_3
group by product_no
			   ) k where k.cnt>=2
               with ur"



	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	#puts $DEC_RESULT_VAL1

	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R003',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]!=0 } {
		set grade 2
	        set alarmcontent " 02004��02008�ӿ��û�һ��У��R003У�鲻ͨ��,��9��֮ǰ���"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 
### ���� 201107 У�鲻ͨ��������ԭУ�鲻׼ȷ ����������֮��
# 20110819
set sql_buff "
SELECT CNT1 - CNT2 
FROM 
	(
	select COUNT(0) CNT1
	from
	( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
		   from bass1.g_a_02008_day
		   where  time_id/100 <= $curr_month
		   ) f 
	where    f.row_id=1 
	    ) A        
	, (
	select COUNT(0) CNT2
	from
	(select user_id,create_date,product_no,brand_id,sim_code,usertype_id
			,row_number() over(partition by user_id order by time_id desc ) row_id   
	from bass1.g_a_02004_day
	where time_id/100 <= $curr_month ) e
	where e.row_id=1
	) B 
"

set RESULT_VAL [get_single $sql_buff]

	if {[format %.3f [expr ${RESULT_VAL} ]]!=0 } {
		set grade 2
	        set alarmcontent " 02004��02008�ӿ��û�һ��У��R003У�鲻ͨ��,��9��֮ǰ���"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }


		 
#	set handle [aidb_open $conn]
#	set sql_buff "ALTER TABLE test1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
# 
# 
#	set handle [aidb_open $conn]
#	set sql_buff "insert into test1
#select user_id,product_no,usertype_id,sim_code from
# (
# select user_id,product_no,usertype_id,sim_code,row_number()over(partition by user_id order by time_id desc) row_id from G_A_02004_DAY
# where time_id<=$timestamp
# ) k
# where k.row_id=1 with ur"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle 
# 
#
#	set handle [aidb_open $conn]
#	set sql_buff "ALTER TABLE test2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle 
#
#	set handle [aidb_open $conn]
#	set sql_buff "insert into test2
#   select user_id,usertype_id from
#   (
#   select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id from G_A_02008_DAY
#   where time_id<=$timestamp
#   ) k
#   where k.row_id=1 with ur"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#
#
##	#--R011:һ���绰���벻��ͬʱ��Ӧ������������û�ID
##
##  set handle [aidb_open $conn]
##	set sql_buff "
##	 select value(cnt,0) from
##	 (
##	 select count(a.product_no) cnt from 
##                test1 a,
##                test2 b
##               where a.user_id = b.user_id and a.usertype_id <> '3' and a.sim_code <> '1' and 
##                     b.usertype_id not in ('2010','2020','2030','9000')
##               group by a.product_no
##               having count(a.user_id) > 1 
##   ) k            
##               with ur"
##               
#	#--R011:һ���绰���벻��ͬʱ��Ӧ������������û�ID
#
#  set handle [aidb_open $conn]
#	set sql_buff "select count(a.product_no) from 
#                (select a.user_id,a.product_no,usertype_id,sim_code from
#                 (select time_id,user_id,product_no,usertype_id,sim_code from G_A_02004_DAY where time_id <= $timestamp ) a,
#                 (select max(time_id) as time_id,user_id as user_id from G_A_02004_DAY where time_id<=$timestamp 
#                   group by user_id
#                  ) b
#                  where a.time_id=b.time_id and a.user_id=b.user_id 
#                 )a,
#                 
#                (select a.user_id,usertype_id from
#                 (select time_id,user_id,usertype_id from G_A_02008_DAY where time_id <= $timestamp ) a,
#                 (select max(time_id) as time_id,user_id as user_id from G_A_02008_DAY where time_id<=$timestamp 
#                     group by user_id)b
#                  where a.time_id=b.time_id and a.user_id=b.user_id
#                 ) b
#               where a.user_id = b.user_id and a.usertype_id <> '3' and a.sim_code <> '1' and 
#                     b.usertype_id not in ('2010','2020','2030','9000')
#               group by a.product_no
#               having count(a.user_id) > 1 
#               with ur"
#
#	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	aidb_commit $conn
#	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]
#
#	puts $DEC_RESULT_VAL1
#
#	#--��У��ֵ����У������
#	set handle [aidb_open $conn]
#	set sql_buff "\
#		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R01109',$DEC_RESULT_VAL1,0,0,0) "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace $errmsg 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#
#	#--�ж�
#	#--�쳣
#	#--1��һ���绰���벻��ͬʱ��Ӧ������������û�ID
#
#
#	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
#		set grade 2
#	        set alarmcontent " R01109У�鲻ͨ��"
#	        WriteAlarm $app_name $optime $grade ${alarmcontent}
#		 }
		 		 

	#--R010:δ�����Ź�˾��Ч����û�״̬�������⴫9000

  set handle [aidb_open $conn]
	set sql_buff "select count(*) from bass1.G_A_02008_DAY where usertype_id='9000' and time_id=$timestamp  with ur"

	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	#puts $DEC_RESULT_VAL1

	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R010',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1��δ�����Ź�˾��Ч����û�״̬�������⴫9000


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R010У�鲻ͨ��,�û�״̬���������ϴ�9000��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }



  #����02008����Ψһ�Լ��
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select user_id,count(*) cnt from bass1.g_a_02008_day
	              where time_id =$timestamp
	             group by user_id
	             having count(*)>1
	            ) as a
	            "
	puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	puts $DEC_RESULT_VAL1

	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent "02008�ӿ�����Ψһ��У��δͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	   }




  #����02004����Ψһ�Լ��
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select user_id,count(*) cnt from bass1.g_a_02004_day
	              where time_id =$timestamp
	             group by user_id
	             having count(*)>1
	            ) as a
	            "
	puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set DEC_RESULT_VAL2 [format "%.3f" [expr ${DEC_RESULT_VAL2} /1.00]]

	puts $DEC_RESULT_VAL2

	if {[format %.3f [expr ${DEC_RESULT_VAL2} ]]>0 } {
		set grade 2
	        set alarmcontent "02004�ӿ�����Ψһ��У��δͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	   }








		
	return 0
}
