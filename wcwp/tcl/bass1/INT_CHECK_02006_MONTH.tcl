######################################################################################################
#�������ƣ�INT_CHECK_02006_MONTH.tcl
#У��ӿڣ�
#��������: ��
#У�����: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhanght
#��дʱ�䣺2009-06-25
#�����¼��
#�޸���ʷ: 20091207 �Ż���䣬����ÿ������Ч��
#          20100120 �����ͻ��ھ��޸� usertype_id not in ('2010','2020','2030','9000') 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
            
        #����  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        
        #��Ȼ�µ�һ�� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6]01
        
        #���µ�һ�� yyyymmdd
        set l_timestamp [string range $optime_month 0 3][string range $optime_month 5 6]01
        
        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        
        #���� yyyy-mm-dd
        set optime $op_time
        
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        
        #������
        set app_name "INT_CHECK_02006_MONTH.tcl"

        #�������һ�� yyyy-mm-dd
        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
        
        puts $this_month_last_day

        #�������һ��,��ʽ yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        
        puts $last_month_day
        
        #�������һ�� yyyymmdd
        
        set last_month_last_day [GetLastDay [string range $op_month 0 5]01]
        
        puts $last_month_last_day

	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month 
	                    and rule_code in ('R076','R077','R078','R079','R080','R081','R082')"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#--R076:���ֱ��в����������û������������û�����Ϊ��

	set sqlbuf "alter table bass1.int_check_online_user_tmp1 activate not logged initially with empty table"
	
	exec_sql $sqlbuf  


	set sqlbuf "insert into bass1.int_check_online_user_tmp1
				select user_id,brand_id from
                  (
                  select user_id,brand_id,row_number()over(partition by user_id order by time_id desc) row_id
                  from G_A_02004_DAY
                  where time_id<=$last_month_day
                  ) k
                  where k.row_id=1
				"
	exec_sql $sqlbuf  

  puts $sqlbuf  
  set handle [aidb_open $conn]
	set sql_buff "select count(a.user_id) from 
               ( select a.user_id,used_point
                 from 
                 (select time_id as time_id,user_id as user_id,used_point  from bass1.g_i_02006_month where time_id=$op_month ) a
                 )a,
                 bass1.int_check_online_user_tmp1 b
                where a.user_id = b.user_id and b.brand_id = '2' and bigint(a.used_point) <> 0
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

	puts $DEC_RESULT_VAL1

	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R076',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1�����ֱ��в����������û������������û�����Ϊ��


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R076У�鲻ͨ��:���ֱ��к��������û������������û�����Ϊ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }

puts "----------------R076check---------------finished"
######################################################################
##������ʱ����������Ż�
######################################################################

    set sqlbuf "alter table bass1.int_check_online_user_tmp activate not logged initially with empty table"
    
    puts $sqlbuf

	exec_sql $sqlbuf  

    set sqlbuf "alter table bass1.int_check_online_user_tmp2 activate not logged initially with empty table"
    
    puts $sqlbuf
    
    exec_sql $sqlbuf
    
    set sqlbuf "alter table bass1.int_check_online_user_tmp3 activate not logged initially with empty table"
    
    puts $sqlbuf
    
    exec_sql $sqlbuf    

    set sqlbuf "insert into bass1.int_check_online_user_tmp3(time_id,user_id)
                select distinct time_id,user_id from G_A_02008_DAY 
                 where time_id < $l_timestamp
                   and usertype_id not in ('2010','2020','2030','9000') "
                
    exec_sql $sqlbuf
    
    puts $sqlbuf
    
    set sqlbuf "insert into bass1.int_check_online_user_tmp2(time_id,user_id)
                select max(time_id) as time_id,user_id as user_id 
                from G_A_02008_DAY
                where time_id<= $l_timestamp group by user_id"
    exec_sql $sqlbuf
    
    puts $sqlbuf
    
    exec db2 connect to bassdb user bass1 using bass1
    
    exec db2 runstats on table bass1.int_check_online_user_tmp3 with distribution and detailed indexes all
    
    exec db2 runstats on table bass1.int_check_online_user_tmp2 with distribution and detailed indexes all
    
    exec db2 terminate
    
	set sqlbuf "insert into bass1.int_check_online_user_tmp
				select distinct a.user_id 
				  from bass1.int_check_online_user_tmp3 a,
                       bass1.int_check_online_user_tmp2 b
                 where a.time_id=b.time_id 
                   and a.user_id=b.user_id"

	exec_sql $sqlbuf  

   puts $sqlbuf
   #--R080 �³������û��������ѻ���Ϊ0�ı�����30%
   
   set sqlbuf "select count(a.user_id)
               from 
                 (
	             select a.user_id
                 from (select time_id as time_id,user_id as user_id,point_sum  from bass1.g_i_02006_month where time_id=$op_month and bigint(point_sum) = 0) a        
                 )a,
                bass1.int_check_online_user_tmp b
                where a.user_id = b.user_id  with ur"
               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select count(a.user_id)
               from bass1.int_check_online_user_tmp a,
                    (select * from G_I_02006_MONTH where time_id =$op_month) b
               where a.user_id = b.user_id
                with ur"
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R080',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
  set  RESULT_VAL3 [expr $RESULT_VAL1/$RESULT_VAL2]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.3} {
		set grade 2
	        set alarmcontent "R080 У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R080	�³������û��������ѻ���Ϊ0�ı�����30%"

		 		 



   #--R081 �³������û���ǰ�ɶһ�����Ϊ0�ı�����30%
   
   set sqlbuf "select count(a.user_id)
               from 
               (
	             select a.user_id
                 from (select time_id as time_id,user_id as user_id,used_point  from bass1.g_i_02006_month where time_id=$op_month and bigint(used_point) = 0) a
                 )a,
                 bass1.int_check_online_user_tmp b
                where a.user_id = b.user_id   with ur"
               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select count(a.user_id)
               from bass1.int_check_online_user_tmp a,
                   (select * from G_I_02006_MONTH where time_id =$op_month) b
               where a.user_id = b.user_id
                with ur"
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R081',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
  set  RESULT_VAL3 [expr $RESULT_VAL1/$RESULT_VAL2]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.3} {
		set grade 2
	        set alarmcontent "R081 У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R081 �³������û���ǰ�ɶһ�����Ϊ0�ı�����30%"


   #--R082 �³������û��ۼ������ѻ���Ϊ0�ı�����20%
   
   set sqlbuf "select count(a.user_id)
               from 
                 (
	             select a.user_id
                 from (select time_id as time_id,user_id as user_id,used_pointlj  from bass1.g_i_02006_month where time_id=$op_month and bigint(used_pointlj) = 0 ) a
                 )a,
                 bass1.int_check_online_user_tmp b
                where a.user_id = b.user_id  with ur"
               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select count(a.user_id)
               from bass1.int_check_online_user_tmp a,
                    (select * from G_I_02006_MONTH where time_id =$op_month) b
               where a.user_id = b.user_id
               with ur"
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R082',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
  set  RESULT_VAL3 [expr $RESULT_VAL1/$RESULT_VAL2]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.2} {
		set grade 2
	        set alarmcontent "R082 У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R082 �³������û��ۼ������ѻ���Ϊ0�ı�����20%"


	#--R077:�ۼ������ѻ��֡ݵ������ѻ���

  set handle [aidb_open $conn]
	set sql_buff "select count(*) from G_I_02006_MONTH 
	               where time_id = $op_month 
	                  and bigint(coms_pointlj) < bigint(point_sum)  
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

	puts $DEC_RESULT_VAL1

	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R077',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1���ۼ������ѻ��֡ݵ������ѻ���


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R077У�鲻ͨ��:�ۼ������ѻ���<�������ѻ���"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }





##
##R078У�鲻ͨ��������02006���ݵ�����
##
    #������ʱ��
    set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_i_02006_month_add
	                 (
						time_id          integer      ,
						user_id          character(20),
						point_sum        character(8) ,
						free_point       character(8) ,
						used_point       character(8) ,
						t_used_point     character(8) ,
						tone_used_point  character(8) ,
						ttwo_used_point  character(8) ,
                                                tthree_used_point character(8) ,
						used_pointlj     character(8) ,
						coms_pointlj     character(8) ,
						cash_pointlj     character(8) ,
						canuse_point     character(8) ,
						off_point        character(8) ,
						change_point     character(8) 
                      )
                      partitioning key
                      (user_id)
                      using hashing
                     with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #���ۻ��һ�����С�ڵ��¶һ����ֵļ�¼����session��
    set handle [aidb_open $conn]
	set sql_buff "
	          insert into session.g_i_02006_month_add
	          select a.time_id,a.user_id,a.point_sum,a.free_point,a.used_point,a.t_used_point,a.tone_used_point,a.ttwo_used_point,a.tthree_used_point,
	                 a.used_pointlj,a.coms_pointlj,char(bigint(a.cash_pointlj)+bigint(b.used_point)) cash_pointlj,a.canuse_point,a.off_point,a.change_point 
	            from 
                  (select * from bass1.g_i_02006_month where time_id=$op_month ) a,
                  (select user_id,sum(bigint(used_point)) as used_point from G_S_02007_MONTH where time_id = $op_month  group by user_id) b
                where a.user_id = b.user_id 
                and bigint(a.cash_pointlj) < bigint(b.used_point)
               "
    
    puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



    #ɾ��02006���������û���¼
	set handle [ aidb_open $conn ]
	set sql_buff "delete from bass1.g_i_02006_month 
	               where time_id=$op_month
	                 and user_id in (select user_id from session.g_i_02006_month_add)
       "

	puts $sql_buff
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



    #�ѵ����Ľ������˽ӿڱ�
	set handle [ aidb_open $conn ]
	set sql_buff "insert into bass1.g_i_02006_month 
                   select * from session.g_i_02006_month_add
               "

	puts $sql_buff
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


##
##
##



	#--R078:�ۼ��Ѷһ�����ֵ�ݱ��¶һ�����ֵ

  set handle [aidb_open $conn]
	set sql_buff "select count(distinct a.user_id) from 
                  (select user_id ,cash_pointlj from bass1.g_i_02006_month where time_id=$op_month ) a,
                  (select user_id,sum(bigint(used_point)) as used_point from G_S_02007_MONTH where time_id = $op_month  group by user_id) b
                where a.user_id = b.user_id and bigint(a.cash_pointlj) < bigint(b.used_point) with ur"

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

	puts $DEC_RESULT_VAL1

	#--��У��ֵ����У������
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R078',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1���ۼ��Ѷһ�����ֵ�ݱ��¶һ�����ֵ


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R078У�鲻ͨ��:�ۼ��Ѷһ�����ֵ<���¶һ�����ֵ"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }







   #--R079 ����һ��ͻ�������һ�����µ׵��жһ��ʸ�ͻ���
   
   set sqlbuf "select count(distinct user_id ) from G_S_02007_MONTH where time_id = $op_month with ur"
               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select count(a.user_id) from 
               (select user_id,coms_pointlj from bass1.g_i_02006_month where time_id = $op_month )a,
               (select distinct user_id,Brand_id from G_A_02004_DAY where time_id < $last_month_day )b
               where a.user_id = b.user_id and ((b.brand_id = '1' and bigint(a.coms_pointlj) >= 1000) or (b.brand_id = '3' and bigint(a.coms_pointlj) >= 500)) with ur"
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R079',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
#  set  RESULT_VAL3 [expr $RESULT_VAL1/$RESULT_VAL2]
#  puts  $RESULT_VAL3
	if {$RESULT_VAL1>$RESULT_VAL2} {
		set grade 2
	        set alarmcontent "R079 У�鲻ͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "R079 ����һ��ͻ�������һ�����µ׵��жһ��ʸ�ͻ���"


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
