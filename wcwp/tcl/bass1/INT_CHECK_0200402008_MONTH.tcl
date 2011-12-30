######################################################################################################
#�������ƣ�INT_CHECK_0200402008_MONTH.tcl
#У��ӿڣ�
#��������: ��
#У�����: R080,R081,R082,R002,R007,R016,R018,R019,R076
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�zhanght
#��дʱ�䣺2009-06-22
#�����¼��
#�޸���ʷ: 
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
        set app_name "INT_CHECK_0200402008_MONTH.tcl"

        #�������һ�� yyyy-mm-dd
        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
        
        puts $this_month_last_day

        #�������һ��,��ʽ yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        
        puts $last_month_day
        
        #�������һ�� yyyymmdd
        
        set last_month_last_day [GetLastDay [string range $op_month 0 5]01]
        
        puts $last_month_last_day


	#--R002:�����û��������ʶ��(1,2,3)�ı�����С��75��

  set handle [aidb_open $conn]
	set sql_buff "select cast(sum(case when b.user_id is null then 0 else 1 end) as decimal(15,2))/cast(count(a.user_id) as decimal (15,2))
                 from
                (
                 select user_id,usertype_id from
                 (
                  select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id
                  from bass1.G_A_02008_DAY
                 ) k
                where k.row_id=1 
                ) a
                inner join (select distinct user_id from G_A_02004_DAY
                where usertype_id<>'3'
                and SIM_CODE<>'1'
                
                ) c
                 on a.user_id=c.user_id
                
                left outer join (select user_id,region_flag
                from
                (
                select user_id,region_flag,row_number()over(partition by user_id order by time_id desc) row_id 
                from bass1.G_A_02052_MONTH
                ) k
                where k.row_id=1) b
                 on a.user_id=b.user_id
                  and b.REGION_FLAG in('1','2','3')
                where  a.usertype_id not in ('2010','2020','2030','1040','1021','9000')
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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R002',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1�������û��������ʶ��(1,2,3)�ı�����С��75��


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]<0.75 } {
		set grade 2
	        set alarmcontent " R002У�鲻ͨ��:�����û��������ʶ��(1,2,3)�ı���С��75��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }


	#--R007:���ų�Ա�û��е��û���ʶ��Ӧ�����û�����

  set handle [aidb_open $conn]
	set sql_buff "select count(distinct user_id)
                from BASS1.G_I_02049_MONTH 
                where time_id=$op_month
                and user_id not in (select distinct user_id from bass1.G_A_02004_DAY)
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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R00709',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1�����ų�Ա�û��е��û���ʶ��Ӧ�����û�����


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R00709У�鲻ͨ��:���ų�Ա�û��е��û���ʶ�в����û����е��û�"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }


	#--R016:��ʷ�����û���Ӧ���ڵ����˵�

  set handle [aidb_open $conn]
	set sql_buff "select count(user_id) from
               (
               select user_id,usertype_id from
               (
               select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id
                 from BASS1.G_A_02008_DAY
                 where time_id/100<=$op_month
                 ) k
                 where k.row_id=1
                ) l
                where l.usertype_id like '2%' 
                and user_id in (select distinct  a.user_id
                 from BASS1.G_S_03005_MONTH a
                 left outer join (select distinct user_id from BASS1.G_A_02008_DAY
                 where usertype_id like '2%' 
                 and time_id/100=$op_month) b
                 on a.user_id=b.user_id
                 where a.time_id=$op_month
                 and b.user_id is null)  with ur"

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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R016',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1����ʷ�����û���Ӧ���ڵ����˵�


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R016У�鲻ͨ��:��ʷ�����û����ڵ����˵�"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }		 		 



	#--R018:��ͻ�/���û����е��û���ʶӦ�������û�����

  set handle [aidb_open $conn]
	set sql_buff "select count(distinct user_id) from BASS1.G_I_02005_MONTH 
	              where time_id =$op_month
                  and user_id not in (select distinct user_id from BASS1.G_A_02004_DAY)
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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R018',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1����ͻ�/���û����е��û���ʶӦ�������û�����


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R018У�鲻ͨ��:��ͻ�/���û����е��û���ʶ���������û�����"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }




	#--R019:��ͻ�/���û����г�Ϊ���û����ڣ�Ӧ�����û����и��û�������ʱ��

  set handle [aidb_open $conn]
	set sql_buff "select count(distinct a.user_id) from
                (select * from 
                (
                select user_id,chg_vip_time,row_number()over(partition by user_id order by time_id) row_id from BASS1.G_I_02005_MONTH
                where time_id=$op_month
                ) k
                where k.row_id =1) a
                left outer join 
                (
                select * from
                (
                select user_id,create_date,row_number()over(partition by user_id order by time_id) row_id from BASS1.G_A_02004_DAY
                ) k
                where k.row_id=1) b
                on a.user_id=b.user_id
                where bigint(chg_vip_time)<bigint(create_date)
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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R019',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--�ж�
	#--�쳣
	#--1����ͻ�/���û����г�Ϊ���û����ڣ�Ӧ�����û����и��û�������ʱ��


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R019У�鲻ͨ��:��ͻ�/���û����г�Ϊ���û����ڣ������û����и��û�������ʱ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 
		 
		 
	#--R076:���ֱ��в����������û������������û�����Ϊ��

  set handle [aidb_open $conn]
	set sql_buff "select count(a.user_id) from 
               ( select a.user_id,used_point
                 from (select time_id as time_id,user_id as user_id,used_point  from bass1.g_i_02006_month where time_id=$op_month ) a
                 )a,
                (
                  select user_id,brand_id from
                  (
                  select user_id,brand_id,row_number()over(partition by user_id order by time_id desc) row_id
                  from G_A_02004_DAY
                  where time_id<=$last_month_day
                  ) k
                  where k.row_id=1
                )b
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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R076',$DEC_RESULT_VAL1,0,0,0) "
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




   #--R080 �³������û��������ѻ���Ϊ0�ı�����30%
   
   set sqlbuf "select count(a.user_id)
               from 
                 (
	             select a.user_id
                 from (select time_id as time_id,user_id as user_id,point_sum  from bass1.g_i_02006_month where time_id=$op_month and bigint(point_sum) = 0) a        
                 )a,
                (select a.user_id from
                (select time_id,user_id,usertype_id from G_A_02008_DAY where time_id < $l_timestamp) a,
                (select max(time_id) as time_id,user_id as user_id from G_A_02008_DAY where time_id<= $l_timestamp group by user_id)b
                 where a.time_id=b.time_id and a.user_id=b.user_id and a.usertype_id not in ('2010','2020','2030','1040','1021','9000') 
                )b
                where a.user_id = b.user_id  with ur"
               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select count(a.user_id)
               from    (select a.user_id from
                (select time_id,user_id,usertype_id from G_A_02008_DAY where time_id <  $l_timestamp) a,
                (select max(time_id) as time_id,user_id as user_id from G_A_02008_DAY where time_id<= $l_timestamp group by user_id)b
                 where a.time_id=b.time_id and a.user_id=b.user_id and a.usertype_id not in ('2010','2020','2030','1040','1021','9000') 
                )a,
                (select * from G_I_02006_MONTH where time_id =$op_month)b
                where a.user_id = b.user_id
                with ur"
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R080',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
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
                (select a.user_id from
                (select time_id,user_id,usertype_id from G_A_02008_DAY where time_id < $l_timestamp) a,
                (select max(time_id) as time_id,user_id as user_id from G_A_02008_DAY where time_id<$l_timestamp group by user_id)b
                 where a.time_id=b.time_id and a.user_id=b.user_id and a.usertype_id not in ('2010','2020','2030','1040','1021','9000') 
                )b
                where a.user_id = b.user_id   with ur"
               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select count(a.user_id)
               from    (select a.user_id from
                (select time_id,user_id,usertype_id from G_A_02008_DAY where time_id <  $l_timestamp) a,
                (select max(time_id) as time_id,user_id as user_id from G_A_02008_DAY where time_id<= $l_timestamp group by user_id)b
                 where a.time_id=b.time_id and a.user_id=b.user_id and a.usertype_id not in ('2010','2020','2030','1040','1021','9000') 
                )a,
                (select * from G_I_02006_MONTH where time_id =$op_month)b
                where a.user_id = b.user_id
                with ur"
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R081',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
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
                (select a.user_id from
                (select time_id,user_id,usertype_id from G_A_02008_DAY where time_id < $l_timestamp) a,
                (select max(time_id) as time_id,user_id as user_id from G_A_02008_DAY where time_id<=$l_timestamp group by user_id)b
                 where a.time_id=b.time_id and a.user_id=b.user_id and a.usertype_id not in ('2010','2020','2030','1040','1021','9000') 
                )b
                where a.user_id = b.user_id  with ur"
               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select count(a.user_id)
               from    (select a.user_id from
                (select time_id,user_id,usertype_id from G_A_02008_DAY where time_id <  $l_timestamp) a,
                (select max(time_id) as time_id,user_id as user_id from G_A_02008_DAY where time_id<= $l_timestamp group by user_id)b
                 where a.time_id=b.time_id and a.user_id=b.user_id and a.usertype_id not in ('2010','2020','2030','1040','1021','9000') 
                )a,
                (select * from G_I_02006_MONTH where time_id =$op_month)b
                where a.user_id = b.user_id
                with ur"
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R082',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R077',$DEC_RESULT_VAL1,0,0,0) "
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


	#--R078:�ۼ��Ѷһ�����ֵ�ݱ��¶һ�����ֵ

  set handle [aidb_open $conn]
	set sql_buff "select count(distinct a.user_id) from 
                  (
                  select a.user_id,a.cash_pointlj
                    from (select time_id as time_id,user_id as user_id,cash_pointlj as cash_pointlj from bass1.g_i_02006_month where time_id=$op_month ) a
                   )a,
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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R078',$DEC_RESULT_VAL1,0,0,0) "
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
              ( select a.user_id,coms_pointlj
                from (select time_id as time_id,user_id as user_id,coms_pointlj as coms_pointlj from bass1.g_i_02006_month where time_id =$op_month ) a
              )a,
               (select user_id,Brand_id from G_A_02004_DAY where time_id < $last_month_day )b
               where a.user_id = b.user_id and ((b.brand_id = '1' and bigint(a.coms_pointlj) >= 1000) or (b.brand_id = '3' and bigint(a.coms_pointlj) >= 500)) with ur"
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R079',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
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