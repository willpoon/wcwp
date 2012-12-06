#################################################################
#程序名称: INT_CHECK_Z345_DAY.tcl
#功能描述: 生成Z345的校验程序
#规则编号：R020,R021,R022
#规则属性：指标异动
#规则类型：日
#指标摘要：R020：全球通用户到达数变动率
#          R021：神州行用户到达数变动率
#          R022：动感地带用户到达数变动率
#规则描述：R020：定义：全球通用户到达数变动率 = |(当日全球通用户到达数/上日全球通用户到达数 - 1)*100%|
#	       规则：全球通用户到达数变动率 ≤ 3%           每月1-3号    1%-5%  平时<=3%
#          R021：定义：神州行用户到达数变动率 = |(当日神州行用户到达数/上日神州行用户到达数-1)*100%|
#              规则：神州行用户到达数变动率 ≤ 2%
#          R022：定义：动感地带用户到达数变动率 = |(当日动感地带用户到达数/上日动感地带用户到达数-1)*100%|
#              规则：动感地带用户到达数变动率 ≤ 3%
#校验对象：1.BASS1.G_A_02004_DAY  用户
#          2.BASS1.G_A_02008_DAY  用户状态
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.20091125 修正规则ID Z3-->R020;Z4-->R021;Z5-->R022 以及进行语句优化
#            20100127 修改在网用户口径 usertype_id not in ('2010','2020','2030','9000') 不排除数据卡 sim_code<>'1' 20100427 规范规则ID校验值
#################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #程序名
        set app_name "INT_CHECK_Z345_DAY.tcl"


##~   #########################################################################################
##~   #一个电话号码不能同时对应多个非离网的用户ID校验	 add by zhanght on 2009.05.18
	
	
	##~   set handle [aidb_open $conn]
	##~   set sql_buff "ALTER TABLE PRODUCT_XHX3 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle

 

	##~   set handle [aidb_open $conn]
	##~   set sql_buff "ALTER TABLE CHECK_0200402008_DAY_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle
 
 
	##~   set handle [aidb_open $conn]
	##~   set sql_buff "insert into CHECK_0200402008_DAY_1
##~   select user_id,product_no,usertype_id,sim_code from
 ##~   (
 ##~   select user_id,product_no,usertype_id,sim_code,row_number()over(partition by user_id order by time_id desc) row_id from G_A_02004_DAY
 ##~   where time_id<=$timestamp
 ##~   ) k
 ##~   where k.row_id=1 
 ##~   and k.usertype_id <> '3'
 ##~   with ur"
        ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle 
 

	##~   set handle [aidb_open $conn]
	##~   set sql_buff "ALTER TABLE CHECK_0200402008_DAY_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle 

	##~   set handle [aidb_open $conn]
	##~   set sql_buff "insert into CHECK_0200402008_DAY_2
   ##~   select user_id,usertype_id from
   ##~   (
   ##~   select user_id,usertype_id,row_number()over(partition by user_id order by time_id desc) row_id from G_A_02008_DAY
   ##~   where time_id<=$timestamp
   ##~   ) k
   ##~   where k.row_id=1 
   ##~   and k.usertype_id not in ('2010','2020','2030','9000') with ur"
        ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle
 

 
 	##~   set handle [aidb_open $conn]
	##~   set sql_buff "ALTER TABLE CHECK_0200402008_DAY_4 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle 



 	##~   set handle [aidb_open $conn]
	##~   set sql_buff "insert into CHECK_0200402008_DAY_4
	##~   select a.product_no from CHECK_0200402008_DAY_1 a,
         ##~   CHECK_0200402008_DAY_2 b
         ##~   where a.user_id=b.user_id
  ##~   with ur       
	
	##~   "
        ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle 
	
	##~   set handle [aidb_open $conn]
	##~   set sql_buff " 
 ##~   insert into  PRODUCT_XHX3
  ##~   select product_no
   ##~   from  CHECK_0200402008_DAY_4
   ##~   group by product_no
   ##~   having count(*) >=2 
   ##~   with ur"
 
 
  ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle


	##~   set handle [aidb_open $conn]
	##~   set sql_buff "ALTER TABLE product_xhx4 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle
	

	##~   set handle [aidb_open $conn]
	##~   set sql_buff "insert into product_xhx4  select distinct k.user_id from
##~   (
##~   select user_id,row_number()over(partition by product_no order by int(create_date) desc) row_id from G_A_02004_DAY 
##~   where product_no in (select product_no from PRODUCT_XHX3) 
##~   ) k
##~   where k.row_id<>1 with ur"
        ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle


	##~   set handle [aidb_open $conn]
	##~   set sql_buff "insert into G_A_02008_DAY select distinct $timestamp,user_id,'2020' from G_A_02004_DAY where user_id in(select user_id from product_xhx4) with ur"
        ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle




##~   ################################################################################
  ##~   set handle [aidb_open $conn]
	##~   set sql_buff "delete from bass1.g_rule_check where time_id=${timestamp} 
		        ##~   and rule_code in ('R020','R021','R022') "
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
        ##~   aidb_close $handle
  
  
 ##~   ############################
 ##~   # 20091125 modify 建立临时表，供下面使用
 ##~   ############################
 
 	##~   set handle [aidb_open $conn]
	##~   set sql_buff "ALTER TABLE check_temp_02004_now ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle

 	##~   set handle [aidb_open $conn]
	##~   set sql_buff "insert into check_temp_02004_now 
	              ##~   SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
    	 	         ##~   WHERE TIME_ID <= INT($timestamp)
    	  	       ##~   GROUP BY USER_ID
	    ##~   "
        ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle


 	##~   set handle [aidb_open $conn]
	##~   set sql_buff "ALTER TABLE check_temp_02004_last ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle

 	##~   set handle [aidb_open $conn]
	##~   set sql_buff "insert into check_temp_02004_last 
	              ##~   SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
    	 	         ##~   WHERE TIME_ID <= INT($last_day)
    	  	       ##~   GROUP BY USER_ID
	    ##~   "
        ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle


 	##~   set handle [aidb_open $conn]
	##~   set sql_buff "ALTER TABLE check_temp_02008_now ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle

 	##~   set handle [aidb_open $conn]
	##~   set sql_buff "insert into check_temp_02008_now 
	              ##~   SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
    		 				##~   WHERE TIME_ID <= INT($timestamp)
    		 				##~   GROUP BY USER_ID
	    ##~   "
  ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle



 	##~   set handle [aidb_open $conn]
	##~   set sql_buff "ALTER TABLE check_temp_02008_last ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
        ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle

 	##~   set handle [aidb_open $conn]
	##~   set sql_buff "insert into check_temp_02008_last 
	              ##~   SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
    		 				##~   WHERE TIME_ID <= INT($last_day)
    		 				##~   GROUP BY USER_ID
	    ##~   "
  ##~   puts $sql_buff
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   aidb_close $handle

  ##~   exec db2 connect to bassdb user bass1 using bass1
  
  ##~   exec db2 runstats on table bass1.g_a_02008_day with distribution and detailed indexes all
  
  ##~   exec db2 runstats on table bass1.check_temp_02004_now with distribution and detailed indexes all
  
  ##~   exec db2 terminate
  
  ##~   exec db2 connect to bassdb user bass1 using bass1
  
  ##~   exec db2 runstats on table bass1.g_a_02008_day with distribution and detailed indexes all
  
  ##~   exec db2 runstats on table bass1.check_temp_02008_now with distribution and detailed indexes all
  
  ##~   exec db2 terminate    

	##~   #--R020:全球通用户到达数变动率
	##~   #--本日全球通用户到达数
        ##~   set handle [aidb_open $conn]
	##~   set sql_buff "SELECT
    		##~   COUNT(*)
    	##~   FROM
    		##~   (SELECT
    		 	##~   A.TIME_ID,
    		 	##~   A.USER_ID,
    		 	##~   A.USERTYPE_ID,
    		 	##~   A.SIM_CODE
    		##~   FROM BASS1.G_A_02004_DAY  A,
    		     ##~   BASS1.check_temp_02004_now B
    	    ##~   WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='1'
    	    ##~   )T,

    		##~   (SELECT
    	 		##~   A.TIME_ID,
    	 		##~   A.USER_ID,
    	 		##~   A.USERTYPE_ID
    		 ##~   FROM BASS1.G_A_02008_DAY A,
    		      ##~   BASS1.check_temp_02008_now B
    		##~   WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID
    		##~   ) M
   		##~   WHERE T.USER_ID = M.USER_ID
    		  ##~   AND T.TIME_ID <= INT($timestamp)
    		  ##~   AND M.TIME_ID <= INT($timestamp)
    		  ##~   AND T.USERTYPE_ID <> '3'
    		  ##~   AND M.USERTYPE_ID NOT IN ('2010','2020','2030','9000');"
     ##~   puts $sql_buff
	 ##~   if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		##~   WriteTrace $errmsg 1001
		##~   return -1
	##~   }
	##~   if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		##~   WriteTrace $errmsg 1002
		##~   return -1
	##~   }
	##~   aidb_commit $conn
	##~   set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	##~   #puts $DEC_RESULT_VAL1

	##~   #--昨日全球通用户到达数
	##~   set handle [aidb_open $conn]

	##~   set sql_buff "SELECT
    		##~   COUNT(*)
    	##~   FROM
    		##~   (SELECT
    		 	##~   A.TIME_ID,
    		 	##~   A.USER_ID,
    		 	##~   A.USERTYPE_ID,
    		 	##~   A.SIM_CODE
    		##~   FROM BASS1.G_A_02004_DAY  A,
             ##~   BASS1.check_temp_02004_last B
    	    ##~   WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='1'
    	    ##~   )T,

    		##~   (SELECT
    	 		##~   A.TIME_ID,
    	 		##~   A.USER_ID,
    	 		##~   A.USERTYPE_ID
    		 ##~   FROM BASS1.G_A_02008_DAY A,
              ##~   BASS1.check_temp_02008_last B
    		##~   WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID
    		##~   ) M
   		##~   WHERE T.USER_ID = M.USER_ID
    		  ##~   AND T.TIME_ID <= INT($last_day)
    		  ##~   AND M.TIME_ID <= INT($last_day)
    		  ##~   AND T.USERTYPE_ID <> '3'
    		  ##~   AND M.USERTYPE_ID NOT IN ('2010','2020','2030','9000');"
     
     ##~   puts $sql_buff
	 ##~   if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		##~   WriteTrace $errmsg 1003
		##~   return -1
	##~   }

	##~   if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		##~   WriteTrace $errmsg 1004
		##~   return -1
	##~   }

	##~   aidb_commit $conn

	##~   set DEC_RESULT_VAL2 [format "%.3f" [expr ${DEC_RESULT_VAL2} /1.00]]

	##~   #puts $DEC_RESULT_VAL2

	##~   #--将校验值插入校验结果表
	##~   set handle [aidb_open $conn]
	##~   set sql_buff "\
		##~   INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R020',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,1.000 * $DEC_RESULT_VAL1 / $DEC_RESULT_VAL2 - 1,0) "
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn

	##~   #--判断
	##~   #--异常
	##~   #--1：全球通用户到达数变动率<= 3%超标

	##~   #set DEC_RESULT_VAL2 "0.2"
	##~   #set DEC_RESULT_VAL1 "0.1"

	##~   if {[format "%.3f" [expr ${DEC_RESULT_VAL1} / ${DEC_RESULT_VAL2} -1]]>0.030 || [format "%.3f" [expr ${DEC_RESULT_VAL1} / ${DEC_RESULT_VAL2} -1]]<-0.030} {
		##~   set grade 2
	        ##~   set alarmcontent "准确性指标R020：全球通用户到达数变动率超出集团考核范围"
	        ##~   WriteAlarm $app_name $optime $grade ${alarmcontent}
		 ##~   }

	 ##~   #--------------------------------------------------------------------
	 ##~   #--R021:神州行用户到达数变动率
         ##~   #--本日神州行用户到达数
        ##~   set handle [aidb_open $conn]

	##~   set sql_buff "SELECT
    		##~   COUNT(*)
    	##~   FROM
    		##~   (SELECT
    		 	##~   A.TIME_ID,
    		 	##~   A.USER_ID,
    		 	##~   A.USERTYPE_ID,
    		 	##~   A.SIM_CODE
    		##~   FROM BASS1.G_A_02004_DAY  A,
             ##~   BASS1.check_temp_02004_now B
    	    ##~   WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='2'
    	    ##~   )T,

    		##~   (SELECT
    	 		##~   A.TIME_ID,
    	 		##~   A.USER_ID,
    	 		##~   A.USERTYPE_ID
    		 ##~   FROM BASS1.G_A_02008_DAY A,
              ##~   BASS1.check_temp_02008_now B
    		##~   WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID
    		##~   ) M
   		##~   WHERE T.USER_ID = M.USER_ID
    		  ##~   AND T.TIME_ID <= INT($timestamp)
    		  ##~   AND M.TIME_ID <= INT($timestamp)
    		  ##~   AND T.USERTYPE_ID <> '3'
    		  ##~   AND M.USERTYPE_ID NOT IN ('2010','2020','2030','9000');"
     
     ##~   puts $sql_buff
	 ##~   if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		##~   WriteTrace $errmsg 1001
		##~   return -1
	##~   }

	##~   if [catch {set SZX_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		##~   WriteTrace $errmsg 1002
		##~   return -1
	##~   }

	##~   aidb_commit $conn
	##~   set SZX_RESULT_VAL1 [format "%.3f" [expr ${SZX_RESULT_VAL1} /1.00]]
	##~   #puts $SZX_RESULT_VAL1

	##~   #--昨日神州行用户到达数
	##~   set handle [aidb_open $conn]

	##~   set sql_buff "SELECT
    		##~   COUNT(*)
    	##~   FROM
    		##~   (SELECT
    		 	##~   A.TIME_ID,
    		 	##~   A.USER_ID,
    		 	##~   A.USERTYPE_ID,
    		 	##~   A.SIM_CODE
    		##~   FROM BASS1.G_A_02004_DAY  A,
             ##~   BASS1.check_temp_02004_last B
    	    ##~   WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='2'
    	    ##~   )T,

    		##~   (SELECT
    	 		##~   A.TIME_ID,
    	 		##~   A.USER_ID,
    	 		##~   A.USERTYPE_ID
    		 ##~   FROM BASS1.G_A_02008_DAY A,
              ##~   BASS1.check_temp_02008_last B
    		##~   WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID
    		##~   ) M
   		##~   WHERE T.USER_ID = M.USER_ID
    		  ##~   AND T.TIME_ID <= INT($last_day)
    		  ##~   AND M.TIME_ID <= INT($last_day)
    		  ##~   AND T.USERTYPE_ID <> '3'
    		  ##~   AND M.USERTYPE_ID NOT IN ('2010','2020','2030','9000');"
     
     ##~   puts $sql_buff
	 ##~   if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		##~   WriteTrace $errmsg 1003
		##~   return -1
	##~   }

	##~   if [catch {set SZX_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		##~   WriteTrace $errmsg 1004
		##~   return -1
	##~   }

	##~   aidb_commit $conn

	##~   set SZX_RESULT_VAL2 [format "%.3f" [expr ${SZX_RESULT_VAL2} /1.00]]

	##~   #puts $SZX_RESULT_VAL2

	##~   ##--将校验值插入校验结果表
	##~   set handle [aidb_open $conn]
	##~   set sql_buff "\
		##~   INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R021',$SZX_RESULT_VAL1,$SZX_RESULT_VAL2,1.000 * $SZX_RESULT_VAL1 / $SZX_RESULT_VAL2 - 1,0) "
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn

	 ##~   #--判断
         ##~   #--异常：
         ##~   #--1：神州行用户到达数变动率<= 2%超标

         ##~   #set SZX_RESULT_VAL2 "0.2"
	 ##~   #set SZX_RESULT_VAL1 "0.1"

	 ##~   if {[format "%.3f" [expr ${SZX_RESULT_VAL1} / ${SZX_RESULT_VAL2} -1]]>0.020 || [format "%.3f" [expr ${SZX_RESULT_VAL1} / ${SZX_RESULT_VAL2} -1]]<-0.020} {
	 	##~   set grade 2
	        ##~   set alarmcontent "准确性指标R021：神州行用户到达数变动率超出集团考核范围"
	        ##~   WriteAlarm $app_name $optime $grade ${alarmcontent}
	 	  	##~   }

   ##~   #-----------------------------------------------------------------------
   ##~   #--R022:动感地带用户到达数变动率
         ##~   #--本日动感地带用户到达数
        ##~   set handle [aidb_open $conn]

	##~   set sql_buff "SELECT
    		##~   COUNT(*)
    	##~   FROM
    		##~   (SELECT
    		 	##~   A.TIME_ID,
    		 	##~   A.USER_ID,
    		 	##~   A.USERTYPE_ID,
    		 	##~   A.SIM_CODE
    		##~   FROM BASS1.G_A_02004_DAY  A,
             ##~   BASS1.check_temp_02004_now B
    	    ##~   WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='3'
    	    ##~   )T,

    		##~   (SELECT
    	 		##~   A.TIME_ID,
    	 		##~   A.USER_ID,
    	 		##~   A.USERTYPE_ID
    		 ##~   FROM BASS1.G_A_02008_DAY A,
              ##~   BASS1.check_temp_02008_now B
    		##~   WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID
    		##~   ) M
   		##~   WHERE T.USER_ID = M.USER_ID
    		  ##~   AND T.TIME_ID <= INT($timestamp)
    		  ##~   AND M.TIME_ID <= INT($timestamp)
    		  ##~   AND T.USERTYPE_ID <> '3'
    		  ##~   AND M.USERTYPE_ID NOT IN ('2010','2020','2030','9000');"
     
     ##~   puts $sql_buff
	 ##~   if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		##~   WriteTrace $errmsg 1001
		##~   return -1
	##~   }

	##~   if [catch {set MZONE_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		##~   WriteTrace $errmsg 1002
		##~   return -1
	##~   }

	##~   aidb_commit $conn
	##~   set MZONE_RESULT_VAL1 [format "%.3f" [expr ${MZONE_RESULT_VAL1} /1.00]]
	##~   #puts $MZONE_RESULT_VAL1

	##~   #--昨日动感地带用户到达数
	##~   set handle [aidb_open $conn]

	##~   set sql_buff "SELECT
    		##~   COUNT(*)
    	##~   FROM
    		##~   (SELECT
    		 	##~   A.TIME_ID,
    		 	##~   A.USER_ID,
    		 	##~   A.USERTYPE_ID,
    		 	##~   A.SIM_CODE
    		##~   FROM BASS1.G_A_02004_DAY  A,
             ##~   BASS1.check_temp_02004_last B
    	    ##~   WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='3'
    	    ##~   )T,

    		##~   (SELECT
    	 		##~   A.TIME_ID,
    	 		##~   A.USER_ID,
    	 		##~   A.USERTYPE_ID
    		 ##~   FROM BASS1.G_A_02008_DAY A,
              ##~   BASS1.check_temp_02008_last B
    		##~   WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID
    		##~   ) M
   		##~   WHERE T.USER_ID = M.USER_ID
    		  ##~   AND T.TIME_ID <= INT($last_day)
    		  ##~   AND M.TIME_ID <= INT($last_day)
    		  ##~   AND T.USERTYPE_ID <> '3'
    		  ##~   AND M.USERTYPE_ID NOT IN ('2010','2020','2030','9000');"
     
     ##~   puts $sql_buff
	 ##~   if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		##~   WriteTrace $errmsg 1003
		##~   return -1
	##~   }

	##~   if [catch {set MZONE_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		##~   WriteTrace $errmsg 1004
		##~   return -1
	##~   }

	##~   aidb_commit $conn

	##~   set MZONE_RESULT_VAL2 [format "%.3f" [expr ${MZONE_RESULT_VAL2} /1.00]]

	##~   #puts $MZONE_RESULT_VAL2

	##~   ##--将校验值插入校验结果表
	##~   set handle [aidb_open $conn]
	##~   set sql_buff "\
		##~   INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R022',$MZONE_RESULT_VAL1,$MZONE_RESULT_VAL2,1.000 * $MZONE_RESULT_VAL1 / $MZONE_RESULT_VAL2 - 1,0) "
	##~   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		##~   WriteTrace "$errmsg" 2005
		##~   aidb_close $handle
		##~   return -1
	##~   }
	##~   aidb_commit $conn

	 ##~   #--判断
         ##~   #--异常：
         ##~   #--1：动感地带用户到达数变动率<=3%超标

         ##~   #set MZONE_RESULT_VAL2 "0.2"
	 ##~   #set MZONE_RESULT_VAL1 "0.1"

	 ##~   if {[format "%.3f" [expr ${MZONE_RESULT_VAL1} / ${MZONE_RESULT_VAL2} -1]]>0.030 || [format "%.3f" [expr ${MZONE_RESULT_VAL1} / ${MZONE_RESULT_VAL2} -1]]<-0.030} {
	 	##~   set grade 2
	        ##~   set alarmcontent "准确性指标R022：动感地带用户到达数变动率超出集团考核范围"
	        ##~   WriteAlarm $app_name $optime $grade ${alarmcontent}

	 	##~   }


	##~   aidb_close $handle

	return 0
}