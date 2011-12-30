######################################################################################################
#程序名称：	INT_CHECK_R196TO207_DAY.tcl
#校验接口：	02054,02059,02061
#运行粒度: DAY
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2011-05-26 
#问题记录：
#修改历史:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
				
        #自然月
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        
        #程序名
        set app_name "INT_CHECK_R196194_DAY.tcl"



########################################################################################################3

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  where time_id=$timestamp 
 	  and rule_code in ('R196','R198','R200','R203','R204','R205','R207') 
 	  "

	  exec_sql $sqlbuf
#现在所有的手机邮箱数据都没有订购。	  校验结果都为0 ， 通过。
#R196			新增	月	02_集团客户	手机邮箱（ADC）使用集团客户数	手机邮箱（ADC）使用集团客户数≤使用手机邮箱（ADC）的集团个人客户数	0.05		

#	手机邮箱（ADC）使用集团客户数	手机邮箱（ADC）使用集团客户数
#

	 set RESULT_VAL1 0
	 set RESULT_VAL2 0
	 set RESULT_VAL  0

	set sql_buff "
			select count(0) cnt
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02054_day where  MANAGE_MODE = '2'
			and time_id <= $timestamp and ENTERPRISE_BUSI_TYPE = '1220'
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL1 [get_single $sql_buff]
	 puts $RESULT_VAL1

#	 使用手机邮箱（ADC）的集团个人客户数
#note : 手机邮箱个人订购不再02059里，在02061里。
##去掉						and length(trim(user_id)) = 14
##的条件
	set sql_buff "
				select count(0)
				from 
				(
						select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
						from 
						(
						select * from G_A_02061_DAY
						where 
								 ENTERPRISE_BUSI_TYPE = '1220'
						and  MANAGE_MODE = '2'
						and time_id <= $timestamp
						) t
				) t2
				where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL2 [get_single $sql_buff]
	 puts $RESULT_VAL2
	 
	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
	 
 	#检查合法性: 0 - 正常； 大于0 - 非正常
	if { $RESULT_VAL > 0 } {
		set grade 2
	        set alarmcontent " R196 校验不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}
		 
	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R196',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		
	exec_sql $sql_buff

#R198			新增	月	02_集团客户	手机邮箱（MAS）使用集团客户数	手机邮箱（MAS）使用集团客户数≤使用手机邮箱（MAS）的集团个人客户数	0.05		


	 set RESULT_VAL1 0
	 set RESULT_VAL2 0
	 set RESULT_VAL  0

	set sql_buff "
			select count(0) cnt
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02054_day where  MANAGE_MODE = '1'
			and time_id <= $timestamp and ENTERPRISE_BUSI_TYPE = '1220'
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL1 [get_single $sql_buff]
	 puts $RESULT_VAL1

#note : 手机邮箱个人订购不再02059里，在02061里。
#rm:						and length(trim(user_id)) = 14

	set sql_buff "
				select count(0)
				from 
				(
						select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
						from 
						(
						select * from G_A_02061_DAY
						where 
								 ENTERPRISE_BUSI_TYPE = '1220'
						and  MANAGE_MODE = '1'
						and time_id <= $timestamp
						) t
				) t2
				where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL2 [get_single $sql_buff]
	 puts $RESULT_VAL2
	 
	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
	 
 	#检查合法性: 0 - 正常； 大于0 - 非正常
	if { $RESULT_VAL > 0 } {
		set grade 2
	        set alarmcontent " R198 校验不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}
		 
	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R198',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		
	exec_sql $sql_buff		 
	

#R200			新增	月	02_集团客户	手机邮箱（企业版）使用集团客户数	手机邮箱（企业版）使用集团客户数≤使用手机邮箱（企业版）集团个人客户数	0.05		
#集团是通过02054，02061取的

##接口单元名称：企业邮箱业务订购情况
##接口单元编码： 02056
#
#
#	 set RESULT_VAL1 0
#	 set RESULT_VAL2 0
#	 set RESULT_VAL  0
#
#	set sql_buff "
#			select count(0) cnt
#			from 
#			(
#			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
#			from (
#			select *
#			from G_A_02056_DAY where   time_id <= $timestamp and ENTERPRISE_BUSI_TYPE = '1210'
#			) t 
#			) t2 where rn = 1 and STATUS_ID ='1'
#	"
#	 set RESULT_VAL1 [get_single $sql_buff]
#	 puts $RESULT_VAL1
#
##	 使用手机邮箱（ADC）的集团个人客户数
##note : 手机邮箱个人订购不再02059里，在02061里。
##手机邮箱企业版订购在02061暂无上报。
##rm 						and length(trim(user_id)) = 14
#
#	set sql_buff "
#				select count(0)
#				from 
#				(
#						select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
#						from 
#						(
#						select * from G_A_02061_DAY
#						where 
#								 ENTERPRISE_BUSI_TYPE = '1210'
#						and time_id <= $timestamp
#						) t
#				) t2
#				where rn = 1 and STATUS_ID ='1'
#	"
#	 set RESULT_VAL2 [get_single $sql_buff]
#	 puts $RESULT_VAL2
#	 
#	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
#	 
# 	#检查合法性: 0 - 正常； 大于0 - 非正常
#	if { $RESULT_VAL > 0 } {
#		set grade 2
#	        set alarmcontent " R200 校验不通过"
#	        puts ${alarmcontent}	        
#	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
#	}
#		 
#	#--将校验值插入校验结果表
#	set sql_buff "
#		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R200',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
#		"
#		
#	exec_sql $sql_buff		 
#	



#R200			新增	月	02_集团客户	手机邮箱（MAS）使用集团客户数	手机邮箱（MAS）使用集团客户数≤使用手机邮箱（MAS）的集团个人客户数	0.05		


	 set RESULT_VAL1 0
	 set RESULT_VAL2 0
	 set RESULT_VAL  0

	set sql_buff "
			select count(0) cnt
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02054_day where  MANAGE_MODE = '3'
			and time_id <= $timestamp and ENTERPRISE_BUSI_TYPE = '1220'
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL1 [get_single $sql_buff]
	 puts $RESULT_VAL1

#note : 手机邮箱个人订购不再02059里，在02061里。
#rm:						and length(trim(user_id)) = 14

	set sql_buff "
				select count(0)
				from 
				(
						select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
						from 
						(
						select * from G_A_02061_DAY
						where 
								 ENTERPRISE_BUSI_TYPE = '1220'
						and  MANAGE_MODE = '3'
						and time_id <= $timestamp
						) t
				) t2
				where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL2 [get_single $sql_buff]
	 puts $RESULT_VAL2
	 
	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
	 
 	#检查合法性: 0 - 正常； 大于0 - 非正常
	if { $RESULT_VAL > 0 } {
		set grade 2
	        set alarmcontent " R200 校验不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}
		 
	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R200',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		
	exec_sql $sql_buff		 
	
	
	
#R203			新增	月	02_集团客户	集团V网使用集团客户数	集团V网使用集团客户数＜集团V网成员当月到达数	0.05		


	 set RESULT_VAL1 0
	 set RESULT_VAL2 0
	 set RESULT_VAL  0

	set sql_buff "
			select count(0) cnt
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02054_day where  time_id <= $timestamp 
					and ENTERPRISE_BUSI_TYPE in ('1000','1010')
					and MANAGE_MODE = '3'
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL1 [get_single $sql_buff]
	 puts $RESULT_VAL1

#rm						and length(trim(user_id)) = 14

	set sql_buff "
				select count(0)
				from 
				(
						select t.*,row_number()over(partition by enterprise_id,user_id order by time_id desc ) rn 
						from 
						(
						select * from G_A_02059_DAY
						where 
								 ENTERPRISE_BUSI_TYPE  in ('1000','1010')				
						and time_id <= $timestamp
						and MANAGE_MODE = '3'
						) t
				) t2
				where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL2 [get_single $sql_buff]
	 puts $RESULT_VAL2
	 
	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
	 
 	#检查合法性: 0 - 正常； 大于0 - 非正常
	if { $RESULT_VAL > 0 } {
		set grade 2
	        set alarmcontent " R203 校验不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}
		 
	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R203',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		
	exec_sql $sql_buff		 


#R204			新增	月	02_集团客户	无线商务电话使用集团客户数	无线商务电话使用集团客户数≤无线商务电话使用终端数	0.05		


	 set RESULT_VAL1 0
	 set RESULT_VAL2 0
	 set RESULT_VAL  0

	set sql_buff "
			select count(0) cnt
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02054_day where  time_id <= $timestamp 
					and ENTERPRISE_BUSI_TYPE in ('1040')
						and MANAGE_MODE = '3'
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL1 [get_single $sql_buff]
	 puts $RESULT_VAL1
#rm						and length(trim(user_id)) = 14

	set sql_buff "
				select count(0)
				from 
				(
						select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
						from 
						(
						select * from G_A_02059_DAY
						where 
								 ENTERPRISE_BUSI_TYPE  in ('1040')						
						and time_id <= $timestamp
						and MANAGE_MODE = '3'						
						) t
				) t2
				where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL2 [get_single $sql_buff]
	 puts $RESULT_VAL2
	 
	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
	 
 	#检查合法性: 0 - 正常； 大于0 - 非正常
	if { $RESULT_VAL > 0 } {
		set grade 2
	        set alarmcontent " R204 校验不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}
		 
	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R204',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		
	exec_sql $sql_buff		 



#R205			新增	月	02_集团客户	数据专线使用集团客户数	数据专线使用集团客户数≤数据专线端口数	0.05		
#此校验仅用02057即可

	 set RESULT_VAL1 0
	 set RESULT_VAL2 0
	 set RESULT_VAL  0

	set sql_buff "
			select count(0) cnt 
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02054_day where  time_id <= $timestamp 
					and ENTERPRISE_BUSI_TYPE in ('1180')
					and MANAGE_MODE = '3'						
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL1 [get_single $sql_buff]
	 puts $RESULT_VAL1


	set sql_buff "
			select value(sum(bigint(PORT_NUMS)),0) PORT_NUMS   
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02057_day where  time_id <= $timestamp 
					and ENTERPRISE_BUSI_TYPE in ('1180')
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL2 [get_single $sql_buff]
	 puts $RESULT_VAL2
	 
	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
	 
 	#检查合法性: 0 - 正常； 大于0 - 非正常
	if { $RESULT_VAL > 0 } {
		set grade 2
	        set alarmcontent " R205 校验不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}
		 
	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R205',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		
	exec_sql $sql_buff		 

#R207			新增	月	02_集团客户	BLACKBERRY使用集团客户数	BLACKBERRY使用集团客户数≤使用BLACKBERRY集团个人客户数	0.05		
#BlackBerry、手机邮箱、M2M等，不在此02059接口上报。在02060。
	 set RESULT_VAL1 0
	 set RESULT_VAL2 0
	 set RESULT_VAL  0

	set sql_buff "
			select count(0) cnt 
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02054_day where  time_id <= $timestamp 
					and ENTERPRISE_BUSI_TYPE in ('1230')
					and MANAGE_MODE = '3'	
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL1 [get_single $sql_buff]
	 puts $RESULT_VAL1

#rm 						and length(trim(user_id)) = 14

	set sql_buff "
				select count(0)
				from 
				(
						select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
						from 
						(
						select * from G_A_02060_DAY
						where 
								 ENTERPRISE_BUSI_TYPE  in ('1230')						
						and time_id <= $timestamp
						and MANAGE_MODE = '3'	
						) t
				) t2
				where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL2 [get_single $sql_buff]
	 puts $RESULT_VAL2
	 
	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
	 
 	#检查合法性: 0 - 正常； 大于0 - 非正常
	if { $RESULT_VAL > 0 } {
		set grade 2
	        set alarmcontent " R207 校验不通过"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}
		 
	#--将校验值插入校验结果表
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R207',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		
	exec_sql $sql_buff		 



##R208			新增	月	02_集团客户	城管通当月计费客户数	城管通当月计费客户数≤城管通当月使用客户数	0.05		
#此校验不适用于本tcl;
#	 set RESULT_VAL1 0
#	 set RESULT_VAL2 0
#	 set RESULT_VAL  0
#
#	set sql_buff "
#			select count(0) cnt 
#			from 
#			(
#			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
#			from (
#			select *
#			from g_a_02054_day where  time_id <= $timestamp 
#					and ENTERPRISE_BUSI_TYPE in ('1230')
#			) t 
#			) t2 where rn = 1 and STATUS_ID ='1'
#	"
#	 set RESULT_VAL1 [get_single $sql_buff]
#	 puts $RESULT_VAL1
#
#
#	set sql_buff "
#				select count(0)
#				from 
#				(
#						select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
#						from 
#						(
#						select * from G_A_02060_DAY
#						where 
#								 ENTERPRISE_BUSI_TYPE  in ('1230')						
#						and time_id <= $timestamp
#						and length(trim(user_id)) = 14
#						) t
#				) t2
#				where rn = 1 and STATUS_ID ='1'
#	"
#	 set RESULT_VAL2 [get_single $sql_buff]
#	 puts $RESULT_VAL2
#	 
#	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
#	 
# 	#检查合法性: 0 - 正常； 大于0 - 非正常
#	if { $RESULT_VAL > 0 } {
#		set grade 2
#	        set alarmcontent " R207 校验不通过"
#	        puts ${alarmcontent}	        
#	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
#	}
#		 
#	#--将校验值插入校验结果表
#	set sql_buff "
#		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R207',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
#		"
#		
#	exec_sql $sql_buff		 
#		
	return 0
}

