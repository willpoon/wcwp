
######################################################################################################		
#接口名称: 用户有效通信情况信息                                                               
#接口编码：21022                                                                                          
#接口说明：上报月末最后一天仍在网用户当月每日通信情况。
#程序名称: G_S_21022_MONTH.tcl                                                                            
#功能描述: 生成21022的数据
#运行粒度: MONTH
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20120605
#问题记录：
#修改历史: 1. panzw 20120605	中国移动一级经营分析系统省级数据接口规范 (V1.8.1) 
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      #set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01] 
      set last_month [GetLastMonth [string range $op_month 0 5]]
      #set curr_month_first_day [string range $timestamp 0 5]01
      #puts $curr_month_first_day
      #yyyy--mm-dd
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      

        global app_name
        set app_name "G_S_21022_MONTH.tcl"
          
    #删除本期数据
	set sql_buf "delete from bass1.G_S_21022_MONTH where time_id=$op_month"
	exec_sql $sql_buf
#
	set sql_buf "ALTER TABLE BASS1.G_S_21022_MONTH_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf

	set sql_buf "ALTER TABLE BASS1.G_S_21022_MONTH_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf
	
	set sql_buf "ALTER TABLE BASS1.G_S_21022_MONTH_1TOP ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf

	set sql_buf "ALTER TABLE BASS1.G_S_21022_MONTH_2TOP ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf
		
	set sql_buf "ALTER TABLE BASS1.G_S_21022_MONTH_TARGETUSER ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf
	
	set sql_buf "ALTER TABLE BASS1.G_S_21022_MONTH_ALLUSER ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf
	
	
	select 
	$timestamp CALL_DT
	,a.user_id USER_ID
	,sum(BILL_CALL_COUNTS) ACT_CALLCNT
	,sum(BILL_DURATION) ACT_CALLDUR
	from  bass2.DW_CALL_$timestamp a
	where CALLTYPE_ID = 0
	group by a.user_id 
	
	
	select 
	$timestamp CALL_DT
	,a.user_id USER_ID
	,sum(BILL_COUNTS) SMS_SENDCNT
	from  bass2.DW_NEWBUSI_SMS_$timestamp a
	where CALLTYPE_ID = 4
	group by a.user_id 
	
	
	
	set sql_buf "
		insert into G_S_21022_MONTH_1
		select
		USER_ID
		,LAC_ID
		,CELL_ID
		,char(sum(CALL_COUNTS))
		,char(sum(CALL_DURATION_M))
		 from  bass2.dw_call_cell_$op_month
		 where calltype_id = 0
		 AND LAC_ID IS NOT NULL AND CELL_ID IS NOT NULL
		group by 
		USER_ID
		,LAC_ID
		,CELL_ID
		having sum(CALL_COUNTS) > 0
		with ur
	"
	exec_sql $sql_buf





        
		
	set sql_buf "
insert into G_S_21022_MONTH_1TOP
select 
         USER_ID
        ,LAC_ID
        ,CELL_ID
        ,ACT_CALLCNT
        ,ACT_CALLDUR
        ,'1' TYPE
        ,RN
       from  
       (select i.*,row_number()over(partition by user_id order by ACT_CALLCNT desc  ) rn 
        from G_S_21022_MONTH_1 i
        ) o where rn <= 10
		with ur
	"
	exec_sql $sql_buf
		

	set sql_buf "
		insert into G_S_21022_MONTH_2
		(
			 USER_ID
			,LAC_ID
			,CELL_ID
			,GPRS_UPFLOW
			,GPRS_DOWNFLOW		
		)
			select
			USER_ID
			,LAC_ID
			,CELL_ID
			,char(bigint(sum(UPFLOW1+UPFLOW2)/1024/1024))
			,char(bigint(sum(DOWNFLOW1+DOWNFLOW2)/1024/1024))
			 from  bass2.dw_newbusi_gprs_$op_month
			  WHERE LAC_ID IS NOT NULL AND CELL_ID IS NOT NULL
			group by 
			USER_ID
			,LAC_ID
			,CELL_ID
			having sum(UPFLOW1+UPFLOW2) + sum(DOWNFLOW1+DOWNFLOW2) > 0
		with ur
	"
	exec_sql $sql_buf



		
	set sql_buf "
	insert into G_S_21022_MONTH_2TOP
	(
			 USER_ID
			,LAC_ID
			,CELL_ID
			,GPRS_UPFLOW
			,GPRS_DOWNFLOW
			,TYPE
			,RN
	)
select 
         USER_ID
        ,LAC_ID
        ,CELL_ID
        ,GPRS_UPFLOW
        ,GPRS_DOWNFLOW
        ,'2' TYPE
        ,RN
       from  
       (select i.*,row_number()over(partition by user_id order by BIGINT(GPRS_UPFLOW)+BIGINT(GPRS_DOWNFLOW) desc  ) rn 
        from G_S_21022_MONTH_2 i
        ) o where rn <= 10
		with ur
	"
	exec_sql $sql_buf
		
		


    set sql_buff "
	    insert into G_S_21022_MONTH_TARGETUSER
					  (
						 TIME_ID
						,USER_ID
						,LAC_ID
						,CELL_ID
						,ACT_CALLCNT
						,ACT_CALLDUR
						,GPRS_UPFLOW
						,GPRS_DOWNFLOW
						,FINAL_TYPE
						)
select   $op_month time_id
		,USER_ID
        ,LAC_ID
        ,CELL_ID
        ,char(max(case when type = '1' then    bigint(val1) else 0 end ))
        ,char(max(case when type = '1' then    bigint(val2) else 0 end ))
        ,char(max(case when type = '2' then    bigint(val1) else 0 end ))
        ,char(max(case when type = '2' then    bigint(val2) else 0 end ))
		,char(min(bigint(TYPE)))
from (        
        select 
         USER_ID
        ,LAC_ID
        ,CELL_ID
        ,ACT_CALLCNT val1
        ,ACT_CALLDUR val2
        ,TYPE
        ,RN
        from G_S_21022_MONTH_1TOP
        union all
        select 
         USER_ID
        ,LAC_ID
        ,CELL_ID
        ,GPRS_UPFLOW val1 
        ,GPRS_DOWNFLOW val2
        ,TYPE
        ,RN
        from G_S_21022_MONTH_2TOP
       ) o 
group by        
         USER_ID
        ,LAC_ID
        ,CELL_ID
	with ur
	    "

    exec_sql $sql_buff 





    set sql_buff "
	    insert into G_S_21022_MONTH_ALLUSER
					  (
						 TIME_ID
						,USER_ID
						,LAC_ID
						,CELL_ID
						,ACT_CALLCNT
						,ACT_CALLDUR
						,GPRS_UPFLOW
						,GPRS_DOWNFLOW
						)
select   $op_month time_id
		,USER_ID
        ,LAC_ID
        ,CELL_ID
        ,char(max(case when type = '1' then    bigint(val1) else 0 end ))
        ,char(max(case when type = '1' then    bigint(val2) else 0 end ))
        ,char(max(case when type = '2' then    bigint(val1) else 0 end ))
        ,char(max(case when type = '2' then    bigint(val2) else 0 end ))
from (        
        select 
         USER_ID
        ,LAC_ID
        ,CELL_ID
        ,ACT_CALLCNT val1
        ,ACT_CALLDUR val2
		,'1' type
        from G_S_21022_MONTH_1
        union all
        select 
         USER_ID
        ,LAC_ID
        ,CELL_ID
        ,GPRS_UPFLOW val1 
        ,GPRS_DOWNFLOW val2
		,'2' type
        from G_S_21022_MONTH_2
       ) o 
group by        
         USER_ID
        ,LAC_ID
        ,CELL_ID
	with ur
	    "

    exec_sql $sql_buff 



    set sql_buff "
	    insert into G_S_21022_MONTH
					  (
						 TIME_ID
						,USER_ID
						,LAC_ID
						,CELL_ID
						,ACT_CALLCNT
						,ACT_CALLDUR
						,GPRS_UPFLOW
						,GPRS_DOWNFLOW
						)
select   $op_month time_id
        ,a.USER_ID
        ,a.LAC_ID
        ,a.CELL_ID
        ,case when a.FINAL_TYPE = '2' then c.ACT_CALLCNT else a.ACT_CALLCNT end ACT_CALLCNT
        ,case when a.FINAL_TYPE = '2' then c.ACT_CALLDUR else a.ACT_CALLDUR end ACT_CALLDUR
        ,case when a.FINAL_TYPE = '1' then c.GPRS_UPFLOW else c.GPRS_UPFLOW end GPRS_UPFLOW
        ,case when a.FINAL_TYPE = '1' then c.GPRS_DOWNFLOW else c.GPRS_DOWNFLOW end GPRS_DOWNFLOW
from G_S_21022_MONTH_TARGETUSER a
join    (
		 select user_id from bass2.dw_product_$op_month  where usertype_id in (1,2,9) and userstatus_id in (1,2,3,6,8)  and test_mark<>1
		) b ON a.USER_ID = b.user_id 
join G_S_21022_MONTH_ALLUSER c on  a.user_id = c.user_id and a.LAC_ID = c.LAC_ID and a.CELL_ID = c.CELL_ID
	with ur
	    "

##~   --left join G_S_21022_MONTH_1 c on a.user_id = c.user_id and a.LAC_ID = c.LAC_ID and a.CELL_ID = c.CELL_ID
##~   --left join G_S_21022_MONTH_2 d on a.user_id = d.user_id and a.LAC_ID = d.LAC_ID and a.CELL_ID = d.CELL_ID


    exec_sql $sql_buff 




  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_S_21022_MONTH"
  set pk   "USER_ID||LAC_ID||CELL_ID"
        chkpkunique ${tabname} ${pk} ${op_month}
        #
        
  aidb_runstats bass1.G_S_21022_MONTH 3


	return 0
}
