######################################################################################################
#接口单元名称：业务扣费主动提醒日汇总 
#接口单元编码：22082
#接口单元说明：采集业务扣费主动提醒服务日报数据，包括“扣费提醒短信发送量、短信回复量、业务退订量、客户投诉量、外呼量”等;可计算出“短信回复率、业务退订率”等。
#程序名称: G_S_22082_DAY.tcl
#功能描述: 生成22082的数据
#运行粒度: 日
#源    表：
#1. bass2.DW_HIS_DSMP_SMS_SEND_MESSAGE_DM_$curr_month 
#2. bass2.Dim_pm_sp_operator_code 
#3. bass2.Dim_pm_serv_type_vs_expr
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzhiwei
#编写时间：2011-04-26
#问题记录：1.
#修改历史: 1. 1.7.2 规范
#					2011-06-15 19:55:12修改取消量的取法
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
      set curr_month [string range $timestamp 0 5]
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      

      
  #删除本期数据
	set sql_buff "delete from bass1.G_S_22082_DAY where time_id=$timestamp"
	exec_sql $sql_buff


 

	#1.求扣费提醒发送量等
	#一次性建表
	##drop table BASS1.G_S_22082_DAY_1;
	##CREATE TABLE BASS1.G_S_22082_DAY_1
	## (
	## 	OP_TIME            	CHAR(8)
	##	,sp_id VARCHAR(20)
	##	,BUSI_CODE varchar(20)
	##	,alert_sms_cnt integer
	##	,reply_sms_cnt integer
	##	,cancel_cnt integer
	##	,hotline_out_cnt integer
	##	,complaint_cnt integer
	## )
	##  DATA CAPTURE NONE
	## IN TBS_APP_BASS1
	## INDEX IN TBS_INDEX
	##  PARTITIONING KEY
	##   (BUSI_CODE
	##   ) USING HASHING;
	##
	##ALTER TABLE BASS1.G_S_22082_DAY_1
	##  LOCKSIZE ROW
	##  APPEND OFF
	##  NOT VOLATILE;
#扣费提醒发送量
#短信回复量	客户对已订购业务的扣费有疑问，直接回复否或发送代码到10086901的短信数量。不含重复回复量。
#业务成功退订量  <包月从 TONGYI_TUIDING 取退订> <点播统计不计费量>
#热线外呼量 0
#投诉量 0
	set sql_buff "ALTER TABLE BASS1.G_S_22082_DAY_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

	set sql_buff "
			INSERT INTO  BASS1.G_S_22082_DAY_1
			( 
				OP_TIME
				,SP_ID
				,BUSI_CODE
				,ALERT_SMS_CNT
				,REPLY_SMS_CNT
				,CANCEL_CNT
				,HOTLINE_OUT_CNT
				,COMPLAINT_CNT
			)
			 select 
				replace(char(date(a.create_date)),'-','')
				,ext1 sp_id
				,ext4 sp_busi_code
				,count(0) alert_sms_cnt
				,count(distinct case when trim(confirm_code) <>'是' 
					and return_message is not null 
				      then bill_id||RSP_SEQ||ext1||ext4 end ) reply_sms_cnt
				,sum(case when trim(confirm_code) <>'是' and  trim(confirm_code) = trim(return_message)  
					then 1 else 0 end ) cancel_cnt
				,0 hotline_out_cnt
				,0 complaint_cnt
				from bass2.DW_HIS_DSMP_SMS_SEND_MESSAGE_DM_$curr_month  a 
				where  RSP_SEQ LIKE '10086901%' 
				and  a.OP_TIME  = '$op_time'
				and  replace(char(date(a.create_date)),'-','') = '$timestamp' 
				and ext4 is not null 
				group by 
				 replace(char(date(a.create_date)),'-','')
				,ext1
				,ext4
			 with ur 
	"
	exec_sql $sql_buff

  aidb_runstats bass1.G_S_22082_DAY_1 3

	#1. 划分计费类型
	#一次性建表
##	drop table BASS1.G_S_22082_DAY_2;
##	CREATE TABLE BASS1.G_S_22082_DAY_2
##	 (
##		 sp_id 			     VARCHAR(20)
##		,BUSI_CODE 	     varchar(20)
##		,DELAY_TIME      INTEGER
##		,BILL_FLAG       SMALLINT 
##	 )
##	  DATA CAPTURE NONE
##	 IN TBS_APP_BASS1
##	 INDEX IN TBS_INDEX
##	  PARTITIONING KEY
##	   (sp_id,BUSI_CODE
##	   ) USING HASHING;
##	
##	ALTER TABLE BASS1.G_S_22082_DAY_2
##	  LOCKSIZE ROW
##	  APPEND OFF
##	  NOT VOLATILE;


	set sql_buff "ALTER TABLE BASS1.G_S_22082_DAY_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

	set sql_buff "
			INSERT INTO  BASS1.G_S_22082_DAY_2
			( 
				 SP_ID
				,BUSI_CODE
				,DELAY_TIME
				,BILL_FLAG
			)
		 select  SP_CODE
			 ,BUSI_CODE
			 ,delay_time
			 ,bill_flag
		from (
			 select distinct char(c.SP_ID) SP_CODE,c.BUSI_CODE,delay_time,a.bill_flag
			 ,row_number()over(partition by c.SP_ID,c.BUSI_CODE 
			 	order by  value(b.SP_CODE,0) asc) rn 
			 from   bass1.G_S_22082_DAY_1 c 
			 left join   bass2.Dim_pm_sp_operator_code a on  char(a.SP_CODE) = c.SP_ID and a.OPERATOR_CODE = c.BUSI_CODE
			 left join bass2.Dim_pm_serv_type_vs_expr b on  a.sp_type = b.serv_type 
		    ) a where a.rn = 1 
			 with ur 
	"
	exec_sql $sql_buff

  aidb_runstats bass1.G_S_22082_DAY_2 3
  #
#2011-06-15 18:26:56
#取消量：对于包月，从tongyi_tuiding  出；对于点播，从kj出
#SP_CODE                        SYSIBM    VARCHAR                  20     0 Yes   
#SER_CODE                       SYSIBM    VARCHAR                  32     0 Yes   
#OPER_CODE                      SYSIBM    VARCHAR                  20     0 Yes   

	set sql_buff "ALTER TABLE BASS1.G_S_22082_DAY_3 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff



	set sql_buff "
		insert into G_S_22082_DAY_3
			select
			     '1' SRC   
					,char(SP_ID) SP_ID
					,SP_CODE
					,count(0) CANCEL_BUSI_CNT
	       from   
	       	BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_$curr_month a
	       	where replace(char(date(a.create_date)),'-','') = '$timestamp'
	       	and  (PHONE_ID,char(SP_ID),SP_CODE) in   
	       							( select BILL_ID,EXT1,EXT4
									       	from bass2.DW_HIS_DSMP_SMS_SEND_MESSAGE_DM_$curr_month  b 
												where  RSP_SEQ LIKE '10086901%' 
												and  b.OP_TIME  = '$op_time'
												and  replace(char(date(b.create_date)),'-','') = '$timestamp' 
												and ext4 is not null 
									     )
	        group by char(SP_ID),SP_CODE
		"
		
exec_sql $sql_buff


	set sql_buff "
		insert into G_S_22082_DAY_3
			select 
				 '2' SRC   
				,a.SP_CODE
				,a.OPER_CODE
				,sum(case when BILL_COUNTS = 0 then COUNTS else 0 end ) CANCEL_CNT
	from   bass2.dw_newbusi_kj_$timestamp  a 
	where exists (select 1 from  G_S_22082_DAY_1 b  where  a.OPER_CODE = b.BUSI_CODE
				 and a.SP_CODE = char(b.SP_ID) 
	  )
	  and not exists (select 1 from G_S_22082_DAY_3 c where a.OPER_CODE = c.OPER_CODE
	  								 and a.sp_code = c.sp_code)
	  group by a.SP_CODE,a.OPER_CODE
"
exec_sql $sql_buff

  aidb_runstats bass1.G_S_22082_DAY_3 3


	set sql_buff "
	insert into bass1.G_S_22082_DAY
		  (
			 TIME_ID
			,OP_TIME
			,BUSI_BILLING_TYPE
			,ALERT_SMS_CNT
			,REPLY_SMS_CNT
			,CANCEL_CNT
			,HOTLINE_OUT_CNT
			,COMPLAINT_CNT
		  )
 select      $timestamp TIME_ID
             ,a.OP_TIME op_time
             ,case when b.bill_flag = 3 and DELAY_TIME = 72 then '11' 
	           when b.bill_flag = 3 and DELAY_TIME = 0 then '12'
		   else '20' end  BUSI_BILLING_TYPE		
             ,char(sum(a.ALERT_SMS_CNT))           
             ,char(sum(a.REPLY_SMS_CNT))      
             ,char(sum(value(case when c.src = '2' and b.bill_flag = 3 then 0 else c.CANCEL_CNT end,0))) CANCEL_CNT
             ,char(sum(a.HOTLINE_OUT_CNT)) 	 
             ,char(sum(a.COMPLAINT_CNT)) 	 	     
        from   bass1.G_S_22082_DAY_1 a 
               join  bass1.G_S_22082_DAY_2 b  on a.SP_ID = b.SP_ID and a.BUSI_CODE = b.BUSI_CODE	
               left join  bass1.G_S_22082_DAY_3 c  on a.SP_ID = c.SP_CODE and a.BUSI_CODE = c.OPER_CODE	
	group by 
		a.OP_TIME
		,case when b.bill_flag = 3 and DELAY_TIME = 72 then '11' 
	           when b.bill_flag = 3 and DELAY_TIME = 0 then '12'
		   else '20' end 
  "
 
exec_sql $sql_buff

  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_S_22082_DAY"
	set pk 			"OP_TIME||BUSI_BILLING_TYPE"
	chkpkunique ${tabname} ${pk} ${timestamp}


#2011-06-27 17:09:46 业务成功退订量>扣费提醒发送量
set sql_buff "
			select count(0) from   g_s_22082_day
			where bigint(ALERT_SMS_CNT) < bigint(CANCEL_CNT)
			and time_id=$timestamp
"

	chkzero 	$sql_buff 3

	
  aidb_runstats bass1.$tabname 3
  
	return 0
}
