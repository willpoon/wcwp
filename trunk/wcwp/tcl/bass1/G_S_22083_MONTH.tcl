######################################################################################################
#接口单元名称：业务扣费主动提醒月汇总 
#接口单元编码：22083
#接口单元说明：采集业务扣费主动提醒服务月报数据，包括当月“客户退订的业务名称、业务代码、业务提供商、业务退订量、业务在网客户数/当月业务点播量”等。
#程序名称: G_S_22083_MONTH.tcl
#功能描述: 生成22083的数据
#运行粒度: 月
#源    表：
#1. bass2.DW_HIS_DSMP_SMS_SEND_MESSAGE_$op_month 
#2. bass2.Dim_pm_sp_operator_code
#3. bass2.Dim_pm_serv_type_vs_expr 
#4. bass2.DW_PRODUCT_SP_INFO_$op_month 
#5. bass2.DW_PRODUCT_INS_OFF_INS_PROD_$op_month
#6. bass2.dw_product_$op_month 
#7. bass2.DIM_PROD_UP_PRODUCT_ITEM
#8.  bass2.ODS_DIM_UP_SP_SERVICE_$timestamp 
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzhiwei
#编写时间：2011-04-26
#问题记录：1.
#修改历史: 1. 1.7.2 规范
#修改历史: 2011-06-15 19:55:26 修改取消订购取法
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      #set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01] 
      set last_month [GetLastMonth [string range $op_month 0 5]]
      #set curr_month_first_day [string range $timestamp 0 5]01
      #puts $curr_month_first_day
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      


  #删除本期数据
	set sql_buff "delete from bass1.G_S_22083_MONTH where time_id=$op_month"
	exec_sql $sql_buff
	
 

	#1.求客户在收到扣费主动提醒短信后取消订购关系的业务:成功退订量|投诉量
	#一次性建表
	##drop table BASS1.G_S_22083_MONTH_1;
	##CREATE TABLE BASS1.G_S_22083_MONTH_1
	## (
	##	sp_id VARCHAR(20)
	##	,BUSI_CODE varchar(20)
	##	,cancel_cnt integer
	##	,complaint_cnt integer
	## )
	##  DATA CAPTURE NONE
	## IN TBS_APP_BASS1
	## INDEX IN TBS_INDEX
	##  PARTITIONING KEY
	##   (sp_id,BUSI_CODE
	##   ) USING HASHING;
	##
	##ALTER TABLE BASS1.G_S_22083_MONTH_1
	##  LOCKSIZE ROW
	##  APPEND OFF
	##  NOT VOLATILE;
#2011-06-15 16:56:05
#取主动提醒后退订


#业务名称:指客户在收到扣费主动提醒短信后取消订购关系的业务名称全称，区分包月业务和点播业务。
#退订量:
#客户回复否或发送代码到10086901后成功退订业务的数量，
#包月业务指取消业务订购关系，
#点播业务指不计费量。对于重复退订同一业务的按一次计算。。
#2011-06-15 17:00:27
#去掉 having cancel_cnt > 0 的条件
#				having sum(case when trim(confirm_code) <>'是' and  trim(confirm_code) = trim(return_message)  
#					then 1 else 0 end ) > 0
	set sql_buff "ALTER TABLE BASS1.G_S_22083_MONTH_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

#trim(confirm_code) <>'是' 剔除（请在24小时内回复“是”确认订购，回复其他内容和不回复则不订购。中国移动）


	set sql_buff "
			INSERT INTO  BASS1.G_S_22083_MONTH_1
			( 
				 SP_ID
				,BUSI_CODE
				,CANCEL_CNT
				,COMPLAINT_CNT
			)
			 select 
				ext1 sp_id
				,ext4 sp_busi_code
				,sum(case when trim(confirm_code) <>'是' and  trim(confirm_code) = trim(return_message)  
					then 1 else 0 end ) cancel_cnt
				,0 complaint_cnt
				from bass2.DW_HIS_DSMP_SMS_SEND_MESSAGE_$op_month  a 
				where  RSP_SEQ LIKE '10086901%'
				and char(date(CREATE_DATE)) like  '$optime_month%' 
				and ext4 is not null 
				group by 
				ext1
				,ext4
			 with ur 
	"
	exec_sql $sql_buff

  aidb_runstats bass1.G_S_22083_MONTH_1 3


	#1. 划分计费类型
	#一次性建表
##	drop table BASS1.G_S_22083_MONTH_2;
##	CREATE TABLE BASS1.G_S_22083_MONTH_2
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
##	ALTER TABLE BASS1.G_S_22083_MONTH_2
##	  LOCKSIZE ROW
##	  APPEND OFF
##	  NOT VOLATILE;

###取计费类型
	set sql_buff "ALTER TABLE BASS1.G_S_22083_MONTH_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

	set sql_buff "
			INSERT INTO  BASS1.G_S_22083_MONTH_2
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
			 from   bass1.G_S_22083_MONTH_1 c 
			 left join   bass2.Dim_pm_sp_operator_code a 
				on  char(a.SP_CODE) = c.SP_ID and a.OPERATOR_CODE = c.BUSI_CODE
			 left join bass2.Dim_pm_serv_type_vs_expr b 
				on  a.sp_type = b.serv_type 
		    ) a where a.rn = 1 
			 with ur 
	"
	exec_sql $sql_buff

  aidb_runstats bass1.G_S_22083_MONTH_2 3
  
  #s3 sp 企业 维表


	#一次性建表
	##drop table BASS1.G_S_22083_MONTH_3;
	##CREATE TABLE BASS1.G_S_22083_MONTH_3
	## (
	##        SP_CODE                 VARCHAR(20)         
	##        ,SP_NAME                 VARCHAR(100)    
	## )
	##  DATA CAPTURE NONE
	## IN TBS_APP_BASS1
	## INDEX IN TBS_INDEX
	##  PARTITIONING KEY
	##   (SP_CODE
	##   ) USING HASHING;
	##
	##ALTER TABLE BASS1.G_S_22083_MONTH_3
	##  LOCKSIZE ROW
	##  APPEND OFF
	##  NOT VOLATILE;
	####取SP业务维表
	set sql_buff "ALTER TABLE BASS1.G_S_22083_MONTH_3 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff
	#
	set sql_buff "
	insert into  BASS1.G_S_22083_MONTH_3
		select t.sp_code ,t.sp_name
		from
		(      
			select a.sp_code ,a.sp_name
			    ,row_number()over(partition by a.sp_code 
				order by EXPIRE_DATE desc , VALID_DATE desc  ) rn 
			from   bass2.DW_PRODUCT_SP_INFO_$op_month a
		) t where t.rn = 1 
	with ur 
	"
	exec_sql $sql_buff
	aidb_runstats bass1.G_S_22083_MONTH_3 3

	#1.求新增订购用户数（月）
	#一次性建表
	##drop table BASS1.G_S_22083_MONTH_4;
	##CREATE TABLE BASS1.G_S_22083_MONTH_4
	## (sp_id VARCHAR(20)
	##	,BUSI_CODE varchar(20)
	##	,ORDER_CNT integer
	## )
	##  DATA CAPTURE NONE
	## IN TBS_APP_BASS1
	## INDEX IN TBS_INDEX
	##  PARTITIONING KEY
	##   (BUSI_CODE
	##   ) USING HASHING;
	##
	##ALTER TABLE BASS1.G_S_22083_MONTH_4
	##  LOCKSIZE ROW
	##  APPEND OFF
	##  NOT VOLATILE;
####取包月订购
	set sql_buff "ALTER TABLE BASS1.G_S_22083_MONTH_4 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

	set sql_buff "
			INSERT INTO  BASS1.G_S_22083_MONTH_4
			( sp_id
				,BUSI_CODE
				,ORDER_CNT
			)
			select 
			   b.SUPPLIER_ID sp_id
			  ,b.EXTEND_ID2 BUSI_CODE
				,max(a.order_cnt) order_cnt
				  from (
						select OFFER_ID,count(0)  order_cnt
						from bass2.DW_PRODUCT_INS_OFF_INS_PROD_$op_month p ,
						(select distinct user_id from bass2.dw_product_$op_month 
							where userstatus_id in (1,2,3,6,8)
  						and usertype_id in (1,2,9) 
  					) u
						where substr(replace(char(date(p.VALID_DATE)),'-',''),1,6) <= '$op_month'
						and expire_date >  '$ThisMonthFirstDay'
						and p.PRODUCT_INSTANCE_ID = u.user_id
						group by OFFER_ID
					) a ,
					(
						SELECT PRODUCT_ITEM_ID,SUPPLIER_ID,EXTEND_ID2 ,name FROM bass2.DIM_PROD_UP_PRODUCT_ITEM
								WHERE ITEM_TYPE='OFFER_PLAN'
						  AND DEL_FLAG='1'
						  AND SUPPLIER_ID IS NOT NULL
					) b 
				where a.OFFER_ID = b.PRODUCT_ITEM_ID
				group by 
				 b.SUPPLIER_ID 
			  ,b.EXTEND_ID2 
			 with ur 
	"
	exec_sql $sql_buff

  aidb_runstats bass1.G_S_22083_MONTH_4 3	  


#2011-06-15 17:01:40
#取包月业务退订
	set sql_buff "ALTER TABLE BASS1.G_S_22083_MONTH_5 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff
	set sql_buff "
	insert into G_S_22083_MONTH_5
	(
         SRC        
        ,SP_ID
        ,BUSI_CODE
        ,CANCEL_CNT	
	)
	select  '1' SRC
					,char(SP_ID) SP_ID
					,SP_CODE
					,count(0) CANCEL_BUSI_CNT
	       from   
	       	BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_$op_month a
	       	where replace(char(date(a.create_date)),'-','') like '$op_month%' 
	       	and (PHONE_ID,char(SP_ID),SP_CODE) in   
	       							( select BILL_ID,EXT1,EXT4
									       	from bass2.DW_HIS_DSMP_SMS_SEND_MESSAGE_DM_$op_month  b 
												where  RSP_SEQ LIKE '10086901%' 
												and  replace(char(date(b.create_date)),'-','')  like '$op_month%'
												and   replace(char(date(a.create_date)),'-','') >= replace(char(date(b.create_date)),'-','')
												and ext4 is not null 
									     )
	        group by char(SP_ID),SP_CODE
"
exec_sql $sql_buff


#点播退订&订购
#  
	set sql_buff "
		insert into G_S_22083_MONTH_5
			select 
				 '2' SRC
				,a.SP_CODE
				,a.OPER_CODE
				,sum(case when BILL_COUNTS = 0 then COUNTS else 0 end ) CANCEL_CNT
				,sum(BILL_COUNTS ) order_cnt
	from   bass2.DW_NEWBUSI_KJ_$op_month  a 
	where exists (select 1 from  G_S_22083_MONTH_1 b  where  a.OPER_CODE = b.BUSI_CODE
				 and a.SP_CODE = char(b.SP_ID) 
	  )
	  and not exists (select 1 from G_S_22083_MONTH_5 c where a.OPER_CODE = c.BUSI_CODE
	  								 and a.sp_code = c.SP_ID)
	  group by a.SP_CODE,a.OPER_CODE
"
exec_sql $sql_buff

  aidb_runstats bass1.G_S_22083_MONTH_5 3	  
  #2011-04-30 16:51:31ODS_DIM_UP_SP_SERVICE dim 未开发，暂用ods代替。这个维表不全，要用DIM_PM_SP_OPERATOR_CODE这个,补抽operator_name
  #2011-05-01 16:51:42 已完善 DIM_PM_SP_OPERATOR_CODE
  
##取消量：对于包月，从统一退订出,对于点播，从KJ统的出。
##订购：  对于包月，订单出；对于点播，从KJ统的出。
##对于订购小于取消的，把取消量加入订购，作为订购。
	set sql_buff "
	insert into bass1.G_S_22083_MONTH
		  (
			 TIME_ID
			,OP_TIME
			,BUSI_CODE
			,BUSI_NAME
			,BUSI_PROVIDER_NAME
			,BUSI_BILLING_TYPE
			,CANCEL_CNT
			,COMPLAINT_CNT
			,ORDER_CNT
		  )
		 select      $op_month TIME_ID
			     ,'$op_month' op_time
			     ,a.BUSI_CODE
			     ,substr(c.OPERATOR_NAME,1,60) BUSI_NAME
			     ,max(substr(d.SP_NAME,1,60)) BUSI_PROVIDER_NAME
			     ,min(case when b.bill_flag = 3 then '10' 
			       else '20' end)  BUSI_BILLING_TYPE		
			     ,char(sum(value(case when b.bill_flag = 3 and f.src = '2' then 0 else  f.cancel_cnt end,a.CANCEL_CNT))) 	CANCEL_CNT 
			     ,char(sum(a.COMPLAINT_CNT)) COMPLAINT_CNT	 
			     ,char(sum(value(value(f.ORDER_CNT,e.order_cnt),0))) ORDER_CNT	     
			from   bass1.G_S_22083_MONTH_1 a 
			       join  bass1.G_S_22083_MONTH_2 b  on a.SP_ID = b.SP_ID and a.BUSI_CODE = b.BUSI_CODE
			      join table(
					select 
					 OPERATOR_CODE
					,char(SP_CODE) SP_CODE
					,BILL_FLAG
					,SP_TYPE
					,OPERATOR_NAME
					from 
				       ( select a.*,row_number()over(partition by OPERATOR_CODE,SP_CODE order by OPERATOR_NAME desc )  rn 
					     from bass2.DIM_PM_SP_OPERATOR_CODE a 
				       ) c where rn = 1 
				)         c on  a.SP_ID = char(c.SP_CODE) and a.BUSI_CODE = c.OPERATOR_CODE
JOIN BASS1.G_S_22083_MONTH_3 D ON a.sp_id = d.SP_CODE
				left join BASS1.G_S_22083_MONTH_4 e on a.sp_id = e.SP_ID and a.BUSI_CODE = e.BUSI_CODE
				left join BASS1.G_S_22083_MONTH_5 f on a.sp_id = f.SP_ID and a.BUSI_CODE = f.BUSI_CODE
			group by 
	             a.BUSI_CODE
	             ,substr(c.OPERATOR_NAME,1,60)
			having sum(value(f.cancel_cnt,a.CANCEL_CNT)) > 0			       
  "
 
exec_sql $sql_buff

set sql_buff "
		update bass1.G_S_22083_MONTH
		set ORDER_CNT = char(bigint(ORDER_CNT) + bigint(cancel_cnt))
		where  bigint(CANCEL_CNT)>bigint(ORDER_CNT)
		and TIME_ID = $op_month
" 
exec_sql $sql_buff


  aidb_runstats bass1.G_S_22083_MONTH 3	  

	
  #1.检查chkpkunique
	set tabname "G_S_22083_MONTH"
	set pk 			"OP_TIME||BUSI_CODE||BUSI_NAME"
	chkpkunique ${tabname} ${pk} ${op_month}


#2011-06-27 17:09:46 西藏出现 (22083)业务扣费主动提醒 “订购用户数<=0 ”
set sql_buff "
select count(0) from   G_S_22083_MONTH
where bigint(ORDER_CNT) <= 0
and time_id = $op_month
"

	chkzero 	$sql_buff 1

#西藏出现  “点播类:订购用户数<业务退订量”，
set sql_buff "
select count(0) from   G_S_22083_MONTH
where bigint(ORDER_CNT) < bigint(CANCEL_CNT)
and time_id = $op_month
"

	chkzero 	$sql_buff 2
	

	return 0
}
