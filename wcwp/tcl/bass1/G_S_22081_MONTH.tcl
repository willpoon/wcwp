######################################################################################################
#接口单元名称：统一查询和退订月汇总 
#接口单元编码：22081
#接口单元说明：采集增值业务0000统一查询和退订服务月报数据，包括当月“客户退订的业务名称、业务代码、业务提供商、业务退订量、业务在网客户数”等。
#程序名称: G_S_22081_MONTH.tcl
#功能描述: 生成22081的数据
#运行粒度: 月
#源    表：
#1. bass2.DW_PRODUCT_INS_OFF_INS_PROD_$op_month
#2.bass2.dw_product_$op_month 
#3.bass2.DIM_PROD_UP_PRODUCT_ITEM
#4. bass2.DW_PRODUCT_SP_INFO_$op_month
#5. bass2.dw_product_unite_cancel_order_$op_month
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzhiwei
#编写时间：2011-04-26
#问题记录：1.
#修改历史: 1. 1.7.2 规范
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      

  #删除早前测试数据
	set sql_buff "delete from bass1.G_S_22081_MONTH where time_id < 201104"
	exec_sql $sql_buff


  #删除本期数据
	set sql_buff "delete from bass1.G_S_22081_MONTH where time_id=$op_month"
	exec_sql $sql_buff
	#1.求订购用户数
	#一次性建表
	##drop table BASS1.G_S_22081_MONTH_1;
	##CREATE TABLE BASS1.G_S_22081_MONTH_1
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
	##ALTER TABLE BASS1.G_S_22081_MONTH_1
	##  LOCKSIZE ROW
	##  APPEND OFF
	##  NOT VOLATILE;

	set sql_buff "ALTER TABLE BASS1.G_S_22081_MONTH_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

	set sql_buff "
			INSERT INTO  BASS1.G_S_22081_MONTH_1
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
						where substr(replace(char(date(VALID_DATE)),'-',''),1,6) <= '$op_month'
						and expire_date >  '$ThisMonthFirstDay'
						and p.PRODUCT_INSTANCE_ID = u.user_id
						group by OFFER_ID
					) a ,
					(
						SELECT PRODUCT_ITEM_ID,SUPPLIER_ID,EXTEND_ID2 ,name 
						 FROM bass2.DIM_PROD_UP_PRODUCT_ITEM
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

  aidb_runstats bass1.G_S_22081_MONTH_1 3


	#一次性建表
	##drop table BASS1.G_S_22081_MONTH_2;
	##CREATE TABLE BASS1.G_S_22081_MONTH_2
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
	##ALTER TABLE BASS1.G_S_22081_MONTH_2
	##  LOCKSIZE ROW
	##  APPEND OFF
	##  NOT VOLATILE;
	#
	set sql_buff "ALTER TABLE BASS1.G_S_22081_MONTH_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff
	#
	set sql_buff "
	insert into  BASS1.G_S_22081_MONTH_2
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
	aidb_runstats bass1.G_S_22081_MONTH_2 3

## 2011-07-27 
# 由于：大量业务为点播类业务 , 故要把点播的去掉（2），保留包月的（3），（1）是免费，也去掉。

	set sql_buff "ALTER TABLE BASS1.G_S_22081_MONTH_3 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff


	set sql_buff "
	insert into G_S_22081_MONTH_3
          select distinct  SP_CODE,BUSI_CODE
                from (
                         select char(c.SP_ID) SP_CODE ,c.BUSI_CODE, BILL_FLAG
                         ,row_number()over(partition by c.SP_ID,c.BUSI_CODE 
                                order by  value(a.SP_CODE,0) asc) rn 
                         from   bass1.G_S_22081_MONTH_1 c 
                          join   bass2.Dim_pm_sp_operator_code a 
                                on  char(a.SP_CODE) = c.SP_ID and a.OPERATOR_CODE = c.BUSI_CODE
                    ) a where a.rn = 1 and a.BILL_FLAG = 3 
	"
	exec_sql $sql_buff
	aidb_runstats bass1.G_S_22081_MONTH_3 3
	
  #少量取不到订购的，关联时去掉。
	set sql_buff "
	insert into bass1.G_S_22081_MONTH
		  (
         TIME_ID
        ,OP_TIME
        ,BUSI_CODE
        ,BUSI_NAME
        ,BUSI_PROVIDER_NAME
        ,CANCEL_CNT
        ,COMPLAINT_CNT
        ,ORDER_CNT
		  )
	select 
	  $op_month TIME_ID
	  ,a.op_time
	  ,a.BUSI_CODE
	  ,a.BUSI_NAME
	  ,max(a.BUSI_PROVIDER_NAME)
	  ,max(char(a.CANCEL_CNT))
	  ,max(char(a.COMPLAINT_CNT))
	  ,max(char(b.ORDER_CNT))
	from (
		select   substr(replace(char(date(a.create_date)),'-',''),1,6) op_time
			 ,a.sp_code BUSI_CODE
			 ,a.name BUSI_NAME
			 ,char(a.sp_id) sp_id
			 ,b.sp_name BUSI_PROVIDER_NAME
			 ,count(distinct case when a.sts = 1 then a.phone_id||char(a.sp_id)||a.sp_code  end ) CANCEL_CNT
			 ,'0' COMPLAINT_CNT
			 ,'0' ORDER_CNT
			 from  bass2.dw_product_unite_cancel_order_$op_month a ,
				BASS1.G_S_22081_MONTH_2 b 
			 where char(a.sp_id) = b.sp_code
			  and substr(replace(char(date(a.create_date)),'-',''),1,6) = '$op_month'
			 group by substr(replace(char(date(a.create_date)),'-',''),1,6)
			  ,a.sp_code
			 ,a.name
			 ,char(a.sp_id)
			 ,b.sp_name
	     ) a 
	    inner join  BASS1.G_S_22081_MONTH_1 b 
	     on  a.BUSI_CODE = b.BUSI_CODE and a.sp_id = b.sp_id
	    inner join bass1.G_S_22081_MONTH_3 c 
	     on  a.BUSI_CODE = c.BUSI_CODE and a.sp_id = c.sp_id	    
	    group by 
	   a.op_time
	  ,a.BUSI_CODE
	  ,a.BUSI_NAME
	  with ur 
  "     
  exec_sql $sql_buff

	set sql_buff "
	update bass1.G_S_22081_MONTH 
  set order_cnt = cancel_cnt
  where bigint(cancel_cnt) > bigint(order_cnt)
  with ur 
 "
	exec_sql $sql_buff
	
  #1.检查chkpkunique
	set tabname "G_S_22081_MONTH"
	set pk 			"OP_TIME||BUSI_CODE||BUSI_NAME"
	chkpkunique ${tabname} ${pk} ${op_month}
	
	return 0
}
