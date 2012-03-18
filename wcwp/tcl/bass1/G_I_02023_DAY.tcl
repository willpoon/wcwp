######################################################################################################
#接口单元名称：用户选择全球通专属叠加资费套餐
#接口单元编码：02023
#接口单元说明：仅上报用户的全球通专属数据包的订购记录：
#1.	全球通套餐可选包供客户自行选择，只有选择全球通全网统一资费上网套餐、商旅套餐、本地套餐的客户可以选择专属数据包中任意可选包。
#2.	用户当日在网。
#3.	套餐生效日期，指用户通信行为开始依据该档套餐进行计费的起始日期。生效日期应该早于或等于当天。
#程序名称: G_I_02023_DAY.tcl
#功能描述: 生成02023的数据
#运行粒度: 日
#源    表：
#1. bass2.Dw_product_ins_off_ins_prod_ds 
#2. BASS1.ALL_DIM_LKP 
#3. bass2.dw_product_$timestamp
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzhiwei
#编写时间：2011-04-26
#问题记录：1.
#修改历史: 1. 1.7.2 规范
#修改历史: 2. 这个业务5月1日才上线，5.1之前没有数。上线后跟踪5.2出数情况。
#2011-05-18 11:51:24 panzhiwei 原函数fn_get_all_dim有bug，不能返回大于10的字段值，改使用fn_get_all_dim_ex()　函数。
#2011-05-18 11:51:24 panzhiwei 联合02022校验user_id的合法性。
#2011-12-31 更新统一资费管理编码
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
		global app_name
		set app_name "G_I_02023_DAY.tcl"        


  #删除本期数据
	set sql_buff "delete from bass1.g_i_02023_day where time_id=$timestamp"
	exec_sql $sql_buff


	#1.求订购用户
	#一次性建表
	##drop table BASS1.G_I_02023_DAY_1;
	##CREATE TABLE BASS1.G_I_02023_DAY_1
	## (
	##		 USER_ID            	CHAR(20)            ----用户标识 主键   
	##		,ADD_PKG_ID         	CHAR(30)            ----叠加套餐标识 主键
	##		,VALID_DT           	CHAR(8)             ----套餐生效日期      
	## )
	##  DATA CAPTURE NONE
	## IN TBS_APP_BASS1
	## INDEX IN TBS_INDEX
	##  PARTITIONING KEY
	##   (USER_ID
	##   ) USING HASHING;
	##
	##ALTER TABLE BASS1.G_I_02023_DAY_1
	##  LOCKSIZE ROW
	##  APPEND OFF
	##  NOT VOLATILE;
	
	
	
	set sql_buff "ALTER TABLE BASS1.G_I_02023_DAY_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

	set sql_buff "
			INSERT INTO  BASS1.G_I_02023_DAY_1
			( 
				 USER_ID
				,ADD_PKG_ID
				,VALID_DT
			)
			select 
			distinct 
				USER_ID
				,ADD_PKG_ID
				,VALID_DT
			FROM (
				SELECT
				 a.PRODUCT_INSTANCE_ID as USER_ID
				,bass1.fn_get_all_dim_ex('BASS_STD1_0115',char(a.offer_id)) as ADD_PKG_ID
				,replace(char(date(a.VALID_DATE)),'-','') as VALID_DT 
				,row_number()over(partition by a.PRODUCT_INSTANCE_ID,a.offer_id 
					order by EXPIRE_DATE desc ,VALID_DATE desc  ) rn 
				from  bass2.Dw_product_ins_off_ins_prod_ds a 
				     ,(select xzbas_value  as offer_id 
					from  BASS1.ALL_DIM_LKP 
					where BASS1_TBID = 'BASS_STD1_0115'
					      and bass1_value like 'QW_QQT_DJ%'
				      ) b 
				where char(a.offer_id) = b.offer_id 
				and a.state=1
				and a.valid_type = 1 
				 and a.OP_TIME = '$op_time'
				 and date(a.VALID_DATE)<='$op_time'
				 and date(a.expire_date) >= '$op_time'
			    ) AS T where t.rn = 1 
			 with ur 
	"
	exec_sql $sql_buff

	  aidb_runstats bass1.G_I_02023_DAY_1 3


	set sql_buff "
	insert into bass1.g_i_02023_day
		  (
			 TIME_ID
			,USER_ID
			,ADD_PKG_ID
			,VALID_DT
		  )
	select 
			$timestamp as TIME_ID
			,a.USER_ID
			,c.new_pkg_id ADD_PKG_ID
			,a.VALID_DT
		from  bass1.G_I_02023_DAY_1 as a 
		      ,bass2.dw_product_$timestamp as b
		      ,bass1.DIM_QW_QQT_PKGID  c
		    where a.user_id = b.user_id
		    and a.add_pkg_id = c.old_pkg_id
		    and b.usertype_id in (1,2,9) 
		    and b.userstatus_id in (1,2,3,6,8)
		    and b.test_mark<>1
	with ur 
  "
	exec_sql $sql_buff
	
##~   剔除一个只有叠加套餐没有主套餐的用户特例
	set sql_buff "
		delete from (
						select * from    bass1.g_i_02023_day where time_id=$timestamp
						and user_id = '89160000950064'
						) t
		with ur
"
	exec_sql $sql_buff

# 检查02023 的user_id 是否在 02022 中:如果DEC_RESULT_VAL1 = 0 - 正常 ; DEC_RESULT_VAL1 > 0 - 不正常


	set sql_buff "
	select count(0) from (
				select user_id from    bass1.g_i_02023_day where time_id=$timestamp
				except
				select user_id from    bass1.g_i_02022_day ) a 
	with ur
  "
	chkzero $sql_buff 1

#3.	套餐生效日期，指用户通信行为开始依据该档套餐进行计费的起始日期。生效日期应该早于或等于当天。
  #进行结果数据检查
  #检查chkpkunique
  set tabname "g_i_02023_day"
	set pk 			"USER_ID||ADD_PKG_ID"
	chkpkunique ${tabname} ${pk} ${timestamp}

	return 0
}

