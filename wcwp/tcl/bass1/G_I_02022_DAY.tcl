######################################################################################################
#接口单元名称：用户选择全球通全网统一基础资费套餐
#接口单元编码：02022
#接口单元说明：仅上报全球通用户全网统一资费套餐的订购记录，每用户最多一条记录：
#1.	用户当日在网。
#2.	每个用户当月只能选择一款基础套餐。
#3.	套餐生效日期，指用户通信行为开始依据该档基础套餐进行计费的起始日期。生效日期应该早于或等于当日。
#程序名称: G_I_02022_DAY.tcl
#功能描述: 生成02022的数据
#运行粒度: 日
#源    表：
#1.bass2.ODS_PRODUCT_INS_PROD_$timestamp
#2.BASS1.ALL_DIM_LKP 
#3.bass2.dwd_product_test_phone_$timestamp
#4.bass2.dw_product_$timestamp
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzhiwei
#编写时间：2011-04-26
#问题记录：1.
#修改历史: 1. 1.7.2 规范
# 这个业务5月1日才上线，5.1之前没有数。上线后跟踪5.2出数情况。
#	2011-05-18 11:51:24 panzhiwei 1.加入：					      and XZBAS_COLNAME not like '套餐减半%' 条件；
#	2011-05-18 11:51:24 panzhiwei 2.改用：					     Dw_product_ins_off_ins_prod_ds 出数；
#	2011-05-18 19:45:56 panzhiwei 备注：不包括测试用户
#	2011-05-27 10:10:49 panzhiwei 加入     and date(a.expire_date) > '$op_time'和rownumber()函数。剔除无效办理。
#	2011-12-31 更新统一资费管理编码
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp

		set app_name "G_I_02022_DAY.tcl"        

  #删除本期数据
	set sql_buff "delete from bass1.g_i_02022_day where time_id=$timestamp"
	exec_sql $sql_buff


  #直接来源于二经用户表数据，新的接口表
	set sql_buff "
	insert into bass1.g_i_02022_day
		  (
			 TIME_ID
			,USER_ID
			,BASE_PKG_ID
			,VALID_DT
		  )
select 	TIME_ID,USER_ID,BASE_PKG_ID,VALID_DT
from (
	select 
		$timestamp TIME_ID
		,char(a.product_instance_id)  USER_ID
		,e.NEW_PKG_ID BASE_PKG_ID
		,replace(char(date(a.VALID_DATE)),'-','') VALID_DT
		,row_number()over(partition by a.product_instance_id order by expire_date desc ,VALID_DATE desc ) rn 
	from  bass2.Dw_product_ins_off_ins_prod_ds a
	,(select xzbas_value  as offer_id 
					from  BASS1.ALL_DIM_LKP 
					where BASS1_TBID = 'BASS_STD1_0114'
					      and bass1_value like 'QW_QQT_JC%'
					      and XZBAS_COLNAME not like '套餐减半%'
				      ) c
	,(select user_id from bass2.dw_product_$timestamp
		    where usertype_id in (1,2,9) 
		    and userstatus_id in (1,2,3,6,8)
		    and test_mark<>1) d
	,bass1.DIM_QW_QQT_PKGID e
	where  char(a.offer_id) = c.offer_id 
	  and char(a.product_instance_id)  = d.user_id
	  and bass1.fn_get_all_dim_ex('BASS_STD1_0114',char(a.offer_id))  = e.OLD_PKG_ID	  
	  and a.state =1
	  and a.valid_type in (1,2)
	  and a.OP_TIME = '$op_time'	  
	  and date(a.VALID_DATE)<='$op_time'	
    and date(a.expire_date) >= '$op_time'
	  and not exists (	select 1 from bass2.dwd_product_test_phone_$timestamp b 
				where a.product_instance_id = b.USER_ID  and b.sts = 1
			 ) 
) t where t.rn = 1
	 with ur
  "
	exec_sql $sql_buff


  #进行结果数据检查
  #检查chkpkunique
  set tabname "g_i_02022_day"
	set pk 			"user_id"
	chkpkunique ${tabname} ${pk} ${timestamp}
  
  #检查在网用户是否在用户表里头
	set sql_buff "select count(*) from 
	            (
		     select user_id from bass1.g_i_02022_day
		      where time_id =$timestamp
		       except
		  select user_id from bass2.dw_product_$timestamp
		    where usertype_id in (1,2,9) 
		    and userstatus_id in (1,2,3,6,8)
		    and test_mark<>1
	            ) as a
	            "
  set DEC_RESULT_VAL1 [get_single $sql_buff]
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]
	puts $DEC_RESULT_VAL1

	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent "02022<用户选择全球通全网统一基础资费套餐>有用户不在用户表里头"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }

	return 0
}

