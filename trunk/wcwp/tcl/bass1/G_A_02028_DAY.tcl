
######################################################################################################		
#接口名称: 用户选择BlackBerry（BIS）资费套餐                                                               
#接口编码：02028                                                                                          
#接口说明：本接口为日增量接口，首次上报订购状态为正常的全量订购关系。
#程序名称: G_A_02028_DAY.tcl                                                                            
#功能描述: 生成02028的数据
#运行粒度: DAY
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20110922
#问题记录：
#修改历史: 1. panzw 20110922	1.7.5 newly added
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
    #本月 yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
      
    #上个月 yyyymm
    set last_month [GetLastMonth [string range $op_month 0 5]]
    puts $last_month

        #程序名
        global app_name
        set app_name "G_A_02028_DAY.tcl"
	
  #删除本期数据
  	set sql_buff "delete from bass1.G_A_02028_DAY where time_id <= 20111001"
	exec_sql $sql_buff
	set sql_buff "delete from bass1.G_A_02028_DAY where time_id=$timestamp"
	exec_sql $sql_buff

	
  #直接来源于二经用户表数据，新的接口表
	set sql_buff "
		insert into bass1.G_A_02028_DAY
		select 
		$timestamp time_id
		,USER_ID
		,PKG_ID
		,ORDER_DT
		,STS_CD
		from table (
		select 
		PRODUCT_INSTANCE_ID USER_ID
		,case 
			when OFFER_ID = 113000000013 then '9999121202900981011001'
			when OFFER_ID = 113000000014 then '9999121202901081011001' 
			end pkg_id
		,replace(char(date(VALID_DATE)),'-','') ORDER_DT
		,case when STATE = 1 then '1' else '2' end STS_CD
		from bass2.dw_product_ins_off_ins_prod_ds a
		where OFFER_ID in (113000000013,113000000014)
			and a.valid_type in (1,2)
			and date(a.expire_date) >= '$op_time'
			and a.VALID_DATE < a.expire_date
		except
		select 
			USER_ID
			,PKG_ID
			,ORDER_DT
			,STS_CD
		from (	
			select 
			 TIME_ID
			,USER_ID
			,PKG_ID
			,ORDER_DT
			,STS_CD
			,row_number()over(partition by user_id order by TIME_ID desc ) rn 
			from G_A_02028_DAY a 
		) t where rn = 1 
		) o
		with ur

  "
	exec_sql $sql_buff

	
	set tabname "G_A_02028_DAY"
	set pk 			"user_id"
	chkpkunique ${tabname} ${pk} ${timestamp}


	return 0
}

