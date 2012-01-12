
######################################################################################################		
#接口名称: 用户选择资费套餐（资费统一编码）                                                               
#接口编码：02027                                                                                          
#接口说明：1. 用户当月曾经在网，不考虑其月末是否离网。不包括历史离网用户。
#程序名称: G_I_02027_MONTH.tcl                                                                            
#功能描述: 生成02027的数据
#运行粒度: MONTH
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20120112
#问题记录：
#修改历史: 1. panzw 20120112	统一资费管理v1.1 
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_month 4 5]01
      puts $ThisMonthFirstDay      
 set this_month_last_day [string range $op_month 0 3][string range $op_month 4 5][GetThisMonthDays [string range $op_month 0 5]01]
        global app_name
        set app_name "G_I_02027_MONTH.tcl"    

  #删除本期数据
	set sql_buff "delete from bass1.G_I_02027_MONTH where time_id=$op_month"
	exec_sql $sql_buff





	set sql_buff "
	insert into bass1.G_I_02027_MONTH
		  (
			 TIME_ID
			,USER_ID
			,PKG_ID
			,EFF_DT
		  )
	select distinct
	  $op_month TIME_ID
	  ,c.user_id
	  ,b.PKG_ID
	  ,c.valid_date
	from BASS1.G_I_02026_MONTH_LOAD a
	join bass1.G_I_02026_MONTH b on a.NEW_PKG_ID = b.pkg_id
	join ( select user_id,offer_id,valid_date 
			from (
				select user_id,offer_id,valid_date
						,row_number()over(partition by user_id,offer_id order by valid_date desc ) rn 
				from (
					select b.user_id ,char(a.OFFER_ID) OFFER_ID 
						, replace(char(a.VALID_DATE),'-','') VALID_DATE 
						from bass2.DW_PRODUCT_INS_OFF_INS_PROD_$op_month  a
						,bass2.dw_product_$op_month b
						where  char(a.product_instance_id) = b.user_id
								and (b.USERSTATUS_ID in (1,2,3,6,8) or b.MONTH_OFF_MARK = 1) 
								and b.USERTYPE_ID in (1,2,9)
								and replace(char(a.VALID_DATE),'-','') <= '$this_month_last_day'
								and replace(char(a.expire_date ),'-','') >= '$ThisMonthFirstDay'
								--and a.state =1
								--and a.valid_type = 1
				) i
			) o where o.rn = 1
		  ) c on a.OLD_PKG_ID = c.OFFER_ID
	  with ur
  "     
  exec_sql $sql_buff


  #1.检查chkpkunique
	set tabname "G_I_02027_MONTH"
	set pk 			"USER_ID,PKG_ID"
	chkpkunique ${tabname} ${pk} ${op_month}



	return 0
}

