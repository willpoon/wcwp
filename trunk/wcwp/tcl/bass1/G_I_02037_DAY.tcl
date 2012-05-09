
######################################################################################################		
#接口名称: 用户选择WLAN自动认证资费套餐                                                               
#接口编码：02037                                                                                          
#接口说明：仅上报用户选择的WLAN自动认证资费套餐的订购记录，套餐名称为“WLAN自动认证自费”，套餐档次为20，套餐自费编码为999912120590020001，套餐不限时长， 流量1G封顶。
#程序名称: G_I_02037_DAY.tcl                                                                            
#功能描述: 生成02037的数据
#运行粒度: DAY
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20120507
#问题记录：
#修改历史: 1. panzw 20120507	中国移动一级经营分析系统省级数据接口规范 (V1.8.0) 
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
        set app_name "G_I_02037_DAY.tcl"
	
  #删除本期数据
	set sql_buff "delete from bass1.G_I_02037_DAY where time_id=$timestamp"
	exec_sql $sql_buff

  #直接来源于二经用户表数据，新的接口表
	set sql_buff "
			
		insert into bass1.G_I_02037_DAY
		(
				 TIME_ID
				,USER_ID
				,PKG_ID
				,EFF_DT
		)
		select 
			$timestamp TIME_ID
			,a.PRODUCT_INSTANCE_ID USER_ID
			,'999912120590020001' PKG_ID
			,replace(char(date(a.VALID_DATE) ),'-','') EFF_DT
		from  bass2.Dw_product_ins_off_ins_prod_ds a 
			, bass2.dw_product_$timestamp b 
		where  a.state =1 
			and a.PRODUCT_INSTANCE_ID=b.user_id 
			and b.usertype_id in (1,2,9) 
			and b.userstatus_id in (1,2,3,6,8)
			and a.valid_type in (1,2)
			 and a.OP_TIME = '$op_time'
			 and date(a.VALID_DATE)<='$op_time'
			 and date(a.expire_date) >= '$op_time'
			and not exists ( 
					 select 1 from bass2.dwd_product_test_phone_$timestamp b 
					 where a.product_instance_id = b.USER_ID  and b.sts = 1
					)   
			and a.offer_id in (113500001225,113500001225)
		with ur
		
		  "
	exec_sql $sql_buff

	
  #进行结果数据检查
  #1.检查chkpkunique
	set tabname "G_I_02037_DAY"
	set pk 			"user_id"
	chkpkunique ${tabname} ${pk} ${timestamp}
  
  #2.检查在网用户是否在用户表里头
	set sql_buff "select count(*) from 
	            (
		     select user_id from bass1.G_I_02037_DAY
		      where time_id =$timestamp
		       except
		  select user_id from bass2.dw_product_$timestamp
		    where usertype_id in (1,2,9) 
		    and userstatus_id in (1,2,3,6,8)
		    and test_mark<>1
	            ) as a
	            "
	chkzero2 $sql_buff "有用户不在用户表里头"

	return 0
}

