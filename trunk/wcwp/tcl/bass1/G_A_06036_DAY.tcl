
######################################################################################################		
#接口名称: 实体渠道购建或租赁信息(日增量)                                                               
#接口编码：06036                                                                                          
#接口说明：记录实体渠道物业来源信息，涉及自营厅、委托经营厅、24小时自助营业厅或社会代理网点（自建他营类）
#程序名称: G_A_06036_DAY.tcl                                                                            
#功能描述: 生成06036的数据
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

   #当天 yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

    #当天 yyyy-mm-dd
    set optime $op_time

    #本月 yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month

    #本月第一天 yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day

    #本月最后一天 yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day

  	set sql_buff "delete from bass1.G_A_06036_DAY where time_id < 20111001"
	exec_sql $sql_buff
	
	
    #删除正式表本月数据
    set sql_buff "delete from bass1.G_A_06036_DAY where time_id=$timestamp"
    puts $sql_buff
    exec_sql $sql_buff

	set sql_buff "ALTER TABLE BASS1.G_A_06036_DAY_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff


    set sql_buff "
    insert into BASS1.G_A_06036_DAY_1
	SELECT
			trim(char(channel_id))
		,	case when PROPERTY_SRC_TYPE in (2,3) then '1'
					  when PROPERTY_SRC_TYPe=1 then '2'
						else '3'
		 	end OWNER_TYPE
		,	case when PROPERTY_SRC_TYPE in (2,3) then left(replace(char(a.create_date),'-',''),6) 
					 else '000101' 
		 	end BUY_MONTH
		,	'' FC_LIC
		,	'' TD_LIC
		,	'' GS_LIC
		,	'' BUY_CHARGE
		,	case when PROPERTY_SRC_TYPE not in (2,3) or PROPERTY_SRC_TYPE is null then left(replace(char(a.create_date),'-',''),8) 
					  else '00010101' 
		 	end RENT_BEGIN_DATE
		,	case when PROPERTY_SRC_TYPE not in (2,3) or PROPERTY_SRC_TYPE is null then '20300101' 
		        else '00010101'
		  end RENT_END_DATE
		,	'' AV_PRICES 
		FROM BASS2.Dim_CHANNEL_INFO A 
		WHERE A.CHANNEL_TYPE_CLASS IN (90105,90102)
	with ur
"
    exec_sql $sql_buff


    set sql_buff "
        insert into BASS1.G_A_06036_DAY
		select 
		$timestamp time_id
		,t.*
		from table(
		select * from g_a_06036_day_1
		except
		select 
			 CHANNEL_ID
			,PROPERTY_TYPE
			,BUY_MONTH
			,PROPERTY_NO
			,LAND_NO
			,IC_NO
			,BUY_PRICE
			,RENT_START_DT
			,RENT_END_DT
			,AVG_REPRISE
		from  (
		select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
		from G_A_06036_DAY a 
		) o where o.rn = 1 
		) t 
		with ur
"
    exec_sql $sql_buff
	return 0
}

