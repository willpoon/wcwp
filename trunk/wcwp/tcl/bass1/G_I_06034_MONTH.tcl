
######################################################################################################		
#接口名称: 自营厅营业员信息                                                               
#接口编码：06034                                                                                          
#接口说明：描述移动公司自营厅内部承担业务办理的营业员员工（营销人员/客服人员）相关信息
#程序名称: G_I_06034_MONTH.tcl                                                                            
#功能描述: 生成06034的数据
#运行粒度: MONTH
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20110922
#问题记录：
#修改历史: 1. panzw 20110922	1.7.5 newly added
#######################################################################################################   


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#set optime_month 2011-06
   #当天 yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

    #当天 yyyy-mm-dd
    set optime $op_time

    #本月 yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
		set last_month [GetLastMonth [string range $op_month 0 5]]
    #本月第一天 yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day

    #本月最后一天 yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day

    #删除正式表本月数据
    set sql_buff "delete from bass1.G_I_06034_MONTH where time_id=$op_month"
    exec_sql $sql_buff

##		, CASE WHEN A.CHANNEL_TYPE_CLASS=90105 THEN '1'
##			 		WHEN A.CHANNEL_TYPE_CLASS=90102 AND A.CHANNEL_TYPE IN (90175,90186,90740,90741,90881) THEN '2'
##			 		ELSE '3'
##		  END  CHANNEL_TYPE

#2011-04-15  修复经纬度让校验通过。数据是通过随机函数生成的。
    #往正式表插入本月数据
    set sql_buff "
    insert into BASS1.G_I_06034_MONTH
	select 
	$op_month
	,char(b.OP_ID)
	,char(b.org_id)
	,b.OP_NAME
	,case when state = 0 then '1' else  '2' end
	from    bass2.dw_channel_info_$op_month a
	, bass2.DIM_BOSS_STAFF b
	where a.channel_type_class=90105 and a.channel_type in (90196,90153,90154,90155,90156,90157,90158,90940,90941,90942,90943) 
	and a.channel_id = b.ORG_ID
	and (b.op_name not like '%测试%' 
	and  b.op_name not like '%自助%' 
	)
	with ur
 "


  exec_sql $sql_buff


	return 0
}
