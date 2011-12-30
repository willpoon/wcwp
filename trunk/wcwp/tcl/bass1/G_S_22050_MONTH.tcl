######################################################################################################

#接口名称：全网性渠道合作伙伴情况

#接口编码：22050

#接口说明：统计所有全网性合作伙伴合作开设的渠道网点相关信息情况

#程序名称: G_S_22050_MONTH.tcl

#功能描述: 生成22050的数据

#运行粒度: 月

#源    表：

#输入参数: 

#输出参数: 返回值:0 成功;-1 失败

#编 写 人：liuqf

#编写时间：2011-01-17

#问题记录：

#修改历史: 
# 2011.11.29  panzw 1.7.7  1.增加属性“合作伙伴名称”；	2.修改属性“合作伙伴”，增加分类“4：中域”、“5：其他（两个或者两个以上省公司合作的合作伙伴）”；

#######################################################################################################





proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {



	#当天 yyyymmdd

        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

        #当天 yyyy-mm-dd

        set optime $op_time

        

        #本月 yyyymm

        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          

        puts $op_month



				#上月  yyyymm

				set last_month [GetLastMonth [string range $op_month 0 5]] 

				puts $last_month          



        #本月第一天 yyyy-mm-dd

        set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01

        puts $this_month_first_day



        #本月最后一天 yyyy-mm-dd

        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]

        puts $this_month_last_day

        

        #删除本期数据

	set sql_buff "delete from bass1.g_s_22050_month where time_id=$op_month"

	puts $sql_buff

  exec_sql $sql_buff

       



  #插入 
#2011.11.29 与boss欧阳确认， 由于西藏无这样的合作伙伴，所以都报0

#1：苏宁
#2：国美
#3：迪信通
#4：中域
#5：其他

	set sql_buff "insert into bass1.g_s_22050_month

               select 
                  $op_month,
                  '$op_month',
                  COMATE,
		  case 
		  when COMATE = '1' then '苏宁'
		  when COMATE = '2' then '国美'
		  when COMATE = '3' then '迪信通'
		  when COMATE = '4' then '中域'
		  else '其他' end comate_name,
		  CHANNELCOUNT,
                  NEWCUSTCOUNT,
                  PAYCOUNT,
                  BUSICOUNT,
                  COMINALCOUNT,
                  OTHERCOUNT,
                  QQTOPERATORCOUNT,
                  SZXOPERATORCOUNT,
                  DGDDOPERATORCOUNT,
                  PAYMONEY 
                from bass1.g_s_22050_month
               where time_id=$last_month

         "





  puts $sql_buff

  exec_sql $sql_buff

  

 

	return 0

}








