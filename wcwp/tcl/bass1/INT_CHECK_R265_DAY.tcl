######################################################################################################
#程序名称：	INT_CHECK_R265_DAY.tcl
#校验接口：	
#运行粒度: MONTH
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2012-06-09 
#问题记录：
#修改历史:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month				
        #自然月
				puts $op_time 
        set curr_month 201206
        ##~   set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        set last_month 201205		
        ##~   set last_month [GetLastMonth [string range $op_month 0 5]]
		
        #程序名
        global app_name
        set app_name "INT_CHECK_R265_DAY.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$last_month and rule_code in (
					 'R265'
					)
			"

		exec_sql $sql_buff





##~   --~   R265	月	03_话单日志	行业网关短信话单中的“服务代码”都在集团客户端口资源使用情况接口的“行业应用代码全码”	"04016 行业网关短信话单
##~   --~   22036 集团客户端口资源使用情况"	行业网关短信话单中的“服务代码”都在集团客户端口资源使用情况接口的“行业应用代码全码”中	0.05	

##~   "Step1.04016（行业网关短信话单）接口中发送状态为0“成功”的“服务代码”集合；
##~   Step2.22036（集团客户端口资源使用情况）接口中截止到统计周期末业务类型=1“行业网关短信”的“行业应用代码全码”集合；
##~   Step3.集合Step1是否均在集合Step2中。"



set sql_buff "


select count(0)
from table(

select distinct  SERV_CODE from G_S_04016_DAY where time_id / 100 = $curr_month and SEND_STATUS = '0'
except
                        select distinct APP_LENCODE from 
                        (
                                        select t.*
                                        ,row_number()over(partition by BILL_MONTH,EC_CODE,APP_LENCODE,APNCODE,BUSI_NAME order by time_id desc ) rn 
                                        from 
                                        G_A_22036_DAY  t
										where  time_id / 100 <= $curr_month
                          ) a
                        where rn = 1    and OPERATE_TYPE = '1'
										And bigint(OPEN_DATE) <= $curr_month
                        
) a
with ur
"

chkzero2 $sql_buff "R265 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R265',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff
  


	return 0
}
