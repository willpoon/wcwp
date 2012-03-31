######################################################################################################
#接口名称：竞争对手渠道网点情况
#接口编码：22056
#接口说明：记录竞争对手的渠道信息。
#程序名称: G_S_22056_MONTH.tcl
#功能描述: 生成22056的数据
#运行粒度: 月
#源    表：
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuqf
#编写时间：2011-01-17
#问题记录：
#修改历史: 
##~   1.7.9: 
##~   竞争对手渠道类型	此字段仅取以下取值：
##~   01：自有实体渠道
##~   02：社会实体渠道（专营）
##~   03：社会实体渠道（非专营）
##~   修改接口22056（竞争对手渠道网点情况）：
##~   1、	增加字段“竞争对手渠道类型”；
##~   2、	定义联合主键“月份”、“所在地市标识”、“所在区域类型”、“竞争对手类型”、“竞争对手渠道类型”；
##~   3、	修改接口单元说明中“竞争对手渠道网点汇总表”；

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
    #本月最后一天 yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_month 4 5][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day
        #删除本期数据
	set sql_buff "delete from bass1.g_s_22056_month where time_id=$op_month"
	puts $sql_buff
  exec_sql $sql_buff
  #插入  
set sql_buff "
insert into bass1.g_s_22056_month			
	select 
	$op_month
	,'$op_month'
	,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(substr(REGION_CODE,2))),'13101') 
	,case when GEOGRAPHY_TYPE in (1,2,3) then '1'
		when GEOGRAPHY_TYPE in (4) then '2'
		else '3' end GEOGRAPHY_TYPE
	,case when operate_type in (0) then '2' 
	when operate_type in (1) then '1'
	end operate_type
	,case when NOTES like '%自办%' or OPPONENT_NAME_CH like '%自办%' then '01'
		  when NOTES like '%专营%' or OPPONENT_NAME_CH like '%专营%' then '02'
		  else '03' end JINZHEN_CHNLTYPE
	,char(count(distinct OPPONENT_ID))
from bass2.dw_opponent_base_info_$this_month_last_day
where operate_type in (0,1)
group by 
coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(substr(REGION_CODE,2))),'13101')
,case when GEOGRAPHY_TYPE in (1,2,3) then '1'
	when GEOGRAPHY_TYPE in (4) then '2'
	else '3' end 
,case when operate_type in (0) then '2' 
	when operate_type in (1) then '1'
	end
case when NOTES like '%自办%' or OPPONENT_NAME_CH like '%自办%' then '01'
		  when NOTES like '%专营%' or OPPONENT_NAME_CH like '%专营%' then '02'
		  else '03' end	
with ur
"
  exec_sql $sql_buff
  
  
##~   2、	定义联合主键“月份”、“所在地市标识”、“所在区域类型”、“竞争对手类型”、“竞争对手渠道类型”；	
  #1.检查chkpkunique
	set tabname "G_S_22056_MONTH"
	set pk 			"OP_TIME||CMCC_ID||AREA_TYPE||JINZHENTYPE||JINZHEN_CHNLTYPE"
	chkpkunique ${tabname} ${pk} ${op_month}
	
	
	return 0
}
