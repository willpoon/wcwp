######################################################################################################
#接口名称：竞争对手和移动语音互通用户数情况
#接口编码：21015
#接口说明：指竞争对手和移动语音互通用户数情况。
#程序名称: G_S_21015_MONTH.tcl
#功能描述: 生成21015的数据
#运行粒度: 月
#源    表：1. bass2.dw_comp_cust_yyyymm(竞争对手用户资料表)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.目前语音日表新业务按月汇总只能区分部分竞争对手的品牌，见bass2.dim_comp_brand。
#          2.还没有测试，因为dw_comp_cust_200703还没有加上月新增，月流失标志
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
      
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_21015_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
              
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_21015_month
                      select
                        $op_month
                        ,'$op_month'
                        ,case when a.comp_brand_id=3 then '021100'   
                               when a.comp_brand_id=4	then '021900'   
                               when a.comp_brand_id=7	then '022000'   
                               when a.comp_brand_id=10	then '031100' 
                               when a.comp_brand_id=9	then '031200'   
                               when a.comp_brand_id=11	then '031900' 
                               when a.comp_brand_id=2	then '032000'   
                               when a.comp_brand_id in(1,8) then	'033000' 
                               when a.comp_brand_id=6	then '034000'   
                               when a.comp_brand_id=5	then '051000'   
                               when a.comp_brand_id=12	then '080000' 
                               when a.comp_brand_id=13	then '990000'
                          else '990000'
                          end 
                        ,char(sum(a.comp_month_new_mark))
                        ,char(sum(a.comp_month_off_mark))
                      from 
                        bass2.dw_comp_cust_$op_month a
                      group by 
                        case when a.comp_brand_id=3 then '021100'   
                               when a.comp_brand_id=4	then '021900'   
                               when a.comp_brand_id=7	then '022000'   
                               when a.comp_brand_id=10	then '031100' 
                               when a.comp_brand_id=9	then '031200'   
                               when a.comp_brand_id=11	then '031900' 
                               when a.comp_brand_id=2	then '032000'   
                               when a.comp_brand_id in(1,8) then	'033000' 
                               when a.comp_brand_id=6	then '034000'   
                               when a.comp_brand_id=5	then '051000'   
                               when a.comp_brand_id=12	then '080000' 
                               when a.comp_brand_id=13	then '990000'
                          else '990000'
                          end  "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle

	return 0
}