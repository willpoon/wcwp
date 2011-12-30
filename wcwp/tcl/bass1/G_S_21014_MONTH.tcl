######################################################################################################
#接口名称：竞争对手和移动新业务用户互通情况
#接口编码：21014
#接口说明：记录中国移动竞争对手和中国移动用户之间互发短信（普通短信）的情况。
#程序名称: G_S_21014_MONTH.tcl
#功能描述: 生成21014的数据
#运行粒度: 月
#源    表：1.bass2.dw_comp_all_yyyymm
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.目前语音日表新业务按月汇总只能区分部分竞争对手的品牌，见bass2.dim_comp_brand。
#修改历史: 1.2009-05-26 zhanght 根据一经规范1.5.8，修改“竞争对手运营商品牌类型编码”、“竞争对手业务类型编码”
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

  #本月 yyyymm
  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
      
  
  #删除本期数据
  set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_21014_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
      
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_21014_month
                      select
                        ${op_month}
                        ,'${op_month}'
                        ,case when comp_brand_id=3 then '021100'   
                               when comp_brand_id=4	then '021900'   
                               when comp_brand_id=7	then '022000'   
                               when comp_brand_id=10	then '031100' 
                               when comp_brand_id=9	then '031200'   
                               when comp_brand_id=11	then '031900' 
                               when comp_brand_id=2	then '032000'   
                               when comp_brand_id in(1,8) then	'033000' 
                               when comp_brand_id=6	then '034000'   
                               when comp_brand_id=5	then '051000'   
                               when comp_brand_id=12	then '080000' 
                               when comp_brand_id=13	then '990000'
                          else '990000'
                          end
                        ,'01' 
                        ,char(count(distinct comp_product_no))
                        ,char(sum(mo_sms_counts + mt_sms_counts))
                      from  bass2.dw_comp_all_$op_month
                      where  sms_mark=1
                      group by 
                        case when comp_brand_id=3 then '021100'   
                               when comp_brand_id=4	then '021900'   
                               when comp_brand_id=7	then '022000'   
                               when comp_brand_id=10	then '031100' 
                               when comp_brand_id=9	then '031200'   
                               when comp_brand_id=11	then '031900' 
                               when comp_brand_id=2	then '032000'   
                               when comp_brand_id in(1,8) then	'033000' 
                               when comp_brand_id=6	then '034000'   
                               when comp_brand_id=5	then '051000'   
                               when comp_brand_id=12	then '080000' 
                               when comp_brand_id=13	then '990000'
                          else '990000'
                          end "
        puts $sql_buff    
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle





  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_21014_month
                      select
                        ${op_month}
                        ,'${op_month}'
                        ,case when comp_brand_id=3 then '021100'   
                               when comp_brand_id=4	then '021900'   
                               when comp_brand_id=7	then '022000'   
                               when comp_brand_id=10	then '031100' 
                               when comp_brand_id=9	then '031200'   
                               when comp_brand_id=11	then '031900' 
                               when comp_brand_id=2	then '032000'   
                               when comp_brand_id in(1,8) then	'033000' 
                               when comp_brand_id=6	then '034000'   
                               when comp_brand_id=5	then '051000'   
                               when comp_brand_id=12	then '080000' 
                               when comp_brand_id=13	then '990000'
                          else '990000'
                          end
                        ,'02' 
                        ,char(count(distinct comp_product_no))
                        ,char(sum(mo_mms_counts + mt_mms_counts))
                      from  bass2.dw_comp_all_$op_month
                      where  mms_mark=1
                      group by 
                        case when comp_brand_id=3 then '021100'   
                               when comp_brand_id=4	then '021900'   
                               when comp_brand_id=7	then '022000'   
                               when comp_brand_id=10	then '031100' 
                               when comp_brand_id=9	then '031200'   
                               when comp_brand_id=11	then '031900' 
                               when comp_brand_id=2	then '032000'   
                               when comp_brand_id in(1,8) then	'033000' 
                               when comp_brand_id=6	then '034000'   
                               when comp_brand_id=5	then '051000'   
                               when comp_brand_id=12	then '080000' 
                               when comp_brand_id=13	then '990000'
                          else '990000'
                          end "
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