######################################################################################################
#接口名称：用户积分回馈历史
#接口编码：02007
#接口说明：记录用户积分兑换情况
#程序名称: G_S_02007_DAY.tcl
#功能描述: 生成02007的数据
#运行粒度: 日
#源    表：1.bass2.dwd_product_exchgscore_yyyymmdd(用户积分兑换情况)
#          2.bass2.dw_product_yyyymmdd(用户日表)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: 
#######################################################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #当天 yyyy-mm-dd
        set optime $op_time
        
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_02007_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
            
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_02007_DAY
                      (
                      time_id
                      ,point_feedback_id
                      ,point_feedback_date
                      ,user_id
                      ,used_point
                      ,redeem_price
                      )
                    select
                      $timestamp
                      ,'-1'
                      ,replace(char(date(a.oper_date)),'-','')
                      ,b.user_id
                      ,char(sum(int(a.exchanged_score)))
                      ,'0'
                    from 
                      bass2.dwd_product_exchgscore_$timestamp a,
                      bass2.dw_product_$timestamp b
                    where 
                      date(a.oper_date)='$optime'
                      and b.userstatus_id in (1,2,3,6)
                      and b.usertype_id in (1,2,9)
                      and (b.crm_brand_id1=1 or b.crm_brand_id1=4)
                      and a.sts=1
                      and a.user_id=b.user_id
                    group by
                      replace(char(date(a.oper_date)),'-','')
                      ,b.user_id "
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
###########参考############
#and b.userstatus_id in (1,2,3,6)  --在网用户--
#and b.usertype_id in (1,2,9) --非虚拟用户--
#and (b.crm_brand_id1=1 or b.crm_brand_id1=4) --全球通和动感地带用户--
#and a.sts=1 --正常记录--
#############################################################