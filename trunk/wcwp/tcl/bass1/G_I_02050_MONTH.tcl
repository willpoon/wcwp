######################################################################################################
#接口名称：集团客户开通行业应用情况
#接口编码：02050
#接口说明：集团客户所开通的行业应用业务局数据。
#程序名称: G_I_02050_MONTH.tcl
#功能描述: 生成02050的数据
#运行粒度: 月
#源    表：1.bass2.dw_newbusi_ismg_yyyymm(移动梦网月清单)
#          2.bass2.dw_enterprise_member_yyyymm(集团成员月表)
#          3.bass2.dw_product_yyyymm(用户月表) 
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败  
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.只有集团用户，没有SI 2.西藏只有校信通，银信通
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]   
        
           
        set handle [aidb_open $conn]
	set sql_buff "\
		DELETE FROM bass1.G_I_02050_MONTH where time_id=$op_month  "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_I_02050_MONTH
                      select
                      $op_month,
                      '0',
                      enterprise_id,
                      substr(enterprise_name,1,20),
                      '',
                      sp_code,
                      ser_code,
                      oper_code,
                      '',
                      case when apptype_id=1 then '110'
                           when apptype_id=2 then '120'
                      	 end,
                      '1',
                      case when bill_flag=3 then '1'
                           else char(bill_flag)	
                      	 end 
                      from
                      (
                      select
                        enterprise_id,
                        max(enterprise_name) as enterprise_name,
                        max(sp_code) as sp_code,
                        max(ser_code) as ser_code,
                        max(oper_code) as oper_code,
                        max(apptype_id) as apptype_id,
                        max(bill_flag) as bill_flag
                      from
                      (
                      select 
                        b.enterprise_id,
                        b.enterprise_name,
                        a.user_id,
                        a.sp_code,
                        a.ser_code,
                        oper_code,
                        a.apptype_id,
                        a.bill_flag 
                      from 
                        bass2.dw_newbusi_ismg_${op_month} a,
                        bass2.dw_enterprise_member_${op_month} b,
                      	bass2.dw_product_${op_month} c 
                      where 
                        a.apptype_id in (1,2) 
                        and a.user_id=c.user_id 
                        and b.cust_id=c.cust_id 
                      )aa
                    group by
                      enterprise_id
                    )cc"
        
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
