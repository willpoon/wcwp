######################################################################################################
#接口名称：集团用户成员
#接口编码：02049
#接口说明：定义集团客户中使用中国移动服务的个人用户成员信息。
#程序名称: G_I_02049_MONTH.tcl
#功能描述: 生成02049的数据
#运行粒度: 月
#源    表：1.bass2.dw_enterprise_member_mid_yyyymm(集团客户成员中间表)
#          2.bass2.dw_product_yyyymm(用户资料)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.该接口用L3、L4来校验，所以应该只上传正常用户，离网用户不上传
#          2.该接口求出的集团客户有2339，比老系统的多6个。原因可能是新系统的数据不准确，要拿四月份的数据。
#修改历史: 1.8月份发现违反了L3校验,根据L3校验在where条件中增加了排出虚拟用户和数据SIM卡用户这样便于调试.@20070903 BY TYM 
#          2. 重写程序
#          3.剔除集团虚拟成员 usertype_id=8 的用户，只抓取在网的，保证数据跟02004接口的一致性>修改人：zhanght 修改时间：2009-05-26
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02049_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02049_month
                    select
                      $op_month
                      ,value(a.enterprise_id,' ')
                      ,a.user_id
                   from 
                     bass2.dw_enterprise_member_mid_$op_month a,
                     bass2.dw_enterprise_msg_$op_month b ,
                     bass2.dw_product_$op_month c
                   where  a.enterprise_id = b.enterprise_id
                      and a.user_id=c.user_id
                      and c.usertype_id in (1,2,3,6)
                    "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

aidb_runstats bass1.g_i_02049_month 3
  
#    set handle [aidb_open $conn]
#	set sql_buff "insert into bass1.g_i_02049_month
#                    select
#                      $op_month
#                      ,a.enterprise_id
#                      ,b.user_id
#                   from 
#                     bass2.dw_enterprise_member_$op_month a,
#                     bass2.dw_product_$op_month b 
#                   where 
#                     a.custstatus_id=1 
#                     and b.userstatus_id in (1,2,3,6)
#                     and b.test_mark=0
#                     and b.crm_brand_id2<>70
#                     and a.cust_id=b.cust_id  "
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle

	return 0
}