######################################################################################################
#接口名称：集团客户与目标市场清单对应关系
#接口编码：01005
#接口说明：1、本接口增量上传目标市场清单客户ID与当月省公司现网集团客户标识间的对应关系，相关概念及CRM改造要求请参见
#《关于明确重要集团客户信息管理省级业务支撑系统改造要求的通知》（业通[2010]76号）。
#          2、目标市场清单客户ID的上报范围以集团公司3月份下发的重要集团客户目标市场清单拍照为准，不允许上报拍照涵盖之外的ID。
#          3、现网集团客户标识不能上报上月集团客户目标市场清单快照里已经存在的集团客户标识。
#
#程序名称: G_A_01005_MONTH.tcl
#功能描述: 生成01005的数据
#运行粒度: 月
#源    表：第一次数据来自于集团下发的数据
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败  
#编 写 人：liuqf
#编写时间：2010-06-24
#问题记录：
#修改历史: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

    #本月 yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]   
        
           
	set sql_buff "DELETE FROM bass1.G_A_01005_MONTH where time_id=$op_month"
    exec_sql $sql_buff




#2011-06-03 17:50:21
#一次性！
#set sql_buff "
#insert into  BASS1.G_A_01005_MONTH
#select distinct 
#         201105 TIME_ID
#        ,ID
#        ,ENTERPRISE_ID
#from     BASS1.G_I_77780_DAY_DOWN20110429 a
#"
#exec_sql $sql_buff

#月增量
#set sql_buff "
#insert into  BASS1.G_A_01005_MONTH
#select distinct 
#         $op_month TIME_ID
#        ,ID
#        ,ENTERPRISE_ID
#from     BASS1.boss_interface a
#"
#exec_sql $sql_buff
#
  #检查chkpkunique
  set tabname "G_A_01005_MONTH"
  set pk    "ID||ENTERPRISE_ID"
  chkpkunique ${tabname} ${pk} $op_month


	return 0
}
