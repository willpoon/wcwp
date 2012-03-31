
######################################################################################################		
#接口名称: "空中充值点业务日汇总"                                                               
#接口编码：22048                                                                                          
#接口说明："统计所有具备空中充值功能的网点情况"
#程序名称: G_S_22048_DAY.tcl                                                                            
#功能描述: 生成22048的数据
#运行粒度: DAY
#源    表：1.BASS2_Dw_agent_acc_info_ds.tcl	空中充值帐号信息
#源    表：2.BASS2_Dwd_channel_dept_ds.tcl	ODS-DWD 渠道组织信息[I04103]
#源    表：3.$dw_acct_payment_dm_yyyymm c
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20110727
#问题记录：
#修改历史: 1. panzw 20110727	1.7.4 newly added
##~   2. panzw 20120331 修改接口22048（空中充值点业务日汇总）：
##~   1、	修改接口单元名称为“空中充值点充值记录”；
##~   2、	修改接口单元说明；
##~   3、	删除字段“所属CMCC运营公司标识”、“实体渠道标识”、“空中充值点类型”、“充值笔数”、“充值金额”、“日冲正笔数”、“日冲正金额”；
##~   4、	增加字段“充值时间”、“MSISDN”、“充值金额”、“冲正金额”；
##~   5、	修改字段“办理日期”属性名称为“充值日期”；
##~   6、	删除联合主键“办理日期”、“空中充值专用手机号”、“所属CMCC运营公司标识”；

#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
      set curr_month [string range $op_time 0 3][string range $op_time 5 6]
      
    #上个月 yyyymm
            global app_name

		set app_name "G_S_22048_DAY.tcl"        

  #删除本期数据
	set sql_buff "delete from bass1.G_S_22048_DAY where time_id=$timestamp"
	exec_sql $sql_buff

#       TIME_ID                 INTEGER(4)          
#       OP_TIME                 CHARACTER(8)        
#       CHRG_NBR                CHARACTER(11)       
#       CMCC_BRANCH_ID          CHARACTER(5)        
#       CHANNEL_ID              CHARACTER(40)       
#       CHRG_WAY_TYPE           CHARACTER(1)        
#       CHRG_CNT                CHARACTER(8)        
#       CHRG_AMT                CHARACTER(14)       
#       CZ_CNT                  CHARACTER(8)        
#       CZ_AMT                  CHARACTER(14)    
#

#GJFK	代销商空中充值扣减	1
#GTFK	代销商空中充值扣减返销	代理商
#GTFG	空中充值返销	1

	set sql_buff "
insert into G_S_22048_DAY
select 
	$timestamp TIME_ID
	,replace(char(OP_TIME),'-','') CHRG_DT
	,replace(substr(char(d.PEER_DATE),12,8),'.','')  CHRG_TM
	,a.mobile_id CHRG_NBR
	,c.key_num PRODUCT_NO
	,char(int(case when c.opt_code = 'GJFK' then c.balance+c.amount else 0 end )) CHRG_AMT
	,char(int(case when d.opt_code = 'GTFG' then d.amount else 0 end )) CZ_AMT
from bass2.dw_agent_acc_info_$timestamp a
join bass2.dwd_channel_dept_$timestamp  b on a.channel_id = b.organize_id
join bass2.dw_acct_payment_dm_$curr_month c on  a.mobile_id=c.key_num
left join (select PAYMENT_ID,OPT_CODE,AMOUNT,BALANCE ,PEER_DATE,key_num
		from bass2.dw_acct_payment_dm_$curr_month
		where  opt_code = 'GTFG' 
	  ) d  on c.PAYMENT_ID=d.PAYMENT_ID 
where c.opt_code in ('GJFK','GTFK')
and c.op_time = '$op_time'
with ur
  "
	exec_sql $sql_buff

	return 0
}
