######################################################################################################
#接口名称：业务月受理量
#接口编码：22025
#接口说明：记录业务月受理量的相关信息，其中"02  客户业务受理类型编码" 如下维值必需提供：
#          话费查询、提供帐单/详单、来电显示业务开、GPRS业务开、WLAN 业务开、移动秘书开、语音信箱开、
#          国际漫游业务开、其它业务功能开、电显示业务关、GPRS业务关、WLAN 业务关、移动秘书关、语音信箱关、
#          国际漫游业务关、其它业务功能关、梦网短信订制、梦网短信取消、梦网彩信订制、梦网彩信取消、
#          彩铃业务订制、彩铃业务取消、停机、复机、变更密码、客户信息变更、缴费卡充值、邮寄账单、
#          转换业务（品牌/优惠）、积分查询、积分兑奖、神州行业务、跨区业务、预约购置SIM卡。
#程序名称: G_S_22025_MONTH.tcl
#功能描述: 生成22025的数据
#运行粒度: 月
#源    表：1.bass2.dw_product_busi_dm_yyyymm(工单流水表)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.20110105 liuqf 对其busi_code、so_mode变动，口径进行变动
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_22025_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
        
     
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_22025_month
                   select  $op_month,
                           '$op_month',
                          case when busi_code in (1079) then '0101'
													     when busi_code in (1040) then '0103'
													     when busi_code in (1198) then '0104'
													     when busi_code in (1196) then '0105'
													     when busi_code in (1101) then '0112'
													     when busi_code in (2210) then '0119'
													     when busi_code in (1046,1047,1048) then '0301'
													     when busi_code in (1001) then '0302'
													     when busi_code in (1002) then '0303'
													     when busi_code in (1022) then '0304'
													     when busi_code in (1021) then '0306'
													     when busi_code in (1011,1013,1015) then '0308'
													     when busi_code in (1006) then '0309'
													     when busi_code in (1206) then '0310'
													     when busi_code in (2602) then '0312'
													     when busi_code in (2737) then '0318'
													     when busi_code in (1070) then '0321'
													     when busi_code in (1080) then '0322'
													     when busi_code in (1415) then '0325'
													     when busi_code in (1071) then '0328'
													     when busi_code in (1084) then '0329'
													     when busi_code in (1416) then '0332'
													else '9999' 
													end as bus_type_id,
                            case  when so_mode in ('5') then '01'
														      when so_mode in ('7') then '08'
														      when so_mode in ('0','1','2') then '11'
														      else '99' 
														end as CHNL_TYPE_ID,
                            char(count(*)) BUS_FREQ         
                     from   bass2.dw_product_busi_dm_$op_month
                  group by  case when busi_code in (1079) then '0101'
														     when busi_code in (1040) then '0103'
														     when busi_code in (1198) then '0104'
														     when busi_code in (1196) then '0105'
														     when busi_code in (1101) then '0112'
														     when busi_code in (2210) then '0119'
														     when busi_code in (1046,1047,1048) then '0301'
														     when busi_code in (1001) then '0302'
														     when busi_code in (1002) then '0303'
														     when busi_code in (1022) then '0304'
														     when busi_code in (1021) then '0306'
														     when busi_code in (1011,1013,1015) then '0308'
														     when busi_code in (1006) then '0309'
														     when busi_code in (1206) then '0310'
														     when busi_code in (2602) then '0312'
														     when busi_code in (2737) then '0318'
														     when busi_code in (1070) then '0321'
														     when busi_code in (1080) then '0322'
														     when busi_code in (1415) then '0325'
														     when busi_code in (1071) then '0328'
														     when busi_code in (1084) then '0329'
														     when busi_code in (1416) then '0332'
														else '9999' 
														end,
														case  when so_mode in ('5') then '01'
														      when so_mode in ('7') then '08'
														      when so_mode in ('0','1','2') then '11'
														      else '99' 
														end
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

	return 0
}