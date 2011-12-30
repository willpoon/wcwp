######################################################################################################
#接口名称：网间语音结算
#接口编码：05001
#接口说明：记录中国移动和其他运营商之间的语音结算信息。
#程序名称: G_S_05001_MONTH.tcl
#功能描述: 生成05001的数据
#运行粒度: 月
#源    表：1. bass2.dw_js_acct_dm_yyyymm(结算帐单)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.因为BOSS源表WJJS.MB_INTERFACE_JYFX_PARA只能区分出现有的枚举值(N:网通,R:铁通,T:电信,U:联通),
#            所以不能细分到集团规范规定的“结算对方运营商品牌类型编码”的要求。
#          2.因为dw_call_roamin的源表WJJS.MI_CDR_WJ的baltype值为-1，所以不能满足“结算业务类型编码”要求。
#          3.要跟西藏互联互通结算月报表.xls核对数据
#          4.经过与BOSS系统做结算的宾礼文协商<西藏互联互通结算月报表.xls>,被告知该报表统计非常复杂
#            需要等到7月份BOSS系统上新系统再说，所以暂时屏蔽程序。
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {


#        #本月 yyyymm
#        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
#      
#        #删除本期数据
#        set handle [aidb_open $conn]
#	set sql_buff "delete from bass1.g_s_05001_month where time_id=$op_month"
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#        
##建立临时表
#        set handle [aidb_open $conn]
#	set sql_buff "declare global temporary table session.g_s_05001_month_tmp
#	              (
#                        self_cmcc_code              varchar(5),  
#                        self_svc_brnd_id            varchar(1),
#                        other_cmcc_code             varchar(6),
#                        other_svc_brnd_id           varchar(6),
#                        stlmnt_fee_item_id          varchar(2),
#                        out_durn                    bigint,         
#                        in_durn                     bigint,
#                        stlmnt_fee                  bigint,
#                        pay_stlmnt_fee              bigint                                      
#	              )with replace on commit preserve rows not logged in tbs_user_temp"
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#插入临时表
#        set handle [aidb_open $conn]
#	set sql_buff "insert into session.g_s_05001_month_tmp
#                      select 
#                        coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(feecode)),'13100')
#                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(selftype)),'2')
#                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0069',char(opertype)),'990000')
#                        ,case
#                           when baltypeid='N' then '041000'
#                           when baltypeid='R' then '051000'
#                           when baltypeid='U' then '021000'
#                           else '031000'
#                         end
#                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0079',char(baltype)),'10')
#                        ,case
#                           when BALMODE=0 then sum(chrgmin)
#                           else 0
#                          end
#                         ,case
#                            when BALMODE=1 then sum(chrgmin)
#                            else 0     
#                          end
#                         ,case 
#                            when BALMODE=1 then sum(balfee)*10
#                            else 0
#                           end
#                         ,case 
#                            when BALMODE=0 then sum(balfee)*10
#                             else 0
#                          end 
#                       from bass2.dw_js_acct_dm_$op_month
#                       group by
#                           coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(feecode)),'13100')
#                           ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(selftype)),'2')
#                           ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0069',char(opertype)),'990000')
#                           ,case
#                             when baltypeid='N' then '041000'
#                             when baltypeid='R' then '051000'
#                             when baltypeid='U' then '021000'
#                             else '031000'
#                            end
#                          ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0079',char(baltype)),'10') 
#                          ,BALMODE "
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}        
#	aidb_commit $conn
#	aidb_close $handle
#        
#        #汇总到结果表
#        set handle [aidb_open $conn]
#	set sql_buff "insert into bass1.g_s_05001_month
#                      select 
#                        $op_month
#                        ,'$op_month'
#                        ,self_cmcc_code
#                        ,self_svc_brnd_id
#                        ,other_cmcc_code
#                        ,other_svc_brnd_id
#                        ,stlmnt_fee_item_id
#                        ,char(sum(out_durn))
#                        ,char(sum(in_durn))
#                        ,char(sum(stlmnt_fee))
#                        ,char(sum(pay_stlmnt_fee))                        
#                      from session.g_s_05001_month_tmp
#                      group by
#                        self_cmcc_code
#                        ,self_svc_brnd_id
#                        ,other_cmcc_code
#                        ,other_svc_brnd_id
#                        ,stlmnt_fee_item_id "
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