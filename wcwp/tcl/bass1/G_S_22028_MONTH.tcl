######################################################################################################
#接口名称：数据业务月投诉
#接口编码：22028
#接口说明：记录数据业务月投诉的相关信息。
#程序名称: G_S_22028_MONTH.tcl
#功能描述: 生成22028的数据
#运行粒度: 月
#源    表：1.bass2.dw_custsvc_work_accept_yyyymm(申告工单受理表)
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
   
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_22028_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
            
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_22028_month
                      select
                        ${op_month}
                        ,'${op_month}'
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0040',char(appeal_type)),'999999')
                        ,case 
                           when appeal_type in ('0100320','0101202','0108100','0108120','0108130','0108140') then '252'
                           when appeal_type in ('0100321','0101211','0108101','0108121','0108131','0108141') then '283'
                           when appeal_type in ('0100313','0101214','0108102','0108122','0108132','0108142') then '310'
                           when appeal_type in ('0100100','0101102','0108103','0108123','0108133','0108143') THEN '370'
                           when appeal_type in ('0100318','0101212','0108104','0108124','0108134','0108144') THEN '340'
                           when appeal_type in ('0100319','0108105','0108125','0108135','0108145') THEN '410'  
                          end
                        ,char(count(*))
                      from 
                        bass2.dw_custsvc_work_accept_$op_month
                      where 
                        appeal_type in ('0100320','0101202','0108100','0108120','0108130','0108140','0100321','0101211','0108101','0108121','0108131','0108141','0100313','0101214','0108102','0108122','0108132','0108142','0100100','0101102','0108103','0108123','0108133','0108143','0100318','0101212','0108104','0108124','0108134','0108144','0100319','0108105','0108125','0108135','0108145')
                        and accept_type=10
                      group by
                        coalesce(bass1.fn_get_all_dim('BASS_STD1_0040',char(appeal_type)),'999999')
                        ,case 
                           when appeal_type in ('0100320','0101202','0108100','0108120','0108130','0108140') then '252'
                           when appeal_type in ('0100321','0101211','0108101','0108121','0108131','0108141') then '283'
                           when appeal_type in ('0100313','0101214','0108102','0108122','0108132','0108142') then '310'
                           when appeal_type in ('0100100','0101102','0108103','0108123','0108133','0108143') THEN '370'
                           when appeal_type in ('0100318','0101212','0108104','0108124','0108134','0108144') THEN '340'
                           when appeal_type in ('0100319','0108105','0108125','0108135','0108145') THEN '410'
                         end"
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
############################备份
#        #本月 yyyymm
#        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
#   
#        #删除本期数据
#        set handle [aidb_open $conn]
#	set sql_buff "delete from bass1.g_s_22028_month where time_id=$op_month"
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#            
#       
#        set handle [aidb_open $conn]
#	set sql_buff "insert into bass1.g_s_22028_month
#                      select
#                        ${op_month}
#                        ,'${op_month}'
#                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0040',char(appeal_type)),'999999')
#                        ,case 
#                           when appeal_type in ('0100320','0101202','0108100','0108120','0108130','0108140') then '252' ---梦网短信
#                           when appeal_type in ('0100321','0101211','0108101','0108121','0108131','0108141') then '283' ---梦网彩信 
#                           when appeal_type in ('0100313','0101214','0108102','0108122','0108132','0108142') then '310' --WAP（手机上网）
#                           when appeal_type in ('0100100','0101102','0108103','0108123','0108133','0108143') THEN '370'--梦网彩信
#                           when appeal_type in ('0100318','0101212','0108104','0108124','0108134','0108144') THEN '340'--语音杂志
#                           when appeal_type in ('0100319','0108105','0108125','0108135','0108145') THEN '410'  --语音杂志
#                          end
#                        ,char(count(*))
#                      from 
#                        bass2.dw_custsvc_work_accept_dm_$op_month
#                      where 
#                        appeal_type in ('0100320','0101202','0108100','0108120','0108130','0108140','0100321','0101211','0108101','0108121','0108131','0108141','0100313','0101214','0108102','0108122','0108132','0108142','0100100','0101102','0108103','0108123','0108133','0108143','0100318','0101212','0108104','0108124','0108134','0108144','0100319','0108105','0108125','0108135','0108145')
#                        and accept_type=10
#                      group by
#                        coalesce(bass1.fn_get_all_dim('BASS_STD1_0040',char(appeal_type)),'999999')
#                        ,case 
#                           when appeal_type in ('0100320','0101202','0108100','0108120','0108130','0108140') then '252' ---梦网短信
#                           when appeal_type in ('0100321','0101211','0108101','0108121','0108131','0108141') then '283' ---梦网彩信 
#                           when appeal_type in ('0100313','0101214','0108102','0108122','0108132','0108142') then '310' --WAP（手机上网）
#                           when appeal_type in ('0100100','0101102','0108103','0108123','0108133','0108143') THEN '370'--梦网彩信
#                           when appeal_type in ('0100318','0101212','0108104','0108124','0108134','0108144') THEN '340'--语音杂志
#                           when appeal_type in ('0100319','0108105','0108125','0108135','0108145') THEN '410'  --语音杂志
#                         end"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}       
#	aidb_commit $conn
#	aidb_close $handle
#########################