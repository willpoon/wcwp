######################################################################################################
#接口名称：集团客户价值评估全量数据
#接口编码：77777
#接口说明：该接口专为提取各省公司集团客户价值评估体系明细数据而设计，
#          各字段的含义与解释请参照集团公司客通[2007]3号文件
#          （关于下发《集团客户价值评估实施工作指导意见》的通知）对价值评估体系实施工作的要求和说明。
#程序名称: G_I_77777_MONTH.tcl
#功能描述: 生成77777的数据
#运行粒度: 月
#源    表：1.bass2.dw_enterprise_msg_yyyymm(集团信息表)
#          2.GRPEV.DMEP_yyyymm
#          3.GRPEV.DMEP_GROUP_SCORE
#          4.GRPEV.DMEP_GROUP_INFO
#          5.GRPEV.DW_GROUP_NUMS_MS
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：tym
#编写时间：2008-11-26
#问题记录：
#修改历史:
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #set op_month 200810
        #本月最后一天 yyyymmdd
        #set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        #set op_month 200810

        #删除本期数据
	set sql_buff "delete from bass1.g_i_77777_month where time_id=$op_month"
	puts $sql_buff
	exec_sql $sql_buff
	
# CREATE TABLE BASS1.G_S_77777_MONTH
# (TIME_ID             INTEGER          NOT NULL,
#  Enterprise_ID     CHARACTER(20) NOT NULL, --集团客户标识
#  Enterprise_Name     CHARACTER(60) NOT NULL, --集团名称
#  GROUP_LEVEL        CHARACTER(1)  NOT NULL, --集团价值分类编码 GRPEV.DMEP_200810.GROUP_LEVEL_SUGGEST + 4 
#  Industry_ID        CHARACTER(2)   NOT NULL, --行业分类编码  bass2.dw_enterprise_msg_200810.Industry_ID  转换
#  STOTAL             CHARACTER(5)  NOT NULL, --价值评估得分 GRPEV.DMEP_GROUP_SCORE.STOTAL 
#  INC_INDIVIDUAL     CHARACTER(14) NOT NULL, --集团成员账单收入 GRPEV.DMEP_GROUP_INFO.INC_INDIVIDUAL
#  INC_INFO_PROD      CHARACTER(14) NOT NULL, --信息化收入 GRPEV.DMEP_GROUP_INFO.INC_INFO_PROD
#  INC_GROUP          CHARACTER(14) NOT NULL, --统一付费收入 GRPEV.DMEP_GROUP_INFO.INC_GROUP  
#  ATT_MEMBER_NUMS    CHARACTER(8)  NOT NULL, --集团员工数   GRPEV.DMEP_GROUP_INFO.ATT_MEMBER_NUMS
#  ATT_VPMN_USER_NUMS CHARACTER(8)  NOT NULL, --集团V网用户数 GRPEV.DW_GROUP_NUMS_MS.ATT_VPMN_USER_NUMS
#  ATT_PROD_NUMS      CHARACTER(2)  NOT NULL, --移动信息化产品使用  GRPEV.DMEP_GROUP_INFO.ATT_PROD_NUMS
#  ATT_INFO_LEVEL     CHARACTER(1)  NOT NULL, --信息化水平 GRPEV.DMEP_GROUP_INFO.ATT_INFO_LEVEL
#  ATT_SIGN_DATE      CHARACTER(8)  NOT NULL, --移动签约时间 GRPEV.DMEP_GROUP_INFO.ATT_SIGN_DATE 
#  ATT_LOSS_RATE      CHARACTER(5)  NOT NULL, --集团个人客户离网率 GRPEV.DMEP_GROUP_INFO.ATT_LOSS_RATE
#  EFF_SOCIETY        CHARACTER(2)  NOT NULL, --行业/政策影响力 GRPEV.DMEP_GROUP_INFO.EFF_SOCIETY
#  EFF_ECONOMY        CHARACTER(2)  NOT NULL  --收入/利税排名 GRPEV.DMEP_GROUP_INFO.EFF_ECONOMY 

	set	sql_buff "insert into bass1.g_i_77777_month 
	           select $op_month,
	                  a.Enterprise_ID,
	                  substr(a.Enterprise_Name,1,60),
	                  char(bigint(b.GROUP_LEVEL_SUGGEST)+4),
	                  coalesce(bass1.fn_get_all_dim('BASS_STD1_0043',char(a.Industry_ID)),'99') as ent_industry_id,
	                  char(int(b.STOTAL*100)),
	                  char(int(b.INC_INDIVIDUAL)),
	                  char(int(b.INC_INFO_PROD)),
	                  char(int(b.INC_GROUP)),
	                  char(int(b.ATT_MEMBER_NUMS)),
	                  char(int(b.ATT_VPMN_USER_NUMS)),
	                  char(int(b.ATT_PROD_NUMS)),
	                  char(int(b.ATT_INFO_LEVEL)),
					          replace(char(b.ATT_SIGN_DATE),'-',''),
	                  char(int(b.ATT_LOSS_RATE)),
	                  char(int(b.EFF_SOCIETY)),
	                  char(int(b.EFF_ECONOMY))
	            from  bass2.dw_enterprise_msg_$op_month a,
	                  GRPEV.DMEP_$op_month b
	            where a.Enterprise_id = b.GROUP_ID and a.ent_status_id = 0"
	puts $sql_buff
	exec_sql $sql_buff
	

	return 0
}



#内部函数部分	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}
#--------------------------------------------------------------------------------------------------------------

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}