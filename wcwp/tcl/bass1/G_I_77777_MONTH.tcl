######################################################################################################
#�ӿ����ƣ����ſͻ���ֵ����ȫ������
#�ӿڱ��룺77777
#�ӿ�˵�����ýӿ�רΪ��ȡ��ʡ��˾���ſͻ���ֵ������ϵ��ϸ���ݶ���ƣ�
#          ���ֶεĺ������������ռ��Ź�˾��ͨ[2007]3���ļ�
#          �������·������ſͻ���ֵ����ʵʩ����ָ���������֪ͨ���Լ�ֵ������ϵʵʩ������Ҫ���˵����
#��������: G_I_77777_MONTH.tcl
#��������: ����77777������
#��������: ��
#Դ    ��1.bass2.dw_enterprise_msg_yyyymm(������Ϣ��)
#          2.GRPEV.DMEP_yyyymm
#          3.GRPEV.DMEP_GROUP_SCORE
#          4.GRPEV.DMEP_GROUP_INFO
#          5.GRPEV.DW_GROUP_NUMS_MS
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�tym
#��дʱ�䣺2008-11-26
#�����¼��
#�޸���ʷ:
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #set op_month 200810
        #�������һ�� yyyymmdd
        #set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        #set op_month 200810

        #ɾ����������
	set sql_buff "delete from bass1.g_i_77777_month where time_id=$op_month"
	puts $sql_buff
	exec_sql $sql_buff
	
# CREATE TABLE BASS1.G_S_77777_MONTH
# (TIME_ID             INTEGER          NOT NULL,
#  Enterprise_ID     CHARACTER(20) NOT NULL, --���ſͻ���ʶ
#  Enterprise_Name     CHARACTER(60) NOT NULL, --��������
#  GROUP_LEVEL        CHARACTER(1)  NOT NULL, --���ż�ֵ������� GRPEV.DMEP_200810.GROUP_LEVEL_SUGGEST + 4 
#  Industry_ID        CHARACTER(2)   NOT NULL, --��ҵ�������  bass2.dw_enterprise_msg_200810.Industry_ID  ת��
#  STOTAL             CHARACTER(5)  NOT NULL, --��ֵ�����÷� GRPEV.DMEP_GROUP_SCORE.STOTAL 
#  INC_INDIVIDUAL     CHARACTER(14) NOT NULL, --���ų�Ա�˵����� GRPEV.DMEP_GROUP_INFO.INC_INDIVIDUAL
#  INC_INFO_PROD      CHARACTER(14) NOT NULL, --��Ϣ������ GRPEV.DMEP_GROUP_INFO.INC_INFO_PROD
#  INC_GROUP          CHARACTER(14) NOT NULL, --ͳһ�������� GRPEV.DMEP_GROUP_INFO.INC_GROUP  
#  ATT_MEMBER_NUMS    CHARACTER(8)  NOT NULL, --����Ա����   GRPEV.DMEP_GROUP_INFO.ATT_MEMBER_NUMS
#  ATT_VPMN_USER_NUMS CHARACTER(8)  NOT NULL, --����V���û��� GRPEV.DW_GROUP_NUMS_MS.ATT_VPMN_USER_NUMS
#  ATT_PROD_NUMS      CHARACTER(2)  NOT NULL, --�ƶ���Ϣ����Ʒʹ��  GRPEV.DMEP_GROUP_INFO.ATT_PROD_NUMS
#  ATT_INFO_LEVEL     CHARACTER(1)  NOT NULL, --��Ϣ��ˮƽ GRPEV.DMEP_GROUP_INFO.ATT_INFO_LEVEL
#  ATT_SIGN_DATE      CHARACTER(8)  NOT NULL, --�ƶ�ǩԼʱ�� GRPEV.DMEP_GROUP_INFO.ATT_SIGN_DATE 
#  ATT_LOSS_RATE      CHARACTER(5)  NOT NULL, --���Ÿ��˿ͻ������� GRPEV.DMEP_GROUP_INFO.ATT_LOSS_RATE
#  EFF_SOCIETY        CHARACTER(2)  NOT NULL, --��ҵ/����Ӱ���� GRPEV.DMEP_GROUP_INFO.EFF_SOCIETY
#  EFF_ECONOMY        CHARACTER(2)  NOT NULL  --����/��˰���� GRPEV.DMEP_GROUP_INFO.EFF_ECONOMY 

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



#�ڲ���������	
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