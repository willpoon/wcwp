######################################################################################################
#�ӿ����ƣ�ʵ��������Ӫ�ɱ���Ϣ
#�ӿڱ��룺22061
#�ӿ�˵������¼ʵ��������Ӫ�ɱ��Ļ�����Ϣ���漰��Ӫ����ί�о�Ӫ����ʵ��������������������������ʵ������
#��������: G_S_22061_MONTH.tcl
#��������: ����22061������
#��������: ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuzhilong
#��дʱ�䣺2010-11-9
#�����¼��
#�޸���ʷ:
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

   #���� yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

    #���� yyyy-mm-dd
    set optime $op_time

    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month

    #���µ�һ�� yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day

    #�������һ�� yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day

    #ɾ����ʽ��������
    set sql_buff "delete from bass1.G_S_22061_MONTH where time_id=$op_month"
    puts $sql_buff
    exec_sql $sql_buff




    #����ʽ����뱾������
    set sql_buff "
    insert into BASS1.G_S_22061_MONTH
		(	 	 TIME_ID
				,STATMONTH          		/* --�·�                       */
			  ,CHANNEL_ID         		/* --ʵ��������ʶ               */
			  ,WATER_FEE          		/* --����ˮ��                   */
			  ,ELE_FEE            		/* --���µ��                   */
			  ,HEAT_FEE           		/* --����ȡů��                 */
			  ,WORK_FEE           		/* --�����˹��ɱ��������       */
			  ,OFFICE_FEE         		/* --���°칫��Ʒ���Ĳķ���     */
			  ,OTHER_FEE          		/* --���������ճ�����           */
		  )
	SELECT
	   $op_month
	 	,'$op_month'
	 	,trim(char(a.CHANNEL_ID))
	 	,'0'	WATER_FEE
	 	,'0'	ELE_FEE
	 	,'0'	HEAT_FEE
	 	,'0'	WORK_FEE
	 	,'0'	OFFICE_FEE
	 	,'0'	OTHER_FEE
	FROM BASS2.DW_CHANNEL_INFO_$op_month A
	WHERE A.CHANNEL_TYPE_CLASS IN (90105,90102)
	"
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
#--------------------------------------------------------------------------------------------------------------



