######################################################################################################
#�ӿ����ƣ����й�˾��Ӫ����
#�ӿڱ��룺06032
#�ӿ�˵������¼�й��ƶ�������Ӫ��˾�Ĵ����������Ϣ����������ֹ������Ч�ĵ�����Ӫ������
#��������: G_I_06032_DAY.tcl
#��������: ����06032������
#��������: ��
#Դ    ��1.bass2.DWD_PS_NET_NUMBER_YYYYMMDD

#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuqf
#��дʱ�䣺2009-11-23
#�����¼��
#�޸���ʷ: 
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #���� yyyy-mm-dd
  set optime $op_time
        
  #ɾ����������
	set sql_buff "delete from bass1.g_i_06032_day where time_id=$timestamp"
	exec_sql $sql_buff
       
	set sql_buff "insert into bass1.g_i_06032_day
                     select
                        distinct
                        $timestamp
                        ,case when region_code = '891' then '13101'
                              when region_code = '892' then '13102'
                              when region_code = '893' then '13103'
                              when region_code = '894' then '13104'
                              when region_code = '895' then '13105'
                              when region_code = '896' then '13106'
                              when region_code = '897' then '13107'
                        end
                        ,case when region_code = '891' then '�����ƶ�ͨ���������ι�˾�����ֹ�˾'
                              when region_code = '892' then '�����ƶ�ͨ���������ι�˾�տ���ֹ�˾'
                              when region_code = '893' then '�����ƶ�ͨ���������ι�˾ɽ�Ϸֹ�˾'
                              when region_code = '894' then '�����ƶ�ͨ���������ι�˾��֥�ֹ�˾'
                              when region_code = '895' then '�����ƶ�ͨ���������ι�˾�����ֹ�˾'
                              when region_code = '896' then '�����ƶ�ͨ���������ι�˾�����ֹ�˾'
                              when region_code = '897' then '�����ƶ�ͨ���������ι�˾����ֹ�˾'
                        end
                        ,case when region_code = '891' then '����'
                              when region_code = '892' then '�տ���'
                              when region_code = '893' then 'ɽ��'
                              when region_code = '894' then '��֥'
                              when region_code = '895' then '����'
                              when region_code = '896' then '����'
                              when region_code = '897' then '����'
                        end
                      from
                        bass2.DWD_PS_NET_NUMBER_$timestamp
                      where
                        region_code in ('891','892','893','894','895','896','897') and length(number_segment) = 7 "
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
	
	puts $sql_buff 
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
	
	
	puts $sql_buff 
	return $result
}
#--------------------------------------------------------------------------------------------------------------

