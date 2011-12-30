######################################################################################################
#�ӿ����ƣ����й�˾��MSISDN�ŶεĶ�Ӧ��ϵ
#�ӿڱ��룺06031
#�ӿ�˵������¼�й��ƶ�������Ӫ��˾��MSISDN�ŶεĶ�Ӧ��ϵ���ýӿ�ֻ����ǰ��Ч���зֹ�˾�ͺŶεĶ�Ӧ��ϵ����ȡ���ĵ��й�˾�����ϱ���
#��������: G_I_06031_DAY.tcl
#��������: ����06031������
#��������: ��
#Դ    ��1.bass2.DWD_PS_NET_NUMBER_YYYYMMDD

#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ��Ļ�ѧ
#��дʱ�䣺2008-12-16
#�����¼��
#�޸���ʷ: 20091123,1.6.4�淶�޸�(ֻ��¼�й��ƶ�������Ӫ��˾��MSISDN�ŶεĶ�Ӧ��ϵ)
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #���� yyyy-mm-dd
  set optime $op_time

		set app_name "G_I_06031_DAY.tcl"        

        
  #ɾ����������
	set sql_buff "delete from bass1.g_i_06031_day where time_id=$timestamp"
	exec_sql $sql_buff
       
	set sql_buff "insert into bass1.g_i_06031_day
                     select
                        distinct
                        $timestamp
                        ,number_segment
                        ,case when region_code = '891' then '13101'
                              when region_code = '892' then '13102'
                              when region_code = '893' then '13103'
                              when region_code = '894' then '13104'
                              when region_code = '895' then '13105'
                              when region_code = '896' then '13106'
                              when region_code = '897' then '13107'
                        end
                      from
                        bass2.DWD_PS_NET_NUMBER_$timestamp
                      where
                        region_code in ('891','892','893','894','895','896','897') and length(trim(number_segment)) = 7 "
	exec_sql $sql_buff



  #����06031����Ψһ�Լ��
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select msisdn,count(*) cnt from bass1.g_i_06031_day
	              where time_id =$timestamp
	             group by msisdn
	             having count(*)>1
	            ) as a
	            "
	puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	puts $DEC_RESULT_VAL1

	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent "06031�ӿ�����Ψһ��У��δͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	   }




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

