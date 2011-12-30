######################################################################################################
#�ӿ����ƣ�ʵ������
#�ӿڱ��룺06030
#�ӿ�˵������¼�й��ƶ���Ӫʵ�����������ʵ����������Ϣ��
#��������: G_A_06030_DAY.tcl
#��������: ����06030������
#��������: ��
#Դ    ��1.bass2.dwd_channel_dept__yyyymmdd  
#          2.bass2.DWD_AGENT_STAT_INFO_yyyymmdd ʵ��������Ϣ�� 
#          3.bass2.dwd_channel_dept_yyyymmdd
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�xiahuaxue
#��дʱ�䣺2008-10-14
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

        #�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 3][string range $op_month 4 5][GetThisMonthDays [string range $op_month 0 5]01]
        puts $this_month_last_day
        
        #ɾ����������
	set sql_buff "delete from bass1.G_A_06030_DAY where time_id=$timestamp"
	puts $sql_buff
  exec_sql $sql_buff
       

  #���� ʵ��������Ϣ  
	set sql_buff "insert into G_A_06030_DAY
                select 
                  $timestamp,
                  char(a.Channel_ID),         
                  coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.channel_city)),'13100') ,                
                  value(c.Channel_Name,''),    
                  value(a.Channel_Name,''),      
                  value(char(a.POSITION),'4'),           
                  value(char(b.STORE_AREA),'0'),         
                  value(char(b.STORE_AREA),'0') as BUSI_AREA,          
                  char(int(value(char(b.OWNER_TYPE),'1'))+1),         
                  char('1110'),       
                  value(char(a.LONGITUDE),'0'),          
                  value(char(a.LATITUDE),'0'),
                  case
                            when a.channel_status=1 then '1'
                            else '0'
                          end
                from bass2.dwd_channel_dept_$timestamp a,
                     bass2.DWD_AGENT_STAT_INFO_$timestamp b,
                	   bass2.dwd_channel_dept_$timestamp c 
                where a.Channel_ID = b.Channel_ID and a.PARENT_CHANNEL_ID = c.Channel_ID 
                except
				select
				 $timestamp,
				  CHANNEL_ID,
				  CMCC_ID,
				  BUSI_HALL_NAME,
				  CHANNEL_NAME,
				  POSITION,
				  STORE_AREA,
				  BUSI_AREA,
				  OWNER_TYPE,
				  CHANNEL_TYPE,
				  LONGITUDE,
				  LATITUDE,
				  channel_status
				 from
				 (
				select
				  CHANNEL_ID,
				  CMCC_ID,
				  BUSI_HALL_NAME,
				  CHANNEL_NAME,
				  POSITION,
				  STORE_AREA,
				  BUSI_AREA,
				  OWNER_TYPE,
				  CHANNEL_TYPE,
				  LONGITUDE,
				  LATITUDE,
				  channel_status,row_number()over(partition by CHANNEL_ID order by time_id desc) row_id
				from G_A_06030_DAY
				  where time_id < $timestamp
				  ) a
				 where a.row_id=1
 "
	puts $sql_buff
  exec_sql $sql_buff

  #ɾ���쳣ʵ��������Ϣ  
	set sql_buff "delete from G_A_06030_DAY where time_id = $timestamp and CMCC_ID = '13100';"
	puts $sql_buff
  exec_sql $sql_buff
  
  #ɾ���쳣ʵ��������Ϣ  
	set sql_buff "delete from G_A_06030_DAY where time_id = $timestamp and OWNER_TYPE not in ('1','2','3');"
	puts $sql_buff
  exec_sql $sql_buff

  #ɾ���쳣ʵ��������Ϣ  
	set sql_buff "delete from G_A_06030_DAY where time_id = $timestamp and POSITION not in ('1','2','3','4','5','6');"
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



