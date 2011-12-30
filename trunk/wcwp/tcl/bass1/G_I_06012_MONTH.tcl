######################################################################################################
#�ӿ����ƣ�SP��ҵ����
#�ӿڱ��룺06012
#�ӿ�˵������ҵ����Ϊ������SP��ַ����ݵı�ʶ����ַ���롢�Ʒѡ�����Ⱦ�����ҵ����Ϊ���ݡ�
#          ��ҵ���������ֱ�ʾ����6λ����"AXY000"��"AXY999"������"XY"��Ӧ�ڸ��ƶ���˾,
#          ������������ҵ��SP��ҵ�������9��ͷ��������8��ͷ��PDAҵ����6��ͷ��
#          ��ʡֻ�ϱ��ṩ���ط��񣨱���/ȫ��ҵ�񱾵ؽ��룩��sp�����ݸ���Ŀǰ��ҵ�����
#          ͬһ��SP��Ӫ�̣��������ˣ��������ṩ��ҵ�����Ͳ�ͬ�����磺�������ţ��ٱ��䣩��
#          �������SP��ҵ���롣
#��������: G_I_06012_MONTH.tcl
#��������: ����06012������
#��������: ��
#Դ    ��1.BASS2.dim_newbusi_spinfo(SPά��)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д ��: tym
#��дʱ�䣺2007-03-22
#�����¼��1.ͳ������ҵ���־ʱ,ֻ�ų���SP_NAMEΪ"�����ƶ�"������SP��
#          2.���Ź�˾�涨�ýӿ���ȫ��������Ҫ���Ѿ�ʧЧ��SP��Ϣ�������ͻ���ɸýӿ�
#            Υ��SP��ҵ���룬SPҵ�����ͱ������Ч����������������У�顣
#            Ϊ������У�飬ȡvalid_date��expire_date��max
#          3.���Ǵ���ͬһ��SP��ҵ�м���SPҵ�����ͱ��룬��������ɲ����ϼ��ŵ�����У�顣
#           ������Υ���˼��Ź涨�ġ��������ṩ��ҵ�����Ͳ�ͬ(���磺�������ţ��ٱ���) 
#           �������SP��ҵ���롱��ҵ��Ҫ��
#�޸���ʷ: 1.SPֻ�ͱ��ص� 20081117 �Ļ�ѧ
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]       
        #set op_month 200809 
        puts $op_month
        #�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        
        #ɾ����������
	set sql_buff "delete from bass1.g_i_06012_month where time_id=$op_month"
  puts $sql_buff
  exec_sql $sql_buff       

	set sql_buff "insert into bass1.g_i_06012_month
                        select
                          $op_month
                          ,substr(sp_code,1,12)
                          ,substr(sp_name,1,50)
                          ,case
                             when sp_type=1 then '01'
                             when sp_type=2 then '02'
                             when sp_type=3 then '03'
                             when sp_type=5 then '05'
                             when sp_type=6 then '04'
                             else '99'
                           end
                          ,case 
                             when sp_region=1 then '3'
                             else '2'
                           end 
                          ,max(valid_date)
                          ,max(expire_date)
                          ,case 
                             when sp_name like '%�����ƶ�%' then '1'
                             else '0'
                           end                                         
                       from  
                         bass2.dim_newbusi_spinfo
                       where 
                         bigint(expire_date)>$this_month_last_day  and (sp_region <> 1 or sp_name like '%�����ƶ�%')
                       group by 
                            substr(sp_code,1,12)
                           ,substr(sp_name,1,50)
                           ,case
                              when sp_type=1 then '01'
                              when sp_type=2 then '02'
                              when sp_type=3 then '03'
                              when sp_type=5 then '05'
                              when sp_type=6 then '04'
                              else '99'
                            end
                           ,case 
                              when sp_region=1 then '3'
                              else '2'
                            end 
                           ,case 
                              when sp_name like '%�����ƶ�%' then '1'
                              else '0'
                            end "
  puts $sql_buff
  exec_sql $sql_buff       

	set sql_buff "delete from bass1.g_i_06012_month where time_id=$op_month and length(ltrim(rtrim(sp_code))) <> 6;"
  puts $sql_buff
  exec_sql $sql_buff       

	set sql_buff "delete from bass1.g_i_06012_month where time_id=$op_month and substr(ltrim(sp_code),1,1) not in ('4','7','9');"
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

