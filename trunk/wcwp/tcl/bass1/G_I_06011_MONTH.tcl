######################################################################################################
#�ӿ����ƣ�SP�������
#�ӿڱ��룺06011
#�ӿ�˵���������������ʹ�ö���/���ŷ�ʽ���ϴ���ҵ��ʱ����Ҫ����ķ��ͺ��룬
#          ��������������ҵ�����û��ն���ʾ�ķ��ͷ����롣��ʡֻ�ϱ��ṩ���ط���
#         ������/ȫ��ҵ�񱾵ؽ��룩��sp�����ݷ�����������ֱ�ʾ��1.ȫ��ҵ�������볤��ͳһΪ 4 λ��
#          ��"1000"��"9999"��2.����ҵ�������볤��ͳһΪ5 λ���� "01000"��"09999"��
#          ����"�ƶ�ɳ��"��"������־"ҵ��������SPҵ�������룺12590XYZZ��������־����
#          12586XY���ƶ�ɳ������
#��������: G_I_06011_MONTH.tcl
#��������: ����06011������
#��������: ��
#Դ    ��1.BASS2.dim_newbusi_spinfo(SPά��)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.���Ź�˾�涨�ýӿ���ȫ��������Ҫ���Ѿ�ʧЧ��SP��Ϣ�������ͻ���ɸýӿ�
#            Υ��SP��ҵ�����SP�������������������У�顣
#            Ϊ������У�飬ȡvalid_date��max
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6] 
        #�������һ�� yyyymmdd
       set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]         

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06011_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06011_month
	                select a.time_id,
	                       a.sp_code,
	                       a.serv_code,
	                       a.valid_date,
	                       a.expire_date
	                from       
                  (       select
                           $op_month time_id
                           ,substr(sp_code,1,12) sp_code
                           ,case when value(substr(serv_code ,1,21),' ') = '#' then '0' else value(substr(serv_code ,1,21),' ') end  serv_code
                           ,max(valid_date) valid_date
                           ,expire_date
                           ,row_number()over(partition by substr(sp_code,1,12) order by $op_month ) row_id
                         from
                           bass2.dim_newbusi_spinfo 
                         where
                           bigint(expire_date)>$this_month_last_day and
                           (sp_region <> 1 or
                           sp_name like '%�����ƶ�%')
                         group by 
                           substr(sp_code,1,12)
                          ,substr(serv_code ,1,21)
                          ,expire_date
                   ) a
                  where a.row_id=1         "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06011_month where time_id=$op_month and length(ltrim(rtrim(sp_code))) <> 6;"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2030
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06011_month where time_id=$op_month and substr(ltrim(sp_code),1,1) not in ('4','7','9')"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2040
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle   

	return 0
}