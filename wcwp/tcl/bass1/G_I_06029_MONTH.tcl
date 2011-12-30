######################################################################################################
#�ӿ����ƣ�SPҵ�����
#�ӿڱ��룺06029
#�ӿ�˵����ҵ������ʾҵ�����������/Ӧ�÷����ṩ���Լ��ƶ���
#          ��ʡֻ�ϱ��ṩ���ط��񣨱���/ȫ��ҵ�񱾵ؽ��룩��sp�����ݡ�
#��������: G_I_06029_MONTH.tcl
#��������: ����06029������
#��������: ��
#Դ    ��1.bass2.dim_newbusi_spinfo(SPά��)
#          2.bass2.dw_newbusi_ismg_yyyymm(�����嵥)
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�tym
#��дʱ�䣺2007-03-22
#�����¼��1.ĿǰSPά���sp_typeֻ��7��ֵ����������BASS_STD1_0004����Ҫ,������CASE��������ӳ���ϵ��
#          2.��Ϊ����һ��SP����,һ����������ѵ�,���Դ���һ��SP�����м��ּƷ����͵ļ�¼��
#�޸���ʷ: 1.2007��7�·����ݣ�����sp_code=931043����Ч����Ϊ2007524�����ݣ�������case���������Щ����
#          2.�ڳ���������SP��ҵ���룬SPҵ����������������Ŀ���
#          3.8�·����ݷ���sp_fee��6λ��,���޸ĳ���,��substr����ȡ���ֶε�ֵ@20070903 by tym
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06029_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

        #----������ʱ��-----#
        set handle [aidb_open $conn]

	set sql_buff "declare global temporary table session.g_i_06029_month_tmp
                     (
                      sp_code             varchar(12 ),
                      operator_code       varchar(10 ),
                      operator_name       varchar(50 ),
                      sp_bill_id          varchar( 1 ),
                      sp_fee              varchar( 10 ),
                      effect_date         varchar( 8 ),
                      invalid_date        varchar( 8 )
                      )
                      partitioning key
                      (sp_code)
                      using hashing
                     with replace on commit preserve rows not logged in tbs_user_temp"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
        set handle [aidb_open $conn]
	set sql_buff "insert into session.g_i_06029_month_tmp
                         select
                           substr(a.sp_code,1,12)
                           ,value(substr(a.oper_code,1,10),' ')
                           ,substr(b.sp_name,1,50)
                            ,case
                               when a.bill_flag=3 then '0'
                               when a.bill_flag=2 then '1'
                               when a.bill_flag=1 then '2'
                              end
                            ,case
                               when a.bill_flag in (1,3) then '0'
                               else rtrim(char(int(a.charge4*100)))
                             end
                            ,case
                               when length(rtrim(b.valid_date))<8 then '20070101'
                               else b.valid_date
                             end
                            ,case
                               when length(rtrim(b.expire_date))<8 then '20101231'
                               else b.expire_date
                             end
                         from 
                           bass2.dw_newbusi_ismg_$op_month a ,
                           (select a.* 
                             from bass2.dim_newbusi_spinfo a,
                                 (select sp_code,max(valid_date) as valid_date 
                                  from bass2.dim_newbusi_spinfo
			                            group by sp_code
			                           )b
                              where a.sp_code=b.sp_code and a.valid_date=b.valid_date
                              and (a.sp_region <> 1 or a.sp_name like '%�����ƶ�%')
                           ) b
                         where a.bill_flag in (1,2,3)
                             and a.sp_code=b.sp_code
                         group by
                            substr(a.sp_code,1,12)
                           ,substr(a.oper_code,1,10)
                           ,substr(b.sp_name,1,50)
                            ,case
                               when a.bill_flag=3 then '0'
                               when a.bill_flag=2 then '1'
                               when a.bill_flag=1 then '2'
                              end
                            ,case
                               when a.bill_flag in (1,3) then '0'
                               else  rtrim(char(int(a.charge4*100)))
                             end
                             ,case
                               when length(rtrim(b.valid_date))<8 then '20070101'
                               else b.valid_date
                             end
                            ,case
                               when length(rtrim(b.expire_date))<8 then '20101231'
                               else b.expire_date
                             end      "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



        #���ܵ������
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06029_month
                 select  a.time_id
                       ,a.sp_code
                       ,a.operator_code
                       ,a.operator_name
                       ,a.sp_bill_id
                       ,a.sp_fee
                       ,a.effect_date
                       ,a.invalid_date
                 from
                 (
                  select
                           $op_month time_id
                           ,sp_code
                           ,operator_code
                           ,operator_name
                           ,sp_bill_id
                           ,substr(rtrim(char(sum(bigint(sp_fee)))),1,5)  sp_fee
                           ,char(max(bigint(effect_date)))  effect_date
                           ,char(max(bigint(invalid_date)))  invalid_date
                           ,row_number()over(partition by sp_code,operator_code order by $op_month desc) row_id
                         from
                           session.g_i_06029_month_tmp
                         group by
                           sp_code
                           ,operator_code
                           ,operator_name
                           ,sp_bill_id 
                 ) a
                 where a.row_id=1     "
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
	set sql_buff "delete from bass1.g_i_06029_month where time_id=$op_month and length(ltrim(rtrim(sp_code))) <> 6;"
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
	set sql_buff "delete from bass1.g_i_06029_month where time_id=$op_month and substr(ltrim(sp_code),1,1) not in ('4','7','9')"
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