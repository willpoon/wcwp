######################################################################################################
#�ӿ����ƣ����ſͻ���ͳ������
#�ӿڱ��룺03014
#�ӿ�˵������¼�ɼ��ſͻ�ͳһ���ѵĸ������룬�������������ڼ��ſͻ��˵�������һ���Է�������֮�ͣ�
#          ��Ҫ��ʡ������ϵͳ�����߽��кϲ����ϴ���
#��������: G_S_03014_MONTH.tcl
#��������: ����03014������
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#        set op_time 2008-06-01
#        set optime_month 2008-05-31
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        puts $timestamp
        #----���������һ��---#,��ʽ yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        puts $last_month_day

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        set objecttable "G_S_03014_MONTH"
        #set db_user $env(DB_USER)

        set this_month_firstday [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_firstday 

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.$objecttable where time_id=$op_month"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

#01	�û���ʶ	�μ��ӿڵ�Ԫ���û����û���ʶ��	char(20)
#02	���Ÿ��˷Ƿ�ͳ����Ʒ�������	�μ�ά��ָ��˵���е�BASS_STD1_0105��	char(4)
#03	���˷Ƿ�ͳ����Ŀ��Ŀ����		char(1)
#04	Ӧ�ս��	�Żݺ��Ӧ�ս���λ:�� 	number(12)
#05	�Żݽ��	���еĽ���ֵ, ��λ:��	number(12)


        #����G_S_03014_MONTH��session������session.tmp03014
	set sql_buff "declare global temporary table session.tmp03014
	        (
	                user_id                char(20)   not null ,
                  standard_product       char(4)    not null ,
                  acct_type              char(1)    not null ,
                  income                 bigint,
                  fav_income             bigint
                )
               partitioning key (user_id) using hashing
               with replace on commit preserve rows not logged in tbs_user_temp"
  puts $sql_buff
  exec_sql $sql_buff
	puts "����G_S_03014_MONTH��session������session.tmp03014"






  #���ɼ��Ų�Ʒ��ͳ������
	set sql_buff "insert into session.tmp03014
               	select  b.user_id as user_id
                       ,service_id 
                       ,'1'
               	       ,sum(bigint(non_unipay_fee))  as non_unipay_fee
               	       ,0
                  from bass2.dw_enterprise_unipay_$op_month a,
                       (select acct_id,min(user_id) as user_id from  bass2.dw_product_$op_month group by acct_id) b 
                 where a.acct_id = b.acct_id and a.test_mark = 0 
                 group by b.user_id,service_id"
  puts $sql_buff
  exec_sql $sql_buff
	puts "��������ͨ��ͳ������(����)"

  
 #���ɲ���ͨ����
 set sql_buff "insert into session.tmp03014
               	select  a.user_id as user_id
                       ,'0450' 
                       ,'1'
               	       ,sum(fee)
               	       ,0
                  from bass2.dw_enterprise_industry_apply  a
                 where a.apptype_id = 4 and a.op_time = '$this_month_firstday' 
                 group by a.user_id;"

  puts $sql_buff
  exec_sql $sql_buff




	#============����ʽ���в�������============================
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_03014_MONTH
                  select
                    ${op_month},
                    aa.user_id,
                    aa.ent_prod_id,
                    aa.acct_type,
                    char(aa.income),
                    char(aa.fav_income)
                  from
                  (
                   select
                     user_id as user_id ,
                     coalesce(bass1.fn_get_all_dim('BASS_STD1_0105',char(standard_product)),'0000') as ent_prod_id,
                     acct_type as acct_type,
                     sum(income*100) as income,
                     sum(fav_income*100) as fav_income
                  from
                      session.tmp03014
                  where coalesce(bass1.fn_get_all_dim('BASS_STD1_0105',char(standard_product)),'0000')  <> '0000'
                  group by
                      user_id ,
                      standard_product,
                      acct_type
                )aa"

          puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "����ʽ���в�������"
	
	
	
	
	
#H041	УѶͨ	УѶͨ����������            	  Ԫ	
#H056	����ͨ	��������ͨ����ͨ����������	    Ԫ	
#H081	ũ��ͨ	ũ��ͨ����������	              Ԫ	
#H011 ����ͨ	����ͨ����������    	          Ԫ
#����ͨ(2034)��УѶͨ(2035)������ͨ(2037)��ũ��ͨ(2036) ����ͨĿǰ������ҵӦ�ÿ�����
    set sql_buff "insert into bass1.G_S_03014_MONTH
                   select
                    ${op_month},
                    b.user_id, 
                    case b.apptype_id
                              when 1 then '0120'
                              when 2 then '0130'
                              when 3 then '0140'
                              when 4 then '0450'   
                          end, 
                    '1' ,
                    char(sum(bigint(b.fee*100))) as income,
                    '0'
              from bass2.dw_enterprise_industry_apply b 
             where b.op_time = '$this_month_firstday' and b.apptype_id in (1,2,3,4) and b.user_id is not null 
             group by b.user_id,
                    case b.apptype_id
                              when 1 then '0120'
                              when 2 then '0130'
                              when 3 then '0140'
                              when 4 then '0450'
                          end" 
                 
        puts $sql_buff                                    
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "��ҵӦ�������� �������"
	
##����ͨ������ʱͳ��	
#    set sql_buff "insert into bass1.G_S_03014_MONTH
#                   select
#                    ${op_month},
#                    a.user_id, 
#                    '0450', 
#                    '1' ,
#                    char(sum(bigint(fee*100))) as income,
#                    '0'
#              from (select a.user_id as user_id ,a.city_id,a.product_no,4,
#		                       sum(coalesce(b.fee,0)) as fee
#		                  from bass2.dw_product_$op_month a,
#		                       (select user_id,sum(base_fee+info_fee+month_fee+func_fee) as fee
#                                                    from bass2.dw_newbusi_ismg_$op_month
#                                                    where sp_code in ('901848','400002','900139')
#                                                    group by user_id) b
#                     where a.user_id=b.user_id
#               group by a.user_id,a.city_id,a.product_no) a
#			   group by a.user_id" 
#                 
#        puts $sql_buff                                    
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	puts "��ҵӦ�������� �������"
	
	
	aidb_close $handle

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
