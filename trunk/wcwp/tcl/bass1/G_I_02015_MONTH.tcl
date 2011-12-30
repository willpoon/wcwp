######################################################################################################
#�ӿڵ�Ԫ���ƣ��û�ѡ���ƶ����������ײ�
#�ӿڵ�Ԫ���룺02015
#�ӿڵ�Ԫ˵�����������µ����һ��24ʱ�����ж����ƶ����������ײ͵���Ч�û��Ķ�����ϵ��¼
#��������: G_I_02015_MONTH.tcl
#��������: ����02015������
#��������: ��
#Դ    ��1.bass2.dwd_product_sprom_active_yyyymmdd(�û��ײ͹�ϵ(����))
#          2.
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuqf
#��дʱ�䣺2010-05-24
#�����¼��1.
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        puts $timestamp
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        
        #----���������һ��---#,��ʽ yyyymmdd
        puts $last_month_day
        
        set thisyear [string range $op_time 0 3]
        puts $thisyear
        
        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
       
        set this_month_firstday [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_firstday 

        #��������
	    set app_name "G_I_02015_MONTH.tcl"

        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02015_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

####90004024	5Ԫ���������ײ�
####90004025	20Ԫ���������ײ�
####90004026	50Ԫ���������ײ�
####90004027	100Ԫ���������ײ�
####90004028	200Ԫ���������ײ�
####99001676	10Ԫ��������������(����\100M)
####99001677	20Ԫ��������������(����\200M)
####90004050	300Ԫ���������ײ�
####90008961	3����50Ԫ���������ײ͵�һ������150Ԫ
####90004047	50Ԫ���������ײ�(������ʡ��\ʡ��)
####90004048	80Ԫ���������ײ�(������ʡ��\ʡ��)
####90004049	120Ԫ���������ײ�(������ʡ��\ʡ��)
####90004052	50Ԫ���������ײ�(������)
####90004053	100Ԫ���������ײ�(������)
####90004054	200Ԫ���������ײ�(������)
####90004055	300Ԫ���������ײ�(������)
####90004023	10Ԫ���������ײ�
####90004305	������������ײ�(�70M)
####73900001	ר���ƶ����������ײ� G3 5Ԫ�ײ�
####73900002	ר���ƶ����������ײ� G3 10Ԫ�ײ�
####73900003	ר���ƶ����������ײ� G3 20Ԫ�ײ�
####73900004	ר���ƶ����������ײ� G3 50Ԫ�ײ�
####73900005	ר���ƶ����������ײ� G3 100Ԫ�ײ�
####73900006	ר���ƶ����������ײ� G3 200Ԫ�ײ�

    set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02015_month
	     (time_id,prod_id,user_id,valid_date)
		select
		    $op_month,
			case when b.prod_id in (90004024,73900001) then '0'
			     when b.prod_id in (90004023,73900002) then '1'
			     when b.prod_id in (90004025,73900003) then '2'
			     when b.prod_id in (90004026,73900004) then '3'
			     when b.prod_id in (90004027,73900005) then '4'
			     when b.prod_id in (90004028,73900006) then '5'
			 else '6'
		    end prod_id,
			 a.user_id,
			 replace(char(date(a.valid_date)),'-','') valid_date
		from bass2.dwd_product_sprom_active_${last_month_day} a,
			 bass2.dim_product_item b
		where a.sprom_id=b.prod_id
		  and b.prod_id in (90004024,90004025,90004026,90004027,90004028,99001676,99001677,90004050,90008961,90004047,90004048,90004049,90004052,90004053,90004054,90004055,90004023,90004305,73900001,73900002,73900003,73900004,73900005,73900006)
		  and replace(char(date(a.valid_date)),'-','')<='${last_month_day}'
		  and replace(char(date(a.expire_date)),'-','')>'${last_month_day}'
		"
    puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


  #����ҵ��ھ�����
  #������ʱ��
  set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_i_02015_month_tmp
	             (
							  time_id     integer         not null,
							  prod_id     character(1)    not null,
							  user_id     character(20)   not null,
							  valid_date  character(8)    not null
                )
                partitioning key
                (user_id)
                using hashing
               with replace on commit preserve rows not logged in tbs_user_temp
	          "
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
	set sql_buff "insert into session.g_i_02015_month_tmp
					select time_id,prod_id,user_id,valid_date 
					from 
					(
					    select time_id,prod_id,user_id,valid_date,row_number() over(partition by user_id order by valid_date desc ) row_id
					     from bass1.g_i_02015_month
					    where user_id in 
					    (
					        select user_id from 
					        (
					            select prod_id,user_id,count(*) cnt from bass1.g_i_02015_month
					            where time_id =$op_month
					            group by prod_id,user_id
					            having count(*)>1
					        ) b
					    )
					    and time_id =$op_month
					) c
					where c.row_id=1
	      "
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
	set sql_buff "delete from  bass1.g_i_02015_month
					where user_id in 
					(
					    select user_id from 
					    (
					    select prod_id,user_id,count(*) cnt from bass1.g_i_02015_month
					    where time_id =$op_month
					    group by prod_id,user_id
					    having count(*)>1
					) b
					)
					and time_id =$op_month
	      "
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
	set sql_buff "insert into  bass1.g_i_02015_month
					select * from session.g_i_02015_month_tmp
	      "
    puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



  #��������Ψһ�Լ��
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select prod_id,user_id,count(*) cnt from bass1.g_i_02015_month
	              where time_id =$op_month
	             group by prod_id,user_id
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
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	puts $DEC_RESULT_VAL1

	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent "02015�ӿڶ����ƶ����������ײ�����Ψһ��У��δͨ��"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }


	return 0
}