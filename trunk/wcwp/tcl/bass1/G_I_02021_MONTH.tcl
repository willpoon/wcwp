######################################################################################################
#�ӿ����ƣ��û�ѡ������ʷ��ײ�
#�ӿڱ��룺02021
#�ӿڵ�Ԫ˵�����û�ѡ��ĵ����ײ�/��ѡ�����ϱ����������������ײͶ�����¼��
#1.	�û�������������������������ĩ�Ƿ���������������ʷ�����û���
#2.	ÿ���û�����ѡ������ײͣ�Ҳ���Բ�ѡ�񡣿���ѡ��һ������ײͣ�Ҳ����ͬʱѡ�񼸿�����ײ͡����¸��û������ͨ����Ϊ�������ݸõ��ײͽ��мƷѴ���
#3.	�ײ���Ч���ڣ�ָ�û�ͨ����Ϊ��ʼ���ݸõ������ײͽ��мƷѵ���ʼ���ڡ���Ч�����е��·�Ӧ�����ڻ���ڵ��� ��
#��������: G_I_02021_MONTH.tcl
#��������: ����02021������
#��������: ��
#Դ    ��1.
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuqf
#��дʱ�䣺2011-02-21
#�����¼��1.
#�޸���ʷ: 1. 1.7.1 �淶
#						2011-05-03 16:32:56 �Ż����룬ʹ���Ӳ�ѯ��
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {


      #���� yyyymm
      #set op_time 2011-01-01
      #set optime_month 2011-01
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      #set op_month 201102
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      #�������һ�� yyyymmdd
      set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
      #set this_month_last_day 20110231
      #���µĵ�һ�� yyyymmdd
      set one  "01"
      set this_month_one_day ${op_month}${one}
      
      puts $op_time
      puts $op_month
      puts $this_month_last_day
      puts $this_month_one_day
		global app_name
		set app_name "G_I_02021_MONTH.tcl"        

  #ɾ����������
	set sql_buff "delete from bass1.g_i_02021_month where time_id=$op_month"
	exec_sql $sql_buff

  #�����ʱ��
	set sql_buff "alter table bass1.g_i_02021_month_temp1 activate not logged initially with empty table"
	exec_sql $sql_buff


  #��һ����ץȡ�����û�
	set sql_buff "
	insert into bass1.g_i_02021_month_temp1
		  (
       user_id
		  )
   select product_instance_id user_id from bass2.dw_product_ins_prod_$op_month
    where state in ('1','4','6','8','M','7','C','9')
      and user_type_id =1
      and valid_type = 1
      and bill_id not in ('D15289014474','D15289014454')
    except
    select user_id from bass2.dw_product_test_phone_$op_month
    where sts=1
    with ur
   "

	exec_sql $sql_buff

  aidb_runstats bass1.g_i_02021_month_temp1 3
	

  #�����ʱ��
	set sql_buff "alter table bass1.g_i_02021_month_temp2 activate not logged initially with empty table"
	exec_sql $sql_buff


  #�ڶ�������ȡ�����û������˵����ײ͵���Ϣ
	set sql_buff "
	insert into bass1.g_i_02021_month_temp2
		  (
       user_id
      ,offer_id
      ,create_date
		  )
		select 
		     a.product_instance_id,
		     a.offer_id,
		     replace(char(date(min(a.create_date))),'-','')
		from (select a.offer_id,a.product_instance_id,a.create_date 
					from  bass2.dw_product_ins_off_ins_prod_$op_month a
					where 
					replace(char(date(a.create_date)),'-','')<='$this_month_last_day'
		  		and replace(char(date(a.expire_date)),'-','')>='$this_month_one_day'
		  		and a.state=1
					and a.valid_type = 1
					) a,
		     bass1.g_i_02019_month_4 b 
		where 
				a.offer_id=b.base_prod_id 
	group by a.product_instance_id,a.offer_id
	with ur
   "

	exec_sql $sql_buff

  aidb_runstats bass1.g_i_02021_month_temp2 3


#�б���02020 , 02021user_id��Ψһ
  #����Ŀ���
	set sql_buff "
	insert into bass1.g_i_02021_month
		  (
		   time_id
	    ,user_id
			,over_prod_id
			,prod_valid_date
		  )
		select 
		     $op_month,
		     a.user_id,
		     value(value(e.new_pkg_id,c.ADD_PKG_ID),char(a.offer_id)),
		     value(c.VALID_DT,a.create_date) VALID_DT
		from bass1.g_i_02021_month_temp2 a
		inner join bass1.g_i_02021_month_temp1 b	on  a.user_id=b.user_id
		left join (select xzbas_value  as offer_id ,bass1_value bass1_offer_id
				from  BASS1.ALL_DIM_LKP 
				where BASS1_TBID = 'BASS_STD1_0115'
				and bass1_value like 'QW_QQT_DJ%'
				) d on char(a.offer_id) = d.offer_id
		left join  (select user_id , ADD_PKG_ID,VALID_DT
				from
				(select a.*,row_number()over(partition by user_id,ADD_PKG_ID order by VALID_DT desc ) rn
				from bass1.g_i_02023_day  a
				where time_id  = $this_month_last_day
				) t where  t.rn = 1 
				) c on  a.user_id=c.user_id and d.bass1_offer_id = c.ADD_PKG_ID
		left join bass1.DIM_QW_QQT_PKGID e on  d.ADD_PKG_ID = e.old_pkg_id		
with ur
"

	exec_sql $sql_buff

  aidb_runstats bass1.g_i_02021_month 3

  #���н�����ݼ��
  
  	#���  02023��02021������е�user_idһ���ԡ�
	set sql_buff "
    select 
        (select  count(distinct user_id||ADD_PKG_ID) cnt from bass1.g_i_02023_day a where time_id = $this_month_last_day )                     
         - (select  count(distinct user_id||OVER_PROD_ID) cnt from bass1.g_i_02021_month a where time_id = $op_month and  OVER_PROD_ID like '99991222%')
         from bass2.dual
	with ur 
	"
	exec_sql $sql_buff

	chkzero 	$sql_buff 1
  
  #������û��Ƿ����û�����ͷ
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select user_id from bass1.g_i_02021_month
	              where time_id =$op_month
               except
			 select user_id from bass2.dw_product_$op_month
			 where usertype_id in (1,2,9) 
			   and userstatus_id in (1,2,3,6,8)
			   and test_mark<>1               
	            ) as a
	            with ur
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
	        set alarmcontent "02021�½ӿ��û�ѡ������ʷ��ײ����û������û�����ͷ"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }



  #����û�ѡ������ʷ��ײ��Ƿ����ײͱ���ͷ
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select over_prod_id from bass1.g_i_02021_month
	              where time_id =$op_month
               except
							 select over_prod_id from bass1.g_i_02019_month 
							  where time_id =$op_month            
	            ) as a
	            with ur
	            "

	 puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL2 [format "%.3f" [expr ${DEC_RESULT_VAL2} /1.00]]

	puts $DEC_RESULT_VAL2

	if {[format %.3f [expr ${DEC_RESULT_VAL2} ]]>0 } {
		set grade 2
	        set alarmcontent "02021�½ӿ��û�ѡ������ʷ��ײͲ����ײ���Ϣ����ͷ"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }


#��������Ψһ�Լ��
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select user_id,over_prod_id,count(*) cnt from bass1.g_i_02021_month
	              where time_id =$op_month
	             group by user_id,over_prod_id
	             having count(*)>1
	            ) as a
	            with ur
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
	        set alarmcontent "02021�½ӿ��û�ѡ������ʷ��ײ�����Ψһ��У��δͨ��"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }





	return 0
}

