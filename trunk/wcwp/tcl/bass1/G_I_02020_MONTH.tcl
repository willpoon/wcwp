######################################################################################################
#�ӿ����ƣ��û�ѡ������ʷ��ײ�
#�ӿڱ��룺02020
#�ӿ�˵�����û�ѡ��Ļ����ײ�/��ѡ�����ϱ����������������ײͶ�����¼��ÿ�û����һ����¼��
#1.	�û�������������������������ĩ�Ƿ���������������ʷ�����û���
#2.	ÿ���û�����ֻ��ѡ��һ������ײ͡����¸��û������ͨ����Ϊ�������ݸõ��ײͽ��мƷѴ���
#3.	�ײ���Ч���ڣ�ָ�û�ͨ����Ϊ��ʼ���ݸõ������ײͽ��мƷѵ���ʼ���ڡ���Ч�����е��·�Ӧ�����ڻ���ڵ��¡����ڴ�����Ч���ײͷ��������������ϴ������ڱ����������ϴ���
#��������: G_I_02020_MONTH.tcl
#��������: ����02020������
#��������: ��
#Դ    ��1.
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuqf
#��дʱ�䣺2011-02-18
#�����¼��1.
#�޸���ʷ: 1. 1.7.1 �淶
#�޸���ʷ: 1. 1.7.2 �淶 : ���ȫ��ͨȫ��ͳһ�ʷ��ײ���ת��
#�޸���ʷ: 2011-05-24 18:03:24 �Ż������Ч����ɶ���
#�޸���ʷ: 2011-05-24 18:03:24 ��02022ץȫ���û��ײ�ID
#�޸���ʷ: 2011-05-24 18:03:24 �˶�02022��02020������е�user_idһ���ԡ�
#�޸���ʷ: 2011-06-03 21:53:33 �����ײͱ����ȡ������Ч�����һ�α��
#�޸���ʷ: 2011-06-24 18:16:35 ԭ�ھ�������������������֮��
#�޸���ʷ: 2011-06-24 18:16:35 ����ȫ���ײʹ�02022�����û��ĳ�02022���һ���û���
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {


      #���� yyyymm
      #set op_time 2011-01-01
      #set optime_month 2011-01
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      #�������һ�� yyyymmdd
      set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
      puts $op_time
      puts $op_month

    global app_name
		set app_name "G_I_02020_MONTH.tcl"        


  #ɾ����������
	set sql_buff "delete from bass1.g_i_02020_month where time_id=$op_month"
	exec_sql $sql_buff

##3.	�ײ���Ч���ڣ�ָ�û�ͨ����Ϊ��ʼ���ݸõ������ײͽ��мƷѵ���ʼ���ڡ���Ч�����е��·�Ӧ�����ڻ���ڵ��¡����ڴ�����Ч���ײͷ��������������ϴ������ڱ����������ϴ���
#	set sql_buff "
#	insert into bass1.g_i_02020_month
#		  (
#		   time_id
#	    ,user_id
#			,base_prod_id
#			,prod_valid_date
#		  )
#		select 
#		     $op_month,
#		     char(a.product_instance_id),
#		     value(d.BASE_PKG_ID,char(a.offer_id)),
#		     value(d.VALID_DT,replace(char(date(a.create_date)),'-',''))
#		from bass2.dw_product_ins_prod_$op_month a
#	  left join 
#				(select user_id , BASE_PKG_ID ,VALID_DT
#					from
#					(select a.*,row_number()over(partition by user_id order by VALID_DT desc ) rn
#					from bass1.g_i_02022_day  a
#					where time_id  = $this_month_last_day
#					) t where  t.rn = 1 
#				) d on a.product_instance_id = d.user_id
#		where  a.state in ('1','4','6','8','M','7','C','9')
#		  and a.user_type_id =1
#		  and a.valid_type = 1
#		  and a.bill_id not in ('D15289014474','D15289014454')
#		  and not exists 
#				  (select 1 from  bass2.dw_product_test_phone_$op_month b 
#				  			where a.product_instance_id	= b.user_id and  b.sts=1
#		  		)
#  "
#
#	exec_sql $sql_buff
#
	set sql_buff "ALTER TABLE BASS1.g_i_02020_month_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

	set sql_buff "
insert into bass1.g_i_02020_month_1 (
     user_id    
    ,product_no 
    ,test_flag  
    ,sim_code   
    ,usertype_id  
    ,create_date
    ,brand_id
    ,time_id )
select e.user_id
    ,e.product_no  
    ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
    ,e.sim_code
    ,f.usertype_id  
    ,e.create_date  
    ,e.brand_id
    ,f.time_id       
from (select user_id,create_date,product_no,brand_id,sim_code,usertype_id
                ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.g_a_02004_day
where time_id<=$this_month_last_day ) e
inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
           from bass1.g_a_02008_day
           where time_id<=$this_month_last_day ) f on f.user_id=e.user_id
where e.row_id=1 and f.row_id=1
	"
	exec_sql $sql_buff

##����g_i_02020_month_1��Ҫ�Ǳ��ֺͼ���һ�£�������ͳ��������
	set sql_buff "
insert into 	 bass1.g_i_02020_month
		  (
			time_id
			,user_id
			,base_prod_id
			,prod_valid_date
		  )
		select  $op_month
		,b.user_id
		,value(value(e.new_pkg_id,d.BASE_PKG_ID),char(a.offer_id)) base_prod_id
		,value(d.VALID_DT,replace(char(date(a.create_date)),'-','')) VALID_DT
		from bass2.dw_product_ins_prod_$op_month  a
		join bass2.dw_product_$op_month b on a.product_instance_id = b.user_id
		join BASS1.g_i_02020_month_1 c on  a.product_instance_id = c.USER_ID
		left join (select user_id , BASE_PKG_ID ,VALID_DT
				from
				(select a.*,row_number()over(partition by user_id order by VALID_DT desc ) rn
				from bass1.g_i_02022_day  a
				where time_id  = $this_month_last_day
				) t where  t.rn = 1 
			  ) d on a.product_instance_id = d.user_id
		left join bass1.DIM_QW_QQT_PKGID e on  d.BASE_PKG_ID = e.old_pkg_id		
		where   a.valid_type = 1 
		and (b.userstatus_id  in (1,2,3,6,8) or b.MONTH_OFF_MARK = 1)
		and not exists 
			(select 1 from  bass2.dw_product_test_phone_$op_month e 
				where a.product_instance_id     = e.user_id and  e.sts=1
			)
	"
exec_sql $sql_buff


  #���н�����ݼ��
  
	#���  02022��02020������е�user_idһ���ԡ�
	#1.	�û�������������������������ĩ�Ƿ���������������ʷ�����û���
	#2011-06-23 17:00:06 
	#����02022 ������������ʱ��ֻ������ĩ���һ�졣
	#����02020 �� �Ѿ�����ȫ����
	set sql_buff "
    select 
        (select count(distinct user_id) cnt from bass1.g_i_02022_day a where time_id = $this_month_last_day )
         - (select count(0) cnt from bass1.g_i_02020_month a where time_id = $op_month and  BASE_PROD_ID like '9999142110%')
         from bass2.dual
	with ur 
	"

	chkzero 	$sql_buff 1


	#1.	����û���Ч����
	set sql_buff "
    select count(0) from g_i_02020_month where time_id = $op_month 
    and int(prod_valid_date)/100 > $op_month        
	with ur 
	"
	exec_sql $sql_buff

	chkzero2 	$sql_buff  "prod_valid_date invalid! "


	set sql_buff "
select count(*) from 
                    (
                 select distinct  BASE_PROD_ID from bass1.g_i_02020_month
                      where time_id =$op_month
               except
                 select  BASE_PROD_ID from bass1.g_i_02018_month 
                  		where time_id =$op_month            
                    ) as a
                    with ur
               "
	chkzero 	$sql_buff 2

#02020 ���԰���������       
2012-07-03 �������¾�������ɾ���������������Զ��27���û���
  #��������û��Ƿ����û�����ͷ
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select user_id from bass1.g_i_02020_month
	              where time_id =$op_month
               except
		 select user_id from bass2.dw_product_$op_month
		 where usertype_id in (1,2,9) 
		   and test_mark<>1               
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
	        set alarmcontent "02020�ӿ��û�ѡ������ʷ��ײ����û������û�����ͷ"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }



  #����û�ѡ��Ļ����ײ��Ƿ����ײͱ���ͷ
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select base_prod_id from bass1.g_i_02020_month
	              where time_id =$op_month
               except
							 select base_prod_id from bass1.g_i_02018_month 
							  where time_id =$op_month            
	            ) as a
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
	        set alarmcontent "02020�ӿ��û�ѡ������ײͲ����ײ���Ϣ����ͷ"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }

#2011-06-27 16:33:11 �˲�У�飺
#  3���½ӿڵ����ݴ����սӿ��е����ݣ��޳��½ӿڵ�������Ӱ�죩
	set sql_buff "
    select 
        (select count(0)
	from    bass1.g_i_02022_day
			where time_id = $this_month_last_day  
				)
         - (select count(0)
	         from    bass1.g_i_02020_month 
		where time_id = $op_month
		and base_prod_id like '9999142110%')
         from bass2.dual
	with ur 
	"


	chkzero 	$sql_buff 3


 aidb_runstats bass1.G_I_02020_MONTH 3


	return 0
}

