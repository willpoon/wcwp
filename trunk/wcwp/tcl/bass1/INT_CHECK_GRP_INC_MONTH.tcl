######################################################################################################
#�������ƣ�	INT_CHECK_GRP_INC_MONTH.tcl
#У��ӿڣ�	
#��������: MONTH
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2012-06-09 
#�����¼��
#�޸���ʷ:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month				
        #��Ȼ��
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        set last_month [GetLastMonth [string range $op_month 0 5]]
		
        #������
        global app_name
        set app_name "INT_CHECK_GRP_INC_MONTH.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in (
						 'R296'
						,'R297'
						,'R302'
						,'R307'
						,'R310'
						,'R313'
						,'R316'
						,'R319'
						,'R322'
						,'R324'
						,'R326'
						,'R328'
						,'R330'
						,'R332'
						,'R335'
						,'R337'
						,'R339'
					)
			"

		exec_sql $sql_buff


##~   R292	��	02_���ſͻ�	����ר��ͳһ��������	03017 ���ſͻ�ͳ������	����ר��ͳһ���������±䶯�� �� 30%	0.05		"Step1.ͳ�Ƶ��º����£�03017�����ſͻ�ͳ�����룩�У�ҵ�����ͱ���Ϊ1180(����ר��)�ġ�Ӧ�ս�֮�ͣ��ó�����ֵ������ֵ��
##~   Step2.��(����ֵ-����ֵ)/����ֵ*100%>30%����Υ������"


##~   G_S_03017_MONTH


##~   1180	����ר��

##~   !!!�������ݲ�У�飡��Ҫ�޸�����
##~   select sum(int(income)) from   G_S_03017_MONTH
##~   where ent_busi_id = '1180'
##~   and time_id = $op_month



##~   R293	��	02_���ſͻ�	VOIPר�߿ͻ���������ƥ���ϵ	"02054 ���ſͻ�ҵ�񶩹���ϵ
##~   02057 ר��ҵ�񶩹����
##~   03017 ���ſͻ�ͳ������
##~   03018 ����ҵ����˷�ͳ������"	

##~   ��VOIPר���������0�����Ӧר�߿ͻ���ҲӦ�ô���0	0.05		"Step1.ͳ�������ڣ�03017�����ſͻ�ͳ�����룩��03018������ҵ����˷�ͳ�����룩�ӿ��У�ҵ�����ͱ���Ϊ1160(VOIPר��)������֮�ͣ�
##~   Step2.02054�����ſͻ�ҵ�񶩹���ϵ����02057��ר��ҵ�񶩹�������ӿ��У���ֹ��ͳ������ĩ����״̬��������Ч���������У�����ҵ�����ͱ���Ϊ1160(VOIPר��)�ļ��ſͻ���ʶ������ȥ�أ�
##~   Step3.�Ƚ�Step1��Step2���ֵ��"

##~   R293 ������ ����Ҫ�˲��޸���





##~   R296	��	02_���ſͻ�	A�༯�ſͻ���������	"03004 ��ϸ�˵�
##~   02004 �û�
##~   02008 �û�״̬
##~   01004 ���ſͻ�
##~   02049 �����û���Ա
##~   03017 ����ͳ������"	A�༯�ſͻ��������뻷�Ⱦ���ֵС�ڵ���30%	0.05		"Step1.ͨ��02004���û�����02008���û�״̬����03004����ϸ�˵����ӿڣ��ó�ͳ����ÿλ�����û������룻
##~   Step3.����Step1��Step2�ھ�ͳ�������·ݵ�ֵ������ó����±����µĻ��Ⱦ���ֵ�����������30%����Υ������"

	set sql_buff "ALTER TABLE BASS1.INT_03004_03017_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

	
	set sql_buff "
		insert into bass1.INT_03004_03017_1
		select a.user_id , sum(bigint(FEE_RECEIVABLE)) FEE_RECEIVABLE
		from 
		(select * from G_S_03004_MONTH where time_id = $op_month)  a
		,(select user_id from int_02004_02008_month_$op_month where TEST_FLAG = '0' )  b
		where a.USER_ID = b.user_id
		group by a.user_id
		with ur
		"
	exec_sql $sql_buff

	set sql_buff "ALTER TABLE BASS1.INT_03004_03017_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff
	set sql_buff "
		insert into bass1.INT_03004_03017_2
		select a.user_id , sum(bigint(FEE_RECEIVABLE)) FEE_RECEIVABLE
		from 
		(select * from G_S_03004_MONTH where time_id = $last_month)  a
		,(select user_id from int_02004_02008_month_$last_month where TEST_FLAG = '0' )  b
		where a.USER_ID = b.user_id
		group by a.user_id
		with ur
		"
	exec_sql $sql_buff


##~   Step2.ͨ��02049�������û���Ա���ӿڹ���Step1�����01004�����ſͻ����ӿ��С����ż�ֵ������롱in(4��5)��A�༯�ſͻ����ó�ÿ��A�༯�ſͻ������г�Ա������֮�ͣ�����03017������ͳ�����룩�ӿ���ÿ�Ҽ��ſͻ���ͳ��������ܣ��ó�A�༯�ſͻ����������룻
	set sql_buff "
select mem_fee + uniq_fee
from (
select sum(FEE_RECEIVABLE) mem_fee from table (
        select enterprise_id from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_01004_DAY  t
											where time_id/100 <= $op_month
			  ) a
			where rn = 1	and CUST_STATU_TYP_ID = '20' and    ENT_SCALE_ID in ('4','5')  
) a,(select * from G_I_02049_MONTH where time_id = $op_month ) b 
,INT_03004_03017_1 c 
where a.enterprise_id = b.enterprise_id
and b.user_id = c.user_id 
) a ,(

select sum(bigint(INCOME)) uniq_fee
from G_S_03017_MONTH 
where TIME_ID = $op_month

) b where 1 = 1

		"
	set RESULT_VAL1 0
	set RESULT_VAL1 [get_single $sql_buff]

	set sql_buff "
		select mem_fee + uniq_fee
		from (
		select sum(FEE_RECEIVABLE) mem_fee from table (
				select enterprise_id from 
					(
									select t.*
									,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
									from 
									G_A_01004_DAY  t
													where time_id/100 <= $last_month
					  ) a
					where rn = 1	and CUST_STATU_TYP_ID = '20' and    ENT_SCALE_ID in ('4','5')  
		) a,(select * from G_I_02049_MONTH where time_id = $last_month ) b 
		,INT_03004_03017_2 c 
		where a.enterprise_id = b.enterprise_id
		and b.user_id = c.user_id 
		) a ,(

		select sum(bigint(INCOME)) uniq_fee
		from G_S_03017_MONTH 
		where TIME_ID = $last_month

		) b where 1 = 1
		with ur
"
	set RESULT_VAL2 0
	set RESULT_VAL2 [get_single $sql_buff]
	
	set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL1} / ${RESULT_VAL2} - 1) ]]
		if {  ${RESULT_VAL3} > 0.30 } {
		set grade 2
	        set alarmcontent "exception:R296 ָ�껷���쳣"
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	   			return -1
	} else {
	 	puts "ChnRatio pass!"	
	}
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R296',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
exec_sql $sql_buff


##~   R297	��	02_���ſͻ�	B�༯�ſͻ���������	"03004 ��ϸ�˵�
##~   02004 �û�
##~   02008 �û�״̬
##~   01004 ���ſͻ�
##~   02049 �����û���Ա
##~   03017 ����ͳ������"	B�༯�ſͻ��������뻷�Ⱦ���ֵС�ڵ���30%	0.05		"Step1.ͨ��02004���û�����02008���û�״̬����03004����ϸ�˵����ӿڣ��ó�ͳ����ÿλ�����û������룻
##~   Step2.ͨ��02049�������û���Ա���ӿڹ���Step1�����01004�����ſͻ����ӿ��С����ż�ֵ������롱in(6��7)��B�༯�ſͻ����ó�ÿ��B�༯�ſͻ������г�Ա������֮�ͣ�����03017������ͳ�����룩�ӿ���ÿ�Ҽ��ſͻ���ͳ��������ܣ��ó�B�༯�ſͻ����������룻
##~   Step3.����Step1��Step2�ھ�ͳ�������·ݵ�ֵ������ó����±����µĻ��Ⱦ���ֵ�����������30%����Υ������"

	set sql_buff "
select mem_fee + uniq_fee
from (
select sum(FEE_RECEIVABLE) mem_fee from table (
        select enterprise_id from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_01004_DAY  t
											where time_id/100 <= $op_month
			  ) a
			where rn = 1	and CUST_STATU_TYP_ID = '20' and    ENT_SCALE_ID in ('6','7')  
) a,(select * from G_I_02049_MONTH where time_id = $op_month ) b 
,INT_03004_03017_1 c 
where a.enterprise_id = b.enterprise_id
and b.user_id = c.user_id 
) a ,(

select sum(bigint(INCOME)) uniq_fee
from G_S_03017_MONTH 
where TIME_ID = $op_month

) b where 1 = 1

		"
	set RESULT_VAL1 0
	set RESULT_VAL1 [get_single $sql_buff]

	set sql_buff "
		select mem_fee + uniq_fee
		from (
		select sum(FEE_RECEIVABLE) mem_fee from table (
				select enterprise_id from 
					(
									select t.*
									,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
									from 
									G_A_01004_DAY  t
													where time_id/100 <= $last_month
					  ) a
					where rn = 1	and CUST_STATU_TYP_ID = '20' and    ENT_SCALE_ID in  ('6','7')  
		) a,(select * from G_I_02049_MONTH where time_id = $last_month ) b 
		,INT_03004_03017_2 c 
		where a.enterprise_id = b.enterprise_id
		and b.user_id = c.user_id 
		) a ,(

		select sum(bigint(INCOME)) uniq_fee
		from G_S_03017_MONTH 
		where TIME_ID = $last_month

		) b where 1 = 1
		with ur
"
	set RESULT_VAL2 0
	set RESULT_VAL2 [get_single $sql_buff]
	
	set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL1} / ${RESULT_VAL2} - 1) ]]
		if {  ${RESULT_VAL3} > 0.30 } {
		set grade 2
	        set alarmcontent "exception:R297 ָ�껷���쳣"
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	   			return -1
	} else {
	 	puts "ChnRatio pass!"	
	}
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R297',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
exec_sql $sql_buff




##~   R298	��	02_���ſͻ�	A�༯�ſͻ���Ϣ������	"01004 ���ſͻ�
##~   02049 �����û���Ա
##~   03017 ����ͳ������
##~   03018 ���Ÿ��˷�ͳ������"	A�༯�ſͻ���Ϣ�����뻷�Ⱦ���ֵС�ڵ���30%	0.05
##~   "Step1.�ֱ�ͳ��03017������ͳ�����룩�ӿڹ���01004�����ſͻ����ӿڵ�A�༯�ſͻ��У����㵱��ҵ�����·�����Ϣ������ھ������磺2012����Ϣ������ھ����㡶2012�꼯����Ϣ������ָ��ͳ�ƿھ�������ͳ����Ϣ������֮�ͣ��ó�����ֵ������ֵ��
##~   Step2.02049�������û���Ա������01004�����ſͻ�����A�༯�ſͻ���03018�����Ÿ��˷�ͳ�����룩�ӿڰ��յ�����Ϣ������ͳ�ƿھ����ó����º����µķ�ͳ����Ϣ�����룻
##~   Step3.��Step1��Step2�е�ͳ����Ϣ������ͷ�ͳ����Ϣ��������ܵó���Ϣ�����룬�Ӷ��ֱ��γɵ���A����Ϣ�����뵱��ֵ������A����Ϣ����������ֵ��
##~   Step4.���㵱��ֵ������ֵ�Ļ��Ⱦ���ֵ�����������30%����Υ������"



##~   R299	��	02_���ſͻ�	B�༯�ſͻ���Ϣ������	"01004 ���ſͻ�
##~   02049 �����û���Ա
##~   03017 ����ͳ������
##~   03018 ���Ÿ��˷�ͳ������"	B�༯�ſͻ���Ϣ�����뻷�Ⱦ���ֵС�ڵ���30%	0.05		"Step1.�ֱ�ͳ��03017������ͳ�����룩�ӿڹ���01004�����ſͻ����ӿڵ�B�༯�ſͻ��У����㵱��ҵ�����·�����Ϣ������ھ������磺2012����Ϣ������ھ����㡶2012�꼯����Ϣ������ָ��ͳ�ƿھ�������ͳ����Ϣ������֮�ͣ��ó�����ֵ������ֵ��
##~   Step2.02049�������û���Ա������01004�����ſͻ�����B�༯�ſͻ���03018�����Ÿ��˷�ͳ�����룩�ӿڰ��յ�����Ϣ������ͳ�ƿھ����ó����º����µķ�ͳ����Ϣ�����룻
##~   Step3.��Step1��Step2�е�ͳ����Ϣ������ͷ�ͳ����Ϣ��������ܵó���Ϣ�����룬�Ӷ��ֱ��γɵ���B����Ϣ�����뵱��ֵ������B����Ϣ����������ֵ��
##~   Step4.���㵱��ֵ������ֵ�Ļ��Ⱦ���ֵ�����������30%����Υ������"






##~   R300	��	02_���ſͻ�	IDC����������	"03017 ����ͳ������
##~   03018 ���Ÿ��˷�ͳ������"	IDC���������뻷�Ⱦ���ֵС�ڵ���50%	0.05		"Step1.03017������ͳ�����룩��03018�����Ÿ��˷�ͳ�����룩�ӿ��У�ҵ�����ͱ���Ϊ1190��IDC���ĵ��º���������֮�ͣ�
##~   Step2.����ֵ������ֵ�Ƚϡ�"

##~   ��ҵ������




##~   R302	��	02_���ſͻ�	����ͨ����������	"03017 ����ͳ������
##~   03018 ���Ÿ��˷�ͳ������"	����ͨ���������뻷�Ⱦ���ֵС�ڵ���50%	0.05		"Step1.03017������ͳ�����룩��03018�����Ÿ��˷�ͳ�����룩�ӿ��У�ҵ�����ͱ���Ϊ1330������ͨ���ĵ��º���������֮�ͣ�
##~   Step2.����ֵ������ֵ�Ƚϡ�"


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "
select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,2)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
select sum(bigint(income)) income from   G_S_03017_MONTH
where ent_busi_id = '1330'
and time_id = $op_month
union all
select sum(bigint(income)) income from   G_S_03018_MONTH
where ent_busi_id = '1330'
and time_id = $op_month
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
select sum(bigint(income)) income from   G_S_03017_MONTH
where ent_busi_id = '1330'
and time_id = $last_month
union all
select sum(bigint(income)) income from   G_S_03018_MONTH
where ent_busi_id = '1330'
and time_id = $last_month
   ) b
) bb
on 1=1
with ur
"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R302',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R286 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R307	��	02_���ſͻ�	�ƶ�OA����������	"03017 ����ͳ������ 03018 ���Ÿ��˷�ͳ������"	�ƶ�OA���������뻷�Ⱦ���ֵС�ڵ���50%


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select
aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1250')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1250')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1250')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1250')
   ) b
) bb
on 1=1

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R307',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R307 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}





##~   --R310	��	02_���ſͻ�	�ƶ�CRM����������	"03017 ����ͳ������ 03018 ���Ÿ��˷�ͳ������"	�ƶ�CRM���������뻷�Ⱦ���ֵС�ڵ���50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1280')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1280')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1280')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1280')
   ) b
) bb
on 1=1

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R310',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R310 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R313	��	02_���ſͻ�	�ƶ������浱��������	"03017 ����ͳ������ 03018 ���Ÿ��˷�ͳ������"	�ƶ������浱�������뻷�Ⱦ���ֵС�ڵ���50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1260')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1260')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1260')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1260')
   ) b
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R313',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R313 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}









##~   --R316	��	02_���ſͻ�	BlackBerry����������	"03017 ����ͳ������ 03018 ���Ÿ��˷�ͳ������"	BlackBerry���������뻷�Ⱦ���ֵС�ڵ���50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1230')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1230')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1230')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1230')
   ) b
) bb
on 1=1

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R316',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R316 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R319	��	02_���ſͻ�	��ҵ��վ����������	"03017 ����ͳ������ 03018 ���Ÿ��˷�ͳ������"	��ҵ��վ���������뻷�Ⱦ���ֵС�ڵ���50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "



select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1340')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1340')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1340')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1340')
   ) b
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R319',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R319 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



##~   --R322	��	02_���ſͻ�	�ֻ����䵱��������	"03017 ����ͳ������ 03018 ���Ÿ��˷�ͳ������"	�ֻ����䵱�������뻷�Ⱦ���ֵС�ڵ���50%


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1220')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1220')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1220')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1220')
   ) b
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R322',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R322 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



		  
##~   --R324	��	02_���ſͻ�	���Ų��ŵ���������	"03017 ����ͳ������ 03018 ���Ÿ��˷�ͳ������"	���Ų��ŵ��������뻷�Ⱦ���ֵС�ڵ���50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select
aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1120')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1120')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1120')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1120')
   ) b
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R324',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R324 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



##~   --R326	��	02_���ſͻ�	�ƶ�400����������	"03017 ����ͳ������ 03018 ���Ÿ��˷�ͳ������"	�ƶ�400���������뻷�Ⱦ���ֵС�ڵ���50%






   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1520')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1520')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1520')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1520')
   ) b
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R326',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R326 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R328	��	02_���ſͻ�	��������ֱ������������	"03017 ����ͳ������ 03018 ���Ÿ��˷�ͳ������"	��������ֱ�����������뻷�Ⱦ���ֵС�ڵ���50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "



select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1070')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1070')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1070')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1070')
   ) b
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R328',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R328 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R330	��	02_���ſͻ�	��ҵ���䵱��������	"03017 ����ͳ������ 03018 ���Ÿ��˷�ͳ������"	��ҵ���䵱�������뻷�Ⱦ���ֵС�ڵ���50%





   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "




select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1210')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1210')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1210')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1210')
   ) b
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R330',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R330 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R332	��	02_���ſͻ�	�̻��ܼ�������	03017 ����ͳ������	�̻��ܼ������뻷�Ⱦ���ֵС�ڵ���50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,2)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1560')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1560')
   ) b
) bb
on 1=1




"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R332',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R332 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R335	��	02_���ſͻ�	УѶͨ����������	"03017 ����ͳ������ 03018 ���Ÿ��˷�ͳ������"	УѶͨ���������뻷�Ⱦ���ֵС�ڵ���50%





   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,4)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1310')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1310')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1310')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1310')
   ) b
) bb
on 1=1




"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R335',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R335 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}





##~   --R337	��	02_���ſͻ�	M2M������	"03017 ����ͳ������  03018 ���Ÿ��˷�ͳ������"	M2M�����뻷�Ⱦ���ֵС�ڵ���50%





   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.dan_fee,bb.bef_fee,
     case when bb.bef_fee=0 then 1
          else decimal((aa.dan_fee-bb.bef_fee)*1.0/bb.bef_fee,10,2)
     end
  from
(
select
      value(sum(a.income),0) dan_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$op_month
      and ent_busi_id in('1241','1249')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$op_month
      and ent_busi_id in('1241','1249')
   ) a
 ) aa
inner join
(
select
      value(sum(b.income),0) bef_fee
  from
  (
    select sum(bigint(income)) income
     from bass1.g_s_03018_month
    where time_id=$last_month
      and ent_busi_id in('1241','1249')
    union all
    select sum(bigint(income)) income
     from bass1.g_s_03017_month
    where time_id=$last_month
      and ent_busi_id in('1241','1249')
   ) b
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R337',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R337 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}


##~   --R339	��	02_���ſͻ�	������Ϣ������ҵ����	22303 ���Ų�Ʒҵ�����»���	���¼�����Ϣ������ҵ�������Ⱦ���ֵС�ڵ���50%





   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select  dan_cnts,bef_cnts,
     case when bef_cnts=0 then 1
          else decimal((dan_cnts-bef_cnts)*1.0/bef_cnts,10,2)
     end
  from 
(
select value(sum(bigint(upmessage)+bigint(downmessage)),0) dan_cnts
 from bass1.G_S_22303_MONTH where time_id=$op_month
) a
inner join
(
select value(sum(bigint(upmessage)+bigint(downmessage)),0) bef_cnts
 from bass1.G_S_22303_MONTH where time_id=$op_month
) b
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R339',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R339 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




	return 0
}
