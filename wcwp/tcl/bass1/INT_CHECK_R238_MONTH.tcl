######################################################################################################
#�������ƣ�	INT_CHECK_R238_MONTH.tcl
#У��ӿڣ�	03017
#��������: MONTH
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2011-05-26 
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
        
        #������
        global app_name
        set app_name "INT_CHECK_R238_MONTH.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R238','R239','R240','R241'
 	  				,'R242','R243','R246','R247','R250','R251','R252','R253','R254')
			"

		exec_sql $sql_buff


#R238			����	��	09_��������	��06022�е�������ʶ���������06021��	06022�е�ʵ��������ʶ����06021��ʵ��������ʶ��	0.05		


set sql_buff "
	select count(*) from bass1.g_i_06022_month
	where channel_id not in 
	(select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month )
	  and time_id =$op_month
"

chkzero2 $sql_buff "R238 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R238',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff
  


#R239			����	��	09_��������	��06021��������Ӫ�ķ������������������ʶ���������06022��	��06021��������Ӫ�ķ������������������ʵ��������ʶӦ������06022��	0.05		



set sql_buff "
select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1.g_i_06022_month where time_id =$op_month)
  and time_id =$op_month
  and channel_type <>'3'
  and channel_status='1'
"

chkzero2 $sql_buff "R239 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
  puts $RESULT_VAL
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R239',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff



#R240			����	��	09_��������	��06023�е�������ʶ���������06021��	06023�е�ʵ��������ʶӦ������06021��ʵ��������ʶ��	0.05		


set sql_buff "
select count(*) from bass1.g_i_06023_month
where channel_id not in 
(select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month )
  and time_id =$op_month
"

chkzero2 $sql_buff "R240 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	puts $RESULT_VAL

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R240',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff

  

#R241			����	��	09_��������	��06021��������Ӫ��������ʶ���������06023��	06021��������Ӫ��������ʵ��������ʶ��Ӧ������06023��ʵ��������ʶ��	0.05		



set sql_buff "
select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1.g_i_06023_month where time_id =$op_month)
  and time_id =$op_month
  and channel_status='1'
"

chkzero2 $sql_buff "R241 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R241',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff


  
#R242			����	��	09_��������	��22061�е�������ʶ���������06021��	22061�е�ʵ��������ʶ��Ӧ������06021��ʵ��������ʶ��	0.05		




set sql_buff "

select count(*) from bass1.g_s_22061_month
where channel_id not in 
(select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month )
  and time_id =$op_month
  
"

chkzero2 $sql_buff "R242 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R242',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff



#R243			����	��	09_��������	��06021��������Ӫ�ķ������������������ʶ���������22061��	06021��������Ӫ�ķ������������������ʵ��������ʶӦ������22061��	0.05		



set sql_buff "


select count(*) from bass1.g_i_06021_month
where channel_id not in
(select distinct channel_id from bass1.g_s_22061_month where time_id =$op_month)
  and time_id =$op_month
  and channel_type<>'3'
  and channel_status='1'
  
"

chkzero2 $sql_buff "R243 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R243',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff



  
#R246			����	��	09_��������	��22063�е�������ʶ���������06021�з���Ӫ��������ʶ��	22063�е�ʵ��������ʶӦ������06021�з���Ӫ����ʵ��������ʶ��	0.05		



set sql_buff "


select count(*) from bass1.g_s_22063_month
where channel_id not in
(select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month and channel_type<>'1')
  and time_id =$op_month
  
"

chkzero2 $sql_buff "R246 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R246',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff



  
#R247			����	��	09_��������	22063�в��ܴ�����Ӫ����������ʶ	22063�в�Ӧ������Ӫ����������ʶ	0.05		




set sql_buff "


select count(*) from bass1.g_s_22063_month
where channel_id in
(select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month and channel_type='1')
  and time_id =$op_month

  
"

chkzero2 $sql_buff "R247 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R247',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff





#R250			����	��	09_��������	06021�л�������[��ͨ��Ӫ��+Ʒ�Ƶ�+�콢��] = ʵ����������[��Ӫ��+ί�о�Ӫ��]	06021�л�������Ϊ��ͨ��Ӫ����Ʒ�Ƶ���콢���ʵ����������Ӧ����ʵ����������Ϊ��Ӫ����ί�о�Ӫ����ʵ����������	0.05		

#select channel_id  from    
#BASS1.G_I_06021_MONTH 
#where time_id = 201105 and CHANNEL_STATUS = '1' 
#and   channel_type in ('1','2')
#except
#select channel_id  val1 from    
#BASS1.G_I_06021_MONTH 
#where time_id = 201105 and CHANNEL_STATUS = '1' 
#and CHANNEL_B_TYPE in ('1','2','3')

#02		ʵ����������	���ֶν�ȡ���·��ࣺ
#1����Ӫ��
#2��ί�о�Ӫ��
#3������������
#4��24Сʱ����Ӫҵ��	
#
#11		������������	���ֶν�ȡ���·��ࣺ
#1����ͨ��Ӫ������Ӫ����ί�о�Ӫ����д��
#2��Ʒ�Ƶ꣨��Ӫ����ί�о�Ӫ����д��
#3���콢�꣨��Ӫ����ί�о�Ӫ����д��
#4��24Сʱ����Ӫҵ��
#5��ָ��רӪ�꣨������������д��
#6����Լ����㣨������������д��
#CHANNEL_ID
#100000930                               
#100000845                               
#10320010                                
#100000844                               
#�⼸����������Ӫҵ���еĿ��г�ֵ�㡣105001	10500101	��Ӫ�����г�
#��������Ӫ�����棬ʵ���������� = 1 ��Ӫ�� 
#������������ = 1 
#update BASS1.G_I_06021_MONTH  
#set CHANNEL_B_TYPE = '1'
#where time_id = 201105 
#and CHANNEL_ID in 
#(
#'100000844'
#,'100000845'
#,'100000930'
#,'10320010'   
#)


set sql_buff "

SELECT  ( val1 - val2 ) val 
from 
(
select count(0)  val1 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and CHANNEL_B_TYPE in ('1','2','3')
)  a
,(
select count(0) val2 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and   channel_type in ('1','2')
) b 

"

chkzero2 $sql_buff "R250 not pass! "



set sql_buff "
SELECT   val1 , val2 
from 
(
select count(0)  val1 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and CHANNEL_B_TYPE in ('1','2','3')
)  a
,(
select count(0) val2 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and   channel_type in ('1','2')
) b 

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   
   
	set RESULT_VAL 0
	set RESULT_VAL [format "%.5f" [expr $RESULT_VAL1 - $RESULT_VAL2  ]]
	puts $RESULT_VAL
	
		if {  ${RESULT_VAL} != 0 } {
		set grade 2
	        set alarmcontent " R250 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }
		 
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R250',$RESULT_VAL1,$RESULT_VAL2,0,0) 
		"
		exec_sql $sql_buff



#R251			����	��	09_��������	06021�л�������[24Сʱ����Ӫҵ��] = ʵ����������[24Сʱ������]	06021�л�������Ϊ24Сʱ����Ӫҵ�����ʵ����������Ӧ����ʵ����������Ϊ24Сʱ��������ʵ����������	0.05		


set sql_buff "

SELECT  ( val1 - val2 ) val 
from 
(
select count(0)  val1 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and CHANNEL_B_TYPE in ('4')
)  a
,(
select count(0) val2 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and   channel_type in ('4')
) b 

"

chkzero2 $sql_buff "R251 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R251',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff







#R252			����	��	09_��������	06021�л�������[ָ��רӪ��+��Լ�����] = ʵ����������[����������]	06021�л�������Ϊָ��רӪ�����Լ������ʵ����������Ӧ����ʵ����������Ϊ����������ʵ����������	0.05		



set sql_buff "

SELECT  ( val1 - val2 ) val 
from 
(

select count(0) val1 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and CHANNEL_B_TYPE in  ('5','6')

)  a
,(

select count(0) val2 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and   channel_type in ('3')
) b 

"


chkzero2 $sql_buff "R252 not pass! "



set sql_buff "
SELECT  val1 , val2 
from 
(

select count(0) val1 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and CHANNEL_B_TYPE in  ('5','6')

)  a
,(

select count(0) val2 from    
BASS1.G_I_06021_MONTH 
where time_id = $op_month and CHANNEL_STATUS = '1' 
and   channel_type in ('3')
) b 

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   
   
	set RESULT_VAL 0
	set RESULT_VAL [format "%.5f" [expr $RESULT_VAL1 - $RESULT_VAL2  ]]
	puts $RESULT_VAL
	
		if {  ${RESULT_VAL} != 0 } {
		set grade 2
	        set alarmcontent " R252 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }
		 
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R252',$RESULT_VAL1,$RESULT_VAL2,0,0) 
		"
		exec_sql $sql_buff








#R253			����	��	09_��������	06021�з����������������Ǽ�Ϊ��	06021�з�����������������Ǽ�ӦΪ��	0.05		



set sql_buff "

select count(*) from
(
select distinct channel_star from bass1.g_i_06021_month
where time_id =$op_month
  and channel_status='1'
  and channel_type<>'3'
) aa
where channel_star <>''
"

chkzero2 $sql_buff "R253 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R253',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff





#R254			����	��	09_��������	06021�����������������Ǽ����ܵ��ڿ�	06021�����������������Ǽ����ܵ��ڿ�	0.05		
 
 

set sql_buff "


 select count(*) from
(
select distinct channel_star from bass1.g_i_06021_month
where time_id =$op_month
  and channel_status='1'
  and channel_type='3'
) aa
where channel_star=''

"

chkzero2 $sql_buff "R254 not pass! "

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R254',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff


  	
  
      	
	return 0
}
