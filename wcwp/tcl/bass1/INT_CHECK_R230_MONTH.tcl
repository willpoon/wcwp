######################################################################################################
#�������ƣ�	INT_CHECK_R230_MONTH.tcl
#У��ӿڣ�	21020
#��������: MONTH
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2011-05-26 
#�����¼��
#�޸���ʷ: 1. ������1) ����������2)����������

#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month				
			 set last_month [GetLastMonth [string range $op_month 0 5]]
       puts $last_month
        #��Ȼ��
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        
        #������
        global app_name
        set app_name "INT_CHECK_R230_MONTH.tcl"




###########################################################
#R230			����	��	05_��������	����������Ӫ��Ʒ��Ϊ��ͨ�ƶ����ƶ�����Ҳ����Ϊ��ͨ	����������Ӫ��Ʒ��Ϊ��ͨ�ƶ������������ƶ������ǰ��λ��(130��131��132��155��156��185��186��145��	0.05	��򡢺��ϳ���	

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R230') 
 	  "        

	  exec_sql $sqlbuf


	set sql_buff "
			select count(0) from   G_I_21020_MONTH
				where
				COMP_BRAND_ID = '021000' and 
				 substr(COMP_PRODUCT_NO,length(trim(COMP_PRODUCT_NO))-10,3)  not 
				in ('130','131','132','155','156','185','186','145')   
				and time_id  = $op_month 
		"

	 chkzero2 $sql_buff "R230 У�鲻ͨ��!"
	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R230',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff
  
  

###########################################################
#R231			����	��	05_��������	����������Ӫ��Ʒ��Ϊ�����ƶ����ƶ�����Ҳ����Ϊ����	����������Ӫ��Ʒ��Ϊ�����ƶ������������ƶ������ǰ��λ��(133��153��180��189��	0.05	��򡢺��ϳ���	

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R231') 
 	  "        

	  exec_sql $sqlbuf

	set sql_buff "
			select count(0) from   G_I_21020_MONTH
			where
			COMP_BRAND_ID = '031000' and 
			  substr(COMP_PRODUCT_NO,length(trim(COMP_PRODUCT_NO))-10,3)  not 
			in ('133','153','180','189') 
			and time_id = $op_month 
		"

	 chkzero2 $sql_buff "R231 У�鲻ͨ��!"
	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R231',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff
  

###########################################################
#R232			����	��	05_��������	ĩ��ͨ���ڱ����ڣ����������Ż���Ŵ������������ܶ�Ϊ0	ĩ��ͨ����ͳ�����ڣ�����ͨ�����������������������������߲���ͬʱΪ0	0.05		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R232') 
 	  "        

	  exec_sql $sqlbuf


	set sql_buff "
			select count(0) from   G_I_21020_MONTH
			where char(TIME_ID) = substr(COMP_LAST_DATE,1,6)
			and CALL_COUNTS = '0'
			and SMS_COUNTS = '0'
			and MMS_COUNTS = '0'
			and time_id = $op_month 
		"

	 chkzero2 $sql_buff "R232 У�鲻ͨ��!"
	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R232',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff
  
#R232 ADJ:
#	update G_I_21020_MONTH 
#	set CALL_COUNTS = char(int(rand(1)*5+1))
#	where (
#	char(TIME_ID) = substr(COMP_LAST_DATE,1,6)
#	and CALL_COUNTS = '0'
#	and SMS_COUNTS = '0'
#	and MMS_COUNTS = '0'
#	and time_id = $op_month )
###########################################################

#R233			����	��	05_��������	������ܼ������"��ͨ�ƶ������û���"�ͻ��ܽӿ��е���ͨ�ƶ������ͻ�����ƫ��	������ܼ������"��ͨ�ƶ������û���"�ͻ��ܽӿ��е���ͨ�ƶ������ͻ�����ƫ���3��	0.05		
 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R233') 
 	  "        

	  exec_sql $sqlbuf



set sql_buff "
		insert into BASS1.G_I_21020_MONTH_calendar
		select distinct char(time_id ),substr(char(time_id ),1,4)||'-'||substr(char(time_id ),5,2)||'-'||substr(char(time_id ),7,2)
		from G_S_22073_DAY
		where char(time_id)
		not in (select date_seq from 
				G_I_21020_MONTH_calendar
				)
"
exec_sql $sql_buff 


set sql_buff "

 				select count(distinct COMP_PRODUCT_NO)  from    
				table (
				select COMP_PRODUCT_NO,COMP_LAST_DATE
				from         G_I_21020_MONTH
				where COMP_BRAND_ID = '021000' 
				and time_id = $last_month
				and  COMP_PRODUCT_NO not in (
				select COMP_PRODUCT_NO
				from         G_I_21020_MONTH
				where COMP_BRAND_ID = '021000' 
				and time_id = $op_month
				)
				) a , (select * from  BASS1.G_I_21020_MONTH_calendar where date_seq like '$op_month%') b 
				where 
				(days(date(date2))-
				 days(date(substr(COMP_LAST_DATE,1,4)||'-'
				 ||substr(COMP_LAST_DATE,5,2)||'-'
				 ||substr(COMP_LAST_DATE,7,2))))=90				   
"
  set RESULT_VAL1 0
  set RESULT_VAL1 [get_single $sql_buff]
 


set sql_buff "
			 
			 select count(0) from   
			( 
			 select COMP_PRODUCT_NO,COMP_LAST_DATE
			 from G_I_21020_MONTH where time_id = $last_month and  COMP_BRAND_ID = '021000' 
			) a 
			,
			(
			 select COMP_PRODUCT_NO,COMP_BEGIN_DATE
			 from G_I_21020_MONTH where time_id = $op_month and  COMP_BRAND_ID = '021000' 
			) b 
			,BASS1.G_I_21020_MONTH_calendar c
			where a.COMP_PRODUCT_NO  = b.COMP_PRODUCT_NO 
			and 
			(days(date(date2))-
			 days(date(substr(COMP_LAST_DATE,1,4)||'-'||substr(COMP_LAST_DATE,5,2)||'-'||substr(COMP_LAST_DATE,7,2))))=90
			 and  char(COMP_BEGIN_DATE  ) > char(date_seq)
			  and c.date_seq like '$op_month%'
"
  set RESULT_VAL2 0
  set RESULT_VAL2 [get_single $sql_buff]
 

set sql_buff "
		 select sum(bigint(UNION_MOBILE_LOST_CNT))
from bass1.G_S_22073_DAY
where time_id/100 = $op_month
"
  set RESULT_VAL3 0
  set RESULT_VAL3 [get_single $sql_buff]
 
set sql_buff "
values (($RESULT_VAL1+$RESULT_VAL2)*1.000/$RESULT_VAL3 - 1)
"	 
  set RESULT_VAL 0
  set RESULT_VAL [get_single $sql_buff]

	#���Ϸ���: 0 - �������� ����0 - ����
 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL>0.03||$RESULT_VAL<-0.03 } {
		set grade 2
	  set alarmcontent "R233 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R233',($RESULT_VAL1+$RESULT_VAL2),$RESULT_VAL3,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff
  


###########################################################
#R234			����	��	05_��������	������ܼ������"�����ƶ������û���"�ͻ��ܽӿ��еĵ����ƶ������ͻ�����ƫ��	������ܼ������"�����ƶ������û���"�ͻ��ܽӿ��еĵ����ƶ������ͻ�����ƫ���3��	0.05		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R234') 
 	  "        

	  exec_sql $sqlbuf





set sql_buff "
 				select count(distinct COMP_PRODUCT_NO)  from    
				table (
				select COMP_PRODUCT_NO,COMP_LAST_DATE
				from         G_I_21020_MONTH
				where COMP_BRAND_ID = '031000' 
				and time_id = $last_month
				and  COMP_PRODUCT_NO not in (
				select COMP_PRODUCT_NO
				from         G_I_21020_MONTH
				where COMP_BRAND_ID = '031000' 
				and time_id = $op_month
				)
				) a , (select * from  BASS1.G_I_21020_MONTH_calendar where date_seq like '$op_month%') b 
				where 
				(days(date(date2))-
				 days(date(substr(COMP_LAST_DATE,1,4)||'-'
				 ||substr(COMP_LAST_DATE,5,2)||'-'
				 ||substr(COMP_LAST_DATE,7,2))))=90				   
"
  set RESULT_VAL1 0
  set RESULT_VAL1 [get_single $sql_buff]
 


set sql_buff "
			 
			 select count(0) from   
			( 
			 select COMP_PRODUCT_NO,COMP_LAST_DATE
			 from G_I_21020_MONTH where time_id = $last_month and  COMP_BRAND_ID = '031000' 
			) a 
			,
			(
			 select COMP_PRODUCT_NO,COMP_BEGIN_DATE
			 from G_I_21020_MONTH where time_id = $op_month and  COMP_BRAND_ID = '031000' 
			) b 
			,BASS1.G_I_21020_MONTH_calendar c
			where a.COMP_PRODUCT_NO  = b.COMP_PRODUCT_NO 
			and 
			(days(date(date2))-
			 		days(date(substr(COMP_LAST_DATE,1,4)||'-'||substr(COMP_LAST_DATE,5,2)||'-'||substr(COMP_LAST_DATE,7,2))))
			 =90
			 and  char(COMP_BEGIN_DATE  ) > char(date_seq)
			  and c.date_seq like '$op_month%'
"
  set RESULT_VAL2 0
  set RESULT_VAL2 [get_single $sql_buff]
 

set sql_buff "
		 select sum(bigint(TEL_MOBILE_LOST_CNT))
from bass1.G_S_22073_DAY
where time_id/100 = $op_month
"
  set RESULT_VAL3 0
  set RESULT_VAL3 [get_single $sql_buff]
 
set sql_buff "
values (($RESULT_VAL1+$RESULT_VAL2)*1.000/$RESULT_VAL3 - 1)
"	 
  set RESULT_VAL 0
  set RESULT_VAL [get_single $sql_buff]

	#���Ϸ���: 0 - �������� ����0 - ����
 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL>0.03||$RESULT_VAL<-0.03 } {
		set grade 2
	  set alarmcontent "R234 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R234',($RESULT_VAL1+$RESULT_VAL2),$RESULT_VAL3,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff


      	
	return 0
}
