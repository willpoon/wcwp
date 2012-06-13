######################################################################################################
#�������ƣ�	INT_CHECK_R262_MONTH.tcl
#У��ӿڣ�	21006 21003 21009
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
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        #������
        global app_name
        set app_name "INT_CHECK_R262_MONTH.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R262')
			"

		exec_sql $sql_buff



##~   --~   R262	��	03_������־	����ͨ������	"21003 GSM��ͨ����ҵ����ʹ��
##~   --~   21006 ����������ҵ����ʹ��
##~   --~   21009 ������VPMNҵ��ʹ��"	����ͨ��������0	0.05	


set sql_buff "


select (
select count(0) from G_S_21003_MONTH where time_id = $op_month and bigint(CALL_COUNTS) < 0
) + (
select count(0) from G_S_21006_MONTH where time_id = $op_month and bigint(CALL_COUNTS) < 0
) + (
select count(0) from G_S_21009_DAY where time_id/100 = $op_month and bigint(CALL_COUNTS) < 0
)  ALL_CALL_COUNTS from bass2.dual
"

chkzero2 $sql_buff "R262 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R262',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff
  



##~   --~   R270	��	11_�ʷѶ���	�����û�����ѡ��һ������ʷ��ײ�	"02020 �û�ѡ������ʷ��ײ�
##~   --~   02004 �û�
##~   --~   02008 �û�״̬"	�����û�����ѡ��һ������ʷ��ײ�	0.05	




set sql_buff "

select count(0)
from                       
(select user_id from int_02004_02008_month_$op_month where USERTYPE_ID  in ('1010') and TEST_FLAG = '0' )  a 
left join (select * from G_I_02020_MONTH where time_id = $op_month and bigint(PROD_VALID_DATE)/100 <= $op_month ) b on a.user_id = b.user_id
where b.BASE_PROD_ID is null
with ur
"

chkzero2 $sql_buff "R270 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R270',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff
  


##~   R271	��	11_�ʷѶ���	ѡ��ȫ��ͨȫ��ͳһ�����ʷ��ײ͵��û���	"02020 �û�ѡ������ʷ��ײ�
##~   02022 �û�ѡ��ȫ��ͨȫ��ͳһ�����ʷ��ײ�"	02020��ͳ������ѡ��ȫ��ͨͳһ�ʷ��ײ͵������û�����02022��ͳ������ѡ��ȫ��ͨͳһ�ʷ��ײ͵��û���	0.05	

##~   "��һ����ȡ02020���ײ���Ч����С�ڵ�����ĩ���һ����ײͱ�ʶ�ڡ�ȫ��ͨȫ��ͳһ�ʷѻ����ײͱ�ʶ��ά���е��û����������û����û�״̬���޳���ĩ�����Ͳ����û���
##~   �ڶ�����ȡ02022��ĩ�������ײ���Ч����С�ڵ�����ĩ���û����������û����޳������û���
##~   ���������Ƚ�ǰ��������Ƿ���ȡ�"


set sql_buff "

select (  
select count(0)
from 
(select * from G_I_02020_MONTH 
			where time_id = $op_month and bigint(PROD_VALID_DATE)/100 <= $op_month
			and  BASE_PROD_ID in (select NEW_PKG_ID from  bass1.DIM_QW_QQT_PKGID )
			) a
,(
select user_id from int_02004_02008_month_$op_month
where USERTYPE_ID not in ('2010','2020','2030','9000') and  TEST_FLAG = '0') c 
where a.USER_ID = c.USER_ID
) -
(
select count(0)
from 
(select * from G_I_02022_DAY where time_id = $this_month_last_day and bigint(VALID_DT) / 100 <= $op_month ) a 
,(
select user_id from int_02004_02008_month_$op_month
where USERTYPE_ID not in ('2010','2020','2030','9000') and  TEST_FLAG = '0') c 
where a.USER_ID = c.USER_ID
) from bass2.dual with ur
"

chkzero2 $sql_buff "R271 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R271',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff
  





##~   R272	��	11_�ʷѶ���	ѡ��ȫ��ͨȫ��ͳһ�ʷ�ר�������ʷ��ײ͵��û���	"02021 �û�ѡ������ʷ��ײ�
##~   02023 �û�ѡ��ȫ��ͨר�������ʷ��ײ�"	02021��ͳ������ѡ��ȫ��ͨȫ��ͳһ�ʷ�ר�������ʷ��ײ͵������û�����02023��ͳ������ѡ��ȫ��ͳһ�ʷ�ר�������ʷ��ײ͵��û���	0.05	

##~   G_I_02021_MONTH

##~   G_I_02023_DAY

##~   "��һ����ȡ02021���ײ���Ч����С�ڵ�����ĩ���ҵ����ײͱ�ʶ�ڡ�ȫ��ͨȫ��ͳһ�ʷѵ����ײͱ�ʶ��ά���е��û����������û����û�״̬���޳���ĩ�����Ͳ����û���
##~   �ڶ�����ȡ02023��ĩ�������ײ���Ч����С�ڵ�����ĩ���û����������û����޳������û���
##~   ���������Ƚ�ǰ��������Ƿ���ȡ�"




set sql_buff "


select (
select count(0) 
from
(select *
from G_I_02021_MONTH where time_id = $op_month and bigint(PROD_VALID_DATE)/100 <= $op_month
and  OVER_PROD_ID in (select NEW_PKG_ID from  bass1.DIM_QW_QQT_PKGID where OLD_PKG_ID like 'QW_QQT_DJ%')
) a ,int_02004_02008_month_$op_month b 
where a.user_id = b.user_id
and USERTYPE_ID not in ('2010','2020','2030','9000')
and b.TEST_FLAG = '0'
) -
(
select count(0) 
from (
select * from G_I_02023_DAY where time_id = $this_month_last_day
) a  ,int_02004_02008_month_$op_month b 
where a.user_id = b.user_id
and b.USERTYPE_ID not in ('2010','2020','2030','9000')
and b.TEST_FLAG = '0'
) from bass2.dual with ur


"

chkzero2 $sql_buff "R272 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R272',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  
  
  
  
  
  
  
  
  
  
##~   R278	��	09_������Ӫ	ʵ������������Ϣ���¹�ϵ	"06021 ʵ������������Ϣ
##~   06035 ʵ������������Ϣ����������"	06021�е�ʵ����������Ӧ����06035��ĩ�����е�ʵ����������	0.05	

##~   "��һ����ȡ06021����Ч����С�ڵ���ͳ����ĩ��ʧЧ���ڴ���ͳ����ĩ��������״̬Ϊ������Ӫ��ʵ��������ʶ������
##~   �ڶ�������06035��ĩ�����У�ȡ����״̬Ϊ������Ӫ��ʵ��������ʶ������
##~   ���������Ƚϵ�һ���͵ڶ����Ľ����"





set sql_buff "

select (  
select count(0) from G_I_06021_MONTH where time_id = $op_month
and CHANNEL_STATUS = '1'
) -
(
select count(0)
from
(
select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
from G_A_06035_DAY a
where time_id / 100 <= $op_month
) t where t.rn =1  and CHNL_STATE = '1'
) from bass2.dual with ur


"

chkzero2 $sql_buff "R278 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R278',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  
  
  
  


##~   R279	��	09_������Ӫ	ʵ������������������Ϣ���¹�ϵ	"06022 ʵ������������������Ϣ
##~   06036 ʵ������������������Ϣ����������"	06022�е�ʵ����������Ӧ����06036��ĩ�����е�ʵ����������	0.05	

##~   "��һ����ȡ06022����Ч����С�ڵ���ͳ����ĩ��ʧЧ���ڴ���ͳ����ĩ����ʵ��������ʶ������
##~   �ڶ�����ȡ06036��ĩ�����е�ʵ��������ʶ������
##~   ���������Ƚϵ�һ���͵ڶ����Ľ����"


##~   G_I_06022_MONTH
##~   G_A_06036_DAY



set sql_buff "

select (
select count(0)
from 
(				
select * from G_I_06022_MONTH where time_id = $op_month ) a,
(select * from G_I_06021_MONTH where time_id = $op_month
and CHANNEL_STATUS = '1')  b
where a.channel_id = b.channel_id 
) -
(
select count(0)
from 
(
	select *
	from
	(
	select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
	from G_A_06036_DAY a
	where time_id / 100 <= $op_month
	) t where t.rn =1  
) a,(select * from G_I_06021_MONTH where time_id = $op_month and CHANNEL_STATUS = '1')  b
where a.channel_id = b.channel_id
) from bass2.dual with ur



"

chkzero2 $sql_buff "R279 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R279',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  
  




##~   R280	��	09_������Ӫ	ʵ��������Դ������Ϣ���¹�ϵ	"06023 ʵ��������Դ������Ϣ
##~   06037 ʵ��������Դ������Ϣ����������"	06023�е�ʵ����������Ӧ����06037��ĩ�����е�ʵ����������	0.05	



##~   G_I_06023_MONTH
##~   G_A_06037_DAY


##~   "��һ����ȡ06023����Ч����С�ڵ���ͳ����ĩ��ʧЧ���ڴ���ͳ����ĩ����ʵ��������ʶ������
##~   �ڶ�����ȡ06037��ĩ�����е�ʵ��������ʶ������
##~   ���������Ƚϵ�һ���͵ڶ����Ľ����"




set sql_buff "


select (
select count(0)
from 
(select * from G_I_06023_MONTH a  where time_id = $op_month ) a
,(select * from G_I_06021_MONTH a where time_id = $op_month and CHANNEL_STATUS = '1')  b
where a.CHANNEL_ID = b.CHANNEL_ID 

) - (
select count(0) from 
(
		select *
		from
		(
		select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
		from G_A_06037_DAY a
		where time_id / 100 <= $op_month
		) t where t.rn =1  
) a ,(select * from G_I_06021_MONTH a where time_id = $op_month and CHANNEL_STATUS = '1')  b
where a.CHANNEL_ID = b.CHANNEL_ID 
) from bass2.dual with ur


"

chkzero2 $sql_buff "R280 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R280',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  
  


##~   --~   R282	��	10_���ּƻ�	��ȫ��ͨƷ�ƵĿͻ���Ӧ�ϴ�Ʒ�ƽ�������	02006 �û��������	��ȫ��ͨƷ�ƵĿͻ���Ӧ�ϴ�Ʒ�ƽ�������	0.05	

##~   ��02006��ĩ�����У�ȡƷ�ƽ�������>0���û����ж��Ƿ�������û����пͻ�Ʒ�ơ�ȫ��ͨ�ķǲ����û��У������ڣ���Υ���ù���




set sql_buff "


select sum(points) points
from (
select brand_id,sum(bigint(MONTH_QQT_POINTS)) points
from ( select user_id,brand_id  from bass1.int_02004_02008_month_$op_month ) a 
, (select * from G_I_02006_MONTH where TIME_ID  = $op_month ) b 
where a.user_id = b.user_id 
group by brand_id
) t where brand_id in ('2','3')



"

chkzero2 $sql_buff "R282 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R282',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  
  

##~   --~   R283	��	10_���ּƻ�	��ǰ�ɶһ����ֲ���Ϊ��ֵ	02006 �û��������	��ǰ�ɶһ����ֲ���Ϊ��ֵ	0.05	




set sql_buff "


select count(0)
 from G_I_02006_MONTH 
where bigint(CONVERTIBLE_POINTS) < 0                
and time_id = $op_month
with ur

"

chkzero2 $sql_buff "R283 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R283',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  
  
  
	return 0
}
