######################################################################################################
#�������ƣ�	INT_CHECK_GRP_ORD_DAY.tcl
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
        ##~   set curr_month 201207
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        ##~   set last_month 201206		
        ##~   set last_month [GetLastMonth [string range $op_month 0 5]] op_month����curr_month
        set last_month [GetLastMonth [string range $curr_month 0 5]]
		
        #������
        global app_name
        set app_name "INT_CHECK_GRP_ORD_DAY.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$timestamp and rule_code in (
					 'R265'
					,'R286'
					,'R287'
					,'R288'
					,'R289' -- δУ��
					,'R290' -- δУ��
					,'R291'
					,'R301' -- δУ��
					,'R303'
					,'R304'
					,'R305'
					,'R306'
					,'R308'
					,'R309'
					,'R311'
					,'R312'
					,'R314'
					,'R315'
					,'R317'
					,'R318'
					,'R320'
					,'R321'
					,'R323'
					,'R325'
					,'R327'
					,'R329'
					,'R331'
					,'R333'
					,'R334'
					,'R336'
					,'R338'
					)
			"

		exec_sql $sql_buff





##~   --~   R265	��	03_������־	��ҵ���ض��Ż����еġ�������롱���ڼ��ſͻ��˿���Դʹ������ӿڵġ���ҵӦ�ô���ȫ�롱	"04016 ��ҵ���ض��Ż���
##~   --~   22036 ���ſͻ��˿���Դʹ�����"	��ҵ���ض��Ż����еġ�������롱���ڼ��ſͻ��˿���Դʹ������ӿڵġ���ҵӦ�ô���ȫ�롱��	0.05	

##~   "Step1.04016����ҵ���ض��Ż������ӿ��з���״̬Ϊ0���ɹ����ġ�������롱���ϣ�
##~   Step2.22036�����ſͻ��˿���Դʹ��������ӿ��н�ֹ��ͳ������ĩҵ������=1����ҵ���ض��š��ġ���ҵӦ�ô���ȫ�롱���ϣ�
##~   Step3.����Step1�Ƿ���ڼ���Step2�С�"



set sql_buff "


select count(0)
from table(

select distinct  SERV_CODE from G_S_04016_DAY where time_id / 100 = $curr_month and SEND_STATUS = '0'
except
                        select distinct APP_LENCODE from 
                        (
                                        select t.*
                                        ,row_number()over(partition by EC_CODE,APP_LENCODE,APNCODE,BUSI_NAME order by time_id desc ) rn 
                                        from 
                                        G_A_22036_DAY  t
										where  time_id / 100 <= $curr_month
                          ) a
                        where rn = 1    and OPERATE_TYPE = '1'
										And bigint(OPEN_DATE)/100 <= $curr_month
                        
) a
with ur
"

chkzero2 $sql_buff "R265 not pass!"

	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R265',$RESULT_VAL,0,0,0) 
		"
		exec_sql $sql_buff
  


##~   R286	��	02_���ſͻ�	A�༯�ſͻ�������	01004 ���ſͻ�	A�༯�ſͻ������������±䶯�� �� 30%	0.05	

##~   ͳ�Ƶ��º������У��ֱ�ͳ��01004�����ſͻ����ӿڣ����ͻ�״̬���ͱ��롱Ϊ20���������������ż�ֵ������롱in(4��5)�ļ��ſͻ���ʶ������ȥ�ء����㵱�»���ֵ����>30%����Υ������

##~   ͳ�Ƶ��º������У��ֱ�ͳ��01004�����ſͻ����ӿڣ����ͻ�״̬���ͱ��롱Ϊ20���������������ż�ֵ������롱in(6��7)�ļ��ſͻ���ʶ������ȥ�ء����㵱�»���ֵ����>30%����Υ������





   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0

	set sql_buff "
select 
a.curr_cnts
,b.bef_cnts
,case when bef_cnts=0 then 1
	  else decimal((curr_cnts-bef_cnts)*1.0/bef_cnts,10,4)
 end
from table(
select case when ENT_SCALE_ID in ('4','5') then 'A' 
			 when ENT_SCALE_ID in('6','7') then 'B' 
			 else 'O' end type,count(0) curr_cnts
from (
        select enterprise_id,ENT_SCALE_ID from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_01004_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	and CUST_STATU_TYP_ID = '20'
) o
where ENT_SCALE_ID in ('4','5')
group by case when ENT_SCALE_ID in ('4','5') then 'A' 
				when ENT_SCALE_ID in('6','7') then 'B' 
				else 'O' end
) a,
table(select case when ENT_SCALE_ID in ('4','5') then 'A' 
			 when ENT_SCALE_ID in('6','7') then 'B' 
			 else 'O' end type
			 ,count(0) bef_cnts
from (
        select enterprise_id,ENT_SCALE_ID from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_01004_DAY  t
											where time_id/100 <= $last_month
			  ) a
			where rn = 1	and CUST_STATU_TYP_ID = '20'
) o
where ENT_SCALE_ID in ('4','5')
group by case when ENT_SCALE_ID in ('4','5') then 'A' 
				when ENT_SCALE_ID in('6','7') then 'B' 
				else 'O' end) b
where a.type = b.type
"

   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R286',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.3 } {
                set grade 2
                set alarmcontent " R286 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}


##~   R287	��	02_���ſͻ�	B�༯�ſͻ�������	01004 ���ſͻ�	B�༯�ſͻ������������±䶯�� �� 30%	0.05	


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0

	set sql_buff "
select
 a.curr_cnts
,b.bef_cnts
,case when bef_cnts=0 then 1
	  else decimal((curr_cnts-bef_cnts)*1.0/bef_cnts,10,4)
 end
from table(
select case when ENT_SCALE_ID in ('4','5') then 'A' 
			 when ENT_SCALE_ID in('6','7') then 'B' 
			 else 'O' end type,count(0) curr_cnts
from (
        select enterprise_id,ENT_SCALE_ID from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_01004_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	and CUST_STATU_TYP_ID = '20'
) o
where ENT_SCALE_ID in('6','7')
group by case when ENT_SCALE_ID in ('4','5') then 'A' 
				when ENT_SCALE_ID in('6','7') then 'B' 
				else 'O' end
) a,
table(select case when ENT_SCALE_ID in ('4','5') then 'A' 
			 when ENT_SCALE_ID in('6','7') then 'B' 
			 else 'O' end type
			 ,count(0) bef_cnts
from (
        select enterprise_id,ENT_SCALE_ID from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_01004_DAY  t
											where time_id/100 <= $last_month
			  ) a
			where rn = 1	and CUST_STATU_TYP_ID = '20'
) o
where ENT_SCALE_ID in ('6','7')
group by case when ENT_SCALE_ID in ('4','5') then 'A' 
				when ENT_SCALE_ID in('6','7') then 'B' 
				else 'O' end) b
where a.type = b.type
"

   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R287',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.3 } {
                set grade 2
                set alarmcontent " R287 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



##~   R288	��	02_���ſͻ�	�嵥ID�뼯�ſͻ���ʶ��Ӧ��ϵ֮��Ķ�Զ��¼	01007 ���ſͻ���Ŀ���г��嵥�ͻ���Ӧ��ϵ	01007�ӿ����嵥�ͻ�ID�뼯�ſͻ���ʶ��Ӧ��ϵ�����ڶ�Զ��¼����	0.05


##~   ͳ��01007�����ſͻ���Ŀ���г��嵥�ͻ���Ӧ��ϵ���ӿڡ���Ӧ��ϵ״̬��Ϊ�����ļ�¼�У�Ŀ���г��嵥ID
##~   ����
##~   �嵥ID�뼯�ſͻ���ʶ��һ�Զ��ϵ��
##~   ����
##~   �嵥ID�뼯�ſͻ���ʶ�Ķ��һ��ϵ��
##~   �ļ�¼���������������0����Υ������	

	set sql_buff "
select count(0) from table (
        select * from 
			(
			                select t.*
			                ,row_number()over(partition by t.ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
			                from 
			                G_A_01007_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	
) o
where RELA_STATE = '1'
and ENTERPRISE_ID in (
select ENTERPRISE_ID from table (
        select * from 
			(
			                select t.*
			                ,row_number()over(partition by t.ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
			                from 
			                G_A_01007_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	
) o
where RELA_STATE = '1'
group by ENTERPRISE_ID having count(0) > 1 
) 
and CUST_ID in (
select CUST_ID  from table (
        select * from 
			(
			                select t.*
			                ,row_number()over(partition by t.ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
			                from 
			                G_A_01007_DAY  t
											where time_id/100 <= $curr_month
			  ) a
			where rn = 1	
) o
where RELA_STATE = '1'
group by CUST_ID having count(0) > 1 
)


"

chkzero2 $sql_buff "R288 not pass!"


	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R288',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  

##~   R289	��	02_���ſͻ�	�̻��ܼҼ��ſͻ���ҵ��ʽ��һһ��Ӧ	02065 �̻��ܼ�ҵ�񶩹���ϵ	02065�ӿ��У������̻��ܼ�ҵ��ļ��ſͻ�ֻ�ܶ�Ӧһ��ҵ��ʽ����������������	0.05		

##~   02065���̻��ܼ�ҵ�񶩹���ϵ���ӿ��У�ͳ�Ƽ��ſͻ���ʶ��Ӧȥ�صġ�ҵ��ʽ������������1�����ϵļ��ſͻ���Ӧ����ҵ��ʽ����Υ������



##~   G_A_02065_DAY


##~   --~   select count(0)
##~   --~   from
##~   --~   (
##~   --~   select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
##~   --~   from G_A_02065_DAY a
##~   --~   where time_id / 100 <= $curr_month
##~   --~   ) t where t.rn =1  and CHNL_STATE = '1'


##~   select count(0) from G_A_02065_DAY

##~   �����ݣ��ݲ�У�飡



##~   R290	��	02_���ſͻ�	�̻��ܼҶ������ն��������ڼ��ſͻ���ʶ�Ķ���һһ��Ӧ	"02054 ����ҵ�񶩹���ϵ
##~   02059 ����ҵ������û��󶨹�ϵ
##~   02065 �̻��ܼ�ҵ�񶩹���ϵ"	�����̻��ܼ�ҵ����ն����ڼ��ſͻ�Ҳ�����̻��ܼ�ҵ��Ķ�����ϵ��¼	0.05		"Step1.ͳ��02059������ҵ������û��󶨹�ϵ���ӿ��У���ֹͳ����ĩ����״̬����������ҵ��Ϊ���̻��ܼҡ���Ʒ�ķǿռ��ſͻ���ʶ���ϣ�
##~   Step2.ͳ��02054������ҵ�񶩹���ϵ����02065���̻��ܼ�ҵ�񶩹���ϵ���ӿ��ж���״̬����������ҵ��Ϊ���̻��ܼҡ���Ʒ�ļ��ſͻ���ʶ���ϣ�
##~   Step3.ͳ��Step1���ϲ���Step2���ϵļ��ſͻ���ʶ���������������0����Υ������"	

##~   �����ݣ��ݲ�У�飡




##~   R291	��	02_���ſͻ�	���ſͻ��˿���Դʹ������ӿڵġ����ſͻ���ʶ�����ڼ��ſͻ��ӿڵġ����ſͻ���ʶ����	"22036 ���ſͻ��˿���Դʹ�����
##~   01004 ���ſͻ�"	���ſͻ��˿���Դʹ������ӿڵġ����ſͻ���ʶ�����ڼ��ſͻ��ӿڵġ����ſͻ���ʶ����	0.05		"Step1.ͳ��22036�����ſͻ��˿���Դʹ��������ӿ��У���ֹ��ͳ������ĩ�ͻ�����=0�����ſͻ����ġ����ſͻ���ʶ�����ϣ�
##~   Step2.ͳ��01004�����ſͻ����ӿ��У���ֹͳ������ĩ���ſͻ���ʶ���ռ��ϣ�
##~   Step3.ͳ��Step1���ϲ���Step2�����еļ��ſͻ���ʶ���������������0����Υ������"



	set sql_buff "
		select count(0) from  table (
			select distinct EC_CODE from 
								(
												select t.*
														,row_number()over(partition by EC_CODE,CUST_TYPE
														order by time_id desc ) rn 
												from 
												G_A_22036_DAY  t
												where 
												TIME_ID/100 <= $curr_month										
								  ) a
								where rn = 1  
								and bigint(OPEN_DATE)/100 <= $curr_month
								and  CUST_TYPE = '0'
		) t where 
			EC_CODE not in (                  
		select enterprise_id
		from  table (
					select enterprise_id from 
					(
									select t.*
									,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
									from 
									G_A_01004_DAY  t
													where time_id/100 <= $curr_month
					  ) a
					where rn = 1
			)   t                     
		)
		with ur
"

##~   ����һ�������޴˿ھ���and CUST_STATU_TYP_ID = '20' ������ȥ��

chkzero2 $sql_buff "R291 not pass!"


	set RESULT_VAL 0
	set RESULT_VAL [get_single $sql_buff]
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R291',$RESULT_VAL,0,0,0) 
		"
exec_sql $sql_buff
  


##~   R301	��	02_���ſͻ�	IDCʹ�ü��ſͻ�������	02054 ���ſͻ�ҵ�񶩹���ϵ	IDCʹ�ü��ſͻ����������Ⱦ���ֵС�ڵ���50%	0.05		"Step1.02054�����ſͻ�ҵ�񶩹���ϵ���ӿ��У���ֹͳ���¶���״̬�����ģ�ҵ�����ͱ���Ϊ1190��IDC���ĵ��º����¼��ſͻ���ʶȥ�ظ�����
##~   Step2.����ֵ������ֵ�Ƚϡ�"


##~   ��ҵ��




##~   R303	��	02_���ſͻ�	����ͨʹ�ü��ſͻ�������	02054 ���ſͻ�ҵ�񶩹���ϵ	����ͨʹ�ü��ſͻ����������Ⱦ���ֵС�ڵ���50%	0.05		"Step1.02054�����ſͻ�ҵ�񶩹���ϵ���ӿ��У���ֹͳ���¶���״̬�����ģ�ҵ�����ͱ���Ϊ1330������ͨ���ĵ��º����¼��ſͻ���ʶȥ�ظ�����
##~   Step2.����ֵ������ֵ�Ƚϡ�"


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select
aa.curr_cnts
,bb.bef_cnts
,case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  
		 from bass1.g_a_02054_day 
      where  time_id / 100 <= $curr_month
         ) z
    where row_id=1
      and status_id='1'
	  and enterprise_busi_type in ('1330')
	  and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
      where  time_id / 100 <= $last_month
         ) z
    where row_id=1
      and status_id='1'
	  and enterprise_busi_type in ('1330')
	  and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R303',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R303 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R304	��	02_���ſͻ�	����ͨʹ�ü��Ÿ��˿ͻ�������	02059 ����ҵ������û��󶨹�ϵ	����ͨʹ�ü��Ÿ��˿ͻ����������Ⱦ���ֵС�ڵ���50%
   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select
cc.curr_cnts
,dd.bef_cnts
,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
         where  time_id / 100 <= $curr_month
          ) z
        where status_id='1'
          and row_id=1
		  and  bigint(order_date)/100<=$curr_month
		  and   enterprise_busi_type in('1330')
       ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
         where  time_id / 100 <= $last_month
          ) z
        where status_id='1'
          and row_id=1
		  and  bigint(order_date)/100<=$last_month
		  and   enterprise_busi_type in('1330')
       ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1
"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R304',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R304 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R305	��	02_���ſͻ�	�ƶ�OAʹ�ü��ſͻ�������	02054 ���ſͻ�ҵ�񶩹���ϵ	�ƶ�OAʹ�ü��ſͻ����������Ⱦ���ֵС�ڵ���50%

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select
aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1250')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1250')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1
"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R305',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R305 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R306	��	02_���ſͻ�	ʹ���ƶ�OA�ļ��Ÿ��˿ͻ���	02059 ����ҵ������û��󶨹�ϵ	ʹ���ƶ�OA�ļ��Ÿ��˿ͻ������Ⱦ���ֵС�ڵ���50%


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select
cc.curr_cnts,dd.bef_cnts,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month
				 and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1250')
        and bigint(order_date)/100<=$curr_month
    ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month
				 and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1250')
        and bigint(order_date)/100<=$last_month
    ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R306',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R306 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}






##~   --R308	��	02_���ſͻ�	�ƶ�CRMʹ�ü��ſͻ�������	02054 ���ſͻ�ҵ�񶩹���ϵ	�ƶ�CRMʹ�ü��ſͻ����������Ⱦ���ֵС�ڵ���50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "



select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1280')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1280')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R308',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R308 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R309	��	02_���ſͻ�	ʹ���ƶ�CRM�ļ��Ÿ��˿ͻ���	02059 ����ҵ������û��󶨹�ϵ	ʹ���ƶ�CRM�ļ��Ÿ��˿ͻ������Ⱦ���ֵС�ڵ���50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select cc.curr_cnts,dd.bef_cnts,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month
				 and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1280')
        and bigint(order_date)/100<=$curr_month
    ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month
				 and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1280')
        and bigint(order_date)/100<=$last_month
    ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1
"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R309',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R309 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}


##~   --R311	��	02_���ſͻ�	�ƶ�������ʹ�ü��ſͻ�������	02054 ���ſͻ�ҵ�񶩹���ϵ	�ƶ�������ʹ�ü��ſͻ����������Ⱦ���ֵС�ڵ���50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1260')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1260')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R311',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R311 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



##~   --R312	��	02_���ſͻ�	ʹ���ƶ�������ļ��Ÿ��˿ͻ���	02059 ����ҵ������û��󶨹�ϵ	ʹ���ƶ�������ļ��Ÿ��˿ͻ������Ⱦ���ֵС�ڵ���50%


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select cc.curr_cnts,dd.bef_cnts,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month
				 and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1260')
        and bigint(order_date)/100<=$curr_month
    ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1260')
        and bigint(order_date)/100<=$last_month
    ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R312',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R312 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



##~   --R314	��	02_���ſͻ�	BlackBerryʹ�ü��ſͻ�������	02054 ���ſͻ�ҵ�񶩹���ϵ	BlackBerryʹ�ü��ſͻ����������Ⱦ���ֵС�ڵ���50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1230')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1230')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R314',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R314 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}


##~   --R315	��	02_���ſͻ�	ʹ��BlackBerry�ļ��Ÿ��˿ͻ���	"02059 ����ҵ������û��󶨹�ϵ 02060 BlackBerry��BES�������û��󶨹�ϵ"	ʹ��BlackBerry�ļ��Ÿ��˿ͻ������Ⱦ���ֵС�ڵ���50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select cc.curr_cnts,dd.bef_cnts,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1230')
        and bigint(order_date)/100<=$curr_month
        union all
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02060_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1230')
        and bigint(order_date)/100<=$curr_month
       ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1230')
        and bigint(order_date)/100<=$last_month
        union all
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02060_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1230')
        and bigint(order_date)/100<=$last_month
       ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R315',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R315 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}






##~   --R317	��	02_���ſͻ�	��ҵ��վʹ�ü��ſͻ�������	"02054 ���ſͻ�ҵ�񶩹���ϵ 02055 ��ҵ��վҵ�񶩹����"	��ҵ��վʹ�ü��ſͻ����������Ⱦ���ֵС�ڵ���50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1340')
        and bigint(order_date)/100<=$curr_month
    union all
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.G_A_02055_DAY 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1340')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1340')
        and bigint(order_date)/100<=$last_month
    union all
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.G_A_02055_DAY 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1340')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R317',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R317 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}













##~   --R318	��	02_���ſͻ�	ʹ����ҵ��վ�ļ��Ÿ��˿ͻ���	02059 ����ҵ������û��󶨹�ϵ	ʹ����ҵ��վ�ļ��Ÿ��˿ͻ������Ⱦ���ֵС�ڵ���50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select cc.curr_cnts,dd.bef_cnts,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month
				 and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1340')
        and bigint(order_date)/100<=$curr_month
    ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month
				 and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1340')
        and bigint(order_date)/100<=$last_month
    ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R318',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R318 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R320	��	02_���ſͻ�	�ֻ�����ʹ�ü��ſͻ�������	02054 ���ſͻ�ҵ�񶩹���ϵ	�ֻ�����ʹ�ü��ſͻ����������Ⱦ���ֵС�ڵ���50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 and aa.curr_cnts = 0 then 0 when bb.bef_cnts=0 and aa.curr_cnts <> 0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1220')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1220')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R320',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R320 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R321	��	02_���ſͻ�	ʹ���ֻ�����ļ��Ÿ��˿ͻ���	"02059 ����ҵ������û��󶨹�ϵ 02061 �ֻ���������û��󶨹�ϵ"	ʹ���ֻ�����ļ��Ÿ��˿ͻ������Ⱦ���ֵС�ڵ���50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "

select cc.curr_cnts,dd.bef_cnts,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1220')
        and bigint(order_date)/100<=$curr_month
        union all
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02061_day
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1220')
        and bigint(order_date)/100<=$curr_month
    ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1220')
        and bigint(order_date)/100<=$last_month
        union all
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02061_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1220')
        and bigint(order_date)/100<=$last_month
    ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1

"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R321',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R321 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



##~   --R323	��	02_���ſͻ�	���Ų���ʹ�ü��ſͻ�������	02054 ���ſͻ�ҵ�񶩹���ϵ	���Ų���ʹ�ü��ſͻ����������Ⱦ���ֵС�ڵ���50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 and aa.curr_cnts = 0 then 0 when bb.bef_cnts=0 and aa.curr_cnts <> 0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1120')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1120')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R323',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R323 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}





##~   --R325	��	02_���ſͻ�	�ƶ�400ʹ�ü��ſͻ�������	"02054 ���ſͻ�ҵ�񶩹���ϵ 02064 �ƶ�400ҵ�񶩹����"	�ƶ�400ʹ�ü��ſͻ����������Ⱦ���ֵС�ڵ���50%


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "



select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1520')
        and bigint(order_date)/100<=$curr_month
    union all
    select enterprise_id
      from (select OPEN_DT,enterprise_id,ORD_STS,
         row_number() over(partition by enterprise_id order by time_id desc) row_id  from bass1.G_A_02064_DAY 
		         where time_id/100<=$curr_month
) z
    where ORD_STS='1'
      and row_id=1
	--and enterprise_busi_type in ('1520')
        and bigint(OPEN_DT)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1520')
        and bigint(order_date)/100<=$last_month
    union all
    select enterprise_id
      from (select OPEN_DT,enterprise_id,ORD_STS,
         row_number() over(partition by enterprise_id order by time_id desc) row_id  from bass1.G_A_02064_DAY
		         where time_id/100<=$last_month
) z
    where ORD_STS='1'
      and row_id=1
	--and enterprise_busi_type in ('1520')
        and bigint(OPEN_DT)/100<=$last_month
    ) a
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R325',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R325 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R327	��	02_���ſͻ�	��������ֱ��ʹ�ü��ſͻ�������	02054 ���ſͻ�ҵ�񶩹���ϵ	��������ֱ��ʹ�ü��ſͻ����������Ⱦ���ֵС�ڵ���50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.curr_cnts,bb.bef_cnts,
     case��when bb.bef_cnts=0 and aa.curr_cnts = 0 then 0 when bb.bef_cnts=0 and aa.curr_cnts <> 0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1070')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1070')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R327',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R327 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R329	��	02_���ſͻ�	��ҵ����ʹ�ü��ſͻ�������	"02054 ���ſͻ�ҵ�񶩹���ϵ 02056 ��ҵ����ҵ�񶩹����"	��ҵ����ʹ�ü��ſͻ����������Ⱦ���ֵС�ڵ���50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "



select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 and aa.curr_cnts = 0 then 0 when bb.bef_cnts=0 and aa.curr_cnts <> 0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1210')
        and bigint(order_date)/100<=$curr_month
    union all
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.G_A_02056_DAY 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1210')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1210')
        and bigint(order_date)/100<=$last_month
    union all
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.G_A_02056_DAY 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1210')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1




"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R329',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R329 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



##~   --R331	��	02_���ſͻ�	�̻��ܼҶ������ſͻ���	"02054 ���ſͻ�ҵ�񶩹���ϵ 02065 �̻��ܼ�ҵ�񶩹���ϵ"	�̻��ܼҶ������ſͻ������Ⱦ���ֵС�ڵ���50%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "



select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 and aa.curr_cnts = 0 then 0 when bb.bef_cnts=0 and aa.curr_cnts <> 0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1560')
        and bigint(order_date)/100<=$curr_month
    union all
    select enterprise_id
      from (select order_dt,enterprise_id,sts_cd,
         row_number() over(partition by enterprise_id order by time_id desc) row_id  from bass1.G_A_02065_DAY
		         where time_id/100<=$curr_month
) z
    where STS_CD='1'
      and row_id=1
	--and ent_busi_id in ('1560')
        and bigint(ORDER_DT)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1560')
        and bigint(order_date)/100<=$last_month
    union all
    select enterprise_id
      from (select order_dt,enterprise_id,sts_cd,
         row_number() over(partition by enterprise_id order by time_id desc) row_id  from bass1.G_A_02065_DAY 
		         where time_id/100<=$last_month
) z
    where STS_CD='1'
      and row_id=1
	--and ent_busi_id in ('1560')
        and bigint(ORDER_DT)/100<=$last_month
    ) a
) bb
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R331',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R331 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R333	��	02_���ſͻ�	�̻��ܼҶ����ն���	02059 ����ҵ������û��󶨹�ϵ	�̻��ܼҶ����ն������Ⱦ���ֵС�ڵ���50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select cc.curr_cnts,dd.bef_cnts,
     case when  dd.bef_cnts=0 and cc.curr_cnts = 0 then 0 when dd.bef_cnts=0 and cc.curr_cnts <> 0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1560')
        and bigint(order_date)/100<=$curr_month
       ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1560')
        and bigint(order_date)/100<=$last_month
		) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1


"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R333',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R333 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}





##~   --R334	��	02_���ſͻ�	УѶͨ����ʹ�ü��Ÿ��˿ͻ���	02059 ����ҵ������û��󶨹�ϵ	УѶͨ����ʹ�ü��Ÿ��˿ͻ������Ⱦ���ֵС�ڵ���50%




   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select cc.curr_cnts,dd.bef_cnts,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1310')
        and bigint(order_date)/100<=$curr_month
    ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month and length(trim(user_id)) = 14
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1310')
        and bigint(order_date)/100<=$last_month
    ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R334',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R334 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R336	��	02_���ſͻ�	M2M����ʹ�ü��ſͻ���	02054 ���ſͻ�ҵ�񶩹���ϵ	M2M�������ſͻ������Ⱦ���ֵС�ڵ���50%





   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select aa.curr_cnts,bb.bef_cnts,
     case when bb.bef_cnts=0 then 1
          else decimal((aa.curr_cnts-bb.bef_cnts)*1.0/bb.bef_cnts,10,4)
     end
  from
(
select
      value(count(distinct a.enterprise_id),0) curr_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1241','1249')
        and bigint(order_date)/100<=$curr_month
    ) a
) aa
inner join
(
select
      value(count(distinct a.enterprise_id),0) bef_cnts
  from
  (
    select enterprise_id
      from (select order_date,enterprise_busi_type,enterprise_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type order by time_id desc) row_id  from bass1.g_a_02054_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1241','1249')
        and bigint(order_date)/100<=$last_month
    ) a
) bb
on 1=1



"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R336',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R336 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}




##~   --R338	��	02_���ſͻ�	M2M�����ն���	"02059 ����ҵ������û��󶨹�ϵ 02062 M2M�����û��󶨹�ϵ"	M2M�����ն������Ⱦ���ֵС�ڵ���50%





   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
   	set sql_buff "


select cc.curr_cnts,dd.bef_cnts,
     case when dd.bef_cnts=0 then 1
          else decimal((cc.curr_cnts-dd.bef_cnts)*1.0/dd.bef_cnts,10,4)
     end
  from
  (
    select value(sum(aa.curr_cnts),0) curr_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) curr_cnts
      from
      (
        select enterprise_id,user_id 
         from 
           (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in('1241','1249')
        and bigint(order_date)/100<=$curr_month
        union all
        select enterprise_id,user_id 
         from (select create_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02062_day 
		         where time_id/100<=$curr_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1241','1249')
        and bigint(create_date)/100<=$curr_month
       ) a
     group by a.enterprise_id
     ) aa
   ) cc
inner join
(
    select value(sum(bb.bef_cnts),0) bef_cnts from
    (
    select a.enterprise_id,count(distinct a.user_id) bef_cnts
      from
      (
        select enterprise_id,user_id 
         from (select order_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02059_day
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in('1241','1249')
        and bigint(order_date)/100<=$last_month
        union all
        select enterprise_id,user_id 
         from (select create_date,enterprise_busi_type,enterprise_id,user_id,status_id,
         row_number() over(partition by enterprise_id,enterprise_busi_type,user_id order by time_id desc) row_id  from bass1.g_a_02062_day 
		         where time_id/100<=$last_month
) z
    where status_id='1'
      and row_id=1
	and enterprise_busi_type in ('1241','1249')
        and bigint(create_date)/100<=$last_month
    ) a
     group by a.enterprise_id
     ) bb
 ) dd
on 1=1




"


   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]



set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]


        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R338',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
		"
		exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.5 } {
                set grade 2
                set alarmcontent " R338 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
		}



	return 0
}
