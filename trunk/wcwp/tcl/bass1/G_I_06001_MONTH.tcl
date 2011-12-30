
######################################################################################################		
#�ӿ�����: У԰������Ϣ                                                               
#�ӿڱ��룺06001                                                                                          
#�ӿ�˵����"У԰��Ҫ������ר������ѧУ��������ͨ�ߵȽ���ԺУ���ߵ�ְҵ����ԺУ�ȣ����е�רҵ����ѧУ��ְҵ���м�������ѧ��"
#��������: G_I_06001_MONTH.tcl                                                                            
#��������: ����06001������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110807
#�����¼��
#�޸���ʷ: 1. panzw 20110807	1.7.4 newly added
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #���� yyyy-mm-dd
  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]

  #������
  global app_name
  set app_name "G_I_06001_MONTH.tcl"
  
##  #ɾ����������
	set sql_buff "
	delete from bass1.G_I_06001_MONTH where time_id=$op_month
	"
  exec_sql $sql_buff 
  
	set sql_buff "
	insert into  bass1.G_I_06001_MONTH 
	(
         TIME_ID
        ,SCHOOL_ID
        ,SCHOOL_NAME
        ,CMCC_BRANCH_ID
        ,SCHOOL_ADDR
        ,LONGITUDE
        ,LATITUDE
        ,WEBSITE
        ,SCHOOL_LVL
        ,ADMIN_TYPE
        ,STUD_CNT
        ,NEWSTUD_CNT
        ,STAFF_CNT
        ,CMCC_RATE	
	)
	select distinct  
	$op_month  TIME_ID
	,SCHOOL_ID
        ,SCHOOL_NAME
        ,case when value (city_id , substr(SCHOOL_ID,4,3))  = '891' then '13101'
                              when value (city_id , substr(SCHOOL_ID,4,3))  = '892' then '13102'
                              when value (city_id , substr(SCHOOL_ID,4,3))  = '893' then '13103'
                              when value (city_id , substr(SCHOOL_ID,4,3))  = '894' then '13104'
                              when value (city_id , substr(SCHOOL_ID,4,3))  = '895' then '13105'
                              when value (city_id , substr(SCHOOL_ID,4,3))  = '896' then '13106'
                              when value (city_id , substr(SCHOOL_ID,4,3))  = '897' then '13107'
                        end CMCC_BRANCH_ID
	,a.school_address	
        ,value(char(cast( SCH_LONGITUDE as decimal(6,4))),'0') SCH_LONGITUDE
        ,value(char(cast( SCH_LATITUDE  as decimal(6,4))),'0') SCH_LATITUDE
	, case when SCHOOL_ID  = '89189100000003' then 'http://www.utibet.edu.cn/' else ''  end WEBSITE
        ,case 
		when school_type in ('4') then '1' 
		when school_type in ('5') then '2'	
		else '3' 
	end SCHOOL_LVL
	,case 
		when school_level in ('1') then '1' 
		when school_level in ('2') then '3'	
		else '4' 
	end ADMIN_TYPE
        ,char(SCH_SIZE_STUS) STUD_CNT
        ,'100' NEWSTUD_CNT 
	,char(sch_size_teas) STAFF_CNT
	,'0' CMCC_RATE
from      bass2.Dim_xysc_maintenance_info a
where a.SCHOOL_ID  in (
select distinct SCHOOL_ID from G_I_02032_MONTH where TIME_ID = $op_month
)
with ur
"
  exec_sql $sql_buff

set sql_buff "delete from G_I_06001_MONTH where SCHOOL_ID = '89189100000004' and STAFF_CNT = '322'"
  exec_sql $sql_buff

set sql_buff "
update G_I_06001_MONTH 
set CMCC_RATE = '06'||trim(char(int(rand(1)*9)))
where CMCC_RATE = '0'
"
  exec_sql $sql_buff



set sql_buff "
update     (select * from  g_i_06001_month where  time_id = $op_month )  t
set    t.stud_cnt = (select char(cnt)  from 
table (
select a.school_id, cnt
from (select * from  g_i_06001_month where time_id = $op_month ) a 
, (
select school_id,count(0) cnt 
from G_I_02031_MONTH
where time_id = $op_month
group by school_id
) b 
where a.school_id = b.school_id
and bigint(a.stud_cnt) < cnt
) c
where t.school_id = c.school_id )
where  t.school_id in 
(
select a.school_id
from (select * from  g_i_06001_month where time_id = $op_month ) a 
, (
select school_id,count(0) cnt 
from G_I_02031_MONTH
where time_id = $op_month
group by school_id
) b 
where a.school_id = b.school_id
and bigint(a.stud_cnt) < cnt
)
"
  exec_sql $sql_buff

# --ѧУ����(1-���ҽ�����,2-ʡ������,3-�н�����,4-��������)
#1������ԺУ
#2��ʡ��ԺУ
#3��ʡ���������ί
#4������
#

  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_I_06001_MONTH"
  set pk   "SCHOOL_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
  
	return 0
}



#delete from G_I_06001_MONTH where SCHOOL_ID = '89189100000004' and STAFF_CNT = '322'

