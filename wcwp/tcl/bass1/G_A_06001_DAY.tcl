######################################################################################################
#�ӿ����ƣ�У԰������Ϣ
#�ӿڱ��룺06001
#�ӿ�˵����
#��������: G_A_06001_DAY.tcl
#��������: ����06001������
#��������: ��
#Դ    ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuzhilong
#��дʱ�䣺2010-7-26
#�����¼������û��ҵ����˽ӿ���ʱ�Ϳ��ļ�
#�޸���ʷ: 2011-06-17 17:19:34 panzhiwei ���ݶ���������У԰�г�ר�⣬��ȡУ԰������Ϣ
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set last_day [GetLastDay [string range $timestamp 0 7]]
  #���� yyyy-mm-dd
  set optime $op_time

  #������
  global app_name
  set app_name "G_A_06001_DAY.tcl"
  
##  #ɾ����������

	set sql_buff "delete from bass1.G_A_06001_DAY where time_id=$timestamp"
  exec_sql $sql_buff

##���������ݣ���0
	set sql_buff "
	insert into  bass1.G_A_06001_DAY 
	select 
	       $timestamp  TIME_ID
        ,SCHOOL_ID
        ,SCHOOL_NAME
        ,CMCC_BRANCH_ID
        ,SCHOOL_TYPE
        ,STUD_CNT
        ,NEWSTUD_CNT
        ,'1' STATE 
from table(        
	select distinct 
         SCHOOL_ID
        ,SCHOOL_NAME
        ,case when value(city_id,substr(SCHOOL_ID,4,3)) = '891' then '13101'
                              when value(city_id,substr(SCHOOL_ID,4,3)) = '892' then '13102'
                              when value(city_id,substr(SCHOOL_ID,4,3)) = '893' then '13103'
                              when value(city_id,substr(SCHOOL_ID,4,3)) = '894' then '13104'
                              when value(city_id,substr(SCHOOL_ID,4,3)) = '895' then '13105'
                              when value(city_id,substr(SCHOOL_ID,4,3)) = '896' then '13106'
                              when value(city_id,substr(SCHOOL_ID,4,3)) = '897' then '13107'
                        end CMCC_BRANCH_ID
        ,case when school_type in ('4','5') then '1' else '2' end SCHOOL_TYPE
        ,char(SCH_SIZE_STUS) STUD_CNT
        ,'0' NEWSTUD_CNT 
from      bass2.Dim_xysc_maintenance_info
except
select 
         SCHOOL_ID
        ,SCHOOL_NAME
        ,CMCC_BRANCH_ID
        ,SCHOOL_TYPE
        ,STUD_CNT
        ,NEWSTUD_CNT
from ( 
			select a.*
			,row_number()over(partition by SCHOOL_ID order by time_id desc ) rn 
		  from G_A_06001_DAY a 
		  where time_id < $timestamp
     ) o where o.rn = 1
) t     
"
  exec_sql $sql_buff


  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_A_06001_DAY"
  set pk   "SCHOOL_ID"
        chkpkunique ${tabname} ${pk} ${timestamp}
        #
  aidb_runstats bass1.$tabname 3
  

	return 0
}

