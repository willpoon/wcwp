######################################################################################################
#�ӿ����ƣ����˿ͻ�
#�ӿڱ��룺01002
#�ӿ�˵�������˿ͻ��Ļ������ϡ�
#��������: G_A_01002_DAY.tcl
#��������: ����01002������
#��������: ��
#Դ    ��1.bass2.dwd_cust_msg_yyyymmdd
#          2.bass2.dw_product_bass1_yyyymmdd
#          3.bass1.g_a_01002_Day
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.���û������
#�޸���ʷ: 20091126 �� dw_product_bass1_ �滻ԭ�����û���
#######################################################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #���� yyyy-mm-dd
        set optime $op_time
        
		set app_name "G_A_01002_DAY.tcl"        
        
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_a_01002_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
        #������ʱ��(װ������+�����ĸ��˿ͻ�ID)
        set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_a_01002_tmp1
	              (
	                 cust_id       varchar(20)
	              )
	              partitioning key
	              (cust_id)
	              using hashing
	              with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
        aidb_close $handle
        
        #����ʱ���������(װ������+�����ĸ��˿ͻ�ID)
        set handle [aidb_open $conn]
	set sql_buff "insert into  session.g_a_01002_tmp1
                   select 
                     t.cust_id 
                   from 
                    (
                     ( select distinct
                         a.cust_id
                         ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0028',char(a.custstatus_id)),'20') as custstatus_id 
                      from 
                        bass2.dwd_cust_msg_$timestamp a,
                        bass2.dw_product_bass1_$timestamp b
                      where 
                        a.custtype_id<>2
                        and b.enterprise_mark=0
                        and b.test_mark=0
                        and b.usertype_id in (1,2,9)
                        and a.cust_id=b.cust_id
                      except 
                      select 
                         a.cust_id
                        ,a.custstatus_id
                      from  
                        bass1.g_a_01002_day a,
                        (select max(time_id) as time_id,cust_id from  bass1.g_a_01002_day
                         where time_id<$timestamp
                         group by cust_id
                        ) b
                      where 
                        a.time_id=b.time_id
                        and a.cust_id=b.cust_id) 
                    ) t  "
 
                      
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
        aidb_close $handle   
            
        #������ʱ������(װ������+�����ĸ��˿ͻ�ID)
        set handle [aidb_open $conn]
	set sql_buff "\
	     create index  session.idx_tmp1_ei on session.g_a_01002_tmp1(cust_id) "               
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
        aidb_close $handle     
                     
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_a_01002_day
                    (
                    TIME_ID                        
                    ,CUST_ID                        
                    ,BIRTHDAY                       
                    ,MARRY                          
                    ,OCCUPATION_ID                  
                    ,FOLK_ID                        
                    ,EDUCATION_ID                   
                    ,SEX_ID                         
                    ,NATION_ID                      
                    ,VIP_MARK                       
                    ,CMCC_ID                        
                    ,CREATE_DATE                    
                    ,CHANNEL_ID                     
                    ,CUSTSTATUS_ID 
                    )
                select
                  $timestamp
                  ,a.cust_id
                  ,coalesce(replace(char(date(a.birthday)),'-',''),'19000101') as birthday
                  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0017',char(a.marry)),'3') as marry
                  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0062',char(a.occupation_id)),'99') as occupation_id
                  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0034',char(a.nation_id)),'57')     as folk_id
                  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0026',char(a.education_id)),'01')
                  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0046',char(a.sex_id)),'3')
                  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0116',char(a.nationality)),'900')  as nation_id		  
                  ,case when a.vip_mark=1 then '1' else '0' end as vip_mark
                  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.city_id)),'13101') as cmcc_id
                  ,replace(char(date(a.create_date)),'-','') as create_date
                  ,value(char(a.channel_id),' ')
                  ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0028',char(a.custstatus_id)),'20') as custstatus_id
                from 
                  bass2.dwd_cust_msg_${timestamp} a,
                  session.g_a_01002_tmp1  b
                where 
                  a.custtype_id<>2
                  and a.cust_id=b.cust_id "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle
        
        #�޸ĳ�������С��19000101������
        set handle [aidb_open $conn]
	set sql_buff "update  bass1.g_a_01002_day
                  set BIRTHDAY='19000101'
                  where 
                    time_id=$timestamp 
                    and ((BIRTHDAY<='19000101') or (BIRTHDAY>='20200101')) "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle
	        
# #��ϴ���֤�Ա�
# set handle [aidb_open $conn]
#	set sql_buff "update  bass1.g_a_01002_day
#                 set sex_id ='1'
#                 where 
#                   time_id=$timestamp and card_type = '101' and 
#                   length(card_code) = 15                   and 
#                   substr(card_code,15,1) in ('1','3','5','7','9') "
#       puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}        
#	aidb_commit $conn
#	aidb_close $handle
#
#  set handle [aidb_open $conn]
#	set sql_buff "update  bass1.g_a_01002_day
#                  set sex_id ='2'
#                  where 
#                    time_id=$timestamp and card_type = '101' and 
#                    length(card_code) = 15                   and 
#                    substr(card_code,15,1) in ('2','4','6','8','0') "
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}        
#	aidb_commit $conn
#	aidb_close $handle

#  set handle [aidb_open $conn]
#	set sql_buff "update  bass1.g_a_01002_day
#                  set sex_id ='1'
#                  where 
#                    time_id=$timestamp and  card_type = '101' and 
#                    length(card_code) = 18                    and 
#                    substr(card_code,17,1) in ('1','3','5','7','9') "
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}        
#	aidb_commit $conn
#	aidb_close $handle

#  set handle [aidb_open $conn]
#	set sql_buff "update  bass1.g_a_01002_day
#                  set sex_id ='1'
#                  where 
#                    time_id=$timestamp and  card_type = '101' and 
#                    length(card_code) = 18                    and 
#                    substr(card_code,17,1) in ('2','4','5','8','0') "
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}        
#	aidb_commit $conn
#	aidb_close $handle
#


#/**
#458	�漰����������������������������ͨ������������ֱ�����������޶���
#1��(02004)�û��ӿڡ�����������ʶ����ͨ������������ֱ����������
#�����¹�����д�ض������ʶ��
#��վ�����ߡ����š�wap�������ն˵���������ֱ�������ֱ��Ӧ��д
#'BASS1_WB', 'BASS1_HL','BASS1_SM','BASS1_WP','BASS1_ST', 'BASS1_DS' 
#(�ַ����ִ�Сд)�� ʡ��˾����ռ������6�����������á�
#2�� (01002)���˿ͻ���(01004)���ſͻ���(02013) IPֱͨ���̶��û���
#������������ʶ����������������ʶ���μ�(02004)�û��ӿ��ϱ���	
#1.7.3	2011-5-17	����������20110601����Ч
#**/
#add     

#2011-06-01 21:57:54 trans channel
#add

set sql_buff "alter table bass1.G_A_01002_DAY_CHNL_MID activate not logged initially with empty table"
exec_sql $sql_buff

        
set sql_buff "
insert into G_A_01002_DAY_CHNL_MID
(
         TIME_ID
        ,CUST_ID
        ,BIRTHDAY
        ,MARRY
        ,OCCUPATION_ID
        ,FOLK_ID
        ,EDUCATION_ID
        ,SEX_ID
        ,NATION_ID
        ,VIP_MARK
        ,CMCC_ID
        ,CREATE_DATE
        ,CHANNEL_ID
        ,CUSTSTATUS_ID
)
select 
         a.TIME_ID
        ,a.CUST_ID
        ,a.BIRTHDAY
        ,a.MARRY
        ,a.OCCUPATION_ID
        ,a.FOLK_ID
        ,a.EDUCATION_ID
        ,a.SEX_ID
        ,a.NATION_ID
        ,a.VIP_MARK
        ,a.CMCC_ID
        ,a.CREATE_DATE
        ,case when b.CHANNEL_ID is null then 'BASS1_DS' 
        		else a.channel_id end CHANNEL_ID
        ,a.CUSTSTATUS_ID
from bass1.G_A_01002_DAY a
left join (select distinct CHANNEL_ID  from bass1.G_I_06021_MONTH )  b on a.CHANNEL_ID = b.CHANNEL_ID
where a.time_id = $timestamp
"
exec_sql $sql_buff

	set sql_buff "\
		DELETE FROM bass1.G_A_01002_DAY where time_id=$timestamp
		"
  exec_sql $sql_buff

set sql_buff "
	insert into  bass1.G_A_01002_DAY 
	select distinct
	 TIME_ID
        ,CUST_ID
        ,BIRTHDAY
        ,MARRY
        ,OCCUPATION_ID
        ,FOLK_ID
        ,EDUCATION_ID
        ,SEX_ID
        ,NATION_ID
        ,VIP_MARK
        ,CMCC_ID
        ,CREATE_DATE
        ,CHANNEL_ID
        ,CUSTSTATUS_ID
	from G_A_01002_DAY_CHNL_MID
	with ur
	"
exec_sql $sql_buff




  #����01002����Ψһ�Լ��
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select cust_id,count(*) cnt from bass1.g_a_01002_day
	              where time_id =$timestamp
	             group by cust_id
	             having count(*)>1
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
	aidb_close $handle
	
	set DEC_RESULT_VAL2 [format "%.3f" [expr ${DEC_RESULT_VAL2} /1.00]]

	puts $DEC_RESULT_VAL2

	if {[format %.3f [expr ${DEC_RESULT_VAL2} ]]>0 } {
		set grade 2
	        set alarmcontent "01002�ӿ�����Ψһ��У��δͨ��"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	   }



	return 0
}