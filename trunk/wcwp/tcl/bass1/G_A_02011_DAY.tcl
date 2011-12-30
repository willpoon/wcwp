######################################################################################################
#�ӿ����ƣ��û���ͨҵ������ʷ
#�ӿڱ��룺02011
#�ӿ�˵�����û���ͨ��ҵ������ʷ��
#��������: G_A_02011_DAY.tcl
#��������: ����02011������
#��������: ��
#Դ    ��1.bass2.dwd_product_func_yyyymmdd(�û����ܹ�ϵ)
#          2.bass1.int_02011_snapshot(02011�Ŀ�������)
#          3.bass2.dim_product_item(��Ʒ����ά��)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��
#�޸���ʷ: 20081120�в�����ҵ���ܴӹ��ܱ�ͳ�ƣ������ע���ͳ��
#					 20090813 �޸�ȥ�� ����    20091126 �� dw_product_bass1_ �滻ԭ�����û���
#130	�������־��ֲ���Ա	
#131	��ͨ��Ա	
#132	�߼���Ա	
#
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #ǰһ�� yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_a_02011_day where time_id=$timestamp"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
	      
        #01:������ʱ��1(װ�ص������Ч����)
        set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_a_02011_day_tmp1
	              (
	                 busi_code           character(3),
                         user_id             character(20),
                         valid_date          character(8),
                         invalid_date        character(8)
	              )
	              partitioning key
	              (busi_code,user_id,valid_date)
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

        #02:������ʱ��2(װ��������+�޸�����Ч����ʧЧ���ڵ�)
        set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_a_02011_day_tmp2
	              (
	                 busi_code           character(3),
                         user_id             character(20),
                         valid_date          character(8),
                         invalid_date        character(8)
	              )
	              partitioning key
	              (busi_code,user_id,valid_date)
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

        #03:������ʱ��2(װ������֮��ĵ�����Ч����)
        set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_a_02011_day_tmp3
	              (
	                 busi_code           character(3),
                         user_id             character(20),
                         valid_date          character(8),
                         invalid_date        character(8)
	              )
	              partitioning key
	              (busi_code,user_id,valid_date)
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
		
	#03:����������ݲ�����ʱ��1
	set sql_buff "insert into session.g_a_02011_day_tmp1
                           select
                             coalesce(bass1.fn_get_all_dim('BASS_STD1_0075',char(a.prod_id)),'010')
                             ,a.user_id
                             ,replace(char(date(a.valid_date)),'-','') as valid_date
                             ,replace(char(date(a.expire_date)),'-','') as invalid_date
                           from 
                             bass2.dwd_product_func_$timestamp a 
                           where
                             a.service_id>0 
			     with ur
			     "               
        exec_sql $sql_buff      

 set sql_buff "
 INSERT INTO session.g_a_02011_day_tmp1
SELECT DISTINCT 
        '131' BUSI_CODE
        ,b.user_id USER_ID
        ,replace(char(date(a.valid_date)),'-','') VALID_DATE
        ,replace(char(date(a.EXPIRE_DATE)),'-','') INVALID_DATE
from 
bass2.dw_product_ins_off_ins_prod_ds a 
,bass2.dw_product_$timestamp b
where 
a.product_instance_id = b.user_id
and b.usertype_id in (1,2,9)
and b.USERSTATUS_ID in (1,2,3,6,8)
and a.offer_id = 113050001402
and date(a.VALID_DATE) < date(a.EXPIRE_DATE)
and date(a.VALID_DATE) <= '$op_time'
and date(a.EXPIRE_DATE) > '$op_time'
with ur
"  
exec_sql $sql_buff      


 set sql_buff "
 INSERT INTO session.g_a_02011_day_tmp1
SELECT DISTINCT 
	'132' BUSI_CODE
        ,b.user_id USER_ID
        ,replace(char(date(a.valid_date)),'-','') VALID_DATE
        ,replace(char(date(a.EXPIRE_DATE)),'-','') INVALID_DATE
from 
bass2.dw_product_ins_off_ins_prod_ds a 
,bass2.dw_product_$timestamp b
where 
a.product_instance_id = b.user_id
and b.usertype_id in (1,2,9)
and b.USERSTATUS_ID in (1,2,3,6,8)
and a.offer_id = 113050001403
and date(a.VALID_DATE) < date(a.EXPIRE_DATE)
and date(a.VALID_DATE) <= '$op_time'
and date(a.EXPIRE_DATE) > '$op_time'
with ur
"  
exec_sql $sql_buff      



#����ҵ���ܴӹ��ܱ�ͳ��
#351  �ֻ�����          130	�ֻ�����ҵ��           
#600  161�ƶ�����       115	����ҵ��        
#620  �Ų��ܼ�          113	�Ų��ܼ�      
#ע���û���
#  set handle [aidb_open $conn]
#	set sql_buff "insert into session.g_a_02011_day_tmp1
#                      select
#                       distinct a.user_id
#                       ,case
#                          when a.BUSI_TYPE = 130 then '351'
#                          when a.BUSI_TYPE = 113 then '620'      
#                          when a.BUSI_TYPE = 115 and SP_CODE <> '0' then '600'
#                         end
#                       ,a.user_id
#                       ,replace(char(date(a.valid_date)),'-','') as valid_date
#                       ,replace(char(date(a.expire_date)),'-','') as invalid_date
#                      from 
#                        bass2.DW_PRODUCT_REGSP_$timestamp a,
#                        bass2.dw_product_$timestamp b
#                      where
#                            b.userstatus_id in (1,2,3,6) 
#                        and a.BUSI_TYPE in (130,115,113)
#                        and b.usertype_id in (1,2,9)
#                        and a.user_id=b.user_id"
#        puts $sql_buff      
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}       
#	aidb_commit $conn    
#	aidb_close $handle
                        
                        
                        
	#04:����������ݲ�����ʱ��1(����ҵ��)
        set handle [aidb_open $conn]
	set sql_buff "insert into session.g_a_02011_day_tmp1
                           select
                             coalesce(bass1.fn_get_all_dim('BASS_STD1_0075',char(a.prod_id)),'010')
                             ,a.user_id
                             ,replace(char(date(a.valid_date)),'-','') as valid_date
                             ,replace(char(date(a.expire_date)),'-','') as invalid_date
                           from 
                             bass2.dwd_product_func_$timestamp a,
                             bass2.dim_product_item b
                           where
                             a.service_id=0 
                             and a.prod_id=b.prod_id "            
        puts $sql_buff      
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn    
	aidb_close $handle
	
	#05:�޳��ظ�
        set handle [aidb_open $conn]
##	set sql_buff "insert into session.g_a_02011_day_tmp3
##                           select
##                             busi_code
##                             ,user_id
##                             ,max(valid_date)
##                             ,max(invalid_date)
##                           from 
##                             session.g_a_02011_day_tmp1
##                           group by 
##                             busi_code
##                             ,user_id "        
#  �޸�ȥ�� ����
 set sql_buff "insert into session.g_a_02011_day_tmp3
               select t.busi_code
		                 ,t.user_id
		                 ,t.valid_date
		                 ,t.invalid_date
               from (select  busi_code
                             ,user_id
                             ,valid_date
                             ,invalid_date
                             ,row_number() over(partition by busi_code,user_id order by valid_date desc ) row_id
                     from session.g_a_02011_day_tmp1 ) t
               where t.row_id=1                           
                            " 

        puts $sql_buff      
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn    
	aidb_close $handle		
	
        #05:�Ƚ�1��������+�޸�����Ч����ʧЧ���ڵ�,���ݲ�����ʱ��2
        set handle [aidb_open $conn]
	set sql_buff "insert into session.g_a_02011_day_tmp2
                           select
                             busi_code
                             ,user_id
                             ,valid_date
                             ,invalid_date
                           from 
                             session.g_a_02011_day_tmp3
                           except
                           select
                             busi_code
                             ,user_id
                             ,valid_date
                             ,invalid_date
                           from 
                             bass1.int_02011_snapshot "                                            
        puts $sql_buff      
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle        

  #05:�Ƚ�2��һ������ �����Ѿ������ڵļ�¼ ����ЧʧЧ������Ϊ20000101 20000102,���ݲ�����ʱ��2
  set handle [aidb_open $conn]
	set sql_buff "insert into session.g_a_02011_day_tmp2
                 select
                    a.busi_code
                   ,a.user_id
                   ,'20000101'
                   ,'20000102'
                 from ( select busi_code,user_id  from bass1.int_02011_snapshot where valid_date<>'20000101' and invalid_date<>'20000102'
														except 
												select busi_code,user_id from session.g_a_02011_day_tmp3 ) a
                "                                            
        puts $sql_buff      
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn
	aidb_close $handle   
	
	
#        #06:�Ƚ�2���˶�ҵ���,���ݲ�����ʱ��2
#        set handle [aidb_open $conn]
#	set sql_buff "insert into session.g_a_02011_day_tmp2	                  
#                           select
#                             busi_code
#                             ,user_id
#                             ,valid_date
#                             ,invalid_date                             
#                           from 
#                             bass1.int_02011_snapshot
#                           except
#                           select
#                             busi_code
#                             ,user_id
#                             ,valid_date
#                             ,invalid_date                             
#                           from 
#                             session.g_a_02011_day_tmp1 "                                            
#        puts $sql_buff      
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}       
#	aidb_commit $conn 
#	aidb_close $handle
	
	#07:���ܵ������    
        set handle [aidb_open $conn]
##	set sql_buff "insert into bass1.g_a_02011_day	                  
##                           select
##                             $timestamp
##                             ,busi_code
##                             ,user_id
##                             ,max(valid_date)
##                             ,max(invalid_date)                             
##                           from 
##                             session.g_a_02011_day_tmp2
##                           group by 
##                             busi_code
##                             ,user_id "                                          
#  �޸�ȥ�� ���� �����û����й���
 set sql_buff "insert into bass1.g_a_02011_day	                  
                select $timestamp
		                  ,t.busi_code
		                  ,t.user_id
		                  ,t.valid_date
		                  ,t.invalid_date                             
                from ( select busi_code
						                 ,user_id
						                 ,valid_date
						                 ,invalid_date 
						                 ,row_number() over(partition by busi_code,user_id order by valid_date desc ) row_id
                       from session.g_a_02011_day_tmp2 ) t
                inner join bass2.dw_product_bass1_$timestamp b on t.user_id=b.user_id and b.usertype_id in (1,2,9) and b.userstatus_id<>0
                where t.row_id=1
             "  

        puts $sql_buff      
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn 
	aidb_close $handle


	#08:ɾ���Ƿ���¼    
        set handle [aidb_open $conn]
	set sql_buff "delete from  G_A_02011_DAY 
                 where time_id = $timestamp and 
                       valid_Date > invalid_date with ur "                                          
        puts $sql_buff      
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}       
	aidb_commit $conn 
	aidb_close $handle


  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_A_02011_DAY"
        set pk                  "user_id||busi_code"
        chkpkunique ${tabname} ${pk} ${timestamp}
        #
	return 0
}