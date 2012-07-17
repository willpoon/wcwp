
######################################################################################################		
#�ӿ�����: ��ֵҵ��KPI                                                               
#�ӿڱ��룺22076                                                                                          
#�ӿ�˵������¼��ֵҵ����KPI��Ϣ��
#��������: G_S_22076_MONTH.tcl                                                                            
#��������: ����22076������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20110922
#�����¼��
#�޸���ʷ: 1. panzw 20110922	1.7.5 newly added
#######################################################################################################   


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#set optime_month 2011-08
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      #set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01] 
      set last_month [GetLastMonth [string range $op_month 0 5]]
      #set curr_month_first_day [string range $timestamp 0 5]01
      #puts $curr_month_first_day
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      

        #���¸��� ��ʽ yyyymm
        set next_month [GetNextMonth $op_month]
        #���¸��µ�һ��
        set next_month_firstday "${next_month}01"
	
 

  #ɾ����������
	set sql_buff "delete from bass1.G_S_22076_MONTH where time_id=$op_month"
	exec_sql $sql_buff
	
 set sql_buff "alter table bass1.G_S_22076_MONTH_1 activate not logged initially with empty table"
	exec_sql $sql_buff
	
#	02	������ֵҵ��-������ʾ-�ͻ�������	ͳ��������ʹ��������ʾҵ��ı�ʡ�û���	number(12)	��λ����
set sql_buff "	 
insert into G_S_22076_MONTH_1
select 'P02'
,(
select char(count(distinct a.user_id))
 from	bass2.DW_PRODUCT_FUNC_$op_month a 
 , bass2.dw_product_$op_month b
 where replace(char(a.valid_date),'-','')    < '$next_month_firstday'
 and   replace(char(a.expire_date),'-','')  >= '$next_month_firstday'
 and b.usertype_id in (1,2,9) and a.user_id=b.user_id
 and a.SERVICE_ID in (305011,305015,305018)
 and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1 and a.STS in (1)
 and a.service_id = 305011
 ) index_value 
from  bass2.dual
with ur
    "
exec_sql $sql_buff

#03
set sql_buff "	  
insert into G_S_22076_MONTH_1
select 'P03'
,(
select char(sum(a.CALL_DURATION_M))
 from	bass2.DW_NEWBUSI_CALL_$op_month a , bass2.dw_product_$op_month b
 where b.usertype_id in (1,2,9)
   and b.userstatus_id in (1,2,3,6,8) 
   and b.test_mark <> 1
   and a.user_id=b.user_id
   and a.SVCITEM_ID in (100015,100016,100017,100018)
   and a.OPPOSITE_ID in (72,12,32,60,61,62,63,64,65,83,91,101,102,103,104,109,122,259000,258600,2586,259010,259200,259150,259100,259090,259080,259070,259060,259050,259040,259030,78)
) from bass2.dual
with ur
    "
exec_sql $sql_buff

#04

set sql_buff "	  
insert into G_S_22076_MONTH_1
select 'P04'
,(
select char(sum(a.CALL_DURATION_M))
 from	bass2.DW_NEWBUSI_CALL_$op_month a , bass2.dw_product_$op_month b
 where b.usertype_id in (1,2,9)
   and b.userstatus_id in (1,2,3,6,8) 
   and b.test_mark <> 1
   and a.user_id=b.user_id
   and a.SVCITEM_ID in (100015,100016,100017,100018)
   and a.OPPOSITE_ID  in (3,13,115,2,1,4,116,14)
) from bass2.dual
with ur
    "
exec_sql $sql_buff

##	05	��Ե����-����-ʡ��-����

        set sql_buff "declare global temporary table session.G_S_22076_MONTH_0
                      (
                         HLR_CODE       varchar(20)
                      )
                      partitioning key
                      (HLR_CODE)
                      using hashing
                      with replace on commit preserve rows not logged in tbs_user_temp"

exec_sql $sql_buff

 set sql_buff "
insert into session.G_S_22076_MONTH_0
select distinct  hlr_code  
from   bass2.DIM_GSM_HLR_INFO
where prov_code = '891'
with ur
"

exec_sql $sql_buff


        set sql_buff "\
             create index  session.idx_22076_MONTH_0 on session.G_S_22076_MONTH_0(hlr_code) "   
exec_sql $sql_buff
	     
	     
	     
	     
set sql_buff "	   
insert into G_S_22076_MONTH_1
select 'P05'
,(
select CHAR(sum(COUNTS))
                  from
                       (select user_id,counts, substr((case when substr(a.opp_number,1,2) = '86' then substr(a.opp_number,3,11) else a.opp_number
			end ),1,7) hlr_code  
		       from bass2.dw_newbusi_sms_$op_month a
		       where  a.calltype_id=0 and a.svcitem_id in (200004)
		       ) a
			,session.G_S_22076_MONTH_0 c
                  where  a.hlr_code = c.hlr_code
) from bass2.dual
with ur
    "
exec_sql $sql_buff		
                

#06	��Ե����-����-ʡ��-����


        set sql_buff "declare global temporary table session.G_S_22076_MONTH_2
                      (
                         HLR_CODE       varchar(20)
                      )
                      partitioning key
                      (HLR_CODE)
                      using hashing
                      with replace on commit preserve rows not logged in tbs_user_temp"

exec_sql $sql_buff

 set sql_buff "
insert into session.G_S_22076_MONTH_2
select distinct  hlr_code  
from   bass2.DIM_GSM_HLR_INFO
where prov_code <> '891'
with ur
"

exec_sql $sql_buff


        set sql_buff "\
             create index  session.idx_22076_MONTH_2 on session.G_S_22076_MONTH_2(hlr_code) "   
exec_sql $sql_buff
	     
	     

set sql_buff "	   
insert into G_S_22076_MONTH_1
select 'P06'
,(
		select CHAR(sum(COUNTS))
                  from
                       (select user_id,counts, substr((case when substr(a.opp_number,1,2) = '86' then substr(a.opp_number,3,11) else a.opp_number
			end ),1,7) hlr_code  
		       from bass2.dw_newbusi_sms_$op_month a
		       where  a.calltype_id=0 and a.svcitem_id in (200004)) a
			,session.G_S_22076_MONTH_2 c
                  where  a.hlr_code = c.hlr_code
) from bass2.dual
with ur
    "
exec_sql $sql_buff

			
#	07	��Ե����-�������γ���
#200003	���ʵ�Ե����	2	��ͨ����	2	����ҵ��

## ���޷�ȷ�����ܺ����Ƿ��ڹ��ڻ��ǹ���
set sql_buff "	       
insert into G_S_22076_MONTH_1
select 'P07'
,(
	select char(count(0))
	from bass2.dw_newbusi_sms_$op_month a
	where  a.calltype_id=0 and a.svcitem_id in (200003)
	and a.ROAMTYPE_ID = 100
) from bass2.dual
with ur
    "
exec_sql $sql_buff

#08  CU9181	���Ŷ���ʹ�ÿͻ���
set sql_buff "
insert into G_S_22076_MONTH_1
select 'P08'
,(
select  char(sum(b.counts))
                      from bass2.dw_enterprise_member_mid_$op_month a
		      ,bass2.dw_newbusi_ismg_$op_month b
                      where a.user_id = b.user_id 
                         and b.sp_code  = '931007' 
			 and b.calltype_id in (1)
			 and calltype_id = 1 
) from bass2.dual
with ur
    "
exec_sql $sql_buff

#	09	��Ե����-����-ʡ��-����	ͳ�����ڱ�ʡ�ƶ��û���������ʡ�ƶ��û��Ĳ��ţ������Ų������У���	number(15)	��λ����

#20120704 : ԭ�еĿھ���SVCITEM_ID in  (400001)
#�滻�ɣ� and drtype_id in (90605,103)
# and app_type = 0 


set sql_buff "
insert into G_S_22076_MONTH_1
select 'P09'
,(
 select char(sum(counts))
 from 
 table (
 select a.user_id,counts,substr((case when substr(a.opp_number,1,2) = '86' then substr(a.opp_number,3,11) else a.opp_number
			end ),1,7) hlr_code   
			from bass2.DW_NEWBUSI_MMS_$op_month a 
 where a.SEND_STATUS in (0,1,2,3) 
 and a.drtype_id in (90605,103)
 and a.app_type = 0 
 and a.calltype_id in (0)
 ) a
 ,(select distinct  hlr_code  from   bass2.DIM_GSM_HLR_INFO
                where prov_code = '891'
              ) c
 where 
  a.hlr_code = c.hlr_code
) from bass2.dual  
with ur  
    "
exec_sql $sql_buff

#	10	��Ե����-����-ʡ��-����	ͳ�������ڱ�ʡ�ƶ��û���������ʡ�ƶ��û��Ĳ��š�	number(15)	��λ����
set sql_buff "
insert into G_S_22076_MONTH_1
select 'P10'
,(
 select char(sum(counts))
 from 
 table (
 select a.user_id,counts,substr((case when substr(a.opp_number,1,2) = '86' then substr(a.opp_number,3,11) else a.opp_number
			end ),1,7) hlr_code   from bass2.DW_NEWBUSI_MMS_$op_month a 
 where a.SEND_STATUS in (0,1,2,3) 
 and a.drtype_id in (90605,103)
 and a.app_type = 0 
 and  a.calltype_id in (0)
 ) a
 ,session.G_S_22076_MONTH_2 c
 where 
  a.hlr_code = c.hlr_code
) from bass2.dual  
with ur
    "
exec_sql $sql_buff

#	11	��Ե����-����-ʡ��-����	ͳ�������ڱ�ʡ�ƶ��û���������ʡ�ƶ��û��Ĳ��š�	number(15)	��λ����

set sql_buff "
insert into G_S_22076_MONTH_1
select 'P11'
,(
 select char(sum(counts))
 from 
 table (
 select a.user_id,counts,substr((case when substr(a.opp_number,1,2) = '86' then substr(a.opp_number,3,11) else a.opp_number
			end ),1,7) hlr_code   from bass2.DW_NEWBUSI_MMS_$op_month a 
 where a.SEND_STATUS in (0,1,2,3) 
 and a.drtype_id in (90605,103)
 and a.app_type = 0 
 and  a.calltype_id in (1)
 ) a
 ,session.G_S_22076_MONTH_2 c
 where 
  a.hlr_code = c.hlr_code
) from bass2.dual  
with ur
    "
exec_sql $sql_buff

#	12	��Ե����-����-����	ͳ�������ڱ�ʡ�ƶ��û�������������Ӫ���û��Ĳ��š�	number(15)	��λ����

set sql_buff "
insert into G_S_22076_MONTH_1
select 'P12'
,(
 select char(sum(counts))
 from 
 table (
 select a.user_id,counts  from bass2.DW_NEWBUSI_MMS_$op_month a 
 where a.SEND_STATUS in (0,1,2,3) 
 and a.SVCITEM_ID in  (400002)
 and  a.calltype_id in (0)
 ) a
) from bass2.dual  
with ur
    "
exec_sql $sql_buff
  

#	13	��Ե����-����-����	ͳ�������ڱ�ʡ�ƶ��û�������������Ӫ���û��Ĳ��š�	number(15)	��λ����

set sql_buff "
insert into G_S_22076_MONTH_1
select 'P13'
,(
 select char(sum(counts))
 from bass2.DW_NEWBUSI_MMS_$op_month a 
 where a.SEND_STATUS in (0,1,2,3) 
 and a.SVCITEM_ID in  (400002)
 and  a.calltype_id in (1)
 ) a
  from bass2.dual
with ur
"
exec_sql $sql_buff
  
#	14	���Ų���-����	ͳ�������ڱ�ʡ�ƶ��û������Լ���ƽ̨�Ĳ��š�	number(15)	��λ����


 
 set sql_buff "
insert into G_S_22076_MONTH_1
select 'P14'
,(
select char(count(0))
from   bass2.DW_NEWBUSI_MMS_$op_month a
where SER_CODE = '106540320000'
 and  a.calltype_id in (1)
 ) a
  from bass2.dual
with ur
"
exec_sql $sql_buff


#	15	��������ҵ��ռ��GPRS����	ͳ�������ڳ����š�GPRS�����⣬���������˼��������õ�����ҵ�������GPRS���������û���ʡ����ʡ�����γ��õ������	number(15)	��λ��MB




set sql_buff "
insert into G_S_22076_MONTH_1
select 'P15'
,(
select char(bigint(sum(RATING_RES/1024)))  from   bass2.dw_newbusi_gprs_$op_month
where SERVICE_CODE not in( 
 '999999'	
,'4020000001'	
,'4000000006'	
,'4000000005'	
,'4000000004'	
,'4000000003'	
,'4000000002'	
,'4000000001'	
,'2111000000'	
,'1010000002'	
,'1010000001'	
)
) from bass2.dual  
with ur
  "
exec_sql $sql_buff

  
#CU9260	����ҵ��ͻ�������
#	16	����ͻ�������	ͳ�������ڿ�ͨ�˲��壨�����˲���ͼ��Ų��壬����������Ϊ�����û�����������ĩ�����Ŀͻ�������������ĩ��ȡ�����幦�ܵĿͻ���	number(12)	��λ����

set sql_buff "
insert into G_S_22076_MONTH_1
select 'P16'
,(
select CHAR(count(distinct a.user_id))
                 from    BASS2.dw_product_func_$op_month  a , BASS2.dw_product_$op_month b
                 where b.usertype_id in (1,2,9) and a.user_id=b.user_id 
			and a.service_id in (305120)
			AND replace(char(a.valid_date),'-','')    < '$next_month_firstday'
			and   replace(char(a.expire_date),'-','')  >= '$next_month_firstday'
			and b.userstatus_id in (1,2,3,6,8) 
			and b.test_mark <> 1 
			and a.STS in (1)
) from bass2.dual
with ur
"
exec_sql $sql_buff

#	17	VPMN-����	ͳ��������VPMN��ʽ���͵Ķ��ţ���ͳ������	number(15)	��λ����

#	
#	
#	
#	        set sql_buff "declare global temporary table session.G_S_22076_MONTH_vpmn
#	                      (
#	        PRODUCT_NO              VARCHAR(16)         
#	        ,VPMN_ID                 VARCHAR(20)     
#	                      )
#	                      partitioning key
#	                      (PRODUCT_NO)
#	                      using hashing
#	                      with replace on commit preserve rows not logged in tbs_user_temp"
#	
#	exec_sql $sql_buff
#	
#	 set sql_buff "
#	insert into session.G_S_22076_MONTH_vpmn
#	   select PRODUCT_NO,VPMN_ID from   (
#	  select PRODUCT_NO,VPMN_ID
#	  ,row_number()over(partition by PRODUCT_NO order by EXPIRE_DATE desc , VALID_DATE desc ) rn
#	  from  bass2.dw_vpmn_member_$op_month a
#	 where replace(char(a.valid_date),'-','')    < '$next_month_firstday'
#	 and   replace(char(a.expire_date),'-','')  >= '$next_month_firstday'
#	 and PRODUCT_NO is not null
#	 ) a where rn = 1
#	"
#	
#	 
#	exec_sql $sql_buff
#	
#	        set sql_buff "\
#	             create index  session.idx_22076_MONTH_vpmn on session.G_S_22076_MONTH_vpmn(PRODUCT_NO) "   
#	exec_sql $sql_buff
#		     
#		     
#	
#	#200004	���ڵ�Ե����	2	��ͨ����	2	����ҵ��
#	 set sql_buff "
#	 
#	 insert into G_S_22076_MONTH_1
#	select 'P17'
#	,(
#		select CHAR(sum(counts))
#			       from (select PRODUCT_NO,OPP_NUMBER,counts from  bass2.dw_newbusi_sms_$op_month a
#				where  a.calltype_id=0 and a.svcitem_id in (200004)) a
#			       join session.G_S_22076_MONTH_vpmn b on a.PRODUCT_NO = b.PRODUCT_NO
#			       join session.G_S_22076_MONTH_vpmn c on a.OPP_NUMBER = c.PRODUCT_NO
#			       where 
#			        b.VPMN_ID=c.VPMN_ID
#	) from bass2.dual
#	"
#	
#	exec_sql $sql_buff
#	

## ����ȡ��������������������ҵ��չ�йأ���0

 set sql_buff "
 
 insert into G_S_22076_MONTH_1
select 'P17'
,'0' from bass2.dual
with ur
"

exec_sql $sql_buff

  

#	18	����ռ��GPRS����	ͳ�������ڲ���ҵ�������GPRS���������û���ʡ����ʡ�����γ��õ������	number(15)	��λ��MB
#	����ϵͳ����ȡ������������


set sql_buff "
insert into G_S_22076_MONTH_1
select 'P18'
,(
select char(sum(RATING_RES/1024)) from   bass2.dw_newbusi_gprs_$op_month
where SERVICE_CODE in( '1010000001','1010000002')
) from bass2.dual
with ur
"
exec_sql $sql_buff


			       
#	19	��Ե����-����-����	ͳ�������ڱ�ʡ�ƶ��û������Թ���������Ӫ���û��Ķ���	number(15)	��λ����

set sql_buff "
insert into G_S_22076_MONTH_1
select 'P19'
,(
		select CHAR(sum(COUNTS))
		       from bass2.dw_newbusi_sms_$op_month a
		       where  a.calltype_id=1 
		       and a.svcitem_id in (200001,200002,200005)
) from bass2.dual
with ur
"
exec_sql $sql_buff



			    

		set sql_buff "
	INSERT INTO  BASS1.G_S_22076_MONTH
select	$op_month,'$op_month'
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P02')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P03')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P04')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P05')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P06')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P07')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P08')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P09')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P10')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P11')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P12')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P13')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P14')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P15')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P16')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P17')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P18')
,(select INDEX_VALUE from G_S_22076_MONTH_1 where INDEX_NAME = 'P19')
from bass2.dual
with ur
	"
	exec_sql $sql_buff


  #1.���chkpkunique
	set tabname "G_S_22076_MONTH"
	set pk 			"OP_MONTH"
	chkpkunique ${tabname} ${pk} ${op_month}
	
  aidb_runstats bass1.G_S_22076_MONTH 3
	
	return 0
}


 


                           
##				   
##	#	
##	#	�ӿڵ�Ԫ���ƣ���ֵҵ��KPI
##	#	�ӿڵ�Ԫ���룺 22076
##	#	�ӿڵ�Ԫ˵������¼��ֵҵ����KPI��Ϣ��
##	#	1�����ڡ�����ռ��GPRS������ָ�꣬�����޷���ȡ�ģ�������ϵͳ����ȡ��������������
##	#	2������ָ��ھ�˵�� �μ� ��2011�궨��ͳ�Ʊ����ƶȡ���
##	#	������ֵҵ��-������ʾ-�ͻ������� 
##	#	��Ե�������ڡ����䡢ʡ��
##	#	���Ŷ���
##	#	����ͻ�������
##	#	
##	ʮ�������š�����ʡ�ʼ�������ͨ���
##	
##	
##	select
##	                       b.crm_brand_id3,
##	                       case when a.svcitem_id in (200004)               then 1
##	                            when a.svcitem_id in (200001,200002,200005) and a.opposite_id in (13) then 23
##								              when a.svcitem_id in (200001,200002,200005) and a.opposite_id in (115,2) then 22
##								              when a.svcitem_id in (200001,200002,200005) and a.opposite_id in (14) then 24
##								              when a.svcitem_id in (200001,200002,200005) and a.opposite_id in (1,4,116) then 21
##								              when a.svcitem_id in (200001,200002,200005) then 23
##	                            when a.svcitem_id in (200003)               then 3
##	                       else 0 end,
##	                       sum(a.bill_counts)
##	                  from
##	                        dw_newbusi_sms_$year$month a,dw_product_$year$month b
##	                  where a.user_id = b.user_id and a.calltype_id=0
##	                     and a.svcitem_id in (200001,200002,200003,200004,200005)
##	                  group by
##	                       b.crm_brand_id3,
##	                       case when a.svcitem_id in (200004)               then 1
##	                            when a.svcitem_id in (200001,200002,200005) and a.opposite_id in (13) then 23
##								              when a.svcitem_id in (200001,200002,200005) and a.opposite_id in (115,2) then 22
##								              when a.svcitem_id in (200001,200002,200005) and a.opposite_id in (14) then 24
##								              when a.svcitem_id in (200001,200002,200005) and a.opposite_id in (1,4,116) then 21
##								              when a.svcitem_id in (200001,200002,200005) then 23
##	                            when a.svcitem_id in (200003)               then 3
##	                       else 0 end
##			       
##			       
##	
##	CU9181	���Ŷ���ʹ�ÿͻ���
##	
##	���Ŷ��żƷ���	��	DS6111
##	
##	select  case
##	                                  when c.crm_brand_id3 in ($rep_global_brand_id) then 2
##	                                  when c.crm_brand_id3 in ($rep_mzone_brand_id) then 3
##	                                  else 4
##	                              end,
##	                              sum(b.counts)
##	                      from dw_enterprise_member_mid_$year$month a,dw_newbusi_ismg_$year$month b,
##	                           dw_product_$year$month c
##	                      where a.user_id = b.user_id and a.user_id = c.user_id
##	                         and b.sp_code  = '931007' and b.calltype_id in (1)
##	                      group by case
##	                                  when c.crm_brand_id3 in ($rep_global_brand_id) then 2
##	                                  when c.crm_brand_id3 in ($rep_mzone_brand_id) then 3
##	                                  else 4
##	                               end
##	
##	
##	CU9260	����ҵ��ͻ�������
##	
##	select
##	                            b.crm_brand_id3 as crm_brand_id3,
##	                            count(distinct a.user_id) as numbs1
##	                 from    dw_product_func_$year$month  a , dw_product_$year$month b
##	                 where b.usertype_id in (1,2,9) and a.user_id=b.user_id and a.service_id in (305120)
##	                 and date(a.valid_date) <date('$next_month') and date(a.expire_date) >=date('$next_month')
##	                 and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1 and a.STS in (1)
##	                 group by
##	                            b.crm_brand_id3
##				    
##				    
##	
##	��Ϣ�㲥�Ʒ���	��	DS6131
##	���У�����	��	DS6141
##	      ����	��	DS6151
##	      
##	
##	#	
##	#	���Ա���	��������	��������	��������	��ע
##	#	00	��¼�к�	Ψһ��ʶ��¼�ڽӿ������ļ��е��кš�	number(8)	
##	#	01	�·�	��ʽ��YYYYMM	char(6)	����
##	#	02	������ֵҵ��-������ʾ-�ͻ�������	ͳ��������ʹ��������ʾҵ��ı�ʡ�û���	number(12)	��λ����
##	#	03	������ֵҵ��-��ת-������-�Ʒѷ�����	ͳ�������ڱ�ʡ�ƶ��û���ת�������ƶ������ͨ����	number(15)	��λ������
##	#	04	������ֵҵ��-��ת-������-�Ʒѷ�����	ͳ�������ڱ�ʡ�ƶ��û���ת������������Ӫ�̺����ͨ����	number(15)	��λ������
##	#	05	��Ե����-����-ʡ��-����	ͳ�������ڱ�ʡ�ƶ��û���������ʡ�ƶ��û��Ķ��ţ������Ŷ������У���	number(15)	��λ����
##	#	06	��Ե����-����-ʡ��-����	ͳ�������ڱ�ʡ�ƶ��û���������ʡ�ƶ��û��Ķ��š�	number(15)	��λ����
##	#	07	��Ե����-�������γ���	ͳ�������ڱ�ʡ�ƶ��û��ڹ������γ���״̬�·����������û��Ķ��ţ��������ƶ��û��͹���������Ӫ���û�����	number(15)	��λ����
##	#	08	���Ŷ���-����	ͳ�����ڱ�ʡ�ƶ��û������Լ���ƽ̨�Ķ��š�	number(15)	��λ����
##	#	09	��Ե����-����-ʡ��-����	ͳ�����ڱ�ʡ�ƶ��û���������ʡ�ƶ��û��Ĳ��ţ������Ų������У���	number(15)	��λ����
##	#	10	��Ե����-����-ʡ��-����	ͳ�������ڱ�ʡ�ƶ��û���������ʡ�ƶ��û��Ĳ��š�	number(15)	��λ����
##	#	11	��Ե����-����-ʡ��-����	ͳ�������ڱ�ʡ�ƶ��û���������ʡ�ƶ��û��Ĳ��š�	number(15)	��λ����
##	#	12	��Ե����-����-����	ͳ�������ڱ�ʡ�ƶ��û�������������Ӫ���û��Ĳ��š�	number(15)	��λ����
##	#	13	��Ե����-����-����	ͳ�������ڱ�ʡ�ƶ��û�������������Ӫ���û��Ĳ��š�	number(15)	��λ����
##	#	14	���Ų���-����	ͳ�������ڱ�ʡ�ƶ��û������Լ���ƽ̨�Ĳ��š�	number(15)	��λ����
##	#	15	��������ҵ��ռ��GPRS����	ͳ�������ڳ����š�GPRS�����⣬���������˼��������õ�����ҵ�������GPRS���������û���ʡ����ʡ�����γ��õ������	number(15)	��λ��MB
##	#	16	����ͻ�������	ͳ�������ڿ�ͨ�˲��壨�����˲���ͼ��Ų��壬����������Ϊ�����û�����������ĩ�����Ŀͻ�������������ĩ��ȡ�����幦�ܵĿͻ���	number(12)	��λ����
##	#	17	VPMN-����	ͳ��������VPMN��ʽ���͵Ķ��ţ���ͳ������	number(15)	��λ����
##	#	18	����ռ��GPRS����	ͳ�������ڲ���ҵ�������GPRS���������û���ʡ����ʡ�����γ��õ������	number(15)	��λ��MB
##	#	����ϵͳ����ȡ������������
##	#	19	��Ե����-����-����	ͳ�������ڱ�ʡ�ƶ��û������Թ���������Ӫ���û��Ķ���	number(15)	��λ����
##	
##			select CHAR(sum(COUNTS))
##	                  from
##	                       (select user_id,counts
##			       from bass2.dw_newbusi_sms_$op_month a
##			       where  a.calltype_id=1 and a.svcitem_id in (200001,200002,200005)) a
##				,bass2.dw_product_$op_month b
##	                  where a.user_id = b.user_id 
##			  
##				
##	
##	200001	��ͨ��Ե����(C��)	2	��ͨ����	2	����ҵ��
##	200002	С��ͨ��Ե����	2	��ͨ����	2	����ҵ��
##	200005	��ͨ��Ե����(G��)	2	��ͨ����	2	����ҵ��
##	
##	
##	
##	���ͼ��Ų���	��	DS8281
##	
##	
##	CU9420	���ţ�MMS��ҵ��ʹ�ÿͻ���
##	CU9421	���У���Ե����ʹ�ÿͻ���
##	
##	�������� ��MMS���Ʒ���	��	DS8101
##	���У���Ե����	��	DS8201
##	
##	
##	select count(distinct a.user_id) 
##	 from	bass2.DW_NEWBUSI_MMS_$op_month a , bass2.dw_product_$op_month b
##	 where a.SEND_STATUS in (0,1,2,3) and  b.usertype_id in (1,2,9) and a.user_id=b.user_id
##	    and a.SVCITEM_ID in  (400001,400002,400005)
##	 and a.calltype_id in (0)
##	
##	
##	