
######################################################################################################		
#接口名称: 增值业务KPI                                                               
#接口编码：22076                                                                                          
#接口说明：记录增值业务月KPI信息。
#程序名称: G_S_22076_MONTH.tcl                                                                            
#功能描述: 生成22076的数据
#运行粒度: MONTH
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20110922
#问题记录：
#修改历史: 1. panzw 20110922	1.7.5 newly added
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

        #求下个月 格式 yyyymm
        set next_month [GetNextMonth $op_month]
        #求下个月第一天
        set next_month_firstday "${next_month}01"
	
 

  #删除本期数据
	set sql_buff "delete from bass1.G_S_22076_MONTH where time_id=$op_month"
	exec_sql $sql_buff
	
 set sql_buff "alter table bass1.G_S_22076_MONTH_1 activate not logged initially with empty table"
	exec_sql $sql_buff
	
#	02	语音增值业务-来电显示-客户到达数	统计周期内使用来电显示业务的本省用户。	number(12)	单位：户
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

##	05	点对点短信-网内-省内-上行

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
                

#06	点对点短信-网内-省际-上行


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

			
#	07	点对点短信-国际漫游出访
#200003	国际点对点短信	2	普通短信	2	数据业务

## 暂无法确定接受号码是否在国内还是国外
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

#08  CU9181	集团短信使用客户数
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

#	09	点对点彩信-网内-省内-上行	统计周期本省移动用户发送至本省移动用户的彩信（含集团彩信上行）。	number(15)	单位：条

#20120704 : 原有的口径：SVCITEM_ID in  (400001)
#替换成： and drtype_id in (90605,103)
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

#	10	点对点彩信-网内-省际-上行	统计周期内本省移动用户发送至外省移动用户的彩信。	number(15)	单位：条
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

#	11	点对点彩信-网内-省际-下行	统计周期内本省移动用户接收自外省移动用户的彩信。	number(15)	单位：条

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

#	12	点对点彩信-网间-上行	统计周期内本省移动用户发送至其他运营商用户的彩信。	number(15)	单位：条

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
  

#	13	点对点彩信-网间-下行	统计周期内本省移动用户接收自其他运营商用户的彩信。	number(15)	单位：条

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
  
#	14	集团彩信-下行	统计周期内本省移动用户接收自集团平台的彩信。	number(15)	单位：条


 
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


#	15	其他数据业务占用GPRS流量	统计周期内除彩信、GPRS上网外，其他单独核减流量费用的数据业务产生的GPRS流量，含用户在省内与省际漫游出访的情况。	number(15)	单位：MB




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

  
#CU9260	彩铃业务客户到达数
#	16	彩铃客户到达数	统计周期内开通了彩铃（含个人彩铃和集团彩铃，后者需折算为个人用户数），且月末在网的客户数，不包括月末已取消彩铃功能的客户数	number(12)	单位：户

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

#	17	VPMN-短信	统计周期内VPMN方式发送的短信，仅统计上行	number(15)	单位：条

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
#	#200004	网内点对点短信	2	普通短信	2	数据业务
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

## 西藏取不到这样的数，可能与业务开展有关，报0

 set sql_buff "
 
 insert into G_S_22076_MONTH_1
select 'P17'
,'0' from bass2.dual
with ur
"

exec_sql $sql_buff

  

#	18	彩信占用GPRS流量	统计周期内彩信业务产生的GPRS流量，含用户在省内与省际漫游出访的情况。	number(15)	单位：MB
#	网管系统先行取数汇总至二经


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


			       
#	19	点对点短信-网间-下行	统计周期内本省移动用户接收自国内其他运营商用户的短信	number(15)	单位：条

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


  #1.检查chkpkunique
	set tabname "G_S_22076_MONTH"
	set pk 			"OP_MONTH"
	chkpkunique ${tabname} ${pk} ${op_month}
	
  aidb_runstats bass1.G_S_22076_MONTH 3
	
	return 0
}


 


                           
##				   
##	#	
##	#	接口单元名称：增值业务KPI
##	#	接口单元编码： 22076
##	#	接口单元说明：记录增值业务月KPI信息。
##	#	1、对于“彩信占用GPRS流量”指标，二经无法获取的，由网管系统先行取数汇总至二经。
##	#	2、以下指标口径说明 参见 “2011年定期统计报表制度”：
##	#	语音增值业务-来电显示-客户到达数 
##	#	点对点短信网内、网间、省际
##	#	集团短信
##	#	彩铃客户到达数
##	#	
##	十六、短信、彩信省际及互联互通情况
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
##	CU9181	集团短信使用客户数
##	
##	集团短信计费量	条	DS6111
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
##	CU9260	彩铃业务客户到达数
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
##	信息点播计费量	条	DS6131
##	其中：上行	条	DS6141
##	      下行	条	DS6151
##	      
##	
##	#	
##	#	属性编码	属性名称	属性描述	属性类型	备注
##	#	00	记录行号	唯一标识记录在接口数据文件中的行号。	number(8)	
##	#	01	月份	格式：YYYYMM	char(6)	主键
##	#	02	语音增值业务-来电显示-客户到达数	统计周期内使用来电显示业务的本省用户。	number(12)	单位：户
##	#	03	语音增值业务-呼转-至网内-计费分钟数	统计周期内本省移动用户呼转至其他移动号码的通话。	number(15)	单位：分钟
##	#	04	语音增值业务-呼转-至网间-计费分钟数	统计周期内本省移动用户呼转至其他国内运营商号码的通话。	number(15)	单位：分钟
##	#	05	点对点短信-网内-省内-上行	统计周期内本省移动用户发送至本省移动用户的短信（含集团短信上行）。	number(15)	单位：条
##	#	06	点对点短信-网内-省际-上行	统计周期内本省移动用户发送至外省移动用户的短信。	number(15)	单位：条
##	#	07	点对点短信-国际漫游出访	统计周期内本省移动用户在国际漫游出访状态下发送至国内用户的短信（含国内移动用户和国内其他运营商用户）。	number(15)	单位：条
##	#	08	集团短信-下行	统计周期本省移动用户接收自集团平台的短信。	number(15)	单位：条
##	#	09	点对点彩信-网内-省内-上行	统计周期本省移动用户发送至本省移动用户的彩信（含集团彩信上行）。	number(15)	单位：条
##	#	10	点对点彩信-网内-省际-上行	统计周期内本省移动用户发送至外省移动用户的彩信。	number(15)	单位：条
##	#	11	点对点彩信-网内-省际-下行	统计周期内本省移动用户接收自外省移动用户的彩信。	number(15)	单位：条
##	#	12	点对点彩信-网间-上行	统计周期内本省移动用户发送至其他运营商用户的彩信。	number(15)	单位：条
##	#	13	点对点彩信-网间-下行	统计周期内本省移动用户接收自其他运营商用户的彩信。	number(15)	单位：条
##	#	14	集团彩信-下行	统计周期内本省移动用户接收自集团平台的彩信。	number(15)	单位：条
##	#	15	其他数据业务占用GPRS流量	统计周期内除彩信、GPRS上网外，其他单独核减流量费用的数据业务产生的GPRS流量，含用户在省内与省际漫游出访的情况。	number(15)	单位：MB
##	#	16	彩铃客户到达数	统计周期内开通了彩铃（含个人彩铃和集团彩铃，后者需折算为个人用户数），且月末在网的客户数，不包括月末已取消彩铃功能的客户数	number(12)	单位：户
##	#	17	VPMN-短信	统计周期内VPMN方式发送的短信，仅统计上行	number(15)	单位：条
##	#	18	彩信占用GPRS流量	统计周期内彩信业务产生的GPRS流量，含用户在省内与省际漫游出访的情况。	number(15)	单位：MB
##	#	网管系统先行取数汇总至二经
##	#	19	点对点短信-网间-下行	统计周期内本省移动用户接收自国内其他运营商用户的短信	number(15)	单位：条
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
##	200001	联通点对点短信(C网)	2	普通短信	2	数据业务
##	200002	小灵通点对点短信	2	普通短信	2	数据业务
##	200005	联通点对点短信(G网)	2	普通短信	2	数据业务
##	
##	
##	
##	发送集团彩信	条	DS8281
##	
##	
##	CU9420	彩信（MMS）业务使用客户数
##	CU9421	其中：点对点彩信使用客户数
##	
##	三、彩信 （MMS）计费量	条	DS8101
##	其中：点对点彩信	条	DS8201
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