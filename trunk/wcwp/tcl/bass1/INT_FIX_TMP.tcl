#2012-04-24

proc Deal_fix02062 { op_time optime_month } {
## 剔除已经失效的数据 ，通过把02062中为“有效”的用户和用户表中失效的用户关联。
##

#当天 yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#当天 yyyy-mm-dd
set optime $op_time
set curr_month [string range $op_time 0 3][string range $op_time 5 6]
set last_day [GetLastDay [string range $timestamp 0 7]]

# fetch canceled
set sql_buff "
insert into BASS1.G_A_02062_DAY
(
         TIME_ID
        ,ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,PRODUCT_NO
        ,INDUSTRY_ID
        ,GPRS_TYPE
        ,DATA_SOURCE
        ,CREATE_DATE
        ,STATUS_ID
)
select 
         a.TIME_ID
        ,a.ENTERPRISE_ID
        ,a.USER_ID
        ,a.ENTERPRISE_BUSI_TYPE
        ,a.MANAGE_MODE
        ,a.PRODUCT_NO
        ,a.INDUSTRY_ID
        ,a.GPRS_TYPE
        ,a.DATA_SOURCE
        ,a.CREATE_DATE
        ,'2' STATUS_ID
from (
        select 
         $timestamp TIME_ID
        ,ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,PRODUCT_NO
        ,INDUSTRY_ID
        ,GPRS_TYPE
        ,DATA_SOURCE
        ,CREATE_DATE
        ,STATUS_ID
        from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id,USER_ID order by time_id desc ) rn 
			                from 
			                G_A_02062_DAY  t
							where time_id <= $timestamp
			  ) a
			where rn = 1	and STATUS_ID = '1'
     ) a , bass2.dw_product_$timestamp b 
     where a.USER_ID = b.user_id 
     and b.userstatus_id not in (1,2,3,6,8)
	with ur
	
"
exec_sql $sql_buff

return 0  

}


#2011-10-20

proc Deal_fiximsi { op_time optime_month } {


#当天 yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#当天 yyyy-mm-dd
set optime $op_time
set curr_month [string range $op_time 0 3][string range $op_time 5 6]
set last_day [GetLastDay [string range $timestamp 0 7]]


set sql_buff "
alter table bass1.G_A_02004_DAY_IMSI activate not logged initially with empty table
"
exec_sql $sql_buff

set sql_buff "
insert into G_A_02004_DAY_IMSI
select 
         a.TIME_ID
        ,a.USER_ID
        ,a.CUST_ID
        ,a.USERTYPE_ID
        ,a.CREATE_DATE
        ,a.USER_BUS_TYP_ID
        ,a.PRODUCT_NO
        ,value(b.IMSI,a.imsi)
        ,a.CMCC_ID
        ,a.CHANNEL_ID
        ,a.MCV_TYP_ID
        ,a.PROMPT_TYPE
        ,a.SUBS_STYLE_ID
        ,a.BRAND_ID
        ,a.SIM_CODE
from 	
(
select 
        $timestamp TIME_ID
        ,USER_ID
        ,CUST_ID
        ,USERTYPE_ID
        ,CREATE_DATE
        ,USER_BUS_TYP_ID
        ,PRODUCT_NO
        ,IMSI
        ,CMCC_ID
        ,CHANNEL_ID
        ,MCV_TYP_ID
        ,PROMPT_TYPE
        ,SUBS_STYLE_ID
        ,BRAND_ID
        ,SIM_CODE
    ,row_number() over(partition by user_id order by time_id desc ) row_id   
from bass1.g_a_02004_day
where time_id<=$last_day ) a
, bass2.dw_product_bass1_$timestamp b
where a.row_id = 1
and a.user_id = b.user_id
and a.user_id not in (select distinct  user_id from g_a_02004_day where time_id = $timestamp )
"
exec_sql $sql_buff

  aidb_runstats bass1.G_A_02004_DAY_IMSI 3

set sql_buff "
insert into G_A_02004_DAY
select a.*
from G_A_02004_DAY_IMSI a
where time_id = 20111020
"
exec_sql $sql_buff


  #1.检查chkpkunique
set tabname "g_a_02004_day"
set pk                  "USER_ID"
chkpkunique ${tabname} ${pk} ${timestamp}


return 0  

}


#2011-10-20


proc Deal_fixqixintong { op_time optime_month } {
## 剔除已经失效的数据
## 加入未添加的数据
##

#当天 yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#当天 yyyy-mm-dd
set optime $op_time
set curr_month [string range $op_time 0 3][string range $op_time 5 6]
set last_day [GetLastDay [string range $timestamp 0 7]]

# fetch canceled
set sql_buff "
insert into BASS1.G_A_02054_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
			select distinct
			 $timestamp TIME_ID
			,ENTERPRISE_ID
			,ENTERPRISE_BUSI_TYPE
			,MANAGE_MODE
			,ORDER_DATE
			,'2' STATUS_ID
                from 
                (
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02054_day 
			where  ENTERPRISE_BUSI_TYPE = '1330'
			) t 
		) t2 
		where rn = 1 
			and STATUS_ID ='1'
			and ENTERPRISE_ID not in (
				select CUST_PARTY_ROLE_ID  ENTERPRISE_ID 
				from bass2.dw_product_ins_off_ins_prod_ds a
				where a.offer_id = 112091001001
				and product_spec_id = 172000000910
				and IS_MAIN_OFFER = 1
				and date(VALID_DATE) < date(EXPIRE_DATE)
				and date(EXPIRE_DATE) > '$op_time'
			)
with ur
"
exec_sql $sql_buff

#fetch ordered



set sql_buff "
insert into BASS1.G_A_02054_DAY
			(
				TIME_ID
				,ENTERPRISE_ID
				,ENTERPRISE_BUSI_TYPE
				,MANAGE_MODE
				,ORDER_DATE
				,STATUS_ID
			)
select 
distinct 
$timestamp
,CUST_PARTY_ROLE_ID   ENTERPRISE_ID
,'1330' ENTERPRISE_BUSI_TYPE
,'2' MANAGE_MODE
,replace(char(date(A.done_date)),'-','') as ORDER_DATE
,'1' STATUS_ID
from bass2.dw_product_ins_off_ins_prod_ds a
where a.offer_id = 112091001001
	and product_spec_id = 172000000910
	and IS_MAIN_OFFER = 1
	and date(VALID_DATE)  < date(EXPIRE_DATE)
	and date(EXPIRE_DATE) > '$op_time'
	and CUST_PARTY_ROLE_ID not in 
	(
		select ENTERPRISE_ID
		from 
		(
		select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
		from (
		select *
		from g_a_02054_day 
		where  ENTERPRISE_BUSI_TYPE = '1330'
		) t 
		) t2 
		where rn = 1 
		and STATUS_ID ='1'
	) with ur
"
exec_sql $sql_buff

return 0  

}





proc Deal_fixchewutong { op_time optime_month } {
## 剔除已经失效的数据
## 加入未添加的数据
##

#当天 yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#当天 yyyy-mm-dd
set optime $op_time
set curr_month [string range $op_time 0 3][string range $op_time 5 6]
set last_day [GetLastDay [string range $timestamp 0 7]]

# fetch canceled
set sql_buff "
insert into BASS1.G_A_02062_DAY
			(
         TIME_ID
        ,ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,PRODUCT_NO
        ,INDUSTRY_ID
        ,GPRS_TYPE
        ,DATA_SOURCE
        ,CREATE_DATE
        ,STATUS_ID
			)
			select distinct
	$timestamp TIME_ID
        ,ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,PRODUCT_NO
        ,INDUSTRY_ID
        ,GPRS_TYPE
        ,DATA_SOURCE
        ,CREATE_DATE
        ,'2' STATUS_ID
                from 
                (
			select t.*,row_number()over(partition by USER_ID order by time_id desc) rn 
			from (
			select *
			from G_A_02062_DAY 
			where  ENTERPRISE_BUSI_TYPE = '1241'
			) t 
		) t2 
			where rn = 1 
			and STATUS_ID ='1'
			and USER_ID not in (
				select product_instance_id
				from bass2.dw_product_ins_off_ins_prod_ds a
				where a.offer_id = 112001005701
				and is_main_offer = 0
				and date(VALID_DATE) < date(EXPIRE_DATE)
				and date(EXPIRE_DATE) > '$op_time'
			)
with ur
"
exec_sql $sql_buff

#fetch ordered



set sql_buff "
insert into BASS1.G_A_02062_DAY
			(
         TIME_ID
        ,ENTERPRISE_ID
        ,USER_ID
        ,ENTERPRISE_BUSI_TYPE
        ,MANAGE_MODE
        ,PRODUCT_NO
        ,INDUSTRY_ID
        ,GPRS_TYPE
        ,DATA_SOURCE
        ,CREATE_DATE
        ,STATUS_ID
			)
select 
distinct 
$timestamp TIME_ID
        ,a.CUST_PARTY_ROLE_ID ENTERPRISE_ID
        ,a.product_instance_id USER_ID
        ,'1241' ENTERPRISE_BUSI_TYPE
        ,'3' MANAGE_MODE
        , b.PRODUCT_NO
        ,'2' INDUSTRY_ID
        ,'1' GPRS_TYPE
        ,'cmm.hn' DATA_SOURCE
        ,replace(char(date(A.done_date)),'-','') as CREATE_DATE 
	,'1' STATUS_ID
from bass2.dw_product_ins_off_ins_prod_ds a ,bass2.dw_product_$timestamp b
where a.offer_id = 112001005701
	and a.product_instance_id = b.user_id 
	and a.IS_MAIN_OFFER = 0
	and b.TEST_MARK = 0
	and date(a.VALID_DATE)  < date(a.EXPIRE_DATE)
	and date(a.EXPIRE_DATE) > '$op_time'
	and a.CUST_PARTY_ROLE_ID not in 
	(
		select ENTERPRISE_ID
		from 
		(
		select t.*,row_number()over(partition by USER_ID order by time_id desc) rn 
		from (
		select *
		from G_A_02062_DAY 
		where  ENTERPRISE_BUSI_TYPE = '1241'
		) t 
		) t2 
		where rn = 1 
		and STATUS_ID ='1'
	) with ur
"
exec_sql $sql_buff

return 0  

}



proc ADJ_04008_R107 { op_time optime_month } {

puts $op_time
puts $optime_month

#当天 yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
puts $timestamp
puts $op_month
#当天 yyyy-mm-dd
set optime $op_time
set curr_month [string range $op_time 0 3][string range $op_time 5 6]
set last_day [GetLastDay [string range $timestamp 0 7]]
set this_month_last_day [string range $curr_month 0 5][GetThisMonthDays [string range $curr_month 0 5]01]

puts $optime
puts $curr_month

puts $timestamp
puts $this_month_last_day
	
if { $timestamp == $this_month_last_day } {

set sql_buff "
update ( select * from  BASS1.G_S_04008_DAY where time_id = ${timestamp} ) t 
set TOLL_CALL_FEE = char(bigint(TOLL_CALL_FEE)+400) 
with ur
"
exec_sql $sql_buff

}

return 0  

}



proc ADJ_04008_R108 { op_time optime_month } {

puts $op_time
puts $optime_month

#当天 yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
puts $timestamp
puts $op_month
#当天 yyyy-mm-dd
set optime $op_time
set curr_month [string range $op_time 0 3][string range $op_time 5 6]
set last_day [GetLastDay [string range $timestamp 0 7]]
set this_month_last_day [string range $curr_month 0 5][GetThisMonthDays [string range $curr_month 0 5]01]

puts $optime
puts $curr_month

puts $timestamp
puts $this_month_last_day
	
if { $timestamp == $this_month_last_day } {

set sql_buff "
update (select * from  BASS1.G_S_04008_DAY  where time_id = ${timestamp} ) t 
set BASE_BILL_DURATION = char(bigint(BASE_BILL_DURATION)-5)
with ur
"
exec_sql $sql_buff

}

return 0  

}




proc ADJ_R235_MONTH1 { op_time optime_month } {

puts $op_time
puts $optime_month

#当天 yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
puts $timestamp
puts $op_month
#当天 yyyy-mm-dd
set optime $op_time
set curr_month [string range $op_time 0 3][string range $op_time 5 6]
set last_day [GetLastDay [string range $timestamp 0 7]]
set this_month_last_day [string range $curr_month 0 5][GetThisMonthDays [string range $curr_month 0 5]01]

puts $optime
puts $curr_month

puts $timestamp
puts $this_month_last_day
	

set sql_buff "
ALTER TABLE g_s_03004_03005_R235_adj ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE
"
exec_sql $sql_buff

set sql_buff "
ALTER TABLE G_S_03004_MONTH_ADJ_BAK ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE
"
exec_sql $sql_buff

set sql_buff "
ALTER TABLE G_S_03005_MONTH_ADJ_BAK ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE
"
exec_sql $sql_buff

set sql_buff "
insert into g_s_03004_03005_R235_adj  
select '$op_month'
,a.user_id   
, case when c.user_id is null then '0' else '1' end iffeezero  
, case when d.user_id is null then '0' else '1' end if03004  
from  (                  
	select distinct user_id 
	from BASS1.G_S_21003_MONTH_mobile   a                          
	,int_02004_02008_month_stage b                  
	where a.product_no = b.product_no                 
	and  b.usertype_id NOT IN ('2010','2020','2030','9000')                 
	and b.test_flag = '0' 
	) a
left join  (
			select user_id                 
			from g_s_03004_month                 
			where time_id = $op_month                 
			and                   ( int(substr(ACCT_ITEM_ID,2))/100 in (1,2,3)                                 
			or ACCT_ITEM_ID in ('0401','0403','0407'))                  
			group by user_id                   
			having sum(bigint(FEE_RECEIVABLE)) > 0 
			) b  on  a.user_id = b.user_id
left join (    select  user_id from  g_s_03004_month                   
				where time_id = $op_month                
				and ( int(substr(ACCT_ITEM_ID,2))/100 in (1,2,3)                         
				or ACCT_ITEM_ID in ('0401','0403','0407')
				)                 
				group by user_id  having sum(bigint(FEE_RECEIVABLE)) <= 0                              
		  ) c on a.user_id = c.user_id
left join (select distinct  user_id from  g_s_03004_month where time_id = $op_month ) d on a.user_id = d.user_id
where b.user_id is  null
with ur
"
exec_sql $sql_buff

#调整0101科目

set sql_buff "
insert into G_S_03004_MONTH_ADJ_BAK  
select a.* from G_S_03004_MONTH a
	,g_s_03004_03005_R235_adj b  
	where a.time_id = $op_month 
	and a.ACCT_ITEM_ID = '0101' 
	and a.user_id = b.user_id  
	and b.IFFEEZERO = '1'
    and a.FEE_RECEIVABLE = '0'
	"
exec_sql $sql_buff

set sql_buff "
insert into G_S_03005_MONTH_ADJ_BAK  
select a.* from G_S_03005_MONTH a
	,g_s_03004_03005_R235_adj b  
	where a.time_id = $op_month 
	and a.ITEM_ID = '0100'
	and a.user_id = b.user_id  
	and b.IFFEEZERO = '1'
	and a.SHOULD_FEE = '0'
	and b.user_id in (select distinct user_id from G_S_03004_MONTH_ADJ_BAK)

"
exec_sql $sql_buff


set sql_buff "
delete 
 from G_S_03005_MONTH_ADJ_BAK a
where (user_id,acct_id) not in  (select user_id,max(acct_id) acct_id from G_S_03005_MONTH_ADJ_BAK group by user_id)
"
exec_sql $sql_buff


set sql_buff "
delete from 
(select * from G_S_03004_MONTH  a 
where time_id = $op_month
and  a.FEE_RECEIVABLE = '0'
and  a.ACCT_ITEM_ID = '0101' 
) a
where exists (select 1 from g_s_03004_03005_R235_adj b where a.user_id = b.user_id and b.IFFEEZERO = '1' )
 
"
exec_sql $sql_buff


set sql_buff "
insert into G_S_03004_MONTH
select 
         TIME_ID
        ,USER_ID
        ,ACCT_ITEM_ID
        ,BILL_CYC_ID
        ,'1' FEE_RECEIVABLE
        ,FAV_CHRG
from    G_S_03004_MONTH_ADJ_BAK
where time_id = $op_month
"
exec_sql $sql_buff


set sql_buff "
delete from 
(select * from G_S_03005_MONTH  a 
where time_id = $op_month
and  a.SHOULD_FEE = '0'
and  a.ITEM_ID = '0100' 
) a
where exists (select 1 from g_s_03004_03005_R235_adj b where a.user_id = b.user_id and b.IFFEEZERO = '1' )
"
exec_sql $sql_buff


set sql_buff "
insert into G_S_03005_MONTH
select 
         TIME_ID
        ,ACCT_ID
        ,USER_ID
        ,BILL_CYC_ID
        ,ITEM_ID
        ,'1' SHOULD_FEE
from    G_S_03005_MONTH_ADJ_BAK
where time_id = $op_month
"
exec_sql $sql_buff


set sql_buff "
select (
select sum(bigint(FEE_RECEIVABLE)) from  G_S_03004_MONTH where time_id = $op_month
) 
-
(
select sum(bigint(SHOULD_FEE)) from  G_S_03005_MONTH where time_id = $op_month
) diff030040_3005 
from bass2.dual
"

chkzero2 $sql_buff "03004 - 03005 not agree! "



return 0  

}




#如果ADJ_R235_MONTH1 不足以调整至校验通过，则进一步调整之
proc ADJ_R235_MONTH2 { op_time optime_month } {

puts $op_time
puts $optime_month

#当天 yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
puts $timestamp
puts $op_month
#当天 yyyy-mm-dd
set optime $op_time
set curr_month [string range $op_time 0 3][string range $op_time 5 6]
set last_day [GetLastDay [string range $timestamp 0 7]]
set this_month_last_day [string range $curr_month 0 5][GetThisMonthDays [string range $curr_month 0 5]01]

puts $optime
puts $curr_month

puts $timestamp
puts $this_month_last_day
	

set sql_buff "
ALTER TABLE G_S_03004_MONTH_ADJ_BAK2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE
"
exec_sql $sql_buff

set sql_buff "
ALTER TABLE G_S_03005_MONTH_ADJ_BAK2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE
"
exec_sql $sql_buff

#调整0203科目

set sql_buff "
insert into G_S_03004_MONTH_ADJ_BAK2  
select a.* from G_S_03004_MONTH a
	,g_s_03004_03005_R235_adj b  
	where a.time_id = $op_month 
	and a.ACCT_ITEM_ID = '0203' 
	and a.user_id = b.user_id  
	and b.IFFEEZERO = '1'
	and IF03004 = '1'
    and a.FEE_RECEIVABLE = '0'
	and b.user_id not in (select distinct user_id from G_S_03004_MONTH_ADJ_BAK)
	"
exec_sql $sql_buff

set sql_buff "
insert into G_S_03005_MONTH_ADJ_BAK2
select a.* from G_S_03005_MONTH a
	,g_s_03004_03005_R235_adj b  
	where a.time_id = $op_month 
	and a.ITEM_ID = '0200'
	and a.user_id = b.user_id  
	and b.IFFEEZERO = '1'
	and b.IF03004 = '1'
	and a.SHOULD_FEE = '0'
	and b.user_id not in (select distinct user_id from G_S_03005_MONTH_ADJ_BAK)
	and b.user_id in (select distinct user_id from G_S_03004_MONTH_ADJ_BAK2)
"
exec_sql $sql_buff


set sql_buff "
delete 
 from G_S_03005_MONTH_ADJ_BAK2 a
where (user_id,acct_id) not in  (select user_id,max(acct_id) acct_id from G_S_03005_MONTH_ADJ_BAK2 group by user_id)
"
exec_sql $sql_buff


set sql_buff "
delete from 
(select * from G_S_03004_MONTH  a 
where time_id = $op_month
and  a.FEE_RECEIVABLE = '0'
and  a.ACCT_ITEM_ID = '0203'
and  a.user_id not in (select distinct user_id from G_S_03004_MONTH_ADJ_BAK)
) a
where exists (select 1 from g_s_03004_03005_R235_adj b where a.user_id = b.user_id and b.IFFEEZERO = '1' and  b.IF03004 = '1' )
 
"
exec_sql $sql_buff


set sql_buff "
insert into G_S_03004_MONTH
select 
         TIME_ID
        ,USER_ID
        ,ACCT_ITEM_ID
        ,BILL_CYC_ID
        ,'1' FEE_RECEIVABLE
        ,FAV_CHRG
from    G_S_03004_MONTH_ADJ_BAK2
where time_id = $op_month
"
exec_sql $sql_buff


set sql_buff "
delete from 
(select * from G_S_03005_MONTH  a 
where time_id = $op_month
and  a.SHOULD_FEE = '0'
and  a.ITEM_ID = '0200' 
and  a.user_id not in (select distinct user_id from G_S_03005_MONTH_ADJ_BAK)
) a
where exists (select 1 from g_s_03004_03005_R235_adj b where a.user_id = b.user_id and b.IFFEEZERO = '1' and  b.IF03004 = '1')
"
exec_sql $sql_buff


set sql_buff "
insert into G_S_03005_MONTH
select 
         TIME_ID
        ,ACCT_ID
        ,USER_ID
        ,BILL_CYC_ID
        ,ITEM_ID
        ,'1' SHOULD_FEE
from    G_S_03005_MONTH_ADJ_BAK2
where time_id = $op_month
"
exec_sql $sql_buff

set sql_buff "
select (
select sum(bigint(FEE_RECEIVABLE)) from  G_S_03004_MONTH where time_id = $op_month
) 
-
(
select sum(bigint(SHOULD_FEE)) from  G_S_03005_MONTH where time_id = $op_month
) diff030040_3005 
from bass2.dual
"

chkzero2 $sql_buff "03004 - 03005 not agree! "

return 0  

}





proc Trans91003 { op_time optime_month } {
## 本过程在每月月底跑。（1号前处理。）
##
##~   INT_CHECK_L2_TO_DAY.tcl
##~   source /bassapp/bass1/tcl/INT_FIX_TMP.tcl
##~   Trans91003 $op_time $optime_month
##~   前提条件：BASS2.DIM_TACNUM_DEVID bass2.DIM_TERM_TAC_NEW_LOAD 已加载！
#当天 yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#当天 yyyy-mm-dd
set optime $op_time
set curr_month [string range $op_time 0 3][string range $op_time 5 6]
set last_day [GetLastDay [string range $timestamp 0 7]]
set app_name "INT_CHECK_L2_TO_DAY.tcl"

set sql_buff "
rename bass2.DIM_DEVICE_INFO_EX to DIM_DEVICE_INFO_EX_BAK${timestamp}
"
exec_sql $sql_buff

set sql_buff "
create table bass2.DIM_DEVICE_INFO_EX like bass2.DIM_DEVICE_INFO_EX_BAK${timestamp}  DISTRIBUTE BY HASH(ROW_ID)  IN TBS_DIM
"
exec_sql $sql_buff

set sql_buff "

insert into bass2.DIM_DEVICE_INFO_EX
select 
         ROW_ID
        ,TERM_TYPE
        ,TERM_BRAND
        ,TERM_BRAND_ID
        ,TERM_MODEL
        ,ALIAS
        ,DEV_ID
        ,MODE_2G
        ,MODE_3G
        ,MODE_4G
        ,OS_ID
        ,OS_VERSION
        ,IFWLAN
        ,IFWAP
        ,IFWWW
        ,IFGPRS
        ,IFGPS
        ,IFMMS
        ,FRONT_CAM
        ,REAR_CAM
        ,SCREEN_RESOLUTION
        ,SCREEN_SIZE
        ,SCREEN_DEEP
        ,IFHANDWRITE
        ,TOUCHTYPE
        ,TERM_DESIGN
        ,IFJAVA
        ,IFUSB
        ,IFBLUETEETH
        ,IFIFR
from  bass2.DIM_TERM_TAC_NEW_LOAD a
with ur
"
exec_sql $sql_buff




set sql_buff "
select count(0) from (
select DEV_ID,count(*) from BASS2.DIM_DEVICE_INFO_EX
group by DEV_ID
having count(*)>1
) a 
with ur
"

chkzero2 $sql_buff "BASS2.DIM_DEVICE_INFO_EX 主键不唯一！"


  aidb_runstats BASS2.DIM_DEVICE_INFO_EX 3



##~   生成入DIM_TERM_TAC的中间表DIM_TERM_TAC_TRANS
set sql_buff "
alter table bass2.DIM_TERM_TAC_TRANS activate not logged initially with empty table
"
exec_sql $sql_buff


##~   0-2G手机
##~   1-3G手机
##~   2-3G数据卡
##~   3-家庭网关
##~   4-无线固话
##~   5-上网本
##~   6-阅读器
##~   7-家庭手机
##~   8-平板电脑
##~   NULL – 集团公司未定义

##~   01- 手机
##~   02- 数据卡
##~   03- 上网本
##~   04- 家庭网关
##~   05- 无线固话
##~   06- 手机阅读
##~   07- 平板电脑


##~   select MODE_2G ,MODE_3G ,c.TERM_TYPE ,b.TERM_TYPE,count(0)
##~   from bass2.DIM_TACNUM_DEVID a
##~   ,bass2.DIM_TERM_TAC b
##~   ,bass2.DIM_TERM_TAC_NEW_LOAD c
##~   where 
##~   a.TAC_NUM = b.TAC_NUM 
##~   and a.DEV_ID = c.DEV_ID
##~   and c.MODE_2G= '3'
##~   group by MODE_2G ,MODE_3G ,c.TERM_TYPE ,b.TERM_TYPE
##~   order by 3


set sql_buff "
insert into   bass2.DIM_TERM_TAC_TRANS
select  
a.ROW_ID ID
,b.TAC_NUM
,'' TERM_ID
,a.TERM_MODEL
,'' TERMPROD_ID
,a.TERM_BRAND TERMPROD_NAME
,case when  MODE_3G = '3' then '2' else '1' end  NET_TYPE
,case 
	when a.MODE_2G in ('1') and a.MODE_3G in ('0','1')  and a.TERM_TYPE is null then '0'
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '01' then '1'
	when a.MODE_2G in ('1') and a.MODE_3G in ('0')  and a.TERM_TYPE = '02' then '2' --02- 数据卡2
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '02' then '2'
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '03' then '5'
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '04' then '3'
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '05' then '4'
	when a.MODE_2G in ('1') and a.MODE_3G in ('0')  and a.TERM_TYPE = '05' then '4' --05- 无线固话4
	when a.MODE_2G in ('1') and a.MODE_3G in ('3')  and a.TERM_TYPE = '06' then '6'
	when a.MODE_2G in ('3') and a.MODE_3G in ('1','2','3')  and a.TERM_TYPE = '01' then '1' -- 1-3G手机
	when a.MODE_2G in ('3') and a.MODE_3G in ('0')  		and a.TERM_TYPE = '01' then '0' -- 0-2G手机
	when a.MODE_2G in ('3') and a.MODE_3G in ('1','2','3')  and a.TERM_TYPE = '02' then '2' -- 02- 数据卡2
	when a.MODE_2G in ('3') and a.MODE_3G in ('1','2','3')  and a.TERM_TYPE = '03' then '5' -- 03- 上网本- 5-上网本
	when a.MODE_2G in ('3') and a.MODE_3G in ('1','2','3')  and a.TERM_TYPE = '07' then '8'  --8 新增，定义为 07- 平板电脑
	when 												a.TERM_TYPE = '07' then '8' --8 新增，定义为 07- 平板电脑
	when 												a.TERM_TYPE = '01' then '0'
	when 												upper(a.TERM_MODEL) like '%IPAD%' then '8'
	end  TERM_TYPE
from  bass2.DIM_DEVICE_INFO_EX a
 ,BASS2.DIM_TACNUM_DEVID b 
where a.DEV_ID = b.DEV_ID
with ur
"
exec_sql $sql_buff


set sql_buff "
rename bass2.DIM_TERM_TAC to DIM_TERM_TAC_BAK${timestamp}
"
exec_sql $sql_buff

set sql_buff "
CREATE TABLE bass2.DIM_TERM_TAC like  bass2.DIM_TERM_TAC_BAK${timestamp}  DISTRIBUTE BY HASH(ID)  IN TBS_DIM
"
exec_sql $sql_buff



set sql_buff "

insert into  bass2.DIM_TERM_TAC
(
         ID
        ,TAC_NUM
        ,TERM_ID
        ,TERM_MODEL
        ,TERMPROD_ID
        ,TERMPROD_NAME
        ,NET_TYPE
        ,TERM_TYPE
)
select 
         ID
        ,TAC_NUM
        ,TERM_ID
        ,TERM_MODEL
        ,TERMPROD_ID
        ,TERMPROD_NAME
        ,NET_TYPE
        ,TERM_TYPE
from  bass2.DIM_TERM_TAC_TRANS
with ur
"

exec_sql $sql_buff



##   加入历史数据
set sql_buff "
insert into  bass2.DIM_TERM_TAC
select * from  bass2.DIM_TERM_TAC_BAK${timestamp}
where trim(tac_num) in 
(select trim(tac_num) from bass2.DIM_TERM_TAC where net_type <>'2'
except
select trim(tac_num) from bass2.DIM_TERM_TAC_TRANS
)
with ur
"

exec_sql $sql_buff


set sql_buff "
select count(0) from (
select tac_nuM,count(*) from BASS2.DIM_TERM_TAC
group by tac_nuM
having count(*)>1
) a 
with ur
"

chkzero2 $sql_buff "BASS2.DIM_TERM_TAC 主键不唯一！"


  aidb_runstats BASS2.DIM_TERM_TAC 3


##~   delete from BASS2.DIM_TERM_TAC where TAC_NUM = '宏达'

return 0  

}




proc Deal_fix22036_20120627 { op_time optime_month } {
## 把R265校验中 ， 22036 中没有的 “行业应用代码全码” 加入，修复！
##

#当天 yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#当天 yyyy-mm-dd
set optime $op_time
set curr_month [string range $op_time 0 3][string range $op_time 5 6]
set last_day [GetLastDay [string range $timestamp 0 7]]

# fetch canceled

set sql_buff "
alter table bass1.G_A_22036_DAY_FIX20120627 activate not logged initially with empty table
"
exec_sql $sql_buff



         ##~   ${timestamp} TIME_ID
        ##~   ,'201206' BILL_MONTH
        ##~   ,'1' CUST_TYPE --客户类型
        ##~   ,'' EC_CODE --集团客户标识
        ##~   ,'' SINAME --SI名称 当“客户类型” =1时必须填写
        ##~   ,'1' OPERATE_TYPE
        ##~   ,a.SERV_CODE APP_LENCODE --行业应用代码全码
        ##~   ,'' APNCODE --当“业务类型”=3时填写
        ##~   ,'' BUSI_NAME --集团业务名称
        ##~   ,'${timestamp}' OPEN_DATE
        ##~   ,'1' STS

set sql_buff "
insert into G_A_22036_DAY_FIX20120627
select 
         ${timestamp} TIME_ID
        ,'201206' BILL_MONTH
        ,'1' CUST_TYPE 
        ,'' EC_CODE 
        ,'' SINAME
        ,'1' OPERATE_TYPE
        ,a.SERV_CODE APP_LENCODE
        ,'' APNCODE
        ,'' BUSI_NAME
        ,'${timestamp}' OPEN_DATE
        ,'1' STS
from bass1.G_S_04016_DAY_TMP_SERV_CODE a 
with ur
"
exec_sql $sql_buff


set sql_buff "
insert into G_A_22036_DAY
select a.*
from bass1.G_A_22036_DAY_FIX20120627 a 
where time_id = 20120629
with ur
"
exec_sql $sql_buff

return 0  

}


proc Deal_fix22036 { op_time optime_month } {
## 把R265校验中 ， 22036 中没有的 “行业应用代码全码” 加入，修复！
##

#当天 yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#当天 yyyy-mm-dd
set optime $op_time
set curr_month [string range $op_time 0 3][string range $op_time 5 6]
set last_day [GetLastDay [string range $timestamp 0 7]]

# fetch canceled

set sql_buff "
alter table bass1.G_A_22036_DAY_FIX20120731 activate not logged initially with empty table
"
exec_sql $sql_buff



         ##~   ${timestamp} TIME_ID
        ##~   ,'201206' BILL_MONTH
        ##~   ,'1' CUST_TYPE --客户类型
        ##~   ,'' EC_CODE --集团客户标识
        ##~   ,'' SINAME --SI名称 当“客户类型” =1时必须填写
        ##~   ,'1' OPERATE_TYPE
        ##~   ,a.SERV_CODE APP_LENCODE --行业应用代码全码
        ##~   ,'' APNCODE --当“业务类型”=3时填写
        ##~   ,'' BUSI_NAME --集团业务名称
        ##~   ,'${timestamp}' OPEN_DATE
        ##~   ,'1' STS

set sql_buff "
insert into G_A_22036_DAY_FIX20120731
select 
         ${timestamp} TIME_ID
        --,'201206' BILL_MONTH
        ,'1' CUST_TYPE 
        ,'' EC_CODE 
        ,'' SINAME
        ,'1' OPERATE_TYPE
        ,a.SERV_CODE APP_LENCODE
        ,'' APNCODE
        ,'' BUSI_NAME
        ,'${timestamp}' OPEN_DATE
        ,'1' STS
from bass1.G_S_04016_DAY_TMP_SERV_CODE a 
with ur
"
exec_sql $sql_buff


set sql_buff "
insert into G_A_22036_DAY
select a.*
from bass1.G_A_22036_DAY_FIX20120731 a 
where time_id = 20120731
with ur
"
exec_sql $sql_buff

return 0  

}

##~   select count(0) from bass2.DIM_TERM_TAC where NET_TYPE = '2'





proc Deal_fix02064 { op_time optime_month } {
## 剔除已经失效的数据 ，通过把02062中为“有效”的用户和用户表中失效的用户关联。
##

#当天 yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#当天 yyyy-mm-dd
set optime $op_time
set curr_month [string range $op_time 0 3][string range $op_time 5 6]
set last_day [GetLastDay [string range $timestamp 0 7]]

# fetch canceled
set sql_buff "
insert into BASS1.G_A_02064_DAY
(
         TIME_ID
        ,ENTERPRISE_ID
        ,NUMBER400
        ,BIND_CNT
        ,BIND_CMC_CNT
        ,BIND_OTH_CNT
        ,OPEN_DT
        ,IF_ORD_CALLSHIELD
        ,IF_ORD_CALLLIMIT
        ,IF_ORD_PSWDACCESS
        ,IF_ORD_BLACKLIST
        ,IF_ORD_SMS
        ,ORD_STS
)
select * from G_A_02064_DAY_FIX20120813 where time_id = 20120813
with ur
"
exec_sql $sql_buff

return 0  

}
