#2011-10-20

proc Deal_fiximsi { op_time optime_month } {


#���� yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#���� yyyy-mm-dd
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


  #1.���chkpkunique
set tabname "g_a_02004_day"
set pk                  "USER_ID"
chkpkunique ${tabname} ${pk} ${timestamp}


return 0  

}


#2011-10-20


proc Deal_fixqixintong { op_time optime_month } {
## �޳��Ѿ�ʧЧ������
## ����δ���ӵ�����
##

#���� yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#���� yyyy-mm-dd
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
## �޳��Ѿ�ʧЧ������
## ����δ���ӵ�����
##

#���� yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
#���� yyyy-mm-dd
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

#���� yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
puts $timestamp
puts $op_month
#���� yyyy-mm-dd
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

#���� yyyymmdd
set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
puts $timestamp
puts $op_month
#���� yyyy-mm-dd
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