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

