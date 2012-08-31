
R291	集团客户端口资源使用情况接口的“集团客户标识”均在集团客户接口的“集团客户标识”中
	sel coalesce(count(distinct org_cust_id),0)val1 ,0 val2
	from aipview.tb_sum_indus_port
	where   cust_typ='0'
		and eff_dt<=ADD_MONTHS(CAST($Tx_Date||'01' AS DATE FORMAT 'YYYYMMDD'),1)-1
		and end_dt>ADD_MONTHS(CAST($Tx_Date||'01' AS DATE FORMAT 'YYYYMMDD'),1)-1
		and subscrb_eff_dt<=ADD_MONTHS(CAST($Tx_Date||'01' AS DATE FORMAT 'YYYYMMDD'),1)-1
		and cmcc_prov_prvd_id=$Branch_ID
		and org_cust_id not in
		(	sel org_cust_id
			from dwCnstrntView.tb_prt_org_cust_hist
			where cmcc_prov_prvd_id=$Branch_ID
				and eff_dt<=ADD_MONTHS(CAST($Tx_Date||'01' AS DATE FORMAT 'YYYYMMDD'),1)-1
				and end_dt>ADD_MONTHS(CAST($Tx_Date||'01' AS DATE FORMAT 'YYYYMMDD'),1)-1
			group by 1
		);




select count(0) from  table (
select distinct APP_LENCODE from 
(
	select t.*
	,row_number()over(partition by BILL_MONTH,EC_CODE,APP_LENCODE
,APNCODE,BUSI_NAME 
	order by time_id desc ) rn 
	from 
	G_A_22036_DAY  t
	where 
	TIME_ID/100 <= 201206                                                                        
) a
where rn = 1  
and bigint(OPEN_DATE)/100 <= 201206
and  CUST_TYPE = '0'
) t where 
APP_LENCODE not in (                  
select enterprise_id
from  table (
select enterprise_id from 
(
select t.*
,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
from 
G_A_01004_DAY  t
			where time_id/100 <= 201206
) a
where rn = 1    and CUST_STATU_TYP_ID = '20' 
)   t                     
)
with ur



-- 正确取法：



select * from  table (
select distinct EC_CODE from 
(
	select t.*
	,row_number()over(partition by EC_CODE,CUST_TYPE
	order by time_id desc ) rn 
	from 
	G_A_22036_DAY  t
	where 
	TIME_ID/100 <= 201205    
    and  CUST_TYPE = '0'                                                                    
) a
where rn = 1  
and bigint(OPEN_DATE)/100 <= 201205
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
			where time_id/100 <= 201205
) a
where rn = 1  
)   t                     
)
with ur





select * from  table (
select distinct EC_CODE from 
(
	select t.*
	,row_number()over(partition by EC_CODE,CUST_TYPE
	order by time_id desc ) rn 
	from 
	G_A_22036_DAY  t
	where 
	TIME_ID/100 <= 201206    
    and  CUST_TYPE = '0'                                                                    
) a
where rn = 1  
and bigint(OPEN_DATE)/100 <= 201206
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
			where time_id/100 <= 201206
) a
where rn = 1  
)   t                     
)
with ur


select 
         TIME_ID
        ,BILL_MONTH
        ,CUST_TYPE
        ,EC_CODE
        ,SINAME
        ,OPERATE_TYPE
        ,APP_LENCODE
        ,APNCODE
        ,BUSI_NAME
        ,OPEN_DATE
        ,STS
from 
(
	select t.*
					,row_number()over(partition by EC_CODE,CUST_T
YPE
					order by time_id desc ) rn 
	from 
	G_A_22036_DAY  t
	where 
	TIME_ID/100 <= 201206                                                                        
) a
where rn = 1  
and bigint(OPEN_DATE)/100 <= 201206
and  CUST_TYPE = '0'
) t
where EC_CODE in (

)
with ur





select * 
 from bass2.dwd_enterprise_msg_20120625
 where ENTERPRISE_ID in 
 (
 '89100000000649'
,'89100000001255'
,'89100000064566'
,'89100000065371'
,'89100000065988'
,'89100000066032'
,'89100000066126'
,'89103000168396'
,'89103000222474'
,'89103000390784'
,'89200000001216'
,'89200000007982'
,'89200000008093'
,'89200000026398'
,'89200000035658'
,'89200000035929'
,'89200000063648'
,'89300000003159'
,'89300000035717'
,'89302999078845'
,'89303001003055'
,'89303001226380'
,'89303001463074'
,'89303001464536'
,'89303001515473'
,'89500000000938'
,'89500000005466'
,'89500000006243'
,'89500000006244'
,'89500000006245'
,'89500000006302'
,'89500000006303'
,'89500000007998'
,'89500000008000'
,'89500000033144'
,'89500000041378'
,'89500000042532'
,'89500000044474'
,'89500000063552'
,'89503001019170'
,'89603001229503'
)
 
 
 
 



select 
         TIME_ID
        ,BILL_MONTH
        ,CUST_TYPE
        ,EC_CODE
        ,SINAME
        ,OPERATE_TYPE
        ,APP_LENCODE
        ,APNCODE
        ,BUSI_NAME
        ,OPEN_DATE
        ,STS
from 
(
	select t.*
					,row_number()over(partition by EC_CODE,CUST_TYPE
					order by time_id desc ) rn 
	from 
	G_A_22036_DAY  t
	where 
	TIME_ID/100 <= 201206                                                                        
) a
where rn = 1  
and bigint(OPEN_DATE)/100 <= 201206
and  CUST_TYPE = '0'
and EC_CODE in 
(
 '89100000000649'
,'89100000001255'
,'89100000064566'
,'89100000065371'
,'89100000065988'
,'89100000066032'
,'89100000066126'
,'89103000168396'
,'89103000222474'
,'89103000390784'
,'89200000001216'
,'89200000007982'
,'89200000008093'
,'89200000026398'
,'89200000035658'
,'89200000035929'
,'89200000063648'
,'89300000003159'
,'89300000035717'
,'89302999078845'
,'89303001003055'
,'89303001226380'
,'89303001463074'
,'89303001464536'
,'89303001515473'
,'89500000000938'
,'89500000005466'
,'89500000006243'
,'89500000006244'
,'89500000006245'
,'89500000006302'
,'89500000006303'
,'89500000007998'
,'89500000008000'
,'89500000033144'
,'89500000041378'
,'89500000042532'
,'89500000044474'
,'89500000063552'
,'89503001019170'
,'89603001229503'
)
with ur



