          
R265	行业网关短信话单中的“服务代码”都在集团客户端口资源使用情况接口的“行业应用代码全码”中	
	sel coalesce(count(distinct svc_cd),0) as val1 ,0 val2
	from aipview.TB_SVC_CALL_ISMG_DAILY_$Tx_Date
	where cmcc_prov_prvd_id=$Branch_ID
		and sms_cdr_stat_cd = 0
		and svc_cd not in
			(
			sel indus_app_cd
			from aipview.TB_SUM_INDUS_PORT
			where cmcc_prov_prvd_id=$Branch_ID
			and busn_typ = '1'
			and eff_dt<=ADD_MONTHS(CAST($Tx_Date||'01' AS DATE FORMAT 'YYYYMMDD'),1)-1
			and end_dt>ADD_MONTHS(CAST($Tx_Date||'01' AS DATE FORMAT 'YYYYMMDD'),1)-1
			and subscrb_eff_dt<=ADD_MONTHS(CAST($Tx_Date||'01' AS DATE FORMAT 'YYYYMMDD'),1)-1 
			);





drop table BASS1.G_S_04016_DAY_TMP_SERV_CODE
CREATE TABLE BASS1.G_S_04016_DAY_TMP_SERV_CODE  (
				  time_id integer,
                  SERV_CODE CHAR(21) NOT NULL 
                  )   
                 DISTRIBUTE BY HASH(SERV_CODE)   
                   IN TBS_APP_BASS1 INDEX IN TBS_INDEX NOT LOGGED INITIALLY

ALTER TABLE BASS1.G_S_04016_DAY_TMP_SERV_CODE ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE

insert into bass1.G_S_04016_DAY_TMP_SERV_CODE
select 20120627 time_id,t.* from 
 table(
select distinct  SERV_CODE  SERV_CODE  
from G_S_04016_DAY where time_id / 100 = 201206 and SEND_STATUS = '0'
except
select distinct APP_LENCODE from 
(
			select t.*
			,row_number()over(partition by BILL_MONTH,EC_CODE,APP_LENCODE,APNCODE,BUSI_NAME order by time_id desc ) rn 
			from 
			G_A_22036_DAY  t
													where  time_id / 100 <= 201206
) a
where rn = 1    and OPERATE_TYPE = '1'
													And bigint(OPEN_DATE)/100 <= 201206
) t
with ur



select *from bass1.G_S_04016_DAY_TMP_SERV_CODE


--~   CREATE TABLE BASS1.G_A_22036_DAY_FIX20120627  (
                  --~   TIME_ID INTEGER , 
                  --~   BILL_MONTH CHAR(6) , 
                  --~   CUST_TYPE CHAR(1) , 
                  --~   EC_CODE CHAR(20) , 
                  --~   SINAME CHAR(60) , 
                  --~   OPERATE_TYPE CHAR(1) , 
                  --~   APP_LENCODE CHAR(21) , 
                  --~   APNCODE CHAR(63) , 
                  --~   BUSI_NAME CHAR(60) , 
                  --~   OPEN_DATE CHAR(8) , 
                  --~   STS CHAR(1) )   
                 --~   DISTRIBUTE BY HASH(TIME_ID,  
                 --~   APP_LENCODE)   
                   --~   IN TBS_APP_BASS1 INDEX IN TBS_INDEX NOT LOGGED INITIALLY

insert into G_A_22036_DAY_FIX20120627
select 
         ${timestamp} TIME_ID
        ,'201206' BILL_MONTH
        ,'1' CUST_TYPE --客户类型
        ,'' EC_CODE --集团客户标识
        ,'' SINAME --SI名称 当“客户类型” =1时必须填写
        ,'1' OPERATE_TYPE
        ,a.SERV_CODE APP_LENCODE --行业应用代码全码
        ,'' APNCODE --当“业务类型”=3时填写
        ,'' BUSI_NAME --集团业务名称
        ,'${timestamp}' OPEN_DATE
        ,'1' STS
from bass1.G_S_04016_DAY_TMP_SERV_CODE a 
where time_id = 20120627





insert into G_A_22036_DAY_FIX20120627
select 
         20120627 TIME_ID
        ,'201206' BILL_MONTH
        ,'1' CUST_TYPE --客户类型
        ,'' EC_CODE --集团客户标识
        ,'' SINAME --SI名称 当“客户类型” =1时必须填写
        ,'1' OPERATE_TYPE
        ,a.SERV_CODE APP_LENCODE --行业应用代码全码
        ,'' APNCODE --当“业务类型”=3时填写
        ,'' BUSI_NAME --集团业务名称
        ,'20120627' OPEN_DATE
        ,'1' STS
from bass1.G_S_04016_DAY_TMP_SERV_CODE a 



select distinct  SERV_CODE from G_S_04016_DAY where time_id / 100 = 201206 and SEND_STATUS = '0'
except
                        select distinct APP_LENCODE from 
                        (
                                        select t.*
                                        ,row_number()over(partition by BILL_MONTH,EC_CODE,APP_LENCODE,APNCODE,BUSI_NAME order by time_id desc ) rn 
                                        from 
                                        (select * from G_A_22036_DAY union all select * from G_A_22036_DAY_FIX20120627 )  t
                                                                                where  time_id / 100 <= 201206
                          ) a
                        where rn = 1    and OPERATE_TYPE = '1'
                                                                                And bigint(OPEN_DATE) <= 201206
                        

