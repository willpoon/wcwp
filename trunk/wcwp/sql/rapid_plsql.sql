select * from channel.channel_info@dbl_xzjf

select * from kf.TONGYI_TUIDING

select * from   all_tables@dbl_jfdb
where table_name like '%DR%GPRS%'

commit;

select * from 
xzjf.DR_GPRS_L_20110121@dbl_jfdb


select * from   kf.Kf_Sms_Cmd_Receive_201104

select sts,count(0) from kf.Kf_Sms_Cmd_Receive_201104 a
group by sts

and sts=4 and deal_result=0


select count(0) from kf.Kf_Sms_Cmd_Receive_201104 where cmd_id =405003 and sts=4 and deal_result=0
and to_char (create_date,'yyyymmdd') = '20110401'
; --查询成功的
select count(*) from kf.Kf_Sms_Cmd_Receive_201104 where cmd_id =405003 and sts=4 and deal_result not in(0,1); --查询不成功的
 
 select * from kf.Kf_Sms_Cmd_Receive_201104
 
 
 select * from  KF.THREE_ITEM_STAT
 
 
 select count(0) from   KF.THREE_ITEM_STAT
 
 select TYCX_QUERY             查询量,
             TYCX_TUIDING           退订量,
             TYCX_TUIDING_FAIL      退订失败量,
             TYCX_TUIDING_AVG       人均单次退订业务的数量,
             --TYCX_FIRST20_BUSI_NAME 退定量居前20的业务名称,
             TYCX_TOUSU_LIANG       投诉量,
             KOUFEI_TIXING          扣费提醒发送量,
             KOUFEI_DXHFL           短信回复量,
             KOUFEI_REXWH           热线外呼量,
             KOUFEI_TUIDINGL        业务成功退订量,
             --KOUFEI_TDFIRST20_NAME  退订量居前20的业务名称,
             KOUFEI_TOUSU_LIAN      投诉量,
             MW_SHOULILIANG         受理量,
             MW_TUIFEI              退费金额,
             MW_ZYFIRST20_NAME      收费争议投诉量居前20的业务名称,
             CREATE_DATE            统计的数据时间
        from KF.THREE_ITEM_STAT
        --and to_char(CREATE_DATE,'yyyymmdd')<'20110332' 
        order by CREATE_DATE asc;


select sum(KOUFEI_TIXING)
from KF.THREE_ITEM_STAT

commit;


autocommit on




--crmdb
select * from kf.KF_SMS_DYNAMIC_PARA_201104;
commit;

select to_char(a.CREATE_DATE,'yyyymmdd'),count(0)
from  KF.THREE_ITEM_STAT a 
group by to_char(a.CREATE_DATE,'yyyymmdd')


--22080
 select      
             to_char(a.CREATE_DATE,'yyyymmdd') op_time,
             TYCX_QUERY             查询量,
             TYCX_TUIDING           退订量,
             TYCX_TUIDING_FAIL      退订失败量,
             --TYCX_TUIDING_AVG       人均单次退订业务的数量,
             --TYCX_FIRST20_BUSI_NAME 退定量居前20的业务名称,
             TYCX_TOUSU_LIANG       投诉量
             --KOUFEI_TIXING          扣费提醒发送量,
             --KOUFEI_DXHFL           短信回复量,
             --KOUFEI_REXWH           热线外呼量,
             --KOUFEI_TUIDINGL        业务成功退订量,
             --KOUFEI_TDFIRST20_NAME  退订量居前20的业务名称,
             --KOUFEI_TOUSU_LIAN      投诉量,
             --MW_SHOULILIANG         受理量,
             --MW_TUIFEI              退费金额,
             --MW_ZYFIRST20_NAME      收费争议投诉量居前20的业务名称,
             --CREATE_DATE            统计的数据时间
             , b.tuiding_cnt
        from KF.THREE_ITEM_STAT a ,
              (select  to_char(CREATE_DATE,'yyyymmdd') op_time,count(distinct sp_code) tuiding_cnt
                       from   
                        kf.TONGYI_TUIDING a
                        where sts = 1
                        group by to_char(a.CREATE_DATE,'yyyymmdd')
                    ) b 
        where to_char(CREATE_DATE,'yyyymmdd')>= '20110401' 
and    to_char(a.CREATE_DATE,'yyyymmdd') = b.op_time
/**
--and TYCX_TUIDING < tuiding_cnt

--当天退订业务总数 ??
select * from    kf.TONGYI_TUIDING
commit;


select  to_char(CREATE_DATE,'yyyymmdd') op_time,count(0)
 from   
 kf.TONGYI_TUIDING
 where sts = 1
 group by to_char(CREATE_DATE,'yyyymmdd')
 
 
 select * from  product.UP_SP_info@dbl_ggdb
 where sp_code = '698001'
 
 
 **/
 select count(0),count(distinct sp_code) from product.UP_SP_info@dbl_ggdb
 
 --22081
 select  to_char(a.CREATE_DATE,'yyyymm') op_time
 ,a.sp_code
 ,a.name
 ,' ' 业务提供商名称
 ,sum(case when a.sts = 1 then 1 else 0 end ) 成功退订量
 ,0 投诉量
 ,0 订购用户数
 from  kf.TONGYI_TUIDING a ,product.UP_SP_info@dbl_ggdb b 
 where a.sp_id = b.sp_code
 group by to_char(a.CREATE_DATE,'yyyymm')
  ,a.sp_code
 ,a.name
 
 
 
 select count(0),count(distinct sp_code) from   kf.TONGYI_TUIDING
 
 
 SELECT * FROM DIM_PROD_UP_PRODUCT_ITEMWHERE ITEM_TYPE='OFFER_PLAN'	  AND DEL_FLAG='1'	  AND SUPPLIER_ID IS NOT NULL	  WITH UR;	  SELECT * FROM DIM_NEWBUSI_SPINFOWITH UR;	  SELECT * FROM DW_PRODUCT_INS_OFF_INS_PROD_201103WHERE OFFER_ID=113110146851WITH UR;



 
 
 --22082
 
 select * from   
 product.UP_SP_SERVICE@dbl_ggdb
 
 --0 mianfei
 --2 dianbo
 --3 baoyue
 
 
  select  to_char(a.CREATE_DATE,'yyyymm') op_time
 ,业务计费类型
 ,0 扣费提醒发送量
 ,0 短信回复量
 ,- 业务成功退订量
 ,0 热线外呼量
 ,0 投诉量
 from  kf.TONGYI_TUIDING a ,product.UP_SP_info@dbl_ggdb b 
 where a.sp_id = b.sp_code
 group by to_char(a.CREATE_DATE,'yyyymm')
  ,a.sp_code
 ,a.name
 
 
   select * from ngcp.pm_sp_operator_code@dbl_jfdb 
   where rownum < 100
   
 
   select * from ngcp.pm_sp_operator_code@dbl_jfdb where sp_type
select * from ngcp.pm_serv_type_vs_expr@dbl_jfdb serv_type

 
--22083 不记录业务，无办法提取。
月份
业务代码
业务名称
业务提供商名称
业务计费类型
成功退订量
投诉量
订购用户数

 
 
 select * from  KF.KF_SMS_CMD_RECEIVE_201104@dbl_crmdb where rownum < 1
 
 where 
 
 
 
 select count(0) , count(distinct substr(serv_code,9,4)) from KF.KF_SMS_CMD_RECEIVE_201104 where cmd_id = 405129
 
 
 select distinct prod_id, freebie_id, res_count from ngcp.pm_prod_freeres@dbl_jfdb where freebie_id in (select freebie_id         from ngcp.pm_freebie_prop@dbl_jfdb        where trim(upper(unit_des)) = 'KB') 
        
        
 select  prod_id, sum(res_count)/1024 count from ngcp.pm_prod_freeres@dbl_jfdb where freebie_id in (select freebie_id         from ngcp.pm_freebie_prop@dbl_jfdb        where trim(upper(unit_des)) = 'KB') 
group by prod_id

        
select serv_code from KF.KF_SMS_CMD_RECEIVE_201104 where cmd_id = 405129;  －－这个是所有扣费提醒回复短厅信的服务代码。也是营业是能过serv_code来下发信息给用户的，用户是回复到这个短口的

select count(0) from   

select * from   ngcp.pm_sp_operator_code@dbl_jfdb
where rownum < 1


select  b.operator_code,operator_name 
from (select distinct ext4 from  so.his_dsmp_sms_send_message ) a
join ngcp.pm_sp_operator_code@dbl_jfdb  b on a.ext4 =b.operator_code 




select sm.*,rowid from so.his_dsmp_sms_send_message sm where  RSP_SEQ LIKE '%10086901%'

       
select * from KF.KF_SMS_CMD_RECEIVE_201104 where cmd_id = 405129
and rownum < 100;


 (select * from kf.Kf_Sms_Cmd_Receive_201104  where cmd_id = 405129 and rownum <10
select *  from so.his_dsmp_sms_send_message where  RSP_SEQ LIKE '%10086901%' and rownum <10
 
select count(0),count(distinct PHONE_ID||SERV_CODE)
from 
(select * from kf.Kf_Sms_Cmd_Receive_201104 a  where cmd_id = 405129 and to_char(a.CREATE_DATE,'yyyymm') = '201104' ) a ,
(select *  from so.his_dsmp_sms_send_message  a where  RSP_SEQ LIKE '%10086901%' and  to_char(a.CREATE_DATE,'yyyymm') = '201104'  ) b 
where a.PHONE_ID = b.BILL_ID and a.SERV_CODE = RSP_SEQ
 
 2708	2591


select *  from so.his_dsmp_sms_send_message  a where  RSP_SEQ LIKE '%10086901%' and  to_char(a.CREATE_DATE,'yyyymm') = '201103' 
and ext4 = '600902002002975051'



select count(0),count(distinct PHONE_ID||SERV_CODE)
from 
(select * from kf.Kf_Sms_Cmd_Receive_201104 a  where cmd_id = 405129 and to_char(a.CREATE_DATE,'yyyymm') = '201104' ) a ,
(select *  from so.his_dsmp_sms_send_message  a where  RSP_SEQ LIKE '%10086901%' and  to_char(a.CREATE_DATE,'yyyymm') = '201104'  ) b 
where a.PHONE_ID = b.BILL_ID and a.SERV_CODE = RSP_SEQ
and a.remarks is null 

         
         

select a.*,b.*
from 
(select * from kf.Kf_Sms_Cmd_Receive_201104 a  where cmd_id = 405129 and to_char(a.CREATE_DATE,'yyyymm') = '201104' ) a ,
(select *  from so.his_dsmp_sms_send_message  a where  RSP_SEQ LIKE '%10086901%' and  to_char(a.CREATE_DATE,'yyyymm') = '201104'  ) b 
where a.PHONE_ID = b.BILL_ID and a.SERV_CODE = RSP_SEQ
and a.phone_id = '13638954703'



select  PHONE_ID||SERV_CODE,count(0)
from 
(select * from kf.Kf_Sms_Cmd_Receive_201104 a  where cmd_id = 405129 and to_char(a.CREATE_DATE,'yyyymm') = '201104' ) a ,
(select *  from so.his_dsmp_sms_send_message  a where  RSP_SEQ LIKE '%10086901%' and  to_char(a.CREATE_DATE,'yyyymm') = '201104'  ) b 
where a.PHONE_ID = b.BILL_ID and a.SERV_CODE = RSP_SEQ
group by PHONE_ID||SERV_CODE
 having count(0) > 1
 
         
13658943978 100869013461	13
13628931714 100869019236	7
15889084736 100869010132	2



select  PHONE_ID||SERV_CODE,count(0)
from 
(select * from kf.Kf_Sms_Cmd_Receive_201104 a  where cmd_id = 405129 and to_char(a.CREATE_DATE,'yyyymm') = '201104' ) a ,
(select *  from so.his_dsmp_sms_send_message  a where  RSP_SEQ LIKE '%10086901%' and  to_char(a.CREATE_DATE,'yyyymm') = '201104'  ) b 
where a.PHONE_ID = b.BILL_ID and a.SERV_CODE = RSP_SEQ
and a.remarks is null 
group by PHONE_ID||SERV_CODE
 having count(0) > 1
          
13618900740 100869012672	3




select a.*,b.*
from 
(select * from kf.Kf_Sms_Cmd_Receive_201104 a  where cmd_id = 405129 and to_char(a.CREATE_DATE,'yyyymm') = '201104' ) a ,
(select *  from so.his_dsmp_sms_send_message  a where  RSP_SEQ LIKE '%10086901%' and  to_char(a.CREATE_DATE,'yyyymm') = '201104'  ) b 
where a.PHONE_ID = b.BILL_ID and a.SERV_CODE = RSP_SEQ
and a.phone_id = '15889084736' 

select * 
from 
(select * from kf.Kf_Sms_Cmd_Receive_201104 a  where cmd_id = 405129 and to_char(a.CREATE_DATE,'yyyymm') = '201104' ) a where a.phone_id = '15889084736' and SERV_CODE = '100869010132'

select *
from (select *  from so.his_dsmp_sms_send_message  a where  RSP_SEQ LIKE '%10086901%' and  to_char(a.CREATE_DATE,'yyyymm') = '201104'  ) a
        where a.BILL_ID = '15889084736' and RSP_SEQ = '100869010132'
        
        
        

select * 
from 
(select * from kf.Kf_Sms_Cmd_Receive_201104 a  where cmd_id = 405129 and to_char(a.CREATE_DATE,'yyyymm') = '201104' ) a where a.phone_id = '15889084736' and SERV_CODE = '100869010132'

select *
from (select *  from so.his_dsmp_sms_send_message  a where  RSP_SEQ LIKE '%10086901%' and  to_char(a.CREATE_DATE,'yyyymm') = '201104'  ) a
        where a.BILL_ID = '15889084736' and RSP_SEQ = '100869010132'
        
        
        

13618900740 100869012672	3


select * 
from 
(select * from kf.Kf_Sms_Cmd_Receive_201104 a  where cmd_id = 405129 and to_char(a.CREATE_DATE,'yyyymm') = '201104' ) a where a.phone_id = '13618900740' and SERV_CODE = '100869012672'

select *
from (select *  from so.his_dsmp_sms_send_message  a where  RSP_SEQ LIKE '%10086901%' and  to_char(a.CREATE_DATE,'yyyymm') = '201104'  ) a
        where a.BILL_ID = '13628931714' and RSP_SEQ = '100869019236'
        
--
select * from DW_NEWBUSI_ISMG_201103        
select * from   kf.TONGYI_TUIDING
        
 select      
             to_char(a.CREATE_DATE,'yyyymmdd') op_time
             ,TYCX_QUERY             qry_cnt
             ,TYCX_TUIDING           cancel_cnt
             ,TYCX_TUIDING_FAIL      cancel_fail_cnt
             ,TYCX_TOUSU_LIANG       complaint_cnt
             , b.tuiding_cnt cancel_busi_type_cnt
        from KF.THREE_ITEM_STAT a ,
              (select  to_char(CREATE_DATE,'yyyymmdd') op_time,count(distinct sp_code) tuiding_cnt
                       from   
                        kf.TONGYI_TUIDING a
                        where sts = 1
                        group by to_char(a.CREATE_DATE,'yyyymmdd')
                    ) b 
        where to_char(CREATE_DATE,'yyyymmdd')>= '20110401' 
and    to_char(a.CREATE_DATE,'yyyymmdd') = b.op_time


        
   select * from    product.UP_SP_info@dbl_ggdb
   


 select  to_char(a.CREATE_DATE,'yyyymm') op_time
 ,a.sp_code
 ,a.name
 ,b.sp_name 业务提供商名称
 ,sum(case when a.sts = 1 then 1 else 0 end ) 成功退订量
 ,0 投诉量
 ,0 订购用户数
 from  kf.TONGYI_TUIDING a ,product.UP_SP_info@dbl_ggdb b 
 where a.sp_id = b.sp_code
 group by to_char(a.CREATE_DATE,'yyyymm')
  ,a.sp_code
 ,a.name
 ,b.sp_name 



select count(0) from ngcp.pm_sp_operator_code@dbl_jfdb
select count(0) from ngcp.pm_serv_type_vs_expr@dbl_jfdb

select * from   ngcp.pm_serv_type_vs_expr@dbl_jfdb

select count(0) from ngcp.pm_sp_operator_code@dbl_jfdb a ,ngcp.pm_serv_type_vs_expr@dbl_jfdb b 
 where a.sp_type = b.serv_type
 
 
select * from ngcp.pm_serv_type_vs_expr@dbl_jfdb
select count(0)
from (
select distinct  a.operator_code,sp_type from   ngcp.pm_sp_operator_code@dbl_jfdb a
) a 

select * from product.UP_SP_info@dbl_ggdb

select sp_code,count(0) cnt  from   kf.TONGYI_TUIDING
group by sp_code


select 
from (
select distinct  a.operator_code,sp_type from   ngcp.pm_sp_operator_code@dbl_jfdb a
) a ,
(select sp_code,count(0) cnt  from   kf.TONGYI_TUIDING
group by sp_code
) b where a.operator_code = b.sp_code


select b.sp_code,expr_id,a.delay_time
from (
select distinct  a.operator_code,a.expr_id,b.delay_time from ngcp.pm_sp_operator_code@dbl_jfdb a ,ngcp.pm_serv_type_vs_expr@dbl_jfdb b 
 where a.sp_type = b.serv_type
) a ,
(select sp_code,count(0) cnt  from   kf.TONGYI_TUIDING
group by sp_code
) b where a.operator_code = b.sp_code


select  from    ngcp.pm_sp_operator_code@dbl_jfdb 


select count(0)
from (select distinct operator_code,operator_name from  ngcp.pm_sp_operator_code@dbl_jfdb 
) a



select count(0)
from (
select distinct  a.operator_code,b.expr_id,b.delay_time from ngcp.pm_sp_operator_code@dbl_jfdb a ,ngcp.pm_serv_type_vs_expr@dbl_jfdb b 
 where a.sp_type = b.serv_type
) a ,
(select sp_code,count(0) cnt  from   kf.TONGYI_TUIDING
group by sp_code
) b where a.operator_code = b.sp_code

select * from    ngcp.pm_sp_operator_code@dbl_jfdb

select a.*
from (
select distinct  a.operator_code,b.sp_code,b.expr_id,b.delay_time from ngcp.pm_sp_operator_code@dbl_jfdb a ,ngcp.pm_serv_type_vs_expr@dbl_jfdb b 
 where a.sp_type = b.serv_type
) a ,
(select sp_code,count(0) cnt  from   kf.TONGYI_TUIDING
group by sp_code
) b where a.operator_code = b.sp_code


select confirm_code , count(0)
from so.his_dsmp_sms_send_message
where confirm_code > '9999'
group by confirm_code

7601	13658901768	100865018330	2	0	DSMP	
尊敬的客户，您好！您将订购由深圳腾讯公司提供的超级QQ业务，10.0元/月（由中国移动代收费），请在24小时内回复“是”确认订购，回复其他内容和不回复则不订购�
Ｖ泄贫�	2010-06-26 7:11:01.000	2	2010-06-26 7:11:01.000	2010-06-26 7:13:46.000	2010-06-26 7:13:48.000	[NULL]	是	是	2010-06-26 7:13:46.000	workflow-0158816891	901012	[NULL]	[NULL]	-XXSQQ


select * from   so.his_dsmp_sms_send_message  


select ext1 sp_id
,ext4 sp_busi_code
,count(0) alert_sms_cnt
,count(distinct case when confirm_code is not null or return_message is not null then bill_id||RSP_SEQ||ext1||ext4 end ) reply_sms_cnt
,sum(case when trim(confirm_code) <>'是' and  trim(confirm_code) = trim(return_message)  then 1 else 0 end ) cancel_cnt
,0 hotline_out_cnt
,0 complaint_cnt
from so.his_dsmp_sms_send_message  a 
where  RSP_SEQ LIKE '%10086901%' and  to_char(a.CREATE_DATE,'yyyymm') = '201104'  
and ext4 is not null 
group by ext1
,ext4

select message  
from so.his_dsmp_sms_send_message  a 
where  RSP_SEQ LIKE '10086901%' and  to_char(a.CREATE_DATE,'yyyymm') = '201104'  
and ext4 is not null 
and  return_message is null


select count(0),suM(alert_sms_cnt),sum(reply_sms_cnt),sum(cancel_cnt),sum(null_cnt)
from (
select ext1 sp_id
,ext4 sp_busi_code
,count(0) alert_sms_cnt
,count(distinct case when trim(confirm_code) <>'是' and return_message is not null then bill_id||RSP_SEQ||ext1||ext4 end ) reply_sms_cnt
,sum(case when trim(confirm_code) <>'是' and  trim(confirm_code) = trim(return_message)  then 1 else 0 end ) cancel_cnt
,0 hotline_out_cnt
,0 complaint_cnt
,sum(case when return_message is null then 1 else 0 end ) null_cnt
from so.his_dsmp_sms_send_message  a 
where  RSP_SEQ LIKE '10086901%' and  to_char(a.CREATE_DATE,'yyyymm') = '201104'  
and ext4 is not null 
group by ext1
,ext4
) t 
7520	111022	2063


select ext1 sp_id
,ext4 sp_busi_code
,count(0) alert_sms_cnt
,count(distinct case when trim(confirm_code) <>'是' and return_message is not null then bill_id||RSP_SEQ||ext1||ext4 end ) reply_sms_cnt
,sum(case when trim(confirm_code) <>'是' and  trim(confirm_code) = trim(return_message)  then 1 else 0 end ) cancel_cnt
,0 hotline_out_cnt
,0 complaint_cnt
from so.his_dsmp_sms_send_message  a 
where  RSP_SEQ LIKE '%10086901%' and  to_char(a.CREATE_DATE,'yyyymm') = '201104'  
and ext4 is not null
and exists (select 1 from  kf.Kf_Sms_Cmd_Receive_201104 b  where b.cmd_id = 405129 and to_char(b.CREATE_DATE,'yyyymm') = '201104'  and a.bill_id = b.PHONE_ID and  a.RSP_SEQ = b.SERV_CODE )
group by ext1
,ext4




select count(0),suM(alert_sms_cnt),sum(reply_sms_cnt)
from (
select ext1 sp_id
,ext4 sp_busi_code
,count(0) alert_sms_cnt
,count(distinct case when trim(confirm_code) <>'是' and return_message is not null then bill_id||RSP_SEQ||ext1||ext4 end ) reply_sms_cnt
,sum(case when trim(confirm_code) <>'是' and  trim(confirm_code) = trim(return_message)  then 1 else 0 end ) cancel_cnt
,0 hotline_out_cnt
,0 complaint_cnt
from so.his_dsmp_sms_send_message  a 
where  RSP_SEQ LIKE '%10086901%' and  to_char(a.CREATE_DATE,'yyyymm') = '201104'  
and ext4 is not null
and exists (select 1 from  kf.Kf_Sms_Cmd_Receive_201104 b  where b.cmd_id = 405129 and to_char(b.CREATE_DATE,'yyyymm') = '201104'  and a.bill_id = b.PHONE_ID and  a.RSP_SEQ = b.SERV_CODE )
group by ext1
,ext4
) t 
721	2695	2063

select count(0) from  kf.Kf_Sms_Cmd_Receive_201104 b  where b.cmd_id = 405129 and to_char(b.CREATE_DATE,'yyyymm') = '201104' 

select  PHONE_ID||SERV_CODE,count(0)
from 
(select * from kf.Kf_Sms_Cmd_Receive_201104 a  where cmd_id = 405129 and to_char(a.CREATE_DATE,'yyyymm') = '201104' ) a ,
(select *  from so.his_dsmp_sms_send_message  a where  RSP_SEQ LIKE '%10086901%' and  to_char(a.CREATE_DATE,'yyyymm') = '201104'  ) b 
where a.PHONE_ID = b.BILL_ID and a.SERV_CODE = RSP_SEQ
group by PHONE_ID||SERV_CODE
 having count(0) > 1
 
 
  select      
             to_char(a.CREATE_DATE,'yyyymmdd') op_time
             ,TYCX_QUERY             qry_cnt
             ,TYCX_TUIDING           cancel_cnt
             ,TYCX_TUIDING_FAIL      cancel_fail_cnt
             ,TYCX_TOUSU_LIANG       complaint_cnt
             , b.tuiding_cnt cancel_busi_type_cnt
        from KF.THREE_ITEM_STAT a ,
              (select  to_char(CREATE_DATE,'yyyymmdd') op_time,count(distinct sp_code) tuiding_cnt
                       from   
                        kf.TONGYI_TUIDING a
                        where sts = 1
                        group by to_char(a.CREATE_DATE,'yyyymmdd')
                    ) b 
        where to_char(CREATE_DATE,'yyyymmdd')>= '20110401' 
and    to_char(a.CREATE_DATE,'yyyymmdd') = b.op_time
 
 
 select * from  bass1.g_i_02022_day
 
 
 
 

select * from   ams.scrd_gift_update@lnk_zwdb

SC_MONTH_BACKUP	X_YYMM	用户	积分_月末镜像表SC_PAYMENT	X_YYMM	用户	积分_记录SC_SCORELIST	X_YYYY_YYMM	用户	积分明细表ODS_PRODUCT_SC_MONTH_BACKUP_YYYYMMODS_PRODUCT_SC_PAYMENT_YYYYMMDDODS_PRODUCT_SC_SCORELIST_YYYYMM
SCRD_ORD_GIFT_INFO

select * from    ams.payment_0891_1103@lnk_zwdb
WHERE opt_code='4158' AND state=0 



select * from   ams.scrd_info@lnk_zwdb
select table_name from all_tables@lnk_zwdb where table_name like '%SCRD%'

select  item_id ,count(0) from   ams.SCRD_ORD_INFO@lnk_zwdb
group by item_id


SCRD_ORD_GIFT_INFO

select  item_id ,count(0) from   ams.SCRD_ORD_GIFT_INFO@lnk_zwdb
group by item_id

select * from    ams.SCRD_ORD_GIFT_INFO@lnk_zwdb


select * from channel.CHANNEL_LOCAL_BUSI @dbl_ggdb where 1=1 and entity_type in(72,73 ) and rec_status=1/*ext_field1  充值卡序列号ext_field7  充值卡面值 单位分ext_field9  渠道编号*/



select * from   
ams.scrd_ord_info@lnk_zwdb a
,ams.scrd_gift_update@lnk_zwdb b 
where a.item_id = b.item_id 


  select * from ams.scrd_ord_info





ams.scrd_info




select   a.ord_seq,a.mob_num,order_opr_time,order_sum_point,in_date,sts,ord_type,exp_ord_type,exp_reason,comments
,ord_sts,org_id,item_id,item_name,exp_num,exp_ord_opt_time,item_type,item_point,item_point_value,type1,type2
,b.ITEM_E_PRICE
,b.ITEM_G_POINT
,b.ITEM_M_POINT
,b.ITEM_B_PRICE
,b.ITEM_STATUS
,b.TYPE3
from   
ams.scrd_ord_info@lnk_zwdb a
,ams.scrd_gift_update@lnk_zwdb b 
where a.item_id = b.item_id 



select   
 a.ord_seq
,a.mob_num
,a.order_opr_time
,a.order_sum_point
,a.in_date
,a.sts
,a.ord_type
,a.exp_ord_type
,a.exp_reason
,a.comments
,a.ord_sts
,a.org_id
,a.item_id
,a.item_name
,a.exp_num
,a.exp_ord_opt_time
,a.item_type
,a.item_point
,a.item_point_value
,a.type1
,a.type2
,b.ITEM_E_PRICE
,b.ITEM_G_POINT
,b.ITEM_M_POINT
,b.ITEM_B_PRICE
,b.ITEM_STATUS
,b.TYPE3
from   
ams.scrd_ord_info@lnk_zwdb a
,ams.scrd_gift_update@lnk_zwdb b 
where a.item_id = b.item_id 



select   
substr( ord_seq,1,8)
,substr( ord_seq,1,4)||'-'||substr( ord_seq,5,2)||'-'||substr( ord_seq,7,2)
,a.ord_seq
,a.mob_num
--,a.ord_opr_time
,a.order_sum_point
--,a.in_date
--,a.sts
,a.ord_type
,a.exp_ord_type
,a.exp_reason
--,a.comments
,a.ord_sts
,a.org_id
,a.item_id
,a.item_name
,a.exp_num
,a.exp_ord_opt_time
,b.item_type
,b.item_point
,b.item_point_value
,b.type1
--,b.type2
,b.ITEM_E_PRICE
,b.ITEM_G_POINT
,b.ITEM_M_POINT
,b.ITEM_B_PRICE
,b.ITEM_STATUS
--,b.TYPE3
from   
ams.scrd_ord_info@lnk_zwdb a
,ams.scrd_gift_update@lnk_zwdb b 
where a.item_id = b.item_id 


select    to_date(substr( ord_seq,1,8),'yyyy-dd') ,a.ord_seq ,a.mob_num ,a.order_sum_point ,a.ord_type ,a.exp_ord_type ,a.exp_reason ,a.ord_sts ,a.org_id ,a.item_id ,a.item_name ,a.exp_num ,a.exp_ord_opt_time ,b.item_type ,b.item_point ,b.item_point_value ,b.type1 ,b.ITEM_E_PRICE ,b.ITEM_G_POINT ,b.ITEM_M_POINT ,b.ITEM_B_PRICE ,b.ITEM_STATUS from    ams.scrd_ord_info a ,ams.scrd_gift_update b  where a.item_id = b.item_id 


select (nvl(order_sum_point,0)) from ams.scrd_ord_info@lnk_zwdb 



ams.scrd_ord_info


          
select count(0) from    ams.scrd_ord_gift_info@lnk_zwdb 
select count(0) from   ams.scrd_ord_gift_info@lnk_zwdb 
          

  select count(0) from ams.scrd_ord_info@lnk_zwdb a,
ams.scrd_ord_gift_info@lnk_zwdb  b,
  ams.scrd_gift_update@lnk_zwdb c where a.ord_seq = b.ord_seq and b.item_id = c.item_id
and a.ord_seq like '201104%'
and order_sum_point>0


          
          
  select type1,count(0) from ams.scrd_ord_info@lnk_zwdb a,
ams.scrd_ord_gift_info@lnk_zwdb  b,
  ams.scrd_gift_update@lnk_zwdb c where a.ord_seq = b.ord_seq and b.item_id = c.item_id
and a.ord_seq like '201104%'
and order_sum_point>0
group by type1

          

select    substr( a.ord_seq,1,8)OP_TIME ,a.ord_seq ,a.SUB_ORD_SEQ,a.mob_num ,a.order_sum_point ,a.ord_type ,a.exp_ord_type ,a.exp_reason ,a.ord_sts ,a.org_id ,a.item_id ,a.item_name ,c.item_type ,c.item_point ,c.item_point_value ,c.type1 ,c.ITEM_E_PRICE ,c.ITEM_G_POINT ,c.ITEM_M_POINT ,c.ITEM_B_PRICE ,c.ITEM_STATUS 
from ams.scrd_ord_info@lnk_zwdb a,
ams.scrd_ord_gift_info@lnk_zwdb  b,
  ams.scrd_gift_update@lnk_zwdb c where a.ord_seq = b.ord_seq and c.item_id = b.item_id
and c.file_name like '%CRMPRODUCT_%'

--and a.ord_seq like '201104%'


       
       
       

select count(0) from ams.scrd_ord_info a,
ams.scrd_ord_gift_info  b,
  ams.scrd_gift_update c where a.ord_seq = b.ord_seq and b.item_id = c.item_id and c.file_name like '%CRMPRODUCT_%'; 
       
       
      
          

  select count(0) from ams.scrd_ord_info@lnk_zwdb a,
ams.scrd_ord_gift_info@lnk_zwdb  b,
  ams.scrd_gift_update@lnk_zwdb c where a.ord_seq = b.ord_seq and b.item_id = c.item_id
--and a.ord_seq like '201104%'
and c.file_name like '%CRMPRODUCT_%'

select * from 
  ams.scrd_gift_update@lnk_zwdb
  
  

      
 select * from       
 channel.CHANNEL_LOCAL_BUSI@dbl_ggdb
 

      
      


--重跑当日所有校验
select * from 
app.sch_control_runlog a
where control_code like 'BASS1_INT%'
AND date(a.begintime) =  date(current date)
AND FLAG = 0

update  app.sch_control_runlog a
set flag = -2 
where control_code like 'BASS1_INT_CHECK%'
AND date(a.begintime) =  date(current date)
AND FLAG = 0
                      
                                                                


 SELECT * FROM   market.cm_group_customer_ext@dbl_ggdb
where source = 2



SELECT count(0) FROM   market.cm_group_customer_ext@dbl_ggdb 
 SELECT count(0) FROM  market.cm_individual_info_ext@dbl_ggdb  --成员表
 SELECT count(0) FROM  market.GROUP_RELATION@dbl_ggdb  与集团客户视图关系表
 
 
 SELECT * FROM  market.GROUP_RELATION 
集团客户清单关系表
SELECT * FROM   market.cm_group_customer_ext@dbl_ggdb WHERE group_cust_id='XZ0309698'  清单表
 SELECT * FROM  market.cm_individual_info_ext@dbl_ggdb  --成员表
 SELECT * FROM  market.GROUP_RELATION@dbl_ggdb  与集团客户视图关系表
 
 select count(0),count(distinct group_cust_id ) from   market.cm_group_customer_ext@dbl_ggdb


 SELECT a.group_cust_id,c.group_cust_id FROM market.cm_group_customer@dbl_ggdb a,market.GROUP_RELATION@dbl_ggdb b,market.cm_group_customer_ext@dbl_ggdb c
 WHERE a.group_cust_id=b.zw_group_id
 AND b.qd_group_id=c.group_cust_id
 AND a.group_cust_id='89203001148435'
 
 
 
  SELECT  count(0),count(distinct a.group_cust_id ) , count(distinct c.group_cust_id )
  FROM market.cm_group_customer@dbl_ggdb a,market.GROUP_RELATION@dbl_ggdb b,market.cm_group_customer_ext@dbl_ggdb c
 WHERE a.group_cust_id=b.zw_group_id
 AND b.qd_group_id=c.group_cust_id
 
 

select count(0),count(distinct group_cust_id ) from   market.cm_group_customer_ext@dbl_ggdb

20024	20024


SELECT count(0),count(distinct  zw_group_id),count(distinct qd_group_id ) FROM  market.GROUP_RELATION@dbl_ggdb       
386	346	383



 