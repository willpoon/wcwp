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
; --²éÑ¯³É¹¦µÄ
select count(*) from kf.Kf_Sms_Cmd_Receive_201104 where cmd_id =405003 and sts=4 and deal_result not in(0,1); --²éÑ¯²»³É¹¦µÄ
 
 select * from kf.Kf_Sms_Cmd_Receive_201104
 
 
 select * from  KF.THREE_ITEM_STAT
 
 
 select TYCX_QUERY             ²éÑ¯Á¿,
             TYCX_TUIDING           ÍË¶©Á¿,
             TYCX_TUIDING_FAIL      ÍË¶©Ê§°ÜÁ¿,
             TYCX_TUIDING_AVG       ÈË¾ùµ¥´ÎÍË¶©ÒµÎñµÄÊýÁ¿,
             TYCX_FIRST20_BUSI_NAME ÍË¶¨Á¿¾ÓÇ°20µÄÒµÎñÃû³Æ,
             TYCX_TOUSU_LIANG       Í¶ËßÁ¿,
             KOUFEI_TIXING          ¿Û·ÑÌáÐÑ·¢ËÍÁ¿,
             KOUFEI_DXHFL           ¶ÌÐÅ»Ø¸´Á¿,
             KOUFEI_REXWH           ÈÈÏßÍâºôÁ¿,
             KOUFEI_TUIDINGL        ÒµÎñ³É¹¦ÍË¶©Á¿,
             KOUFEI_TDFIRST20_NAME  ÍË¶©Á¿¾ÓÇ°20µÄÒµÎñÃû³Æ,
             KOUFEI_TOUSU_LIAN      Í¶ËßÁ¿,
             MW_SHOULILIANG         ÊÜÀíÁ¿,
             MW_TUIFEI              ÍË·Ñ½ð¶î,
             MW_ZYFIRST20_NAME      ÊÕ·ÑÕùÒéÍ¶ËßÁ¿¾ÓÇ°20µÄÒµÎñÃû³Æ,
             CREATE_DATE            Í³¼ÆµÄÊý¾ÝÊ±¼ä
        from KF.THREE_ITEM_STAT where to_char(CREATE_DATE,'yyyymmdd')='20110401' 
        --and to_char(CREATE_DATE,'yyyymmdd')<'20110332' 
        order by CREATE_DATE asc;


select sum(KOUFEI_TIXING)
from KF.THREE_ITEM_STAT

commit;


autocommit on




--crmdb
select * from kf.KF_SMS_DYNAMIC_PARA_201104;
commit;



--22080
 select      
             to_char(a.CREATE_DATE,'yyyymmdd') op_time,
             TYCX_QUERY             ²éÑ¯Á¿,
             TYCX_TUIDING           ÍË¶©Á¿,
             TYCX_TUIDING_FAIL      ÍË¶©Ê§°ÜÁ¿,
             --TYCX_TUIDING_AVG       ÈË¾ùµ¥´ÎÍË¶©ÒµÎñµÄÊýÁ¿,
             --TYCX_FIRST20_BUSI_NAME ÍË¶¨Á¿¾ÓÇ°20µÄÒµÎñÃû³Æ,
             TYCX_TOUSU_LIANG       Í¶ËßÁ¿
             --KOUFEI_TIXING          ¿Û·ÑÌáÐÑ·¢ËÍÁ¿,
             --KOUFEI_DXHFL           ¶ÌÐÅ»Ø¸´Á¿,
             --KOUFEI_REXWH           ÈÈÏßÍâºôÁ¿,
             --KOUFEI_TUIDINGL        ÒµÎñ³É¹¦ÍË¶©Á¿,
             --KOUFEI_TDFIRST20_NAME  ÍË¶©Á¿¾ÓÇ°20µÄÒµÎñÃû³Æ,
             --KOUFEI_TOUSU_LIAN      Í¶ËßÁ¿,
             --MW_SHOULILIANG         ÊÜÀíÁ¿,
             --MW_TUIFEI              ÍË·Ñ½ð¶î,
             --MW_ZYFIRST20_NAME      ÊÕ·ÑÕùÒéÍ¶ËßÁ¿¾ÓÇ°20µÄÒµÎñÃû³Æ,
             --CREATE_DATE            Í³¼ÆµÄÊý¾ÝÊ±¼ä
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

--µ±ÌìÍË¶©ÒµÎñ×ÜÊý ??
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
 ,' ' ÒµÎñÌá¹©ÉÌÃû³Æ
 ,sum(case when a.sts = 1 then 1 else 0 end ) ³É¹¦ÍË¶©Á¿
 ,0 Í¶ËßÁ¿
 ,0 ¶©¹ºÓÃ»§Êý
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
 ,ÒµÎñ¼Æ·ÑÀàÐÍ
 ,0 ¿Û·ÑÌáÐÑ·¢ËÍÁ¿
 ,0 ¶ÌÐÅ»Ø¸´Á¿
 ,- ÒµÎñ³É¹¦ÍË¶©Á¿
 ,0 ÈÈÏßÍâºôÁ¿
 ,0 Í¶ËßÁ¿
 from  kf.TONGYI_TUIDING a ,product.UP_SP_info@dbl_ggdb b 
 where a.sp_id = b.sp_code
 group by to_char(a.CREATE_DATE,'yyyymm')
  ,a.sp_code
 ,a.name
 
 
   select * from ngcp.pm_sp_operator_code@dbl_jfdb where sp_type
select * from ngcp.pm_serv_type_vs_expr@dbl_jfdb serv_type

 
--22083 ²»¼ÇÂ¼ÒµÎñ£¬ÎÞ°ì·¨ÌáÈ¡¡£
ÔÂ·Ý
ÒµÎñ´úÂë
ÒµÎñÃû³Æ
ÒµÎñÌá¹©ÉÌÃû³Æ
ÒµÎñ¼Æ·ÑÀàÐÍ
³É¹¦ÍË¶©Á¿
Í¶ËßÁ¿
¶©¹ºÓÃ»§Êý

 
 
 select * from  KF.KF_SMS_CMD_RECEIVE_201104@dbl_crmdb
 
 
 
 
 select count(0) , count(distinct substr(serv_code,9,4)) from KF.KF_SMS_CMD_RECEIVE_201104 where cmd_id = 405129
 
 
 select distinct prod_id, freebie_id, res_count from ngcp.pm_prod_freeres@dbl_jfdb where freebie_id in (select freebie_id         from ngcp.pm_freebie_prop@dbl_jfdb        where trim(upper(unit_des)) = 'KB') 
        
        
 select  prod_id, sum(res_count)/1024 count from ngcp.pm_prod_freeres@dbl_jfdb where freebie_id in (select freebie_id         from ngcp.pm_freebie_prop@dbl_jfdb        where trim(upper(unit_des)) = 'KB') 
group by prod_id

        
select serv_code from KF.KF_SMS_CMD_RECEIVE_201104 where cmd_id = 405129;  £­£­Õâ¸öÊÇËùÓÐ¿Û·ÑÌáÐÑ»Ø¸´¶ÌÌüÐÅµÄ·þÎñ´úÂë¡£Ò²ÊÇÓªÒµÊÇÄÜ¹ýserv_codeÀ´ÏÂ·¢ÐÅÏ¢¸øÓÃ»§µÄ£¬ÓÃ»§ÊÇ»Ø¸´µ½Õâ¸ö¶Ì¿ÚµÄ



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
 ,b.sp_name ÒµÎñÌá¹©ÉÌÃû³Æ
 ,sum(case when a.sts = 1 then 1 else 0 end ) ³É¹¦ÍË¶©Á¿
 ,0 Í¶ËßÁ¿
 ,0 ¶©¹ºÓÃ»§Êý
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


select count(0)
from (
select distinct  a.operator_code,b.expr_id,b.delay_time from ngcp.pm_sp_operator_code@dbl_jfdb a ,ngcp.pm_serv_type_vs_expr@dbl_jfdb b 
 where a.sp_type = b.serv_type
) a ,
(select sp_code,count(0) cnt  from   kf.TONGYI_TUIDING
group by sp_code
) b where a.operator_code = b.sp_code

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
×ð¾´µÄ¿Í»§£¬ÄúºÃ£¡Äú½«¶©¹ºÓÉÉîÛÚÌÚÑ¶¹«Ë¾Ìá¹©µÄ³¬¼¶QQÒµÎñ£¬10.0Ôª/ÔÂ£¨ÓÉÖÐ¹úÒÆ¶¯´úÊÕ·Ñ£©£¬ÇëÔÚ24Ð¡Ê±ÄÚ»Ø¸´¡°ÊÇ¡±È·ÈÏ¶©¹º£¬»Ø¸´ÆäËûÄÚÈÝºÍ²»»Ø¸´Ôò²»¶©¹º¡
£ÖÐ¹úÒÆ¶¯	2010-06-26 7:11:01.000	2	2010-06-26 7:11:01.000	2010-06-26 7:13:46.000	2010-06-26 7:13:48.000	[NULL]	ÊÇ	ÊÇ	2010-06-26 7:13:46.000	workflow-0158816891	901012	[NULL]	[NULL]	-XXSQQ


select * from   so.his_dsmp_sms_send_message  


select ext1 sp_id
,ext4 sp_busi_code
,count(0) alert_sms_cnt
,count(distinct case when confirm_code is not null or return_message is not null then bill_id||RSP_SEQ||ext1||ext4 end ) reply_sms_cnt
,sum(case when trim(confirm_code) <>'ÊÇ' and  trim(confirm_code) = trim(return_message)  then 1 else 0 end ) cancel_cnt
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
,count(distinct case when trim(confirm_code) <>'ÊÇ' and return_message is not null then bill_id||RSP_SEQ||ext1||ext4 end ) reply_sms_cnt
,sum(case when trim(confirm_code) <>'ÊÇ' and  trim(confirm_code) = trim(return_message)  then 1 else 0 end ) cancel_cnt
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
,count(distinct case when trim(confirm_code) <>'ÊÇ' and return_message is not null then bill_id||RSP_SEQ||ext1||ext4 end ) reply_sms_cnt
,sum(case when trim(confirm_code) <>'ÊÇ' and  trim(confirm_code) = trim(return_message)  then 1 else 0 end ) cancel_cnt
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
,count(distinct case when trim(confirm_code) <>'ÊÇ' and return_message is not null then bill_id||RSP_SEQ||ext1||ext4 end ) reply_sms_cnt
,sum(case when trim(confirm_code) <>'ÊÇ' and  trim(confirm_code) = trim(return_message)  then 1 else 0 end ) cancel_cnt
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
 