select * from channel.channel_info@dbl_xzjf

select * from kf.TONGYI_TUIDING

select * from   all_tables@dbl_jfdb
where table_name like '%DR%GPRS%'

commit;

select * from 
xzjf.DR_GPRS_L_20110121@dbl_jfdb

select * from    kf.Kf_Sms_Cmd_Receive_201104
where cmd_id =405003
 and sts=4 
 

select * from product.up_field_type@dbl_ggdb where file_type_id = 22;


select  substr(serv_code,1,8) , count(0) from   kf.Kf_Sms_Cmd_Receive_201105
where serv_code like '10086%'
group by substr(serv_code,1,8) 
order by 2 desc 


select 1 from dual



select sts,count(0) from kf.Kf_Sms_Cmd_Receive_201104 a
group by sts

and sts=4 and deal_result=0

select count(0) from    kf.Kf_Sms_Cmd_Receive_201105 where cmd_id =405003 and sts=4 and deal_result=0
and to_char (create_date,'yyyymmdd') = '20110529'



select count(0) from kf.Kf_Sms_Cmd_Receive_201104 where cmd_id =405003 and sts=4 and deal_result=0
and to_char (create_date,'yyyymmdd') = '20110401'
; --²éÑ¯³É¹¦µÄ
select count(*) from kf.Kf_Sms_Cmd_Receive_201104 where cmd_id =405003 and sts=4 and deal_result not in(0,1); --²éÑ¯²»³É¹¦µÄ
 
 select * from kf.Kf_Sms_Cmd_Receive_201104
 
 
 select * from  KF.THREE_ITEM_STAT
 
 
 select count(0) from   KF.THREE_ITEM_STAT
 
 select TYCX_QUERY             ²éÑ¯Á¿,
             TYCX_TUIDING           ÍË¶©Á¿,
             TYCX_TUIDING_FAIL      ÍË¶©Ê§°ÜÁ¿,
             TYCX_TUIDING_AVG       ÈË¾ùµ¥´ÎÍË¶©ÒµÎñµÄÊýÁ¿,
             --TYCX_FIRST20_BUSI_NAME ÍË¶¨Á¿¾ÓÇ°20µÄÒµÎñÃû³Æ,
             TYCX_TOUSU_LIANG       Í¶ËßÁ¿,
             KOUFEI_TIXING          ¿Û·ÑÌáÐÑ·¢ËÍÁ¿,
             KOUFEI_DXHFL           ¶ÌÐÅ»Ø¸´Á¿,
             KOUFEI_REXWH           ÈÈÏßÍâºôÁ¿,
             KOUFEI_TUIDINGL        ÒµÎñ³É¹¦ÍË¶©Á¿,
             --KOUFEI_TDFIRST20_NAME  ÍË¶©Á¿¾ÓÇ°20µÄÒµÎñÃû³Æ,
             KOUFEI_TOUSU_LIAN      Í¶ËßÁ¿,
             MW_SHOULILIANG         ÊÜÀíÁ¿,
             MW_TUIFEI              ÍË·Ñ½ð¶î,
             MW_ZYFIRST20_NAME      ÊÕ·ÑÕùÒéÍ¶ËßÁ¿¾ÓÇ°20µÄÒµÎñÃû³Æ,
             CREATE_DATE            Í³¼ÆµÄÊý¾ÝÊ±¼ä
        from KF.THREE_ITEM_STAT
        where to_char(CREATE_DATE,'yyyymm')= '201105' 
        order by CREATE_DATE asc;


select sum(KOUFEI_TIXING)
from KF.THREE_ITEM_STAT

commit;


autocommit on


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
             --, b.tuiding_cnt
        from KF.THREE_ITEM_STAT a
        where to_char(CREATE_DATE,'yyyymmdd')>= '20110501' 

--crmdb
select * from kf.KF_SMS_DYNAMIC_PARA_201104;
commit;

select to_char(a.CREATE_DATE,'yyyymmdd'),count(0)
from  KF.THREE_ITEM_STAT a 
group by to_char(a.CREATE_DATE,'yyyymmdd')


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
 
 
   select * from ngcp.pm_sp_operator_code@dbl_jfdb 
   where rownum < 100
   
 
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

 
 
 select * from  KF.KF_SMS_CMD_RECEIVE_201104@dbl_crmdb where rownum < 1
 
 where 
 
 
 
 select count(0) , count(distinct substr(serv_code,9,4)) from KF.KF_SMS_CMD_RECEIVE_201104 where cmd_id = 405129
 
 
 select distinct prod_id, freebie_id, res_count from ngcp.pm_prod_freeres@dbl_jfdb where freebie_id in (select freebie_id         from ngcp.pm_freebie_prop@dbl_jfdb        where trim(upper(unit_des)) = 'KB') 
        
        
 select  prod_id, sum(res_count)/1024 count from ngcp.pm_prod_freeres@dbl_jfdb where freebie_id in (select freebie_id         from ngcp.pm_freebie_prop@dbl_jfdb        where trim(upper(unit_des)) = 'KB') 
group by prod_id

        
select serv_code from KF.KF_SMS_CMD_RECEIVE_201104 where cmd_id = 405129;  £­£­Õâ¸öÊÇËùÓÐ¿Û·ÑÌáÐÑ»Ø¸´¶ÌÌüÐÅµÄ·þÎñ´úÂë¡£Ò²ÊÇÓªÒµÊÇÄÜ¹ýserv_codeÀ´ÏÂ·¢ÐÅÏ¢¸øÓÃ»§µÄ£¬ÓÃ»§ÊÇ»Ø¸´µ½Õâ¸ö¶Ì¿ÚµÄ

select count(0) from   

select * from   ngcp.pm_sp_operator_code@dbl_jfdb
where rownum < 1


select  b.operator_code,operator_name 
from (select distinct ext4 from  so.his_dsmp_sms_send_message ) a
join ngcp.pm_sp_operator_code@dbl_jfdb  b on a.ext4 =b.operator_code 


 so.his_dsmp_sms_send_message




                         select 
                               *
                                from bass2.DW_HIS_DSMP_SMS_SEND_MESSAGE_201105  a 
                                where  RSP_SEQ LIKE '10086901%'
                                and char(date(CREATE_DATE)) like  '2011-05%' 
                                and ext1 = '600902'
                                
                                
                                and ext4 is not null 
                                and ext4 = '30820002' 
                         with ur 
                         

select * from      so.his_dsmp_sms_send_message a
where      ext1 = '600902' 
and    to_char(a.CREATE_DATE,'yyyymm') = '201105'           


select count(0) from      so.his_dsmp_sms_send_message a
where   to_char(a.CREATE_DATE,'yyyymm') = '201105'       
and message like '%²»»Ø¸´%'


select * from      so.his_dsmp_sms_send_message a
where   to_char(a.CREATE_DATE,'yyyymm') = '201105'       
and message like '%²»»Ø¸´%'
and rownum < 100


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


select count(0)  from so.his_dsmp_sms_send_message  a where    to_char(a.CREATE_DATE,'yyyymmdd') = '20110531' 
and  ext4 is not null 
RSP_SEQ LIKE '%10086901%' and


and ext4 = '600902002002975051'

600902	600902002003965174	12	0


select * from    so.his_dsmp_sms_send_message 
where  trim(confirm_code) = trim(return_message) 
trim(confirm_code) <>'ÊÇ'




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
        
        select *  from so.his_dsmp_sms_send_message where rownum < 10
        
        select * from   kf.Kf_Sms_Cmd_Receive_201104
        

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
 
 
 select * from  bass1.g_i_02022_day
 
 
 
 

select * from   ams.scrd_gift_update@lnk_zwdb

SC_MONTH_BACKUP	X_YYMM	ÓÃ»§	»ý·Ö_ÔÂÄ©¾µÏñ±íSC_PAYMENT	X_YYMM	ÓÃ»§	»ý·Ö_¼ÇÂ¼SC_SCORELIST	X_YYYY_YYMM	ÓÃ»§	»ý·ÖÃ÷Ï¸±íODS_PRODUCT_SC_MONTH_BACKUP_YYYYMMODS_PRODUCT_SC_PAYMENT_YYYYMMDDODS_PRODUCT_SC_SCORELIST_YYYYMM
SCRD_ORD_GIFT_INFO

select * from    ams.payment_0891_1103@lnk_zwdb
WHERE opt_code='4158' AND state=0 



select * from   ams.scrd_info@lnk_zwdb
select * from all_tables where table_name like '%CHANNEL_PAY%'
select * from   channel.CHANNEL_PAYMENT_DTL


select  item_id ,count(0) from   ams.SCRD_ORD_INFO@lnk_zwdb
group by item_id


SCRD_ORD_GIFT_INFO

select  item_id ,count(0) from   ams.SCRD_ORD_GIFT_INFO@lnk_zwdb
group by item_id

select * from    ams.SCRD_ORD_GIFT_INFO@lnk_zwdb


select * from channel.CHANNEL_LOCAL_BUSI @dbl_ggdb where 1=1 and entity_type in(72,73 ) and rec_status=1/*ext_field1  ³äÖµ¿¨ÐòÁÐºÅext_field7  ³äÖµ¿¨ÃæÖµ µ¥Î»·Öext_field9  ÇþµÀ±àºÅ*/



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
 

      
      


--ÖØÅÜµ±ÈÕËùÓÐÐ£Ñé
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
                      
                                                                
select count(0) from     market.cm_group_customer_ext@dbl_ggdb
select count(0) from     market.GROUP_RELATION@dbl_ggdb 
select count(0) from    market.cm_individual_info_ext@dbl_ggdb 


 SELECT * FROM   market.cm_group_customer_ext@dbl_ggdb
where source = 2

 SELECT * FROM  market.GROUP_RELATION@dbl_ggdb 

SELECT count(0),count(distinct group_cust_id) FROM   market.cm_group_customer_ext@dbl_ggdb 

 SELECT * FROM  market.cm_individual_info_ext@dbl_ggdb 
 
SELECT count(0) FROM   market.cm_group_customer_ext@dbl_ggdb 
 SELECT count(0) FROM  market.cm_individual_info_ext@dbl_ggdb  --³ÉÔ±±í
 SELECT count(0) FROM  market.GROUP_RELATION@dbl_ggdb  Óë¼¯ÍÅ¿Í»§ÊÓÍ¼¹ØÏµ±í
 
 
 SELECT * FROM  market.GROUP_RELATION 
¼¯ÍÅ¿Í»§Çåµ¥¹ØÏµ±í
SELECT * FROM   market.cm_group_customer_ext@dbl_ggdb WHERE group_cust_id='XZ0309698'  --Çåµ¥±í
 SELECT * FROM  market.cm_individual_info_ext@dbl_ggdb  --³ÉÔ±±í
 SELECT * FROM  market.GROUP_RELATION@dbl_ggdb  --Óë¼¯ÍÅ¿Í»§ÊÓÍ¼¹ØÏµ±í
 
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



 select * from  G_S_22083_MONTH
 
 
 
 
   select * from kf.net_intf_detail_log@lnk_crmdb where to_char(active_date,'yyyymm')='201105';
   
   
   
    
 SELECT * FROM base.cfg_operationWHERE opt_code
in 
(
'_BUSI1'
,'_BUSI4'
,'_BUSI5'
,'_PAUSE'
,'_YDBHK'
,'APNXZ'
,'AT_ALL'
,'BBSCY'
,'BFWG'
,'BLKBY_3'
,'BUSI_2'
,'BUSI_6'
,'BUSI_7'
,'BUSI_8'
,'CHANGE'
,'CM1109'
,'CMNETYX'
,'CMNEWGMM'
,'CMNEYGZF'
,'CMNEYXXH'
,'CWTJCY'
,'CWTSCY'
,'CWTXZ'
,'D_QIUT'
,'DGZZ'
,'DGZZ1'
,'EW_BAT'
,'EWSALE'
,'GACT1'
,'GBG1'
,'GBG2'
,'GBGE'
,'GBGF'
,'GBGJT'
,'GCPP'
,'GCX101'
,'GCX102'
,'GCX107'
,'GCX110'
,'GCX114'
,'GCX202'
,'GCX204'
,'GCX205'
,'GCX206'
,'GCX212'
,'GCX246'
,'GCX248'
,'GCX7'
,'GCX8'
,'GCZJ'
,'GJH1'
,'GKG1'
,'GKG2'
,'GKG5'
,'GKG6'
,'GQT2'
,'GQTE'
,'GRW1'
,'GRW3'
,'GRW42'
,'GTF3'
,'GVN4'
,'GVN5'
,'GXH1'
,'GZL1'
,'GZL2'
,'GZL7'
,'HLWZXXZ'
,'HLWZXZX'
,'IT_BAT'
,'JT6324'
,'JT6345'
,'JT6346'
,'JT6347'
,'JT6348'
,'JT6372'
,'JT6373'
,'JTHB'
,'JTL6384'
,'JTWZJ'
,'LAST7'
,'LJBUSI46'
,'MPAYCZ'
,'MPAYKT'
,'PASSWD'
,'PTCHANGE'
,'QSPALL'
,'QXTBZF'
,'QXTJCY'
,'QXTSCY'
,'QXTWSCY'
,'QXTXZ'
,'QXTZX'
,'QYJZBZF'
,'QYJZXZ'
,'QYJZZX'
,'RESENT'
,'RESTOR'
,'SPLIT'
,'TICH_F'
,'TICH_Z'
,'USER_PAS'
,'USER_RES'
,'USI134'
,'USI138'
,'USI160'
,'USI163'
,'USI164'
,'USI165'
,'USI167'
,'USI171'
,'USI175'
,'USI179'
,'USI186'
,'USI196'
,'USI394'
,'USI451'
,'USI452'
,'USI453'
,'USI457'
,'USI501'
,'USI_11'
,'USI_13'
,'USI_14'
,'USI_15'
,'USI_16'
,'WPOSJCY'
,'WPOSSCY'
,'XXTGCY'
,'XXTJCY'
,'XXTSCY')

 SELECT count(0) FROM base.cfg_operation

select * from  bass2.dw_channel_rule_mm 





select * from   G_S_22068_MONTH



select '2011-06-06',
		       count(distinct product_instance_id),
		       count(distinct case when a.valid_type=1 then product_instance_id  end),
		       count(distinct product_instance_id)-count(distinct case when a.valid_type=1 then product_instance_id  end)
		from bass2.dw_product_ins_off_ins_prod_ds a,
		     bass2.dim_qqt_offer_id b
		where a.offer_id=b.offer_id
		  and b.index=1
		  and date(a.create_date)='2011-06-06'
		  and date(a.expire_date)>'2011-06-06'





select '2011-06-06',
            count(distinct case when  offer_id in (111090001348,111090001331) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001349,111090001332) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001350,111090001333) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001334,111090001351) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001335,111090001352) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001336,111090001353) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001337,111090001354) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001338,111090001355) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001339,111090001356) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001340,111090001357) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001341,111090001358) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001342,111090001359) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001343,111090001360) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001344,111090001361) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001345,111090001362) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001363) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001364) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001365) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001366) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001367) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001368) then  product_instance_id end)
			 from (
			 select product_instance_id,offer_id,create_date,expire_date,valid_date,row_number() over(partition by product_instance_id ,offer_id order by expire_date desc,valid_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_ds
            ) a
			where date(create_date)='2011-06-06'
			  and date(expire_date)>'2011-06-06'
			  and row_id=1




select distinct product_instance_id
		from bass2.dw_product_ins_off_ins_prod_ds a,
		     bass2.dim_qqt_offer_id b
		where a.offer_id=b.offer_id
		  and b.index=1
		  and date(a.create_date)='2011-06-06'
		  and date(a.expire_date)>'2011-06-06'
except
select  distinct product_instance_id
			 from (
			 select product_instance_id,offer_id,create_date,expire_date,valid_date,row_number() over(partition by product_instance_id order by expire_date desc,valid_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_ds
            ) a,bass2.dim_qqt_offer_id b
			where date(create_date)='2011-06-06'
			  and date(expire_date)>'2011-06-06'
			  and row_id=1 and b.index=1
			 and a.offer_id=b.offer_id


select * from    bass2.dw_product_ins_off_ins_prod_ds where product_instance_id = '89160000969350'
select * from   bass2.DIM_PROD_UP_PRODUCT_ITEM where product_item_id 
in 



select tabname from syscat.tables where tabname like '%DIM%PRODUCT%ITEM%'   


select * from   bass2.dim_qqt_offer_id
where offer_id = 111090004095



111090002601	90002601	À´µçÌáÐÑ3Ôª°üÔÂ´ÙÏú	OFFER_PLAN	1	2009-11-10 13:56:37.000000	2006-12-19 0:00:00.000000	2099-12-31 23:59:59.000000	[NULL]	[NULL]	Y	1	[NULL]
111090004095	90004095	ÆÕÍ¨GGPRS°´M¼Æ·Ñ²úÆ·	OFFER_PLAN	1	2010-03-11 11:17:11.000000	2010-01-01 0:00:00.000000	2099-12-31 23:59:59.000000	[NULL]	[NULL]	Y	1	[NULL]








select             count(distinct case when  a.offer_id in (111090001348,111090001331) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001349,111090001332) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001350,111090001333) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001334,111090001351) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001335,111090001352) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001336,111090001353) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001337,111090001354) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001338,111090001355) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001339,111090001356) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001340,111090001357) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001341,111090001358) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001342,111090001359) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001343,111090001360) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001344,111090001361) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001345,111090001362) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001363) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001364) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001365) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001366) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001367) then  a.product_instance_id end),            count(distinct case when  a.offer_id in (111090001368) then  a.product_instance_id end)			 from (			 select product_instance_id,offer_id,create_date,expire_date,valid_date,row_number() over(partition by product_instance_id,offer_id order by expire_date desc,valid_date desc) row_id         from bass2.dw_product_ins_off_ins_prod_ds            ) a,bass2.dim_qqt_offer_id b			where a.offer_id=b.offer_id			  and b.index=1			  and date(a.create_date)='2011-06-06'			  and date(a.expire_date)>='2011-06-06'			  and a.row_id=1


              
              


select '2011-06-06',
            count(distinct case when  offer_id in (111090001348,111090001331) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001349,111090001332) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001350,111090001333) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001334,111090001351) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001335,111090001352) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001336,111090001353) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001337,111090001354) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001338,111090001355) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001339,111090001356) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001340,111090001357) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001341,111090001358) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001342,111090001359) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001343,111090001360) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001344,111090001361) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001345,111090001362) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001363) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001364) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001365) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001366) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001367) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001368) then  product_instance_id end)
			 from (
			 select product_instance_id,offer_id,create_date,expire_date,valid_date
			 ,row_number() over(partition by product_instance_id order by create_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_ds
            ) a
            ,bass2.dim_qqt_offer_id b
			where date(create_date)='2011-06-06'
			  and date(expire_date)>'2011-06-06'
			  and row_id=1
			  and b.index = 1
				and a.offer_id=b.offer_id
              
              


select '2011-06-06',
            count(distinct case when  a.offer_id in (111090001348,111090001331) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001349,111090001332) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001350,111090001333) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001334,111090001351) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001335,111090001352) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001336,111090001353) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001337,111090001354) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001338,111090001355) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001339,111090001356) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001340,111090001357) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001341,111090001358) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001342,111090001359) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001343,111090001360) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001344,111090001361) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001345,111090001362) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001363) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001364) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001365) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001366) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001367) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001368) then  a.product_instance_id end)
			 from (
			 select product_instance_id,offer_id,create_date,expire_date,valid_date
			 ,row_number() over(partition by product_instance_id order by create_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_ds
            ) a
            ,bass2.dim_qqt_offer_id b
			where date(create_date)='2011-06-06'
			  and date(expire_date)>'2011-06-06'
			  and row_id=1
			  and b.index = 1
				and a.offer_id=b.offer_id


 select * from               
(              
select *
			 from (
			 select product_instance_id,offer_id,create_date,expire_date,valid_date
			 ,row_number() over(partition by product_instance_id order by create_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_ds
         where date(create_date)='2011-06-06'
			  and date(expire_date)>'2011-06-06'
            ) a
            where row_id=1
            ) a
            ,bass2.dim_qqt_offer_id b
			  where  b.index = 1
				and a.offer_id=b.offer_id

              
              

select '2011-06-06'
		       ,count(distinct product_instance_id)
		       ,count(distinct a.offer_id)
		       ,count(distinct product_instance_id||char(a.offer_id))		       
		       ,count(distinct case when a.valid_type=1 then product_instance_id  end)
		       ,count(distinct product_instance_id)-count(distinct case when a.valid_type=1 then product_instance_id  end)
		from bass2.dw_product_ins_off_ins_prod_ds a,
		     bass2.dim_qqt_offer_id b
		where a.offer_id=b.offer_id
		  and b.index=1
		  and date(a.create_date)='2011-06-06'
		  and date(a.expire_date)>'2011-06-06'



2	4
389	393

select '2011-06-06',
            count(distinct case when  a.offer_id in (111090001348,111090001331) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001349,111090001332) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001350,111090001333) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001334,111090001351) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001335,111090001352) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001336,111090001353) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001337,111090001354) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001338,111090001355) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001339,111090001356) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001340,111090001357) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001341,111090001358) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001342,111090001359) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001343,111090001360) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001344,111090001361) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001345,111090001362) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001363) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001364) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001365) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001366) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001367) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001368) then  a.product_instance_id end)
			 from (
			 select product_instance_id,a.offer_id,create_date,expire_date,valid_date
			 ,row_number() over(partition by product_instance_id order by create_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_ds a,bass2.dim_qqt_offer_id b
		where a.offer_id=b.offer_id
		  and b.index=1
		  and date(a.create_date)='2011-06-06'
		  and date(a.expire_date)>'2011-06-06'
            ) a
			where row_id=1



select * from   G_S_22062_MONTH
where time_id = 201105


select count(0) from    G_S_22062_MONTH where time_id = 201105
              
              
select * from   bass2.dw_product_ord_offer_dm_201106



                               select
                                 a.org_id,
                                 case when a.channel_type='9' then '2' else '1' end accept_type
                                       ,0
                                       ,0
                                       ,0
                                       ,0
                                       ,0                        
                                 ,count(*)
                                       ,0
                                       ,0
                                       ,0
                                       ,0
                                       ,0
                                       ,0                       
                                  from bass2.dw_product_ord_cust_dm_201105 a,
                                       bass2.dw_product_ord_offer_dm_201105 b,
                                       bass2.dim_prod_up_product_item d,
                                       bass2.dim_pub_channel e,
                                       bass2.dim_sys_org_role_type f,
                                       bass2.dim_cfg_static_data g,
                                       bass2.Dim_channel_info h
                                 where a.customer_order_id = b.customer_order_id
                                   --and date(a.CREATE_DATE) = '2011-05-31'
                                   and date(b.CREATE_DATE) = '2011-05-31'
                                   and b.offer_id = d.product_item_id
                                   and a.org_id = e.channel_id
                                   and a.channel_type = g.code_value
                                   and g.code_type = '911000'
                                   and b.offer_type=2
                                   and e.channeltype_id = f.org_role_type_id
                                   and a.org_id = h.channel_id
                                   and h.channel_type_class in (90105,90102)
                                   and a.channel_type in ('b','9','5')
                                 group by  a.org_id,
                                         case when a.channel_type='9' then '2' else '1' end


select * from   BASS2.dw_acct_payment_dm_201106


select * from base.cfg_static_data where code_type='911000';  


select * from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb
where expr_id = 90000012);




select sum (num), t from (
select count(*) num, to_char(valid_date,'yyyymmdd')  t from so.i_user_order_0891 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (remark not like '%temp_expire_jsj_prod_llf%' or remark is null) and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0891 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) 
  )) group by to_char(valid_date,'yyyymmdd') union all
select count(*) num, to_char(valid_date,'yyyymmdd')  t from so.i_user_order_0892 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (remark not like '%temp_expire_jsj_prod_llf%' or remark is null) and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0892 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) 
  )) group by to_char(valid_date,'yyyymmdd') union all
select count(*) num, to_char(valid_date,'yyyymmdd')  t from so.i_user_order_0893 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (remark not like '%temp_expire_jsj_prod_llf%' or remark is null) and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0893 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) 
  )) group by to_char(valid_date,'yyyymmdd') union all
select count(*) num, to_char(valid_date,'yyyymmdd')  t from so.i_user_order_0894 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (remark not like '%temp_expire_jsj_prod_llf%' or remark is null) and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0894 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) 
  )) group by to_char(valid_date,'yyyymmdd') union all
select count(*) num, to_char(valid_date,'yyyymmdd')  t from so.i_user_order_0895 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (remark not like '%temp_expire_jsj_prod_llf%' or remark is null) and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0895 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) 
  )) group by to_char(valid_date,'yyyymmdd') union all
select count(*) num, to_char(valid_date,'yyyymmdd')  t from so.i_user_order_0896 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (remark not like '%temp_expire_jsj_prod_llf%' or remark is null) and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0896 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) 
  )) group by to_char(valid_date,'yyyymmdd') union all
select count(*) num, to_char(valid_date,'yyyymmdd')  t from so.i_user_order_0897 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (remark not like '%temp_expire_jsj_prod_llf%' or remark is null) and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0897 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) 
  )) group by to_char(valid_date,'yyyymmdd') ) group by t;


select * from so.i_user_order_0891 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0891 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0892 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0893 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0894 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0895 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0896 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0897 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501'
  ));
  


select count(*), to_char(create_time,'yyyymmdd') from (
  select * from so.i_ismp_alarm_his_0891 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0892 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0893 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0894 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0895 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0896 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0897 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501'
  )group by to_char(create_time,'yyyymmdd');


select count(*), to_char(create_time,'yyyymmdd') from (
  select * from so.i_ismp_alarm_his_0891 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) union all
  select * from so.i_ismp_alarm_his_0892 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) union all
  select * from so.i_ismp_alarm_his_0893 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) union all
  select * from so.i_ismp_alarm_his_0894 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) union all
  select * from so.i_ismp_alarm_his_0895 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) union all
  select * from so.i_ismp_alarm_his_0896 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) union all
  select * from so.i_ismp_alarm_his_0897 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) 
  ) group by to_char(create_time,'yyyymmdd');
                                         
                                         
                                         
select count(*), to_char(create_time,'yyyymmdd') from (
  select * from so.i_ismp_alarm_his_0891 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0892 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0893 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0894 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0895 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0896 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0897 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501'
  )group by to_char(create_time,'yyyymmdd');
                                         
                                         
                                         
                                         
select * from so.i_user_order_0891 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0891 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0892 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0893 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0894 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0895 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0896 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' union all
  select * from so.i_ismp_alarm_his_0897 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501'
  ));
  
 

  select * from so.i_ismp_alarm_his_0891 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) 
                                         
                                         
                                         
                                         
select sum (num), t from (
select count(*) num, to_char(valid_date,'yyyymmdd')  t from so.i_user_order_0891 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (remark not like '%temp_expire_jsj_prod_llf%' or remark is null) and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0891 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) 
  )) group by to_char(valid_date,'yyyymmdd') 
union all
select count(*) num, to_char(valid_date,'yyyymmdd')  t from so.i_user_order_0892 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (remark not like '%temp_expire_jsj_prod_llf%' or remark is null) and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0892 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) 
  )) group by to_char(valid_date,'yyyymmdd') 
union all
select count(*) num, to_char(valid_date,'yyyymmdd')  t from so.i_user_order_0893 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (remark not like '%temp_expire_jsj_prod_llf%' or remark is null) and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0893 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) 
  )) group by to_char(valid_date,'yyyymmdd') 
union all
select count(*) num, to_char(valid_date,'yyyymmdd')  t from so.i_user_order_0894 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (remark not like '%temp_expire_jsj_prod_llf%' or remark is null) and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0894 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) 
  )) group by to_char(valid_date,'yyyymmdd') 
union all
select count(*) num, to_char(valid_date,'yyyymmdd')  t from so.i_user_order_0895 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (remark not like '%temp_expire_jsj_prod_llf%' or remark is null) and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0895 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) 
  )) group by to_char(valid_date,'yyyymmdd') 
union all
select count(*) num, to_char(valid_date,'yyyymmdd')  t from so.i_user_order_0896 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (remark not like '%temp_expire_jsj_prod_llf%' or remark is null) and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0896 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) 
  )) group by to_char(valid_date,'yyyymmdd') 
union all
select count(*) num, to_char(valid_date,'yyyymmdd')  t from so.i_user_order_0897 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (remark not like '%temp_expire_jsj_prod_llf%' or remark is null) and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0897 where bill_flag =3 and to_char(create_time,'yyyymmdd') >= '20110501'
  and sp_code in (select sp_code from ngcp.pm_sp_info@dbl_jfdb where sp_type in (select serv_type from ngcp.pm_serv_type_vs_expr@dbl_jfdb where expr_id = 90000010)) 
  )) group by to_char(valid_date,'yyyymmdd') ) group by t;
 





select * from so.i_user_order_0891 where order_sts =2 and valid_date >= to_date ('2011-05-01 00:00:00','YYYY-MM-DD hh24:mi:ss') 
and (msisdn, sp_code, operator_code) in (select user_number, sp_code, operator_code from (
  select * from so.i_ismp_alarm_his_0891 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' 
union all
  select * from so.i_ismp_alarm_his_0892 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' 
union all
  select * from so.i_ismp_alarm_his_0893 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' 
union all
  select * from so.i_ismp_alarm_his_0894 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' 
union all
  select * from so.i_ismp_alarm_his_0895 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' 
union all
  select * from so.i_ismp_alarm_his_0896 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501' 
union all
  select * from so.i_ismp_alarm_his_0897 where bill_flag =2 and to_char(create_time,'yyyymmdd') >= '20110501'
  ));


select * from   so.i_user_order_0891 where operator_code = '-UMGDCZKJ'

select * from   kf.TONGYI_TUIDING where sp_code =  '12530'





select * from   so.i_ismp_alarm_his_0891 where operator_code = '600902002002084961' and rownum < 10

select * from   so.i_ismp_alarm_his_0891 where operator_code = '600902002002084961' and rownum < 10


select * from   product.UP_PRODUCT_ITEM@dbl_ggdb where rownum < 1 
select * from   NGCP.PM_PLANS@dbl_ggdb where rownum<1




  select * from xzjf.dr_kj_20110601@dbl_jfdb where  oper_code = '600902002002084961';
  
  

select * from product.up_product_item@dbl_ggdb where supplier_id = '931067' and extend_id2 in('+MAILMF','+MAILBZ','+MAILVIP');select * from so.ins_off_ins_prod_0891 where offer_id in(113090003319,113090003320,113090003321) and expire_date > sysdate;


select * from res.RES_SALE_ORDER@dbl_ggdbwhere CHANNEL_ID=96000072 and order_status=1  

select ceil(80/6) from dual
--14






select * from   bass2.dw_ent_group_relation_20110721


select * from   Ods.ent_group_relation_yyyymmdd

select * from   bass2.ods_ent_group_relation_20110721

select tabname from syscat.tables where lower(tabname) like '%ent_group_relation%'   

select * from    channel.channel_info @dbl_ggdb
                                                   
                                     