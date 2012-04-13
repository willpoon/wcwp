select * from channel.channel_info@dbl_xzjf
select * from jf.bs_enterprise_customer_code@dbl_jfdb
select * from all_tables@dbl_jfdb where table_name like '%CODE%'   
select * from   xzjf.ALL_SERVICE_CODE_DEF@dbl_jfdb

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
        where to_char(CREATE_DATE,'yyyymmdd')= '20111012' 
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
      select * from ngcp.pm_sp_operator_code@dbl_jfdb a
   where a.OPERATOR_CODE = '-HYJSZDXDY'

 select distinct operator_code,sp_code,bill_flag,sp_type,OPERATOR_NAME from ngcp.pm_sp_operator_code where operator_code is not null
 
 select operator_code,sp_code,bill_flag,sp_type,OPERATOR_NAME
 from (select operator_code,sp_code,bill_flag,sp_type,OPERATOR_NAME
 ,row_number()over(partition by operator_code,sp_code,bill_flag  order  by expire_date desc,valid_date desc ) rn 
 from ngcp.pm_sp_operator_code @dbl_jfdb a
 where operator_code is not null and a.OPERATOR_CODE = 'KSCF'
 and  to_char(a.expire_date,'yyyymm') > '201106'
 ) t where t.rn = 1
 
       select * from ngcp.pm_sp_operator_code@dbl_jfdb a
   where a.OPERATOR_CODE = 'KSCF'
   and to_char(a.expire_date,'yyyymm') > '201106'
   
 KSCF
 
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
                                                   
                                     

select  from   so.ord_busi_oth_f_0891_1107


                                     
select ORDER_BUSI_INFO_ID,BUSIOPER_ORDER_ID,replace(replace(replace(INFO_ID,chr(10)),chr(13)),'$'),INFO_NAME,REGION_ID,EXT_1,EXT_2,EXT_3,EXT_4,EXT_5,replace(replace(replace(EXT_6,chr(10)),chr(13)),'$'),DONE_CODE,OP_ID,ORG_ID,to_char(DONE_DATE,'YYYYMMDDHH24MISS'),to_char(VALID_DATE,'YYYYMMDDHH24MISS'),to_char(EXPIRE_DATE,'YYYYMMDDHH24MISS'),to_char(CREATE_DATE,'YYYYMMDDHH24MISS'),NOTES,ORDER_STATE from SO.ORD_BUSI_OTH_F_0891_1107 where to_char(CREATE_DATE) = '20110729'
                                     

select * from   kf.SYS_CMD_DEF
                                     
select * from                                        
RES.RES_CTMS_EXCHG@dbl_ggdb




select * from sec.sys_staff@dbl_ggdb  where staff_name like '%×ÔÖúÖÕ¶Ë%'

select * from  sec.sys_operator@dbl_ggdb  where operator_id='10100101'

select * from  sec.sys_operator@dbl_ggdb  where operator_id='10100115'select * from  sec.sys_operator@dbl_ggdb  where operator_id='11110204'
select * from  sec.sys_staff_org_relat@dbl_ggdb where staff_id='10100115'
select * from  sec.sys_staff_org_relat@dbl_ggdb where staff_id='10100101'
select * from  sec.sys_staff_org_relat@dbl_ggdb a , channel.channel_info@dbl_ggdb b where a.organize_id = b.channel_id
and a.staff_id 
in (select staff_id from sec.sys_staff@dbl_ggdb  where staff_name like '%×ÔÖúÖÕ¶Ë%')

select * from  sec.sys_organize@dbl_ggdb  where organize_id='10100101'



select * from  sec.sys_staff_org_relat@dbl_ggdb where organize_id='10100101'and  staff_id in (select staff_id from sec.sys_staff@dbl_ggdb  where staff_name like '%×ÔÖúÖÕ¶Ë%')

10100101	89100065
10100101	10100128
10100101	89100064
10100101	10100101


select count(0) from so.ord_so_log_0897_1104 where  notes like '%×ÔÖúÖÕ¶ËÏêµ¥²éÑ¯%';
select * from so.ord_so_log_0897_1104 where  notes like '%×ÔÖúÖÕ¶Ë%';

select distinct trade_id from    
RES.RES_SEL_NUM_CUST_LOG_0891

select * from   RES.RES_SEL_NUM_CUST_LOG_0891 where rownum<10

select * from   RES.RES_SEL_NUM_CUST_LOG where 
open_date is not null




select distinct opt_code,opt_name  from    
RES.RES_SEL_NUM_CUST_LOG_0891
where opt_code is not null

select  opt_code,opt_name ,count(0) from    
RES.RES_SEL_NUM_CUST_LOG_0891
where opt_code is not null
group by opt_code,opt_name


select * from   kf.KF_SMS_REALNAME_CHECK_2011


select * from ngcp.pm_sp_info@dbl_jfdb where 
sp_name like '%¼¯ÍÅ%'
and sp_code 
in
(
400002
,800089
,801001
,801002
,801005
,801006
,801008
,801011
,801013
,801018
,801019
,801027
,801028
,801031
,801034
,801035
,801036
,801038
,801040
,801042
,801044
,801048
,801049
,801054
,801056
,801057
,801060
,801061
,801062
,801065
,801067
,801069
,801071
,801072
,801080
,801081
,801084
,801085
,801086
,801087
,801088
,801089
,801097
,801100
,801104
,801110
,801112
,801114
,801115
,801117
,801118
,801127
,801135
,801137
,801138
,801140
,801142
,801150
,801153
,801154
,801159
,801160
,801161
,801163
,801166
,801169
,801170
,801171
,801174
,801179
,801181
,801183
,801186
,801191
,801192
,801203
,801205
,801207
,801210
,801215
,801217
,801219
,801220
,801222
,801223
,801224
,801225
,801228
,801229
,801232
,801234
,801237
,801242
,801244
,801248
,819003
,819008
,819009
,819011
,819012
,819013
,819014
,819019
,819021
,819211
,819212
,819241
,819242
,819243
,819244
,819246
,819247
,819248
,895250
)
select * from ngcp.pm_sp_operator_code@dbl_jfdb where service_code in (

7777
,6466
,4404
,4403
,4402
,4401
,18392396776
,18392393861
,18392393372
,18392393066
,18392391449
,18392387914
,18392381081
,18392381080
,18392379234
,18392379233
,18392378013
,18392377680
,18392370206
,18392361797
,18392357662
,18392354135
,18392350686
,18392185519
,18392185134
,18392182001
,18392180628
,18392179135
,18392161673
,18392158121
,18392158119
,18392155277
,18392153976
,18392136557
,18392134925
,18392131480
,18392131159
,18392121325
,18392116373
,18392116300
,18392116286
,18392116285
,18392115717
,18392108300
,18392108200
,18392108184
,18392106924
,18392106814
,18392106507
,18392106429
,18392106363
,18392106343
,18392102485
,18392102448
,18392102447
,18392102305
,18392102247
,18392067909
,18392065570
,18392064433
,18392057518
,18392057475
,18392057279
,18392052511
,18392052261
,18392052055
,18392051365
,18392037906
,18392035627
,18392035452
,18392035037
,18392033865
,18392029862
,18392029848
,18392029822
,18392019695
,18392019582
,18392013032
,18391815808
,18391815805
,18391815775
,18391815772
,18391815760
,18391815758
,18391815753
,18391812022
,18391812011
,18391812004
,18391811993
,18391811966
,18391811927
,18391811910
,18391811891
,18391811885
,18391811884
,18391811875
,18391811872
,18391811871
,18391811853
,18391811851
,18391811847
,18391811844
,18391811832
,18391811826
,18391811824
,18391811823
,18391811822
,18391811817
,18391811683
,18391811670
,18391811668
,18391811503
,18391811499
,18391811399
,18391811398
,18391811395
,18391811393
,18391811382
,18391811377
,18391811322
,18391811311
,18391811308
,18391811300
,18391811287
,18391811285
,18391811282
,18391811278
,18391811258
,18391811255
,18391811193
,18391811190
,18391811186
,18391811178
,18391811171
,18391811165
,18391811164
,18391811162
,18391811161
,18391811156
,18391481562
,18391456036
,18391442309
,18391407767
,18391201625
,18383414726
,18382181508
,18373936879
,18373921160
,18373907283
,18373907250
,18373905445
,18373905084
,18373805912
,18373805534
,18373805514
,18373805224
,18373700806
,18373461270
,18373193310
,18373183734
,18373177965
,18373172669
,18373160662
,18373148771
,18373143100
,18373131007
,18373121623
,18373120244
,18373045277
,18373013055
,18373007559
,18373007556
,18373007476
,18373007382
,18373007381
,18373007374
,18373007310
,18373007153
,18373007075
,18363696607
,18363695703
,18363692660
,18363662049
,18363661413
,18363632536
,18363632255
,18363630507
,18363625622
,18363619523
,18363612271
,18363604876
,18363603730
,18363588322
,18363582147
,18363536054
,18363227717
,18363225527
,18363152260
,18363138232
,18363131991
,18363123189
,18363096450
,18363091273
,18363090993
,18363074067
,18363069813
,18363068132
,18363058833
,18363058831
,18363058826
,18363057360
,18363057351
,18363036541
,18363018395
,18363017538
,18363016696
,18363014758
,18359775643
,18359774515
,18359774463
,18359773405
,18359773402
,18359773243
,18359605867
,18359595462
,18359534469
,18359527407
,18359524469
,18359524467
,18359524457
,18359524413
,18359523559
,18359302070
,18359189165
,18359185763
,18359103250
,18359103220
,18356208151
,18356207966
,18356205510
,18356200811
,18356166962
,18356097951
,18356096897
,18356018985
,18355356639
,18355318267
,18354802755
,18354723168
,18354471497
,18354433979
,18354424976
,18354424794
,18354278638
,18354272759
,18354270451
,18354256165
,18354237232
,18354228341
,18354223680
,18354223678
,18354223676
,18354223675
,18354223562
,18354216162
,18354215006
,18354207200
,18354205538
,18354204453
,18354201227
,18354200521
,18353387315
,18353373063
,18353368282
,18353363857
,18353360520
,18353355012
,18353329272
,18353329233
,18353328248
,18353328247
,18353328094
,18353327757
,18353326961
,18353326233
,18353325521
,18353324138
,18353324062
,18353316930
,18353314066
,18353312850
,18353155303
,18353126697
,18353121980
,18353110175
,18352182335
,18351950711
,18351950696
,18351816181
,18351341010
,18345717226
,18345519420
,18345481707
,18345371158
,18345175564
,18345170237
,18345148368
,18345148318
,18345145591
,18345079187
,18345063073
,18341917303
,18341904695
,18341903793
,18339782396
,18337901367
,18309795686
,18309795655
,18309795536
,18309795535
,18309795532
,18309795529
,18309795501
,18309795476
,18309795049
,18309793755
,18309793204
,18309792885
,18309792875
,18309792362
,18309792357
,18309792334
,18309792322
,18309792031
,18309791386
,18309791385
,18309791040
,18309791031
,18309790587
,18309766768
,18309766763
,18309766703
,18309766529
,18309766417
,18309766287
,18309766238
,18309766218
,18309766009
,18309765830
,18309765793
,18309765775
,18309765761
,18309765703
,18309765668
,18309765635
,18309765596
,18309765537
,18309765389
,18309765333
,18309765301
,18309764893
,18309764855
,18309764652
,18309764642
,183097646242
,18309764616
,18309764513
,18309764236
,18309764116
,18309764102
,18309764069
,18309760379
,18309760377
,18309760306
,18309760280
,18309735353
,18309730272
,18309704109
,18308872688
,18308796553
,18308342139
,18307477859
,18306541780
,18306488424
,18306428290
,18306428283
,18306426051
,18306422187
,18306421825
,18306414513
,18306411797
,18306293985
,183062922106
,18305992786
,18305992750
,18305988705
,18305217037
,18304638632
,18304628932
,18304623152
,18304621182
,12582
,12114
,10669988
,10669967
,10669900
,10669889
,10669881
,10669658
,10669588
,10669501
,10669500
,10669388
,10669191
,10669160
,10669133
,10669100
,10669082
,10669077
,10668888
,10668887
,10668882
,10668522
,10668277
,10668199
,10668178
,10668169
,10668158
,10668005
,10668002
,10668001
,10666877
,10666798
,10666666
,10666610
,10666388
,10666060
,10666000
,10665888
,10665757
,10665228
,10665110
,10665108
,10665106
,10664321
,10663904
,10663355
,10663333
,10663223
,10663111
,10662858
,10662599
,10662566
,10662186
,10662000
,10661828
,10661818
,10661778
,10661700
,10661518
,10661388
,10661250
,10661098
,10661090
,10661066
,10661009
,10660090
,10660078
,10660066
,10660058
,10658899
,10658888
,10658885
,10658880
,10658830
,10658800
,10658200
,10658188
,10658178
,10658169
,10658155
,10658150
,10658139
,10658123
,10658116
,10658109
,10658105
,10658103
,10658099
,10658098
,10658078
,10658068
,10658066
,10658039
,10658033
,10658032
,10658027
,10658025
,10658015
,10658011
,10658006
,106580059
,106580058
,106580055
,10658002
,10658000
,1065790100
,106540330001
,106540320000
,1065088
)

801210

select * from   
so.INS_PROD_move_0896@dbl_crmdb
where product_instance_id = '89657334006025' 

select * from all_tables@dbl_crmdb where table_name like '%INS_PROD_MOVE_0896%'

INS_PROD_MOVE

BK_INS_PROD_MOVE_0896_1012

select * from   
BIBAK.BK_INS_PROD_MOVE_0896_1107@dbl_crmdb
where product_instance_id = '89657334006025' 


select * from   
BIBAK.BK_INS_PROD_0896_1107@dbl_crmdb
where product_instance_id = '89657334006025' 


select * from   
BIBAK.BK_INS_PROD_MOVE_0896_1105@dbl_crmdb
where product_instance_id = '89657334006025' 

select * from 
so.ins_prod_0896@dbl_crmdb





106540320000


select distinct sp_name ,serv_code ,prov_code
--select *
  from ngcp.pm_sp_info@dbl_jfdb where serv_code  
in
(
'10658000'
,'4401'
,'106540330001'
,'10662599'
,'10658027'
,'10668887'
,'10658066'
,'10658830'
,'10666798'
,'106540320000'
,'10669889'
,'10669160'
,'4403'
,'10669967'
,'10658880'
,'10669658'
,'10661518'
,'4404'
,'10658006'
,'10668169'
,'10662000'
,'10663355'
,'10666666'
,'10660090'
,'10658899'
,'10664321'
,'10669500'
,'10658109'
,'10658033'
,'1065088'
,'10668001'
,'10661250'
,'10658098'
,'10668005'
,'10658800'
,'10663333'
,'10658039'
,'10660066'
,'10658116'
,'10658139'
,'10658002'
,'10668888'
,'10658068'
,'10669082'
,'10665106'
,'10658032'
,'10661700'
,'10658103'
,'10669100'
,'10662566'
,'10669588'
,'10661098'
,'10666877'
,'10661778'
,'10668178'
,'10666000'
,'10669881'
,'10661818'
,'12582'
,'10658150'
,'10669077'
,'10666060'
)




select * from    so.ins_prod_res_view
2140145

select count(0),count(distinct a.PRODUCT_INSTANCE_ID)
from so.ins_prod_view a,so.ins_prod_res_view b
where a.PRODUCT_INSTANCE_ID = b.INST_PRODUCT_ID
AND RES_EQU_NO2 IS NOT NULL


3481320

2103433	1845889


COUNT(0)	COUNT(DISTINCTA.PRODUCT_INSTANCE_ID)
3481361	1845902


select count(0),count(distinct a.RES_EQU_NO2)
from so.ins_prod_view a,so.ins_prod_res_view b
where a.PRODUCT_INSTANCE_ID = b.INST_PRODUCT_ID
AND RES_EQU_NO2 IS NOT NULL

2104381	1846698
select count(0) from    so.ins_prod_res_view

select * from all_tables where table_name like '%INS_OFF_INS_PROD_0892%'   

select count(0) from    so.ins_prod_res_0891

select * from   so.h_INS_OFF_INS_PROD_0891_1109
where offer_id = 112001005701

89260000361561

OFFER_INSTANCE_ID
100002831014

OFFER_INST_PROD_INST_ID
100002897724

select * from   so.h_INS_OFF_INS_PROD_0892_1107
where OFFER_INST_PROD_INST_ID = 100002897724


select * from   so.h_INS_OFF_INS_PROD_0891_1107
where product_instance_id = '89260000361561'


where offer_id = 112001005701


select tabname from syscat.tables where tabname like '%%'
TRANS	INS_OFF_INS_PROD_0892	
XZSO	INS_OFF_INS_PROD_0892	
SO	INS_OFF_INS_PROD_0892	
NFSO	INS_OFF_INS_PROD_0892	
SO	INS_OFF_INS_PROD_0892_20110411	

select * from   NFSO.INS_OFF_INS_PROD_0892


---------------------------------------------------ÓÃ»§¶©¹º¹ØÏµÄ£ÐÍ--------ÓÃ»§ÊµÀýselect * from so.ins_prod_0891 where product_instance_id='89157332411386'select * from so.ins_prod_0891 where cust_party_role_id='89100000003716'--²úÆ·±íselect * from product.up_product_item@dbl_ggdb where product_item_id='122091109253'112091101001 offer_plan122091109253 service_price--------²ß»®ÊµÀý±íselect * from so.ins_offer_0894 where offer_instance_id='9110000002'select * from so.ins_offer_0891 where offer_id='142000000910'select * from so.ins_offer_0891 where offer_id='112091101001'select * from so.ins_offer_0893 where offer_instance_id='9110000003'--²ß»®±íselect * from product.up_offer@dbl_ggdb where offer_id='111089110017'--¿ÉÓÃoffer_idÔÚproduct.up_product_item±íÖÐ²éµ½¶ÔÓ¦ÐÅÏ¢select distinct(offer_type) from product.up_offer@dbl_ggdb--------ÓÃ»§²ß»®¹ØÁª¹ØÏµ±íso.ins_off_ins_prod_0891 ÖÐµÄis_main_offer×Ö¶Ë±êÊ¶ÊÇ·ñÖ÷²ß»®£¬Í¨³£¼¯ÍÅ²úÆ·Ê¾ÀýÎª0select * from  so.ins_off_ins_prod_0891 where offer_instance_id=9110000002where  PRODUCT_INSTANCE_ID=89160000908640---------------------------------------------------·þÎñÄ£ÐÍselect * from product.up_product_item@dbl_ggdb where product_item_id='122100000132'--------²úÆ··þÎñ°üÊµÀý±íselect * from so.ins_srvpkg_0891 --¿ÉÓÃservicepkg_idÔÚproduct.up_product_item±íÖÐ²éµ½¶ÔÓ¦ÐÅÏ¢where SERVICEPKG_INSTANCE_ID=100026379947select count(distinct(servicepkg_id) ) from  so.ins_srvpkg_0891 select * from product.up_product_item@dbl_ggdb where product_item_id in (select servicepkg_id from  so.ins_srvpkg_0891 )--·þÎñ±íselect * from product.up_service@dbl_ggdb where --¿ÉÓÃservice_idÔÚproduct.up_product_item±íÖÐ²éµ½¶ÔÓ¦ÐÅÏ¢select * from product.up_product_item@dbl_ggdb where product_item_id in (select service_id from product.up_service@dbl_ggdb)--------·þÎñÊµÀý±íselect * from so.ins_srv_0891  where service_id=142000000910where service_instance_id in(100024725646,100024725902)--------·þÎñ°üÊµÀý-·þÎñÊ¾Àý¹ØÁª¹ØÏµ±íselect * from so.ins_srvpkg_ins_srv_0891 where  offer_instance_id=9110000002select t.*,t.rowid from so.ins_prod_0891 t where bill_id ='13889082098'



select * from ams.sc_scorelist_0891_2011@lnk_Zwdb where rownum < 10

ams.sc_payment_0891@lnk_Zwdb

select * from all_tables@lnk_Zwdb where table_name like '%SCORELIST%2011%'



select count(0) from ams.sc_scorelist_0891_2011@lnk_Zwdb where rownum < 10
6401482

select count(0) from ams.sc_scorelist_0891_2012@lnk_Zwdb 


select * from ams.sc_payment_0891@lnk_Zwdb where rownum < 10
AMS.SC_SCORELIST_0$FEE_AREA$_2011
select * from 
ams.sc_scorelist_0891_2011@lnk_Zwdb
where rownum < 10


select * from kf.DXYYT_YEWUL_ZONGBAOBIAO_2012@dbl_crmdb
where rownum < 10





select * from ams.sc_scorelist_0891_2011
where product_instance_id = '89160001196765'




		select
			product_instance_id  as user_id
			,sum( case when  count_cycle_id=201112 and scrtype=1 then orgscr+adjscr else 0 end )   as month_points
			,sum( case when  count_cycle_id=201112 and scrtype=24 then orgscr+adjscr else 0 end )   as month_qqt_points
			,sum( case when  count_cycle_id=201112 and scrtype=25 then orgscr+adjscr else 0 end )   as month_age_points
			,sum( case when  scrtype=5 then orgscr+adjscr else 0 end )   as trans_points
			,sum( case when  scrtype=5 then CURSCR else 0 end )   as convertible_points
			,sum( orgscr+adjscr )   as all_points
			,sum( case when scrtype=1 then orgscr+adjscr else 0 end )   as all_consume_points
			,sum( USRSCR )   as all_converted_points
		from bass2.dwd_product_sc_scorelist_201112
		where   actflag='1' and product_instance_id = '89160001196765'
		group by product_instance_id
		
		





		select
			product_instance_id  as user_id
			,sum( case when  count_cycle_id=201112 and scrtype=1 then orgscr+adjscr else 0 end )   as month_points
			,sum( case when  count_cycle_id=201112 and scrtype=24 then orgscr+adjscr else 0 end )   as month_qqt_points
			,sum( case when  count_cycle_id=201112 and scrtype=25 then orgscr+adjscr else 0 end )   as month_age_points
			,sum( case when  scrtype=5 then orgscr+adjscr else 0 end )   as trans_points
			,sum( CURSCR )   as convertible_points
			,sum( orgscr+adjscr )   as all_points
			,sum( case when scrtype=1 then orgscr+adjscr else 0 end )   as all_consume_points
			,sum( USRSCR )   as all_converted_points
		from ams.sc_scorelist_0891_2011
		where   actflag='1' and product_instance_id = '89357332152792'
		group by product_instance_id
		
        
        
        
        89357332152792
        
        select * from ams.sc_scorelist_0891_2011
where product_instance_id = '89357332152792'


select t.*,t.rowid from base.cfg_static_data@dbl_ggdb t where t.code_type ='735011';select t.*,t.rowid from base.cfg_static_data@dbl_ggdb t where t.code_type = '4_BOSS_VIP_LEVEL';



select * from ams.SC_PAYOUT_0891_1201
where product_instance_id = '89160001196765'


		select 
		a.USER_ID
		,a.SC_PAYMENT_ID
		,b.product_no
		,a.BILLING_CYCLE_ID
		,replace(char(date(STATE_DATE)),'-','') STATE_DATE
from bass2.dwd_product_sc_payout_201112 a
,bass2.dw_product_201112 b 
where a.user_id = b.user_id 


select * from product.up_product_item@dbl_ggdb where product_item_id in (191000000007);



select time_id , count(0) 
--,  count(distinct time_id ) 
from G_S_22056_MONTH 
group by  time_id 
order by 1 


select * from all_tables@dbl_jfdb where table_name like '%DR_WLAN%'

select * from 
 XZJF.DR_WLAN_L_20120326@dbl_jfdb
 where  rownum < 10
 
select auth_type,count(0) from 
 XZJF.DR_WLAN_L_20120326@dbl_jfdb
 group by auth_type
 
 
 
 A05019	XZJF	DR_GGPRS_L	YYYYMMDD		GPRSÇåµ¥£¨±¾µØÄÚÈÝ¼Æ·Ñ£©
A05020	XZJF	DR_GPRS_L	YYYYMMDD		GPRSÇåµ¥£¨±¾µØÓÃ»§³ö¹ú£©

 DR_GGPRS_L
 
 select count(0) from XZJF.DR_GGPRS_L_20120331@dbl_jfdb
 5740528
 
 select count(0) from XZJF.DR_GGPRS_L_20120107@dbl_jfdb
 5441064
 select count(0) from XZJF.DR_GGPRS_L_20120108@dbl_jfdb
 4367270
 select count(0) from XZJF.DR_GGPRS_L_20120109@dbl_jfdb
 
 6542184
 
 select * from  XZJF.DR_GGPRS_L_20120109@dbl_jfdb where row_id < 10;
 $                              
$ desc  bass2.cdr_wlan_20120331 |grep -i time
START_TIME                     SYSIBM    TIMESTAMP                10     0 Yes   
STOP_TIME                      SYSIBM    TIMESTAMP                10     0 Yes   
PROCESS_TIME                   SYSIBM    TIMESTAMP                10     0 Yes   
INPUT_TIME                     SYSIBM    TIMESTAMP                10     0 Yes   

 
 select COUNT(0) from  XZJF.DR_GGPRS_L_20120109@dbl_jfdb
 where to_char(START_TIME,'yyyymmdd') = 20120108
 1159325
 
  select COUNT(0) from  XZJF.DR_GGPRS_L_20120108@dbl_jfdb
 where to_char(START_TIME,'yyyymmdd') = 20120107
 88581
 
   select COUNT(0) from  XZJF.DR_GGPRS_L_20120107@dbl_jfdb
 where to_char(START_TIME,'yyyymmdd') = 20120106
 83924
 
 
 
 select table_name from all_tables where table_name like '%GGPRS%'
 
 select * from  XZJF.DR_GGPRS_L_20120109@dbl_jfdb
 where rownum < 10
 
 
START_TIME	Í¨»°ÆðÊ¼Ê±¼ä
DEAL_DATETIME	Ô­Ê¼»°µ¥´¦ÀíÈÕÆÚÊ±¼ä
BEGIN_TIME1	·Ö»°µ¥¿ªÊ¼Ê±¼ä
BEGIN_TIME2	·Ö»°µ¥¿ªÊ¼Ê±¼ä2
PROCESS_TIME	Åú¼Û´¦ÀíÊ±¼ä
BACKUP_DATE	±¸·ÝÈÕÆÚ
INPUT_TIME	Èë¿âÊ±¼ä


 select COUNT(0) from  XZJF.DR_GGPRS_L_20120109@dbl_jfdb
 where to_char(START_TIME,'yyyymmdd') = 20120108
 
  select START_TIME,DEAL_DATETIME,BEGIN_TIME1,BEGIN_TIME2,PROCESS_TIME,BACKUP_DATE,INPUT_TIME
  from  XZJF.DR_GGPRS_L_20120109@dbl_jfdb
 where rownum < 10
 
 select 
  to_char(START_TIME,'yyyymmdd') START_TIME
 --,to_char(DEAL_DATETIME,'yyyymmdd') DEAL_DATETIME
 --,to_char(BEGIN_TIME1,'yyyymmdd') BEGIN_TIME1
-- ,to_char(BEGIN_TIME2,'yyyymmdd') BEGIN_TIME2
 ,to_char(PROCESS_TIME,'yyyymmdd') PROCESS_TIME
 , BACKUP_DATE
 ,to_char(INPUT_TIME,'yyyymmdd') INPUT_TIME
 ,count(0) cnt
 from  XZJF.DR_GGPRS_L_20120109@dbl_jfdb
 group by 
   to_char(START_TIME,'yyyymmdd') 
 --,to_char(DEAL_DATETIME,'yyyymmdd') 
 --,to_char(BEGIN_TIME1,'yyyymmdd') 
 --,to_char(BEGIN_TIME2,'yyyymmdd') 
 ,to_char(PROCESS_TIME,'yyyymmdd') 
 ,BACKUP_DATE
 ,to_char(INPUT_TIME,'yyyymmdd') 
 
 
 
 
 
 select 
  to_char(START_TIME,'yyyymmdd') START_TIME
  ,to_char(START_TIME,'hh24') START_TIME
 --,to_char(DEAL_DATETIME,'yyyymmdd') DEAL_DATETIME
 --,to_char(BEGIN_TIME1,'yyyymmdd') BEGIN_TIME1
-- ,to_char(BEGIN_TIME2,'yyyymmdd') BEGIN_TIME2
 ,to_char(PROCESS_TIME,'yyyymmdd') PROCESS_TIME
 , BACKUP_DATE
 ,to_char(INPUT_TIME,'yyyymmdd') INPUT_TIME
 ,count(0) cnt
 --,sum(RATING_RES) RATING_RES
 ,sum(data_flow_up1+data_flow_up2) UP
 ,sum(data_flow_down1+data_flow_down2) DOWN
 from  XZJF.DR_GGPRS_L_20120109@dbl_jfdb
 group by 
   to_char(START_TIME,'yyyymmdd') 
   ,to_char(START_TIME,'hh24')
 --,to_char(DEAL_DATETIME,'yyyymmdd') 
 --,to_char(BEGIN_TIME1,'yyyymmdd') 
 --,to_char(BEGIN_TIME2,'yyyymmdd') 
 ,to_char(PROCESS_TIME,'yyyymmdd') 
 ,BACKUP_DATE
 ,to_char(INPUT_TIME,'yyyymmdd') 
 order by 1,2
 
 
 
 
 
 
                 select 
                        20120331 time_id
                        ,'20120331'  CHRG_DT
                        ,replace(substr(char(PEER_DATE),12,8),'.','')  CHRG_TM
                        ,key_num MSISDN
                        ,case 
                                when opt_code in ('4103','4144') then '01'
                                when opt_code in ('4115') then '03'
                        end CHRG_TYPE
                        ,char(bigint(AMOUNT)) CHRG_AMT
                from BASS2.dw_acct_payment_dm_201203 a
                where  replace(char(a.OP_TIME),'-','') = '20120331' 
                and OPT_CODE in ('4103','4144','4115')
                order by 1,2
                with ur


                


 
 select 
  to_char(START_TIME,'yyyymmdd') START_TIME
  ,to_char(START_TIME,'hh24') START_TIME
 --,to_char(DEAL_DATETIME,'yyyymmdd') DEAL_DATETIME
 --,to_char(BEGIN_TIME1,'yyyymmdd') BEGIN_TIME1
-- ,to_char(BEGIN_TIME2,'yyyymmdd') BEGIN_TIME2
 ,to_char(PROCESS_TIME,'yyyymmdd') PROCESS_TIME
 , BACKUP_DATE
 ,to_char(INPUT_TIME,'yyyymmdd') INPUT_TIME
 ,count(0) cnt
 --,sum(RATING_RES) RATING_RES
 ,sum(data_flow_up1+data_flow_up2) UP_flow
 ,sum(data_flow_down1+data_flow_down2) DOWN_flow
 from  XZJF.DR_GGPRS_L_20120108@dbl_jfdb
 group by 
   to_char(START_TIME,'yyyymmdd') 
   ,to_char(START_TIME,'hh24')
 --,to_char(DEAL_DATETIME,'yyyymmdd') 
 --,to_char(BEGIN_TIME1,'yyyymmdd') 
 --,to_char(BEGIN_TIME2,'yyyymmdd') 
 ,to_char(PROCESS_TIME,'yyyymmdd') 
 ,BACKUP_DATE
 ,to_char(INPUT_TIME,'yyyymmdd') 
 order by 1,2
 
                