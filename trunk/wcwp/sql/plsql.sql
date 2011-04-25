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
 
 
 select TYCX_QUERY             查询量,
             TYCX_TUIDING           退订量,
             TYCX_TUIDING_FAIL      退订失败量,
             TYCX_TUIDING_AVG       人均单次退订业务的数量,
             TYCX_FIRST20_BUSI_NAME 退定量居前20的业务名称,
             TYCX_TOUSU_LIANG       投诉量,
             KOUFEI_TIXING          扣费提醒发送量,
             KOUFEI_DXHFL           短信回复量,
             KOUFEI_REXWH           热线外呼量,
             KOUFEI_TUIDINGL        业务成功退订量,
             KOUFEI_TDFIRST20_NAME  退订量居前20的业务名称,
             KOUFEI_TOUSU_LIAN      投诉量,
             MW_SHOULILIANG         受理量,
             MW_TUIFEI              退费金额,
             MW_ZYFIRST20_NAME      收费争议投诉量居前20的业务名称,
             CREATE_DATE            统计的数据时间
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
 
 
--22083 不记录业务，无办法提取。
月份
业务代码
业务名称
业务提供商名称
业务计费类型
成功退订量
投诉量
订购用户数

 
 
 select * from  KF.KF_SMS_CMD_RECEIVE_201104
 
 
 
 select count(0) , count(distinct substr(serv_code,9,4)) from KF.KF_SMS_CMD_RECEIVE_201104 where cmd_id = 405129
 
 
 select distinct prod_id, freebie_id, res_count from ngcp.pm_prod_freeres@dbl_jfdb where freebie_id in (select freebie_id         from ngcp.pm_freebie_prop@dbl_jfdb        where trim(upper(unit_des)) = 'KB') 
        
        
 select  prod_id, sum(res_count)/1024 count from ngcp.pm_prod_freeres@dbl_jfdb where freebie_id in (select freebie_id         from ngcp.pm_freebie_prop@dbl_jfdb        where trim(upper(unit_des)) = 'KB') 
group by prod_id

        
select serv_code from KF.KF_SMS_CMD_RECEIVE_201104 where cmd_id = 405129;  －－这个是所有扣费提醒回复短厅信的服务代码。也是营业是能过serv_code来下发信息给用户的，用户是回复到这个短口的



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