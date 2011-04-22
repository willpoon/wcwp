select * from channel.channel_info@dbl_ggdb

select * from kf.TONGYI_TUIDING

select * from   all_tables
where table_name like '%TONGYI_TUIDING%'




select * from   kf.Kf_Sms_Cmd_Receive_201104

select sts,count(0) from kf.Kf_Sms_Cmd_Receive_201104 a
group by sts

and sts=4 and deal_result=0


select count(0) from kf.Kf_Sms_Cmd_Receive_201104 where cmd_id =405003 and sts=4 and deal_result=0
and to_char (create_date,'yyyymmdd') = '20110401'
; --查询成功的
select count(*) from kf.Kf_Sms_Cmd_Receive_201104 where cmd_id =405003 and sts=4 and deal_result not in(0,1); --查询不成功的
 
 
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
        (select  to_char(CREATE_DATE,'yyyymmdd') op_time,count(0) tuiding_cnt
             from   
             kf.TONGYI_TUIDING a
             where sts = 1
            group by to_char(a.CREATE_DATE,'yyyymmdd')
 ) b 
        where to_char(CREATE_DATE,'yyyymmdd')>= '20110401' 
and    to_char(a.CREATE_DATE,'yyyymmdd') = b.op_time
--and TYCX_TUIDING < tuiding_cnt

--当天退订业务总数 ??
select * from    kf.TONGYI_TUIDING


select  to_char(CREATE_DATE,'yyyymmdd') op_time,count(0)
 from   
 kf.TONGYI_TUIDING
 where sts = 1
 group by to_char(CREATE_DATE,'yyyymmdd')
 
 
 select * from  product.UP_SP_info@dbl_ggdb
 
 
 
 --22081
 select  to_char(a.CREATE_DATE,'yyyymmdd') op_time
 ,a.sp_code
 ,a.name
 --业务提供商名称
 ,sum(case when a.sts = 1 then 1 else 0 end ) 成功退订量
 ,0 投诉量
 ,0 --订购用户数
 from  kf.TONGYI_TUIDING a ,product.UP_SP_info@dbl_ggdb b 
 where a.sp_id = b.sp_code
 group by to_char(a.CREATE_DATE,'yyyymmdd')
  ,a.sp_code
 ,a.name
 
 
 
 
 
 
 SELECT * FROM DIM_PROD_UP_PRODUCT_ITEMWHERE ITEM_TYPE='OFFER_PLAN'	  AND DEL_FLAG='1'	  AND SUPPLIER_ID IS NOT NULL	  WITH UR;	  SELECT * FROM DIM_NEWBUSI_SPINFOWITH UR;	  SELECT * FROM DW_PRODUCT_INS_OFF_INS_PROD_201103WHERE OFFER_ID=113110146851WITH UR;


select * from   
 product.UP_SP_SERVICE@dbl_ggdb
 
 --0 mianfei
 --2 dianbo
 --3 baoyue
 
 
 