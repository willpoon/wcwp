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
; --��ѯ�ɹ���
select count(*) from kf.Kf_Sms_Cmd_Receive_201104 where cmd_id =405003 and sts=4 and deal_result not in(0,1); --��ѯ���ɹ���
 
 
 select * from  KF.THREE_ITEM_STAT
 
 
 select TYCX_QUERY             ��ѯ��,
             TYCX_TUIDING           �˶���,
             TYCX_TUIDING_FAIL      �˶�ʧ����,
             TYCX_TUIDING_AVG       �˾������˶�ҵ�������,
             TYCX_FIRST20_BUSI_NAME �˶�����ǰ20��ҵ������,
             TYCX_TOUSU_LIANG       Ͷ����,
             KOUFEI_TIXING          �۷����ѷ�����,
             KOUFEI_DXHFL           ���Żظ���,
             KOUFEI_REXWH           ���������,
             KOUFEI_TUIDINGL        ҵ��ɹ��˶���,
             KOUFEI_TDFIRST20_NAME  �˶�����ǰ20��ҵ������,
             KOUFEI_TOUSU_LIAN      Ͷ����,
             MW_SHOULILIANG         ������,
             MW_TUIFEI              �˷ѽ��,
             MW_ZYFIRST20_NAME      �շ�����Ͷ������ǰ20��ҵ������,
             CREATE_DATE            ͳ�Ƶ�����ʱ��
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
             TYCX_QUERY             ��ѯ��,
             TYCX_TUIDING           �˶���,
             TYCX_TUIDING_FAIL      �˶�ʧ����,
             --TYCX_TUIDING_AVG       �˾������˶�ҵ�������,
             --TYCX_FIRST20_BUSI_NAME �˶�����ǰ20��ҵ������,
             TYCX_TOUSU_LIANG       Ͷ����
             --KOUFEI_TIXING          �۷����ѷ�����,
             --KOUFEI_DXHFL           ���Żظ���,
             --KOUFEI_REXWH           ���������,
             --KOUFEI_TUIDINGL        ҵ��ɹ��˶���,
             --KOUFEI_TDFIRST20_NAME  �˶�����ǰ20��ҵ������,
             --KOUFEI_TOUSU_LIAN      Ͷ����,
             --MW_SHOULILIANG         ������,
             --MW_TUIFEI              �˷ѽ��,
             --MW_ZYFIRST20_NAME      �շ�����Ͷ������ǰ20��ҵ������,
             --CREATE_DATE            ͳ�Ƶ�����ʱ��
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

--�����˶�ҵ������ ??
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
 --ҵ���ṩ������
 ,sum(case when a.sts = 1 then 1 else 0 end ) �ɹ��˶���
 ,0 Ͷ����
 ,0 --�����û���
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
 
 
 