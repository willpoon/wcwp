######################################################################################################
#�ӿ����ƣ��û����ֻ������
#�ӿڱ��룺02007
#�ӿ�˵������¼�û����ֶһ����
#��������: G_S_02007_MONTH.tcl
#��������: ����02007������
#��������: ��
#Դ    ��1.bass2.Dw_product_ord_so_log_dm_yyyymm
#          2.bass2.Dw_product_sc_payment_dm_yyyymm
#          3.bass2.dwd_product_sc_scorelist_yyyymm
#          4.bass2.dw_product_$op_month
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuqf
#��дʱ�䣺2010-06-09
#�����¼��1.ע��
#�޸���ʷ: 1.�Ϸ����������½ű�/�µ�ҵ��ץȡ�ھ�
#          2.1.7.1�淶�޸� liuqf 20110127
#          3.1.7.2�淶�޸� panzw 2011-04-24 
#          ��"ʡ��˾��"�еġ������ࡱ�2210��2250�����������⣬
#          ��������ֻ��������루1100��1200��1300��2100��2300��
#						1000	�ܲ���	
#						1100	ʵ����	
#						1200	������	
#						1210	������	
#						1220	������	
#						1230	ҵ����	
#						1240	�ն���	
#						1250	������	
#						1300	������	
#						2000	ʡ��˾��	
#						2100	ʵ����	
#						2200	������	
#						2210	������	
#						2220	������	
#						2230	ҵ����	
#						2240	�ն���	
#						2250	������	
#						2300	������	
#  ���е�ȡ���Ѿ�����Ҫ��
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {


        #���� yyyymm
        #set op_time 2011-04-01
        #set optime_month 2011-03
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        puts $op_time
        puts $op_month

		set app_name "G_S_02007_MONTH.tcl"        


##################################################






  #ɾ����������
	set sql_buff "delete from bass1.g_s_02007_month where time_id=$op_month"
	exec_sql $sql_buff

	set sql_buff "alter table bass1.g_s_02007_month_1 activate not logged initially with empty table"
	exec_sql $sql_buff
	set sql_buff "alter table bass1.g_s_02007_month_2 activate not logged initially with empty table"
	exec_sql $sql_buff
	set sql_buff "alter table bass1.g_s_02007_month_2a activate not logged initially with empty table"
	exec_sql $sql_buff
	set sql_buff "alter table bass1.g_s_02007_month_3 activate not logged initially with empty table"
	exec_sql $sql_buff
	set sql_buff "alter table bass1.g_s_02007_month_4 activate not logged initially with empty table"
	exec_sql $sql_buff


##~   ȡ���û��һ�ֵ

	set sql_buff "
	insert into G_S_02007_MONTH_1
			select user_id
				,sum(USRSCR) MONTH_CONVERTED_POINTS 
				,count(distinct sc_payment_id)  MONTH_CONVERTED_CNT
			from bass2.dwd_product_sc_payout_$op_month
			group by user_id
            with ur
	
	"
	exec_sql $sql_buff
	
	
	##~   ȡ���ܲ��һ�
	
	set sql_buff "

insert into G_S_02007_MONTH_2
select 
a.user_id
,a.SC_PAYMENT_ID
,1 lvl
,a.peer_seq
,a.AMOUNT
from  bass2.dw_product_sc_payment_dm_$op_month a
where exists 
 (select 1 from  bass2.ODS_SC_SCRD_ORD_INFO_$op_month  b where op_time like '$op_month%'  and a.peer_seq = b.ORD_SEQ and  a.PRODUCT_NO = b.MOB_NUM )
 and exists 
 (select 1 from  bass2.dwd_product_sc_payout_$op_month  c where a.SC_PAYMENT_ID = c.SC_PAYMENT_ID and a.user_id = c.USER_ID)
			 with ur
	"
	exec_sql $sql_buff
	
	
	
	##~   ȡ�ñ��ضһ�
	
	set sql_buff "
insert into G_S_02007_MONTH_2
select 
a.user_id
,a.SC_PAYMENT_ID
,2 lvl
,a.peer_seq
,a.AMOUNT
from  bass2.dw_product_sc_payment_dm_$op_month a
where not exists 
 (select 1 from  G_S_02007_MONTH_2 b where    a.peer_seq = b.peer_seq and  a.user_id = b.user_id )
 and exists 
 (select 1 from  bass2.dwd_product_sc_payout_$op_month  c where a.SC_PAYMENT_ID = c.SC_PAYMENT_ID and a.user_id = c.USER_ID)
 with ur
			
	"
	exec_sql $sql_buff
	
	##~   ���û�ȥ��
	
 	set sql_buff "
insert into G_S_02007_MONTH_2a
select 
         USER_ID
        ,SC_PAYMENT_ID
        ,LVL
        ,value(PEER_SEQ,'0')
        ,AMOUNT
        from (
                select a.*,row_number()over(partition by user_id order by lvl asc ,value(PEER_SEQ,'0') desc , amount desc ) rn 
                from G_S_02007_MONTH_2 a
        ) t where rn = 1
with ur			
	"
	exec_sql $sql_buff
	
	
 ##~   �ܲ�����ȥ��
 	
 	set sql_buff "
insert into G_S_02007_MONTH_3
select  
	ORD_SEQ 
	,ORDER_SUM_POINT
	,type1 
	,ITEM_TYPE
from 
( select a.* , row_number()over(partition by ORD_SEQ order by ORDER_SUM_POINT desc  ) rn 
 from (select ORD_SEQ,ITEM_TYPE,type1,sum(ORDER_SUM_POINT) ORDER_SUM_POINT  from bass2.ODS_SC_SCRD_ORD_INFO_$op_month
  where op_time like '$op_month%' 
  group by  ORD_SEQ,ITEM_TYPE,type1
 )  a
,G_S_02007_MONTH_2a b 
where  a.ORD_SEQ = b.peer_seq
) t where t.rn = 1
with ur
			
	"
	exec_sql $sql_buff
	
	
	
		
 ##~   ���ط���ȥ��
 	
 	set sql_buff "

insert into G_S_02007_MONTH_4
select 
ord_code
,max(offer_name) offer_name
    from bass2.dw_product_ord_so_log_dm_$op_month a,G_S_02007_MONTH_2a b
    where a.ORD_CODE = b.peer_seq
    and (offer_name like '%����%' or offer_name like '%Mֵ%')
    group by ord_code 
with ur
			
	"
	exec_sql $sql_buff
	
	
	
	
			
 ##~   ���ط���ȥ��
 	
 	set sql_buff "
insert into G_S_02007_MONTH_4
select 
ord_code
,max(offer_name) offer_name
    from bass2.dw_product_ord_so_log_dm_$op_month a,G_S_02007_MONTH_2a b
    where a.ORD_CODE = b.peer_seq
    and  not exists (select 1 from G_S_02007_MONTH_4  c where a.ORD_CODE = c.ord_code)
    and offer_name is not null
    group by ord_code 
with ur
			
	"
	exec_sql $sql_buff
	


 	set sql_buff "
	insert into g_s_02007_month
			select 	
			$op_month
			,value(feedback_id ,
			  case when (d.offer_name like '%����%' or d.offer_name like '%����%') then '2100'
			   when (d.offer_name like '%��ֵ��%' or d.offer_name like '%��%����%') then '2210'
			   when (d.offer_name like '%GPRS�ײ�%' or d.offer_name like '%�ֻ���%' or d.offer_name like '%�����ײ�%' or 
					 d.offer_name like '%�Ͳ���%'   or d.offer_name like '%�����ײ�%' or d.offer_name like '%�������־��ֲ�%') then '2230'
			   when (d.offer_name like '%VIP�ͻ�%') then '2220'
			   when (d.offer_name like '%���ֻ�%') or (d.offer_name like '%���ֻ�%') then '2240'
			   when (d.offer_name like '%��ӰƱ%') or (d.offer_name like '%�ݳ�����Ʊ%') or (d.offer_name like '%��ר��%') then '2100'
			   else '2250'
			end ) feedback_id
			,a.USER_ID
			,char(a.MONTH_CONVERTED_POINTS)
			,char(a.MONTH_CONVERTED_CNT)
		from G_S_02007_MONTH_1 a
		left join  G_S_02007_MONTH_2a b on a.user_id = b.user_id  
		left join (select ORD_SEQ,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00' feedback_id 
						from 
						(
						select
										ORD_SEQ,ITEM_TYPE
										,case 
											when type1 = 'ʵ����' then '00'
											when type1 = '������' then '01'
											when type1 = '������' then '02'
											else type1 end type1 
							 from G_S_02007_MONTH_3 a
						) t
						) c on  b.PEER_SEQ = c.ORD_SEQ
		left join G_S_02007_MONTH_4 d on  b.PEER_SEQ = d.ORD_CODE
with ur
	"

exec_sql $sql_buff

  ##~   #�����м���ʱ��1
	##~   set sql_buff "
	##~   declare global temporary table session.g_i_02007_month_tmp_1
	##~   (   
	  ##~   feedback_id        varchar(4),
		##~   user_id            varchar(20),
		##~   used_point         bigint,
		##~   t_total_point      bigint,
		##~   tone_total_point   bigint,
		##~   ttwo_total_point   bigint,
		##~   tthree_used_point  bigint,
		##~   feedback_cnt       bigint
	##~   )
	##~   partitioning key (user_id) using hashing
	##~   with replace on commit preserve rows not logged in tbs_user_temp"
	##~   puts $sql_buff
	##~   exec_sql $sql_buff

	##~   #��������
	##~   ##2000 ʡ��˾��
	##~   ##2100 ʵ����
	##~   ##2200 ������
	##~   ##2210 ������
	##~   ##2220 ������
	##~   ##2230 ҵ����
	##~   ##2240 �ն���
	##~   ##2250 ������
	##~   ##2300 ������
    
  ##~   #��ȡ���ֻ����嵥
	##~   set sql_buff "
	##~   insert into session.g_i_02007_month_tmp_1
	  ##~   (
        ##~   feedback_id
		##~   ,user_id
		##~   ,used_point
		##~   ,feedback_cnt
	  ##~   )
	##~   select 
	  ##~   case when (a.offer_name like '%����%' or a.offer_name like '%����%') then '2100'
	       ##~   when (a.offer_name like '%��ֵ��%' or a.offer_name like '%��%����%') then '2210'
	       ##~   when (a.offer_name like '%GPRS�ײ�%' or a.offer_name like '%�ֻ���%' or a.offer_name like '%�����ײ�%' or 
	             ##~   a.offer_name like '%�Ͳ���%'   or a.offer_name like '%�����ײ�%' or a.offer_name like '%�������־��ֲ�%') then '2230'
	       ##~   when (a.offer_name like '%VIP�ͻ�%') then '2220'
	       ##~   when (a.offer_name like '%���ֻ�%') or (a.offer_name like '%���ֻ�%') then '2240'
	       ##~   when (a.offer_name like '%��ӰƱ%') or (a.offer_name like '%�ݳ�����Ʊ%') or (a.offer_name like '%��ר��%') then '2100'
	       ##~   else '2250'
	  ##~   end feedback_id,
	  ##~   b.user_id,
	  ##~   sum(b.amount),
	  ##~   sum(a.cnt)
	##~   from (select user_id,ord_code,max(offer_name) offer_name,count(*) cnt from  bass2.dw_product_ord_so_log_dm_$op_month 
	         ##~   group by user_id,ord_code ) a ,
	     ##~   bass2.dw_product_sc_payment_dm_$op_month b
	##~   where a.ord_code=b.peer_seq
	  ##~   and b.operation_type in ('3','8')
	##~   group by 
	  ##~   case when (a.offer_name like '%����%' or a.offer_name like '%����%') then '2100'
	       ##~   when (a.offer_name like '%��ֵ��%' or a.offer_name like '%��%����%') then '2210'
	       ##~   when (a.offer_name like '%GPRS�ײ�%' or a.offer_name like '%�ֻ���%' or a.offer_name like '%�����ײ�%' or 
	             ##~   a.offer_name like '%�Ͳ���%'   or a.offer_name like '%�����ײ�%' or a.offer_name like '%�������־��ֲ�%') then '2230'
	       ##~   when (a.offer_name like '%VIP�ͻ�%') then '2220'
	       ##~   when (a.offer_name like '%���ֻ�%') or (a.offer_name like '%���ֻ�%') then '2240'
	       ##~   when (a.offer_name like '%��ӰƱ%') or (a.offer_name like '%�ݳ�����Ʊ%') or (a.offer_name like '%��ר��%') then '2100'
	       ##~   else '2250'
	  ##~   end,
	 ##~   b.user_id
	##~   "

 	##~   puts $sql_buff
	##~   exec_sql $sql_buff

##~   ##�����ܲ����

    
  ##~   #��ȡ���ֻ����嵥
	##~   set sql_buff "
	##~   insert into session.g_i_02007_month_tmp_1
	  ##~   (
     ##~   feedback_id
		##~   ,user_id
		##~   ,used_point
		##~   ,feedback_cnt
	  ##~   )
	
##~   select 
	  ##~   a.feedback_id,
	  ##~   b.user_id,
	  ##~   sum(a.used_point),
	  ##~   count(distinct mob_num||feedback_id) feedback_cnt
	##~   from (                   
					##~   select mob_num,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00' feedback_id
					##~   ,sum(ORDER_SUM_POINT)  used_point
						##~   from 
						##~   (
						##~   select
										 ##~   mob_num 
										##~   ,ITEM_TYPE
										##~   ,case 
											##~   when type1 = 'ʵ����' then '00'
											##~   when type1 = '������' then '01'
											##~   when type1 = '������' then '02'
											##~   else type1 end type1 
										##~   ,sum(value(ORDER_SUM_POINT,0)) ORDER_SUM_POINT
							 ##~   from bass2.ODS_SC_SCRD_ORD_INFO_${op_month} a
							 			##~   where substr(OP_TIME,1,6) = '$op_month'
							 			  ##~   group by 
										 ##~   mob_num 
										##~   ,ITEM_TYPE
										##~   ,case 
											##~   when type1 = 'ʵ����' then '00'
											##~   when type1 = '������' then '01'
											##~   when type1 = '������' then '02'
											##~   else type1 end 
										##~   having sum(value(ORDER_SUM_POINT,0)) > 0
						##~   ) t
						##~   group by                    
						##~   mob_num,substr(ITEM_TYPE,2,1)||cast(int(substr(type1,2,1))+1 as char(1))||'00'
						##~   ) a, 
	 ##~   bass2.dw_product_$op_month b
	 ##~   where a.mob_num = b.product_no
	    ##~   and b.userstatus_id in (1,2,3,6,8) 
		  ##~   and b.usertype_id in (1,2,9) 
		  ##~   and b.test_mark=0
##~   group by 	  a.feedback_id,
	  ##~   b.user_id
	##~   "

	##~   exec_sql $sql_buff



    #Ϊ���޷�����ʷ���ݣ����Զ������ݽ������⴦��
    #
    #
###	set sql_buff "
###	insert into session.g_i_02007_month_tmp_1
###	  (
###        feedback_id
###		,user_id
###		,used_point
###		,feedback_cnt
###	  )
###		select
###		  case when (b.cond_name like '%����%' or b.cond_name like '%����%') then '2100'
###		       when (b.cond_name like '%��ֵ��%' or b.cond_name like '%��%����%') then '2210'
###		       when (b.cond_name like '%GPRS�ײ�%' or b.cond_name like '%�ֻ���%' or b.cond_name like '%�����ײ�%' or 
###		             b.cond_name like '%�Ͳ���%'   or b.cond_name like '%�����ײ�%' or b.cond_name like '%�������־��ֲ�%') then '2230'
###		       when (b.cond_name like '%VIP�ͻ�%') then '2220'
###		       when (b.cond_name like '%���ֻ�%') or (b.cond_name like '%���ֻ�%') then '2240'
###		       when (b.cond_name like '%��ӰƱ%') or (b.cond_name like '%�ݳ�����Ʊ%') or (b.cond_name like '%��ר��%') then '2100'
###		       else '2250'
###		  end ,
###		  coalesce(c.new_user_id,a.user_id) as user_id,
###		  sum(a.value),
###		  count(*)
###		from BASS2.DWD_CM_BUSI_COIN_20100625 a
###		left outer join 
###		  (select distinct so_nbr,user_id,cond_name from  BASS2.DW_PRODUCT_BUSI_PROMO_201006) b
###		on a.so_nbr = b.so_nbr
###		left join bass2.trans_user_id_20100625 c on a.user_id=c.user_id
###		where a.charge_date > '2010-06-01 00:00:00' and a.charge_date < '2010-07-01 00:00:00'
###		group by                       
###		 case when (b.cond_name like '%����%' or b.cond_name like '%����%') then '2100'
###		       when (b.cond_name like '%��ֵ��%' or b.cond_name like '%��%����%') then '2210'
###		       when (b.cond_name like '%GPRS�ײ�%' or b.cond_name like '%�ֻ���%' or b.cond_name like '%�����ײ�%' or 
###		             b.cond_name like '%�Ͳ���%'   or b.cond_name like '%�����ײ�%' or b.cond_name like '%�������־��ֲ�%') then '2230'
###		       when (b.cond_name like '%VIP�ͻ�%') then '2220'
###		       when (b.cond_name like '%���ֻ�%') or (b.cond_name like '%���ֻ�%') then '2240'
###		       when (b.cond_name like '%��ӰƱ%') or (b.cond_name like '%�ݳ�����Ʊ%') or (b.cond_name like '%��ר��%') then '2100'
###		       else '2250'
###		  end ,
###		  coalesce(c.new_user_id,a.user_id)
###		having sum(a.value) > 0
###	"
###
### 	puts $sql_buff
###	exec_sql $sql_buff
###





  ##~   #�����м���ʱ��2
	##~   set sql_buff "
	##~   declare global temporary table session.g_i_02007_month_tmp_2
	##~   (   
		##~   user_id            varchar(20),
		##~   t_total_point      bigint,
		##~   tone_total_point   bigint,
		##~   ttwo_total_point   bigint,
		##~   tthree_used_point  bigint
	##~   )
	##~   partitioning key (user_id) using hashing
	##~   with replace on commit preserve rows not logged in tbs_user_temp"
	##~   puts $sql_buff
	##~   exec_sql $sql_buff

    
  ##~   #���㵱�������û��Ѷһ��Ļ���
	##~   set sql_buff "
	##~   insert into session.g_i_02007_month_tmp_2
	  ##~   (
     ##~   user_id
		##~   ,t_total_point
		##~   ,tone_total_point
		##~   ,ttwo_total_point
		##~   ,tthree_used_point
	  ##~   )
	##~   select 
	   ##~   a.product_instance_id user_id
	   ##~   ,sum(case when a.year=$syear then b.usrscr else 0 end)       t_total_point
	   ##~   ,sum(case when a.year=$oneyear then b.usrscr else 0 end)     tone_total_point
	   ##~   ,sum(case when a.year=$twoyear then b.usrscr else 0 end)     ttwo_total_point
	   ##~   ,sum(case when a.year<=$threeyear then b.usrscr else 0 end)  tthree_used_point
	 ##~   from bass2.dwd_product_sc_scorelist_$op_month a,bass2.dwd_product_sc_payout_$op_month b
    ##~   where a.sc_scorelist_id=b.sc_scorelist_id
	  ##~   and a.actflag='1'
   ##~   group by a.product_instance_id
   ##~   "

 	##~   puts $sql_buff
	##~   exec_sql $sql_buff


  ##~   #���������û�����������
	##~   set sql_buff "
	##~   insert into bass1.G_S_02007_month
	##~   (
		##~   time_id
		##~   ,point_feedback_id
		##~   ,user_id
		##~   ,used_point
		##~   ,t_used_point
		##~   ,tone_used_point
		##~   ,ttwo_used_point
		##~   ,tthree_used_point
		##~   ,Used_Count
	##~   )
	##~   select
		 ##~   $op_month
		##~   ,a.feedback_id
		##~   ,a.user_id
		##~   ,char(coalesce(a.used_point,0))
		##~   ,char(coalesce(b.t_total_point,0))
		##~   ,char(coalesce(b.tone_total_point,0))
		##~   ,char(coalesce(b.ttwo_total_point,0))
		##~   ,char(coalesce(b.tthree_used_point,0))
		##~   ,char(coalesce(a.feedback_cnt,0))
	##~   from
	   ##~   (
		##~   select
		  ##~   a.feedback_id
		  ##~   ,a.user_id
		  ##~   ,a.used_point
		  ##~   ,a.feedback_cnt
		##~   from 
			##~   session.g_i_02007_month_tmp_1 a,
			##~   bass2.dw_product_$op_month b 
		##~   where a.user_id = b.user_id 
		  ##~   and b.userstatus_id in (1,2,3,6,8) 
		  ##~   and b.usertype_id in (1,2,9) 
		  ##~   and b.test_mark=0
		##~   ) a
	##~   left join session.g_i_02007_month_tmp_2 b on a.user_id = b.user_id
  ##~   "

	##~   exec_sql $sql_buff
	
	#����У�飺����ID����ָ����Χ��
	
	set sql_buff "
		select count(0) 
		from    G_S_02007_MONTH
		where 
		time_id = $op_month
		and POINT_FEEDBACK_ID 	not in ('2210','2220','2230','2240','2250'
								,'1100','1200','1300','2100','2100')
	"
	set RESULT_VAL [get_single $sql_buff]
	puts $RESULT_VAL
		if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent "02007�зǷ��������!"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }


	return 0
}


###########�ο�############
#and b.userstatus_id in (1,2,3,6,8)  --�����û�--
#userstatus_id in (4,5,7,9) --�����û�
#and b.usertype_id in (1,2,9) --�������û�--
#and (b.crm_brand_id1=1 or b.crm_brand_id1=4) --ȫ��ͨ�Ͷ��еش��û�--
#and a.sts=1 --������¼--
#############################################################
