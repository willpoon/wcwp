######################################################################################################
#�ӿ����ƣ����������ؼ������ջ���
#�ӿڱ��룺22066
#�ӿ�˵������¼���е��������ؼ������ջ��ܵ���Ϣ��
#��������: G_S_22066_DAY.tcl
#��������: ����22066������
#��������: ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2011-06-10
#�����¼��
#�޸���ʷ: ref:22065 key_0133
#2011.12.01 1.7.7 
#4���޸Ľӿ�22066�����������ؼ������ջ��ܣ�
#?	�������ԡ���Լ�ƻ��ն�����������������ն������������������ն����������������ж����ֻ�����������
#?	�޸����ԡ��ն���������Ϊ�������ն�����������
#?	�޸����ԡ���ֵ�ɷѱ���������������
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#set dt_list { 2011-07-31	2011-07-30	2011-07-29	2011-07-28	2011-07-27	2011-07-26	2011-07-25	2011-07-24	2011-07-23	2011-07-22	2011-07-21	2011-07-20	2011-07-19	2011-07-18	2011-07-17	2011-07-16	2011-07-15	2011-07-14	2011-07-13	2011-07-12	2011-07-11	2011-07-10	2011-07-09	2011-07-08	2011-07-07	2011-07-06	2011-07-05	2011-07-04	2011-07-03	2011-07-02	2011-07-01  }
#set dt_list { 2011-08-13	2011-08-14	2011-08-15	2011-08-16	2011-08-17	2011-08-18 }
#set dt_list { 2011-07-31	2011-07-30	2011-07-29	2011-07-28 2011-07-20	2011-08-01	2011-08-02	2011-08-03	2011-08-04	2011-08-05	2011-08-06	2011-08-07	2011-08-08	2011-08-09	2011-08-10	2011-08-11	2011-08-12 2011-08-13 }
#
#
#foreach dt ${dt_list} {
#set op_time $dt
#puts $op_time
#p22066 $op_time $optime_month
#}

 # ͨ�� while ѭ��
# set i 0 ���������������� 0 Ϊ ����
#	set i 2
## ���������������� , $i<= n   ,  n Խ��Խ��Զ
#	while { $i<=100 } {
#	        set sql_buff "select char(current date - ( 1+$i ) days) from bass2.dual"
#	        set op_time [get_single $sql_buff]
#	
#	if { $op_time >= "2011-06-01" } {
#	puts $op_time
#	p22066 $op_time $optime_month
#	
#	}
#	incr i
#	}
#
#set op_time  2011-08-14

#
p22066 $op_time $optime_month
         
	return 0
}

proc p22066 { op_time optime_month } {
   #���� yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

    #���� yyyy-mm-dd
    set optime $op_time

    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month

    #���µ�һ�� yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day
		#��Ȼ��
		set curr_month [string range $op_time 0 3][string range $op_time 5 6]
    #�������һ�� yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day
		global app_name
		set app_name "G_S_22066_DAY.tcl"        



	 set p_optime $op_time
 	 set	date_optime	[ai_to_date $p_optime]
 	 scan   $p_optime "%04s-%02s-%02s" year month day
 	 puts   $p_optime

	 #�������һ��
	 #set    last_day_month [GetLastDay [GetNextMonth $year$month]01]
	 #puts   $last_day_month
	 #scan  $last_day_month "%04s%02s%02s" lyear lmonth lday
	 #puts  $lmonth



   #ɾ����ʽ��������
   set sql_buff "delete from bass1.G_S_22066_DAY where time_id=$timestamp"
   exec_sql $sql_buff
######
######		set sql_buf "ALTER TABLE BASS1.G_S_22066_DAY_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
######		exec_sql $sql_buf
######		set sql_buf "ALTER TABLE BASS1.G_S_22066_DAY_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
######		exec_sql $sql_buf
#set sql_buf "ALTER TABLE BASS1.G_S_22066_DAY_REC ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
#exec_sql $sql_buf
set sql_buf "ALTER TABLE BASS1.G_S_22066_DAY_STAT ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
exec_sql $sql_buf

     #Դ��
#   set dw_ow_loading_log_yyyymm               "bass2.dw_ow_loading_log_$year$month"
#   set dw_custsvc_ivr_log_dm_yyyymm           "bass2.dw_custsvc_ivr_log_dm_$year$month"
#   set dw_kf_sms_cmd_receive_dm_yyyymm        "bass2.dw_kf_sms_cmd_receive_dm_$year$month"
#   set dw_product_ord_cust_dm_yyyymm          "bass2.dw_product_ord_cust_dm_$year$month"
#   set dw_product_busi_dm_yyyymm              "bass2.dw_product_busi_dm_$year$month"
#   set dw_product_yyyymm                      "bass2.dw_product_$year$month"
#   set dw_acct_payitem_dm_yyyymm              "bass2.dw_acct_payitem_dm_$year$month"
#   set dw_kf_sms_cmd_receive_dm_yyyymm        "bass2.dw_kf_sms_cmd_receive_dm_$year$month"
#   set dw_kf_cmd_hint_def_dm_yyyymm           "bass2.dw_kf_cmd_hint_def_dm_$year$month"
#   set dw_kf_sms_cmd_receive_dm_yyyymm        "bass2.dw_kf_sms_cmd_receive_dm_$year$month"
#   set dw_product_ord_so_log_dm_yyyymm        "bass2.dw_product_ord_so_log_dm_$year$month"
#   set dw_product_ord_srvpkg_dm_yyyymm        "bass2.dw_product_ord_srvpkg_dm_$year$month"
#   set dw_wcc_user_action_yyyymm              "bass2.dw_wcc_user_action_$year$month"
#

set sql_buff "
declare global temporary table session.stat_market_channel_key_0133_tmp (
              region_id   varchar(12),
              s_index_id  smallint,
              t_index_id  smallint,
              value       decimal(12,2)
          )
          partitioning key
        	(
        	  region_id
        	) using hashing
        with replace on commit preserve rows not logged in tbs_user_temp        
"           
exec_sql $sql_buff

##03 ҵ�������(������ֵ���ѡ���ѯ��)

#201��վ
       set sql_buff "insert into session.stat_market_channel_key_0133_tmp
                   select b.region_id,101,201,count(a.product_instance_id)
                   from 
                      (select product_instance_id
                       from bass2.dw_product_ord_cust_dm_$curr_month a
                       where channel_type = 'B' and    a.op_time = '$op_time' ) a,
                       bass2.dw_product_$timestamp b
                    where a.product_instance_id = b.user_id 
                    group by b.region_id
        "
      
exec_sql $sql_buff
#202����
        set sql_buff  "insert into session.stat_market_channel_key_0133_tmp
                        select b.region_id,101,202,count(*)
                        from bass2.dw_custsvc_ivr_log_dm_$curr_month a,
                             bass2.dw_product_$timestamp b
                        where a.calling = b.product_no 
                         and b.userstatus_id in (1,2,3,6,8)
                         and a.operation_type<>6
			and a.op_time = '$op_time' 
                        group by b.region_id
        "
exec_sql $sql_buff
      



	  #����

	   set sql_buff "insert into session.stat_market_channel_key_0133_tmp
                select b.region_id,101,203,count(*)
                from 
                        (select * from bass2.dw_kf_sms_cmd_receive_dm_$curr_month a where a.op_time = '$op_time'  ) a,
			bass2.dw_product_$timestamp b
			where 
						 a.phone_id = b.product_no 
						and b.userstatus_id in (1,2,3,6,8)
						and b.usertype_id in (1,2,9)
and a.cmd_id in (     select distinct(cmd_id) 
						from bass2.Dwd_sys_cmd_def_$timestamp
							   where sts=0 and cmd_group in(9,10,20,12) 
							       and fun_id <>100001 
						and cmd_desc not like '%��ѯ%' 
					and cmd_type <>11  ) 
						and deal_result=0 
						and sts=4 
						and region_code <> 999
						and a.cmd_id <> 201
									
				group by  b.region_id
   "
exec_sql $sql_buff



	  #wap    
#                ods ��������ʷ���ݣ���������֮
    set sql_buff "insert into session.stat_market_channel_key_0133_tmp
        select d.region_id,101,204,count(*)
         from bass2.dw_product_ord_cust_dm_$curr_month a,
                      bass2.dw_product_ord_busi_dm_$curr_month b,
                       bass2.Dwd_product_ord_busi_other_$timestamp  c,
			bass2.dw_product_$timestamp d
                      where a.customer_order_id = b.customer_order_id
                      and b.busioper_order_id =c.busioper_order_id 
                      and c.ext_6 like '%SRC=206%'   
			and a.product_instance_id = d.user_id  
			and a.op_time = '$op_time' 
			and b.op_time = '$op_time'
				group by d.region_id    
   "
exec_sql $sql_buff

	  
	  #�����ն�    
       set sql_buff "insert into session.stat_market_channel_key_0133_tmp
        select b.region_id,101,205,count(*)
         from  bass2.dw_product_ord_cust_dm_$curr_month a,
               bass2.dw_product_$timestamp b
         where a.product_instance_id = b.user_id
               and  a.channel_type ='9'  
	and a.op_time = '$op_time' 	 
         group by  b.region_id  
   "
 exec_sql $sql_buff
  


##04 �ź��� (�ް���)
set sql_buff "
select 1 from bass2.dual
"
exec_sql $sql_buff
##05 �ն������� (�ް���)
set sql_buff "
select 1 from bass2.dual
"
exec_sql $sql_buff

# 2011.12.01 
# 4���޸Ľӿ�22066�����������ؼ������ջ��ܣ�
# ?	�������ԡ���Լ�ƻ��ն�����������������ն������������������ն����������������ж����ֻ�����������
# ?	�޸����ԡ��ն���������Ϊ�������ն�����������
# ?	�޸����ԡ���ֵ�ɷѱ���������������
#��Լ�ƻ��ն�������

#�ն����������

#���ж����ֻ�������

#�����ն�������

#                   where product_no in (select product_no from  bass2.dwd_ow_loading_log_$timestamp
#                                           where is_success in (0,1) 
#                                           and date(start_time)>='2010-12-01' 
#                                           and date(start_time)<'2011-01-01'
#                                           ) and userstatus_id>0
#                      ) and a.so_mode ='15' and a.op_id=10000475
#                      
##06 ��ֵҵ��ͨ��
## ������������д����
set sql_buff "
        
insert into session.stat_market_channel_key_0133_tmp
                   select c.region_id,104,
                   case when a.echl_type = 1 then 202
                                       when a.echl_type = 2 then 201
                                       when a.echl_type = 3 then 203
                                       when a.echl_type = 5 then 204
                                       when a.echl_type = 6 then 205
                                       else 206 end ,
									   count(a.product_no)
 from (
select echl_type,busi_code_type,case brand_id when 1 then 1 when 4 then 2 else 3 end,
         sum(flow) flows,product_no
      from (
            select 1 as echl_type,busi_code_type,brand_id,product_no,sum(flow) as flow
            from (
      select 21 as busi_code_type, brand as brand_id, count(*) as flow, calling as product_no
      from  (select * from bass2.dw_custsvc_ivr_log_dm_$curr_month  where op_time = '$op_time') a
      where operation_code = '1008611' or operation_code = '20102'
      group by brand, calling
      union all
      select 23 as busi_code_type, brand as brand_id, count(*) as flow, calling as product_no
      from  (select * from bass2.dw_custsvc_ivr_log_dm_$curr_month  where op_time = '$op_time') a
      where operation_code = '20106'
      group by brand, calling
      union all
      select 24 as busi_code_type, brand as brand_id, count(*) as flow, calling as product_no
      from  (select * from bass2.dw_custsvc_ivr_log_dm_$curr_month  where op_time = '$op_time') a
      where operation_type <> 6
      group by brand, calling
               union all
      select 22 as busi_code_type, case when brand_name like '%ȫ��ͨ%' then 1
               when brand_name like '%���еش�%' then 4 else 5 end as brand_id,
       count(*) as flow, bill_id as product_no
      from  (select * from bass2.dw_product_ord_so_log_dm_$curr_month  where op_time = '$op_time') a
      where length(trim(char(op_id))) = 4
      and busi_id in (
      select business_id from  bass2.DIM_SO_BUSIOPTCODE_MAPPING where opt_name not like '%��ѯ%')
      group by case when brand_name like '%ȫ��ͨ%' then 1
        when brand_name like '%���еش�%' then 4 else 5 end, bill_id
                 )a
            group by busi_code_type,brand_id,product_no
            union all
            select 2 as echl_type,busi_code_type,brand_id,product_no,sum(flow) as flow
            from (select b.parent_ob_id as busi_code_type,a.brand_id,
                      count(1) as flow,a.product_no
                   from  bass2.dwd_ow_loading_log_$timestamp a, bass2.dim_sys_object5  b
                   where a.ob_type =1 and b.parent_ob_id in (21,23) and b.check_result = 5 and a.ob_id = b.ob_id
                      and date(a.start_time)='$op_time'
       and (a.is_success in (0,1) or a.err_msg is null
                   or a.err_msg = 'action is null' or a.err_msg = '����ǰ��Ӫ�������޷�����ô���!'
                   or a.err_msg = '������ķ�������У��������벻��ȷ!' or a.err_msg = 'Broken pipe')
                   group by b.parent_ob_id,a.brand_id,a.product_no
                   union all
                   select 22 as busi_code_type,b.brand_id,count(1) as flow,b.product_no
                   from  (select * from bass2.dw_product_busi_dm_$curr_month where op_time = '$op_time') a
                   , bass2.dw_product_$timestamp b
                   where a.user_id=b.user_id and a.user_id in (
                      select user_id from  bass2.dw_product_$timestamp
                      where product_no in (select product_no from  bass2.dwd_ow_loading_log_$timestamp
                                           where is_success in (0,1) 
                                           and date(start_time)='$op_time' 
                                           ) and userstatus_id>0
                      ) and a.so_mode ='15' and a.op_id=10000475
                      and date(a.so_date)='$op_time' 
                   group by b.brand_id,b.product_no
                   union all
                   select 24 as busi_code_type,b.brand_id,count(1) as flow,b.product_no
                   from  (select * from bass2.dw_acct_payitem_dm_$curr_month where op_time = '$op_time') a, bass2.dw_product_$timestamp b
                   where a.user_id=b.user_id and a.paytype_id in ('4468')
                   group by b.brand_id,b.product_no
                  )a
            group by busi_code_type,brand_id,product_no
            union all
            select 3 as echl_type,busi_code_type,brand_id,phone_id as product_no,sum(flow) as flow
            from (select case when c.result_type in ('CXYE','CXZD','CXZE','CXKCYE') then 21
            else 23 end as busi_code_type, a.brand_id,
             count(1) as flow, b.phone_id
      from  bass2.dw_product_$timestamp a,  (select * from bass2.dw_kf_sms_cmd_receive_dm_$curr_month where op_time = '$op_time') b
	,  (select * from bass2.dw_kf_cmd_hint_def_dm_$curr_month where op_time = '$op_time') c
      where a.product_no  = b.phone_id and b.cmd_id = c.cmd_id and b.deal_result = c.cmd_result
       and c.result_jieg in (0)
      and c.result_type in ('CXYE','CXZD','CXZE','CXKCYE','CXJF')
      and a.userstatus_id in (1,2,3,6,8) and a.test_mark <> 1 and a.usertype_id in (1,2,9)
      group by case when c.result_type in ('CXYE','CXZD','CXZE','CXKCYE') then 21
               else 23 end, a.brand_id, b.phone_id
      union all
      select 22 as busi_code_type, a.brand_id,
         count(1) as flow, b.phone_id
      from  bass2.dw_product_$timestamp a,  (select * from bass2.dw_kf_sms_cmd_receive_dm_$curr_month where op_time = '$op_time') b,  (select * from bass2.dw_kf_cmd_hint_def_dm_$curr_month where op_time = '$op_time') c
      where a.product_no  = b.phone_id and b.cmd_id = c.cmd_id and b.deal_result = c.cmd_result
      and replace(char(c.op_time),'-','') = '$timestamp' and c.result_jieg in (0)
      and c.result_type in (select sms_cmd from bass2.stat_ecustsvc_sms_cmd)
      and a.userstatus_id in (1,2,3,6,8) and a.test_mark <> 1 and a.usertype_id in (1,2,9)
      group by a.brand_id, b.phone_id) a
            group by busi_code_type,brand_id,phone_id
            union all
   select 5 as echl_type,22 as busi_code_type,b.brand_id,b.product_no, count(1) as flow
   from  (select * from bass2.dw_product_ord_cust_dm_$curr_month where op_time = '$op_time') a left join  bass2.dw_product_$timestamp b on a.product_instance_id=b.user_id
   where channel_type='6' and source_system_id in (3) and business_id=193000000001 and old_so_order_id is null
   group by b.brand_id, b.product_no
            union all
select 6 as echl_type,24 as busi_code_type,a.brand_id,a.product_no,count(1) as flow
            from  bass2.dw_product_$timestamp a
            , (select * from bass2.dw_acct_payitem_dm_$curr_month where op_time = '$op_time') b
            where a.user_id=b.user_id and b.paytype_id in ('4162','4864') and b.rec_sts=0
            group by a.brand_id,a.product_no
            union all
select 6 as echl_type,22 as busi_code_type,a.brand_id,a.product_no,count(1) as flow
            from  bass2.dw_product_$timestamp a,
             (select * from bass2.dw_product_busi_dm_$curr_month where op_time = '$op_time') b
            where a.user_id=b.user_id and b.so_mode='5' 
            and b.op_id in (select op_id from  bass2.dim_boss_staff where op_name like '%����%')
            and b.process_id not in (2,3,22,14,24,12,13)
            group by a.brand_id,a.product_no
           )a
      group by echl_type,busi_code_type,case brand_id when 1 then 1 when 4 then 2 else 3 end,product_no ) a,
	  (
select b.product_instance_id from (select * from bass2.dw_product_ord_srvpkg_dm_$curr_month where op_time = '$op_time') a,(select * from bass2.dw_product_ord_cust_dm_$curr_month where op_time = '$op_time') b
where 1=1 and a.customer_order_id=b.customer_order_id
   and a.servicepkg_id in (
select PRODUCT_ITEM_ID from bass2.DIM_PROD_UP_PRODUCT_ITEM a where a.SUPPLIER_ID in ('400000','400001','701001','80001','801110','801114','801132','801137','801151','801157','801163','801164','801166','801168','801173','801185','801187','801191','801233','801236','801237','801241','801242','801244','801248','900013','900103','900104','900105','900106','900107','900109','900110','900112','900113','900114','900115','900116','900117','900118','900119','900120','900121','900122','900123','900128','900129','900130','900132','900138','900139','900140','900141','900143','900144','900145','900148','900520','900652','901500','901585','901876','901899','901908','901922','931067','931078','931106','931805','98555') and item_type='SERVICE_PRICE')


) b,bass2.dw_product_$timestamp c
where a.product_no=c.PRODUCT_NO and c.USER_ID=b.PRODUCT_INSTANCE_ID and a.busi_code_type     in (22)
     and c.userstatus_id in (1,2,3,6,8)
		     and c.usertype_id in (1,2,9)
	  group by  c.region_id,104,
                   case when a.echl_type = 1 then 202
                                       when a.echl_type = 2 then 201
                                       when a.echl_type = 3 then 203
                                       when a.echl_type = 5 then 204
                                       when a.echl_type = 6 then 205
                                       else 206 end
"
exec_sql $sql_buff

##07 ��ֵ�ɷѽ��
set sql_buff "insert into session.stat_market_channel_key_0133_tmp
                     select a.region_id,105,
                            case when a.class = 4 then 205 
                                 when a.class = 5 then 201
                            else 206 end ,sum(amount)
		                 from
		                    ( select a.region_id,
		                            case when b.opt_code in ('4464','4864') then 4 
		                                 when b.opt_code in ('4468') then 5 
		                                 else 6 end as class,sum(amount) as amount
                            from  bass2.dw_product_$timestamp a,  (select * from bass2.dw_acct_payment_dm_$curr_month where op_time = '$op_time') b
                            where a.user_id=b.user_id and b.opt_code in ('4464','4864','4468','SJJF2','4115')
                            group by  a.region_id,
                                      case when b.opt_code in ('4464','4864') then 4 
		                                 when b.opt_code in ('4468') then 5 
		                                 else 6 end
                          )  a
		                    group by a.region_id,105,
		                             case when a.class = 4 then 205 
                                 when a.class = 5 then 201
                            else 206 end
        "        
exec_sql $sql_buff

#2011-07-30 17:21:50
##07 ��ֵ�ɷѱ���
set sql_buff "insert into session.stat_market_channel_key_0133_tmp
                     select a.region_id,108,
                            case when a.class = 4 then 205 
                                 when a.class = 5 then 201
                            else 206 end , sum(cnt)
		                 from
		                    ( select a.region_id,
		                            case when b.opt_code in ('4464','4864') then 4 
		                                 when b.opt_code in ('4468') then 5 
		                                 else 6 end as class
		                                 ,count(0) cnt
                            from  bass2.dw_product_$timestamp a
                            ,  (select * from bass2.dw_acct_payment_dm_$curr_month where op_time = '$op_time') b
                            where a.user_id=b.user_id 
                            		and b.opt_code in ('4464','4864','4468','SJJF2','4115')
                            group by  a.region_id,
                                      case when b.opt_code in ('4464','4864') then 4 
		                                 when b.opt_code in ('4468') then 5 
		                                 else 6 end
                          )  a
		                    group by a.region_id,108,
		                             case when a.class = 4 then 205 
                                 when a.class = 5 then 201
                            else 206 end
        "        
exec_sql $sql_buff


##08 ҵ���ѯ��

## �����ն� �굥��ѯ
#	    union all
#	select 6 as echl_type,22 as busi_code_type , a.brand_id ,a.product_no,count(1) as flow
#            from  bass2.dw_product_$timestamp a
#            , (select * from bass2.dw_product_ord_so_log_dm_$curr_month where op_time = '$op_time' ) c
#	, (
#	  select product_item_id ,name from bass2.dim_prod_up_product_item where item_type='BUSINESS'
#	and name like '%��ѯ%'
#	) b
#	where
#		a.user_id = c.USER_ID
#		and c.BUSI_ID  = b.product_item_id
#		and  c.OP_ID in ( select OP_ID from  bass2.DIM_BOSS_STAFF  where op_name like '%�����ն�%' )
#            group by a.brand_id,a.product_no
#

set sql_buff "

insert into session.stat_market_channel_key_0133_tmp
                   select b.region_id,107,
                   case when a.echl_type = 1 then 202
                                       when a.echl_type = 2 then 201
                                       when a.echl_type = 3 then 203
                                       when a.echl_type = 5 then 204
                                       when a.echl_type = 6 then 205
                                       else 206 end,
									   sum(a.flows)
from (
select echl_type,busi_code_type,case brand_id when 1 then 1 when 4 then 2 else 3 end,
         sum(flow) flows,product_no
      from (
            select 1 as echl_type,busi_code_type,brand_id,product_no,sum(flow) as flow
            from (
      select 21 as busi_code_type, brand as brand_id, count(*) as flow, calling as product_no
      from  (select * from bass2.dw_custsvc_ivr_log_dm_$curr_month  where op_time = '$op_time') a
      where operation_code = '1008611' or operation_code = '20102'
      group by brand, calling
      union all
      select 23 as busi_code_type, brand as brand_id, count(*) as flow, calling as product_no
      from  (select * from bass2.dw_custsvc_ivr_log_dm_$curr_month  where op_time = '$op_time') a
      where operation_code = '20106'
      group by brand, calling
      union all
      select 24 as busi_code_type, brand as brand_id, count(*) as flow, calling as product_no
      from  (select * from bass2.dw_custsvc_ivr_log_dm_$curr_month  where op_time = '$op_time') a
      where operation_type <> 6
      group by brand, calling
               union all
      select 22 as busi_code_type, case when brand_name like '%ȫ��ͨ%' then 1
               when brand_name like '%���еش�%' then 4 else 5 end as brand_id,
       count(*) as flow, bill_id as product_no
      from  (select * from bass2.dw_product_ord_so_log_dm_$curr_month  where op_time = '$op_time') a
      where length(trim(char(op_id))) = 4
      and busi_id in (
      select business_id from  bass2.DIM_SO_BUSIOPTCODE_MAPPING where opt_name not like '%��ѯ%')
      group by case when brand_name like '%ȫ��ͨ%' then 1
        when brand_name like '%���еش�%' then 4 else 5 end, bill_id
                 )a
            group by busi_code_type,brand_id,product_no
            union all
            select 2 as echl_type,busi_code_type,brand_id,product_no,sum(flow) as flow
            from (select b.parent_ob_id as busi_code_type,a.brand_id,
                      count(1) as flow,a.product_no
                   from  bass2.dwd_ow_loading_log_$timestamp a, bass2.dim_sys_object5  b
                   where a.ob_type =1 and b.parent_ob_id in (21,23) and b.check_result = 5 and a.ob_id = b.ob_id
                      and date(a.start_time)='$op_time' 
       and (a.is_success in (0,1) or a.err_msg is null
                   or a.err_msg = 'action is null' or a.err_msg = '����ǰ��Ӫ�������޷�����ô���!'
                   or a.err_msg = '������ķ�������У��������벻��ȷ!' or a.err_msg = 'Broken pipe')
                   group by b.parent_ob_id,a.brand_id,a.product_no
                   union all
                   select 22 as busi_code_type,b.brand_id,count(1) as flow,b.product_no
                   from  (select * from bass2.dw_product_busi_dm_$curr_month where op_time = '$op_time') a, bass2.dw_product_$timestamp b
                   where a.user_id=b.user_id and a.user_id in (
                      select user_id from  bass2.dw_product_$timestamp
                      where product_no in (select product_no from  bass2.dwd_ow_loading_log_$timestamp
                                           where is_success in (0,1) and date(start_time)='$op_time' 
                                           ) and userstatus_id>0
                      ) and a.so_mode ='15' and a.op_id=10000475
                      and date(a.so_date)='$op_time' 
                   group by b.brand_id,b.product_no
                   union all
                   select 24 as busi_code_type,b.brand_id,count(1) as flow,b.product_no
                   from  (select * from bass2.dw_acct_payitem_dm_$curr_month where op_time = '$op_time') a, bass2.dw_product_$timestamp b
                   where a.user_id=b.user_id and a.paytype_id in ('4468')
                   group by b.brand_id,b.product_no
                  )a
            group by busi_code_type,brand_id,product_no
            union all
            select 3 as echl_type,busi_code_type,brand_id,phone_id as product_no,sum(flow) as flow
            from (select case when c.result_type in ('CXYE','CXZD','CXZE','CXKCYE') then 21
            else 23 end as busi_code_type, a.brand_id,
             count(1) as flow, b.phone_id
      from  bass2.dw_product_$timestamp a
	,  (select * from bass2.dw_kf_sms_cmd_receive_dm_$curr_month where op_time = '$op_time') b
,  (select * from bass2.dw_kf_cmd_hint_def_dm_$curr_month where op_time = '$op_time') c
      where a.product_no  = b.phone_id and b.cmd_id = c.cmd_id and b.deal_result = c.cmd_result
       and c.result_jieg in (0)
      and c.result_type in ('CXYE','CXZD','CXZE','CXKCYE','CXJF')
      and a.userstatus_id in (1,2,3,6,8) and a.test_mark <> 1 and a.usertype_id in (1,2,9)
      group by case when c.result_type in ('CXYE','CXZD','CXZE','CXKCYE') then 21
               else 23 end, a.brand_id, b.phone_id
      union all
      select 22 as busi_code_type, a.brand_id,
         count(1) as flow, b.phone_id
      from  bass2.dw_product_$timestamp a,  (select * from bass2.dw_kf_sms_cmd_receive_dm_$curr_month where op_time = '$op_time') b,  (select * from bass2.dw_kf_cmd_hint_def_dm_$curr_month where op_time = '$op_time') c
      where a.product_no  = b.phone_id and b.cmd_id = c.cmd_id and b.deal_result = c.cmd_result
      and replace(char(c.op_time),'-','') = '$timestamp' and c.result_jieg in (0)
      and c.result_type in (select sms_cmd from bass2.stat_ecustsvc_sms_cmd)
      and a.userstatus_id in (1,2,3,6,8) and a.test_mark <> 1 and a.usertype_id in (1,2,9)
      group by a.brand_id, b.phone_id) a
            group by busi_code_type,brand_id,phone_id
            union all
   select 5 as echl_type,22 as busi_code_type,b.brand_id,b.product_no, count(1) as flow
   from  (select * from bass2.dw_product_ord_cust_dm_$curr_month where op_time = '$op_time') a left join  bass2.dw_product_$timestamp b on a.product_instance_id=b.user_id
   where channel_type='6' and source_system_id in (3) and business_id=193000000001 and old_so_order_id is null
   group by b.brand_id, b.product_no
            union all
            select 6 as echl_type,24 as busi_code_type,a.brand_id,a.product_no,count(1) as flow
            from  bass2.dw_product_$timestamp a
            , (select * from bass2.dw_acct_payitem_dm_$curr_month where op_time = '$op_time') b
            where a.user_id=b.user_id and b.paytype_id in ('4162','4864') and b.rec_sts=0
            group by a.brand_id,a.product_no
            union all
            select 6 as echl_type,22 as busi_code_type,a.brand_id,a.product_no,count(1) as flow
            from  bass2.dw_product_$timestamp a, (select * from bass2.dw_product_busi_dm_$curr_month where op_time = '$op_time') b
            where a.user_id=b.user_id and b.so_mode='5' and b.op_id in (select op_id from  bass2.dim_boss_staff where op_name like '%����%')
            and b.process_id not in (2,3,22,14,24,12,13)
            group by a.brand_id,a.product_no
           )a
      group by echl_type,busi_code_type,case brand_id when 1 then 1 when 4 then 2 else 3 end,product_no ) a ,
	  bass2.dw_product_$timestamp b
	  where  a.busi_code_type    in (23,21)
	        and b.userstatus_id in (1,2,3,6,8)
		     and b.usertype_id in (1,2,9)
             and a.product_no = b.product_no
	   group by b.region_id,
                   case when a.echl_type = 1 then 202
                                       when a.echl_type = 2 then 201
                                       when a.echl_type = 3 then 203
                                       when a.echl_type = 5 then 204
                                       when a.echl_type = 6 then 205
                                       else 206 end
"
exec_sql $sql_buff

#2011-07-30 16:25:18
#10 ��½�ͻ���

	  #��½�û��� �����������ںͶ����ԡ���22067�ԡ�
	  #201��վ
# 2011-08-20  distinct login_product_no �� �� ������Ƭ����Ƭ�������ظ������ܱ�һ���ࡣ
#�����ö����Ĵ��룬��������dw_product_ �����ú���ֱ�ӹ��� ������  a.userstatus_id>0 �ǲ����� , ����ĳ� a.userstatus_id in (1,2,3,6,8)
	  	   set sql_buff "insert into session.stat_market_channel_key_0133_tmp
	  	       select a.region_id,106,201,count(distinct login_product_no) as count
             from  bass2.dw_product_$timestamp a,bass2.dw_wcc_user_action_ds  b
             where b.login_product_no=a.product_no and b.login_type=1 and b.is_success=1
                   and a.userstatus_id in (1,2,3,6,8)
                   and date(b.login_time) = '$op_time'
             group by a.region_id
        "
        exec_sql $sql_buff

	  
	  #202���� 
# 2011-08-20  distinct login_product_no  ͳ�� �ھ�һ����������ͬһ�ͻ����յ�¼������һ������ȥ�ز��Ұ�ȫ���ۼƺ�ȶ�����ȫ��ȥ�ضࡣ

	  	  	   set sql_buff "insert into session.stat_market_channel_key_0133_tmp
	  	       select b.region_id,106,202,count(distinct a.tel_calling)
	  	       from bass2.dw_custsvc_cps_log_call_dm_$curr_month a,
	  	             bass2.dw_product_$timestamp b
	  	        where a.tel_calling = b.product_no 
	  	        and a.OP_TIME = '$op_time'
	  	        and a.tel_called = '10086'
	  	        and b.userstatus_id in (1,2,3,6,8)
	  	       group by b.region_id
        "
        exec_sql $sql_buff

	  
	  #203����
# 2011-08-20  distinct login_product_no  ͳ�� �ھ�һ����������ͬһ�ͻ����յ�¼������һ������ȥ�ز��Ұ�ȫ���ۼƺ�ȶ�����ȫ��ȥ�ضࡣ

	  	  	   set sql_buff "insert into session.stat_market_channel_key_0133_tmp
	  	       select b.region_id,106,203,count(distinct a.phone_id)
	  	       from  bass2.dw_kf_sms_cmd_receive_dm_$curr_month a,
	  	            bass2.dw_product_$timestamp b
	  	        where  a.phone_id = b.product_no 
						and b.userstatus_id in (1,2,3,6,8)
						and date(a.CREATE_DATE) = '$op_time'
	  	       group by b.region_id
        "
        exec_sql $sql_buff

	  
	  #204wap
##����wap ��¼ ������Ƭ�� �� ���� �������ֹ�˾�����Զ���Ϊ0
	  	 set sql_buff "insert into session.stat_market_channel_key_0133_tmp
	  	   select d.region_id,106,204,count(distinct a.product_instance_id)
         from bass2.dw_product_ord_cust_dm_$curr_month a,
                      bass2.dw_product_ord_busi_dm_$curr_month b,
                      bass2.Dwd_product_ord_busi_other_$timestamp  c,
					           bass2.dw_product_$timestamp d
          where a.customer_order_id = b.customer_order_id
                      and b.busioper_order_id =c.busioper_order_id 
                      and c.ext_6 like '%SRC=206%'   
					           and a.product_instance_id = d.user_id    
					           and  date(c.create_date) = '$op_time'
				  group by d.region_id 
        "
        exec_sql $sql_buff

	  
	  #205�����ն�
# 2011-08-20  distinct login_product_no  ͳ�� �ھ�һ����������ͬһ�ͻ����յ�¼������һ������ȥ�ز��Ұ�ȫ���ۼƺ�ȶ�����ȫ��ȥ�ضࡣ

	  	  	 set sql_buff "insert into session.stat_market_channel_key_0133_tmp
	  	   select b.region_id,106,205,count(distinct a.product_instance_id)
         from 
         (select distinct product_instance_id
         from bass2.dw_product_ord_cust_dm_$curr_month a
         where channel_type='9'
         and  date(a.OP_TIME) = '$op_time'
         union
         select distinct user_id as product_instance_id
               from bass2.dw_acct_payitem_dm_$curr_month a
               where paytype_id in ('4162','4864')
             	and  date(a.OP_TIME) = '$op_time'
         ) a
         , bass2.dw_product_$timestamp b
         where a.product_instance_id = b.user_id
         group by  b.region_id 
         with ur 
        "
        exec_sql $sql_buff

	  
 #2011-08-01  �ص���ֵҵ���˶�
 	  #201��վ
	  	   set sql_buff "insert into session.stat_market_channel_key_0133_tmp
				select 'NotFetch' region_id,109,201
				,count(distinct case when a.op_id = 10000475 then a.CUSTOMER_ORDER_ID end)  as count				
				from
				bass2.dw_product_ord_offer_dm_$curr_month a,
				(
					SELECT PRODUCT_ITEM_ID,SUPPLIER_ID,EXTEND_ID2 ,name FROM bass2.DIM_PROD_UP_PRODUCT_ITEM
						WHERE ITEM_TYPE='OFFER_PLAN'
					AND DEL_FLAG='1'
					AND SUPPLIER_ID IS NOT NULL
				)  d
				where 
				d.PRODUCT_ITEM_ID =a.offer_id
				and a.state=3
				and date(a.DONE_DATE)='$op_time'
				and a.offer_id in (113110176435,113110178916,113110179337,113110179338,113110179339,113110181288,113110198228,113110198230, 113090000003, 113090003319,113090003320,113090003321,111090033319, 111090002601,111090002603,111090009284,111090009286,111090009289,111090009317,111099001631, 111090002901, 113110166406, 113110203699, 111090003215,113110180134, 111090001223,111090009160,111090009188,111090009244,111090009282,111090009299,111093301007,111098000030,112093301001,112093301002, 111000000705, 113110175125,113110195329, 111098000044,113110180139,113110197954, 113110156821,113110166390,113110168281,113110178283,113110208633,113110208635, 111090009253,111090009287,111099900633,111099900635,113110174469,113110174473,113110174479,113110174481,113110174482,113110174483,113110174488,113110174643,113110211995,113110212290)
				group by a.region_id
				with ur 
        "
        exec_sql $sql_buff

	  
	  #203����
	  	   set sql_buff "insert into session.stat_market_channel_key_0133_tmp
				select 'NotFetch' region_id,109,203
				,count(distinct case when a.op_id = 10000047 then a.CUSTOMER_ORDER_ID end) as count
				from
				bass2.dw_product_ord_offer_dm_$curr_month a,
				(
					SELECT PRODUCT_ITEM_ID,SUPPLIER_ID,EXTEND_ID2 ,name FROM bass2.DIM_PROD_UP_PRODUCT_ITEM
						WHERE ITEM_TYPE='OFFER_PLAN'
					AND DEL_FLAG='1'
					AND SUPPLIER_ID IS NOT NULL
				)  d
				where 
				d.PRODUCT_ITEM_ID =a.offer_id
				and a.state=3
				and date(a.DONE_DATE)='$op_time'
				and a.offer_id in (113110176435,113110178916,113110179337,113110179338,113110179339,113110181288,113110198228,113110198230, 113090000003, 113090003319,113090003320,113090003321,111090033319, 111090002601,111090002603,111090009284,111090009286,111090009289,111090009317,111099001631, 111090002901, 113110166406, 113110203699, 111090003215,113110180134, 111090001223,111090009160,111090009188,111090009244,111090009282,111090009299,111093301007,111098000030,112093301001,112093301002, 111000000705, 113110175125,113110195329, 111098000044,113110180139,113110197954, 113110156821,113110166390,113110168281,113110178283,113110208633,113110208635, 111090009253,111090009287,111099900633,111099900635,113110174469,113110174473,113110174479,113110174481,113110174482,113110174483,113110174488,113110174643,113110211995,113110212290)
				group by a.region_id
				with ur 
				
        "
        exec_sql $sql_buff
	  
	  #204wap
	  	   set sql_buff "insert into session.stat_market_channel_key_0133_tmp
				select 'NotFetch' region_id,109,204
				,0 as count
				from
				bass2.dw_product_ord_offer_dm_$curr_month a,
				(
					SELECT PRODUCT_ITEM_ID,SUPPLIER_ID,EXTEND_ID2 ,name FROM bass2.DIM_PROD_UP_PRODUCT_ITEM
						WHERE ITEM_TYPE='OFFER_PLAN'
					AND DEL_FLAG='1'
					AND SUPPLIER_ID IS NOT NULL
				)  d
				where 
				d.PRODUCT_ITEM_ID =a.offer_id
				and a.state=3
				and date(a.DONE_DATE)='$op_time'
				and a.offer_id in (113110176435,113110178916,113110179337,113110179338,113110179339,113110181288,113110198228,113110198230, 113090000003, 113090003319,113090003320,113090003321,111090033319, 111090002601,111090002603,111090009284,111090009286,111090009289,111090009317,111099001631, 111090002901, 113110166406, 113110203699, 111090003215,113110180134, 111090001223,111090009160,111090009188,111090009244,111090009282,111090009299,111093301007,111098000030,112093301001,112093301002, 111000000705, 113110175125,113110195329, 111098000044,113110180139,113110197954, 113110156821,113110166390,113110168281,113110178283,113110208633,113110208635, 111090009253,111090009287,111099900633,111099900635,113110174469,113110174473,113110174479,113110174481,113110174482,113110174483,113110174488,113110174643,113110211995,113110212290)
				group by a.region_id
				with ur 
				
        "
        exec_sql $sql_buff	  

	  #205�����ն�
	  	   set sql_buff "insert into session.stat_market_channel_key_0133_tmp
				select 'NotFetch' region_id,109,205
				,count(distinct case when a.op_id = 10100101 then a.CUSTOMER_ORDER_ID end) as count
				from
				bass2.dw_product_ord_offer_dm_$curr_month a,
				(
					SELECT PRODUCT_ITEM_ID,SUPPLIER_ID,EXTEND_ID2 ,name FROM bass2.DIM_PROD_UP_PRODUCT_ITEM
						WHERE ITEM_TYPE='OFFER_PLAN'
					AND DEL_FLAG='1'
					AND SUPPLIER_ID IS NOT NULL
				)  d
				where 
				d.PRODUCT_ITEM_ID =a.offer_id
				and a.state=3
				and date(a.DONE_DATE)='$op_time'
				and a.offer_id in (113110176435,113110178916,113110179337,113110179338,113110179339,113110181288,113110198228,113110198230, 113090000003, 113090003319,113090003320,113090003321,111090033319, 111090002601,111090002603,111090009284,111090009286,111090009289,111090009317,111099001631, 111090002901, 113110166406, 113110203699, 111090003215,113110180134, 111090001223,111090009160,111090009188,111090009244,111090009282,111090009299,111093301007,111098000030,112093301001,112093301002, 111000000705, 113110175125,113110195329, 111098000044,113110180139,113110197954, 113110156821,113110166390,113110168281,113110178283,113110208633,113110208635, 111090009253,111090009287,111099900633,111099900635,113110174469,113110174473,113110174479,113110174481,113110174482,113110174483,113110174488,113110174643,113110211995,113110212290)
				group by a.region_id
				with ur 
				
        "
        exec_sql $sql_buff	  

set sql_buff "
insert into G_S_22066_DAY_STAT
(
         S_INDEX_ID
        ,T_INDEX_ID
        ,VALUE
)
select s_index_id,t_index_id,sum(value) 
from    session.stat_market_channel_key_0133_tmp 
group by s_index_id,t_index_id
order by 1,2
"
exec_sql $sql_buff


set sql_buff "
insert into G_S_22066_DAY
(
         TIME_ID
        ,DEAL_DATE
        ,E_CHANNEL_TYPE
        ,BUSI_REC_CNT
        ,ACTIVATE_CNT
        --,TERM_SALE_CNT
        ,VAL_BUSI_REC_CNT
        ,VAL_BUSI_CANCEL_CNT
        ,CHRG_REC_CNT
        ,CHRG_REC_FEE
        ,ACCESS_CNT
        ,QRY_REC_CNT
        ,CONTRACT_DZ_SALE_ALLCNT
        ,CONTRACT_DZ_SALE_MOBCNT
        ,CONTRACT_FEIDZ_SALE_CNT
        ,CONTRACT_FEIDZ_SALE_MOBCNT
        ,NAKE_DZ_SALE_CNT
        ,NAKE_DZ_SALE_MOBCNT
        ,NAKE_FEIDZ_SALE_CNT
        ,NAKE_FEIDZ_SALE_MOBCNT			
)                           
select 
$timestamp
,'$timestamp'
,case 
when a.T_INDEX_ID = 201 then '01'
when a.T_INDEX_ID = 202 then '02'
when a.T_INDEX_ID = 203 then '03'
when a.T_INDEX_ID = 204 then '04'
when a.T_INDEX_ID = 205 then '05'
else '00'
end E_CHANNEL_TYPE
,char(int(sum(case when a.s_index_id = 101 then value else 0 end ))) BUSI_REC_CNT
,char(int(sum(case when a.s_index_id = 102 then value else 0 end ))) ACTIVATE_CNT
--,char(int(sum(case when a.s_index_id = 103 then value else 0 end ))) TERM_SALE_CNT
,char(int(sum(case when a.s_index_id = 104 then value else 0 end ))) VAL_BUSI_REC_CNT
,char(int(sum(case when a.s_index_id = 109 then value else 0 end ))) VAL_BUSI_CANCEL_CNT
,char(int(sum(case when a.s_index_id = 108 then value else 0 end ))) CHRG_REC_CNT
,char(int(sum(case when a.s_index_id = 105 then value else 0 end ))) CHRG_REC_FEE
,char(int(sum(case when a.s_index_id = 106 then value else 0 end ))) ACCESS_CNT
,char(int(sum(case when a.s_index_id = 107 then value else 0 end ))) QRY_REC_CNT
-- 2011.12.01 1.7.7 ����4���ֶ��޷���ȡ����0 
--~   ,'0' TERM_CONTRACT_SALE_CNT
--~   ,'0' TERM_SINGLE_SALE_CNT
--~   ,'0' TERM_MOBILE_SALE_CNT
--~   ,'0' TERM_UNITE_SALE_CNT
-- 2012.08.01 1.8.2 ����8���ֶ��޷���ȡ����0 
        ,'0'  CONTRACT_DZ_SALE_ALLCNT
        ,'0'  CONTRACT_DZ_SALE_MOBCNT
        ,'0'  CONTRACT_FEIDZ_SALE_CNT
        ,'0'  CONTRACT_FEIDZ_SALE_MOBCNT
        ,'0'  NAKE_DZ_SALE_CNT
        ,'0'  NAKE_DZ_SALE_MOBCNT
        ,'0'  NAKE_FEIDZ_SALE_CNT
        ,'0'  NAKE_FEIDZ_SALE_MOBCNT		
from G_S_22066_DAY_stat a
where a.T_INDEX_ID in (201,202,203,204,205)
group by 
case 
when a.T_INDEX_ID = 201 then '01'
when a.T_INDEX_ID = 202 then '02'
when a.T_INDEX_ID = 203 then '03'
when a.T_INDEX_ID = 204 then '04'
when a.T_INDEX_ID = 205 then '05'
else '00'
end
"
exec_sql $sql_buff


  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_S_22066_DAY"
  set pk   "DEAL_DATE||E_CHANNEL_TYPE"
        chkpkunique ${tabname} ${pk} ${timestamp}
        #
  aidb_runstats bass1.G_S_22066_DAY 3

return 0  
}

#T_INDEX_ID	T_NAME	T_INDEX_NAME	SEQ
#201	��������	��վ	1
#202	��������	����	2
#203	��������	����	3
#204	��������	WAP	4
#205	��������	�����ն�	5
#206	��������	����	6
#207	ʵ������	����Ӫҵ��	7
#208	ʵ������	ָ��רӪ��	8
#209	ʵ������	��Լ�����	9
#210	ʵ������	����	10
#211	ȫ����	ȫ����	11


#S_INDEX_ID	S_INDEX_NAME	SEQ
#101	1��ҵ�������(������ֵ���ѣ���λ����)	1
#102	&nbsp;&nbsp;&nbsp;&nbsp;������ 	2
#103	&nbsp;&nbsp;&nbsp;&nbsp;�ն��� 	3
#104	&nbsp;&nbsp;&nbsp;&nbsp;��ֵҵ�������	4
#105	2����ֵ���ѽ��(��λ��Ԫ)	5
#106	3����½�û���	6
#107	4��ҵ���ѯ��(��)	7
#108	2����ֵ���ѱ���	5
#109	&nbsp;&nbsp;&nbsp;&nbsp;��ֵҵ���˶���	4


#���ֶν�ȡ���·��ࣺ
#01:�Ż���վ
#02:10086�绰Ӫҵ��
#03: ����Ӫҵ��
#04:WAP��վ
#05:�����նˣ��������е������նˣ�������ʵ��������24СʱӪҵ���ڲ��ŵ������նˣ��������̳��ȳ��������ڷŵ������նˡ���

  #Insert Data Into stat_ecustsvc_brand_tmp
  #parent_ob_id
  #21:���Ѳ�ѯ
  #22:ҵ������������ѣ�
  #23:���ֲ�ѯ
  #24:����Ͷ��
  #25:��վ(��½)
  #26:��վ����
  #27:��������pageview��
  #28:�ͻ�ƽ�����ҳ����
  #29:��������µ�½
  #30:���Ͻɷ�
  #is_success
  #0:�ǳɹ�
  #1:�ɹ�
  #2:ȫ��
  