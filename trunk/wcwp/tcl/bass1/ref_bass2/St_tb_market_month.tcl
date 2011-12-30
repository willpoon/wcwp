#======================================================================================
#��Ȩ������Copyright (c) 2011,AsiaInfo.Report.System
#��������: St_tb_market_month.tcl
#������: �г���Ӫ���-ҵ������-�г�/����ָ��(��������ָ��)�½ӿ���������
#����Ŀ��: ��
#����ָ��: �������
#����ά��: ������/����
#��������: ��
#����ʾ��: crt_basetab -SSt_tb_market_month.tcl -D2011-06-01
#����ʱ��: 2011-7-13
#�� �� ��: AsiaInfo	Liwei
#��������: ��
#�޸���ʷ: 1.2011-7-20 By Liwei ����������ָ��
#          2.2011-7-28 By Liwei ������������˵��
#          3.2011-8-2 By Liwei ���Ƴ���(ȥ�����ע��)��
#          4.2011-8-4 By Liwei �޸�CORP_USER_002/CORP_USER_003ͳ�ƿھ�(ԭ�����ռ���/���ų�Ա��,������Ҫ��������)
#ע������: 1.�����������(���ǰ��)����ʱ,ע��ע�͵���Ӧ�Ĵ����,��������:
#            MK:MARKET_USER_016	����ͻ�������/MARKET_BV_001	��������ҵ��ռ��
#            Jk:CORP_REVE_002	������Ϣ�������(�������ش����-��2011��ǰ/��ھ�)
#            ���ռ��ſͻ�/���ų�Ա��- ÿ���ֹ�¼����������
#            CORP_BV_001:��201005��KPI��,���ų�ԱMOU�ھ��ű��Ϊ��ȷ,201001-201004��������ͳ��
#=======================================================================================
 proc deal {p_optime p_timestamp} {

 	   global conn
 	   global handle

 	   if [catch {set handle [aidb_open $conn]} errmsg] {
 	   	trace_sql $errmsg 1000
 	   	return -1
 	   }

 	   #I.�г���
 	   puts "I.�г���Beginnig..."
 	   if {[St_tb_market_month_mk $p_optime]  != 0} {
 	   	  aidb_roll $conn
 	   	  aidb_close $handle
 	   	 return -1
 	   }
 	   #II.������
 	   puts "II.������Beginnig..."
 	   if {[St_tb_market_month_jk $p_optime]  != 0} {
 	   	  aidb_roll $conn
 	   	  aidb_close $handle
 	   	 return -1
 	   }
 	   aidb_commit $conn
 	   aidb_close $handle

 	   return 0
 }

 proc St_tb_market_month_mk {p_optime} {
 	   global conn
 	   global handle

 	   source	report.cfg

 	   set	  date_optime	[ai_to_date $p_optime]
 	   scan   $p_optime "%04s-%02s-%02s" year month day

	   aidb_close $handle
	   if [catch {set handle [aidb_open $conn]} errmsg] {
	   	      trace_sql $errmsg 1100
	   	      return -1
	   }
	   #רӪ�����
	   set channel_type "90881"

     #���ڴ���
     set lastmonth [GetLastMonth $year$month]
     puts $lastmonth
     set  last_month  [string range [clock format [ clock scan "${year}${month}${day} - 1 month" ] -format "%Y-%m-%d"] 0 9]
     puts $last_month
     set  next_month_day  [string range [clock format [ clock scan "${year}${month}${day} + 1 month" ] -format "%Y-%m-%d"] 0 9]
     puts $next_month_day

     #Դ��
     set   kpi_values                          "bass2.kpi_values"
     set   stat_acct_0011                      "bass2.stat_acct_0011"
     set   stat_market_0076                    "bass2.stat_market_0076"

     set   dw_product_td_yyyymm                "bass2.dw_product_td_$year$month"
     set   dw_product_yyyymm                   "bass2.dw_product_$year$month"
     set   dw_acct_bad_yyyymm                  "bass2.dw_acct_bad_$year$month"
     set   dw_acct_owe_yyyymm                  "bass2.dw_acct_owe_$year$month"
     set   dw_product_ord_cust_dm_yyyymm       "bass2.dw_product_ord_cust_dm_$year$month"
     set   dw_product_ins_off_ins_prod_yyyymm  "bass2.dw_product_ins_off_ins_prod_$year$month"
     set   dw_acct_payitem_yyyymm              "bass2.dw_acct_payitem_$year$month"
     set   dw_res_msisdn_yyyymm                "bass2.dw_res_msisdn_$year$month"
     set   dw_product_ins_off_ins_prod_yyyymm1 "bass2.dw_product_ins_off_ins_prod_$lastmonth"
     set   dim_channel_info                    "bass2.dim_channel_info"

     #ά��
     set   city_lkp                            "bass2.dim_pub_city"
     set   dim_tb_market_define                "bass2.dim_tb_market_define"

     #�����
     set   St_tb_market_month                  "bass2.St_tb_market_month"

     #Step1.������ʱ�����
	   set sql_buf "declare global temporary table session.St_tb_market_month_tmp (
                   item_id                  VARCHAR(20),
                   area_code                integer,
                   item_value               decimal(18,4)
        )
        partitioning key
        	(item_id,area_code
        	) using hashing
        with replace on commit preserve rows not logged in tbs_user_temp"

		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
		 	trace_sql $errmsg 1300
		 	puts "errmsg:$errmsg"
		 	return -1
		 }
     #Step2.��ʼ����ʱ�����
     set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select b.item_id,int(a.city_id),0
         from $city_lkp a,$dim_tb_market_define b
         where b.item_id like 'MARKET%'
         "
         puts $sql_buf

		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
		 	trace_sql $errmsg 1300
		 	puts "errmsg:$errmsg"
		 	return -1
		 }
     #Step3.����ʱ������в���*ֱ�ӻ�ȡ��*����
     #��Դ<KPI>-------------------->
     #MARKET_REVE_001	��Ӫ���루��Ԫ��
     #MARKET_REVE_003	��������
     #MARKET_REVE_004	ARPU
     #MARKET_USER_004	�ƶ������û���
     #MARKET_USER_005	���ŵ����û���
     #MARKET_USER_006	��ͨ�����û���
	   #MARKET_USER_007	TD�ͻ�������
     #MARKET_USER_010	�����ͻ���
     #MARKET_USER_013	��û���
     #MARKET_BV_003	�Ʒ�ʱ��
     #MARKET_USER_012	������
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select case when kpi_id=2701 then 'MARKET_REVE_001'
                     when kpi_id=2731 then 'MARKET_REVE_003'
                     when kpi_id=2713 then 'MARKET_REVE_004'
                     when kpi_id=6003 then 'MARKET_USER_004'
					           when kpi_id in (3102,3105,3146) then 'MARKET_USER_005'
					           when kpi_id in (3104,3154) then 'MARKET_USER_006'
                     when kpi_id=3301 then 'MARKET_USER_007'
                     when kpi_id=2605 then 'MARKET_USER_010'
                     when kpi_id=2602 then 'MARKET_USER_013'
                     when kpi_id=2809 then 'MARKET_BV_003'
                     when kpi_id=2606 then 'MARKET_USER_012'
                end,city_id,
                case when kpi_id=2701 then round(1.0*kpi_value/10000,4)
                     else kpi_value
                end
         from $kpi_values
         where kpi_type=2 and brand_id=104 and kpi_date=$date_optime
            and kpi_id in (2701,2731,2713,6003,3102,3105,3146,3104,3154,3301,2605,2602,2809,2606)
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #MARKET_REVE_002 ����ҵ���������루��Ԫ��
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'MARKET_REVE_002',city_id,round(1.0*sum(income)/10000,4)
         from $stat_acct_0011
         where STAT_ACCT_0011_S_ID in (34,46,61) and STAT_ACCT_0011_T_ID=101 and time_id=$year$month
         group by city_id
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #MARKET_USER_009	�����ͻ���
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'MARKET_USER_009',a.area_code,sum(a.kpi_value)
         from (select city_id as area_code,(-kpi_value) as kpi_value from $kpi_values
               where kpi_type=2 and brand_id=104 and kpi_id in (6003) and kpi_date in ('$last_month')
               union all
               select area_code,sum(item_value) as kpi_value from session.St_tb_market_month_tmp
               where item_id='MARKET_USER_004' group by area_code
              )a
         group by a.area_code
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #MARKET_USER_014	�и߶˿ͻ���
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'MARKET_USER_014',case when grouping(int(city_id))=1 then 890 else int(city_id) end,count(distinct user_id)
         from $dw_product_yyyymm
         where userstatus_id in ($rep_online_userstatus_id) and usertype_id in ($rep_fact_usertype_id) and test_mark <> 1
            and snapshot_mark = 1
         group by cube(int(city_id))
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

	   #MARKET_USER_017	����TD�ͻ���������
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'MARKET_USER_017',a.area_code,sum(a.kpi_value)
         from (select city_id as area_code,(-kpi_value) as kpi_value from $kpi_values
               where kpi_type=2 and brand_id=104 and kpi_id in (3301) and kpi_date in ('$last_month')
               union all
               select area_code,sum(item_value) as kpi_value from session.St_tb_market_month_tmp
               where item_id='MARKET_USER_007' group by area_code
              )a
         group by a.area_code
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
     #Step4.����ʱ������в���*��ֱ�ӻ�ȡ(ͳ��)��*����
     #MARKET_REVE_006	���˽��(���г����¶Ⱦ��ֻ���ھ���-����"Զ����Ŀ"(80000618	ICTҵ�񼯳ɷ�����))
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'MARKET_REVE_006',case when grouping(int(city_id))=1 then 890 else int(city_id) end,sum(fee)
         from (select city_id,sum(result) as fee    from $stat_market_0076
               where time_id=$year$month and flag=0 and city_id<>'890'
               group by city_id
               union all
               select city_id,-sum(unpay_fee) as fee from $dw_acct_owe_yyyymm
               where USER_ID='89160000444549' group by city_id
              )a
         group by cube(int(city_id))
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #MARKET_USER_008	TD��Ծ�ͻ���
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'MARKET_USER_008',case when grouping(int(city_id))=1 then 890 else int(city_id) end,count(distinct user_id)
         from $dw_product_td_yyyymm
         where td_user_mark=1 and month_active_mark=1
         group by cube(int(city_id))
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
     #MARKET_USER_016	����ͻ�������
     #1.��Ӻ�ͳ�ƿھ�
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'MARKET_USER_016',case when grouping(int(region_id))=1 then 890 else int(region_id) end,
             count(distinct product_instance_id)
         from (select region_id,product_instance_id from $dw_product_ins_off_ins_prod_yyyymm
               where offer_id in (111000044444,111000055555,111000066666,113200000080,113090030001,111089250004,111089150004,111089650004,111089350004,111089750004,111089450004,111089550004) and date(expire_date)>='$next_month_day'
               except
               select region_id,product_instance_id from $dw_product_ins_off_ins_prod_yyyymm1
               where offer_id in (111000044444,111000055555,111000066666,113200000080,113090030001,111089250004,111089150004,111089650004,111089350004,111089750004,111089450004,111089550004) and date(expire_date)>=$date_optime
              )a
         group by cube(int(region_id))
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
     ##2.���ǰͳ�ƿھ�
     #set   dw_product_yyyymm1                  "bass2.dw_product_$lastmonth"
	   #set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
     #    select 'MARKET_USER_016',case when grouping(int(city_id))=1 then 890 else int(city_id) end,
     #        count(distinct user_id)
     #    from (select city_id,user_id from $dw_product_yyyymm
     #          where plan_id in (89150004,89250004,89350004,89450004,89550004,89650004,89750004,55555,44444,66666)
	   #             and userstatus_id in ($rep_online_userstatus_id) and usertype_id in ($rep_fact_usertype_id)
	   #          except
	   #          select city_id,user_id from $dw_product_yyyymm1
     #          where plan_id in (89150004,89250004,89350004,89450004,89550004,89650004,89750004,55555,44444,66666)
	   #             and userstatus_id in ($rep_online_userstatus_id) and usertype_id in ($rep_fact_usertype_id)
	   #         )a
	   #    group by cube(int(city_id))
     #    "
     #    puts $sql_buf
     #
     #if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   #	     trace_sql $errmsg 1300
	   #	     puts "errmsg:$errmsg"
	   #	     return -1
	   #}

     #Step6.����ʱ������в���*ռ����*����
     #MARKET_REVE_007	������
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'MARKET_REVE_007',a.area_code,case when b.value=0 then 0 else round(double(a.value)/(b.value*10000),4) end
         from (select area_code,sum(item_value) as value from session.St_tb_market_month_tmp
               where item_id='MARKET_REVE_006' group by area_code
              )a,
              (select area_code,sum(item_value) as value from session.St_tb_market_month_tmp
               where item_id='MARKET_REVE_001' group by area_code
              )b
         where a.area_code=b.area_code
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #MARKET_USER_001	�ƶ��г��ݶ�
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'MARKET_USER_001',a.area_code,case when b.value=0 then 0 else round(double(a.value)/b.value,4) end
         from (select area_code,sum(item_value) as value from session.St_tb_market_month_tmp
               where item_id='MARKET_USER_004' group by area_code
              )a,
              (select area_code,sum(item_value) as value from session.St_tb_market_month_tmp
               where item_id in ('MARKET_USER_004','MARKET_USER_005','MARKET_USER_006') group by area_code
              )b
         where a.area_code=b.area_code
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
     #MARKET_USER_002	�����г��ݶ�
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'MARKET_USER_002',a.area_code,case when b.value=0 then 0 else round(double(a.value)/b.value,4) end
         from (select area_code,sum(item_value) as value from session.St_tb_market_month_tmp
               where item_id='MARKET_USER_005' group by area_code
              )a,
              (select area_code,sum(item_value) as value from session.St_tb_market_month_tmp
               where item_id in ('MARKET_USER_004','MARKET_USER_005','MARKET_USER_006') group by area_code
              )b
         where a.area_code=b.area_code
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
     #MARKET_USER_003	��ͨ�г��ݶ�
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'MARKET_USER_003',a.area_code,1-a.value
         from (select area_code,sum(item_value) as value from session.St_tb_market_month_tmp
               where item_id in ('MARKET_USER_001','MARKET_USER_002') group by area_code
              )a
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #MARKET_USER_011	����������
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'MARKET_USER_011',a.area_code,case when b.value=0 then 0 else round(double(a.value)/b.value,4) end
         from (select area_code,sum(item_value) as value from session.St_tb_market_month_tmp
               where item_id='MARKET_USER_010' group by area_code
              )a,
              (select area_code,sum(item_value) as value from session.St_tb_market_month_tmp
               where item_id in ('MARKET_USER_009') group by area_code
              )b
         where a.area_code=b.area_code
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #MARKET_USER_015	�и߶��û�ռ��
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'MARKET_USER_015',a.area_code,case when b.value=0 then 0 else round(double(a.value)/b.value,4) end
         from (select area_code,sum(item_value) as value from session.St_tb_market_month_tmp
               where item_id='MARKET_USER_014' group by area_code
              )a,
              (select area_code,sum(item_value) as value from session.St_tb_market_month_tmp
               where item_id in ('MARKET_USER_013') group by area_code
              )b
         where a.area_code=b.area_code
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #MARKET_BV_004	MOU
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'MARKET_BV_004',a.area_code,case when b.value=0 then 0 else round(double(a.value)/b.value,4) end
         from (select area_code,sum(item_value) as value from session.St_tb_market_month_tmp
               where item_id='MARKET_BV_003' group by area_code
              )a,
              (select area_code,sum(item_value) as value from session.St_tb_market_month_tmp
               where item_id in ('MARKET_USER_004') group by area_code
              )b
         where a.area_code=b.area_code
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
     #MARKET_BV_001	��������ҵ��ռ��
     #1.��Ӻ�ͳ�ƿھ�
	   set sql_buf "declare global temporary table session.St_tb_market_month_payfee_tmp (
                   area_code                integer,
                   e_deal_cnt               integer,
                   p_deal_cnt               integer
        )
        partitioning key
        	(area_code
        	) using hashing
        with replace on commit preserve rows not logged in tbs_user_temp"

		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
		 	trace_sql $errmsg 1300
		 	puts "errmsg:$errmsg"
		 	return -1
		 }
		 #�����ɷ���
	   set sql_buf "insert into session.St_tb_market_month_payfee_tmp (area_code,e_deal_cnt,p_deal_cnt)
	       select int(region_id),sum(case when channel_type=1 then cnt end) as value1,sum(cnt) as value2
	       from (select region_id,case when channel_type in ('e','4','5','6','9','B') then 1 ELSE 2 END channel_type,
                      count(1) as cnt
               from $dw_product_ord_cust_dm_yyyymm where old_so_order_id is null
               group by region_id,case when channel_type in ('e','4','5','6','9','B') then 1 else 2 end
              )a
         group by int(region_id)
	      "
         puts $sql_buf

		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
		 	trace_sql $errmsg 1300
		 	puts "errmsg:$errmsg"
		 	return -1
		 }
		 #�ɷ���
	   set sql_buf "insert into session.St_tb_market_month_payfee_tmp (area_code,e_deal_cnt,p_deal_cnt)
	       select int(so_city_id),sum(case when pay_type=1 then cnt end) as value1,sum(cnt) as value2
	       from (select so_city_id,case when paytype_id in ('4162','4864','4468') then 1 ELSE 2 END pay_type,
                      count(1) as cnt
               from $dw_acct_payitem_yyyymm
               where rec_sts=0 and so_city_id in ('891','892','893','894','895','896','897')
               group by so_city_id,case when paytype_id in ('4162','4864','4468') then 1 ELSE 2 END
              )a
         group by int(so_city_id)
	      "
         puts $sql_buf

		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
		 	trace_sql $errmsg 1300
		 	puts "errmsg:$errmsg"
		 	return -1
		 }
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
	       select 'MARKET_BV_001',a.area_code,case when a.value2=0 then 0 else round(double(a.value1)/a.value2,4) end
	       from (select case when grouping(area_code)=1 then 890 else area_code end as area_code,
	                sum(e_deal_cnt) as value1,sum(p_deal_cnt) as value2
	             from session.St_tb_market_month_payfee_tmp
	             group by cube(area_code)
              )a
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
     ##2.���ǰͳ�ƿھ�
	   #set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
     #    select 'MARKET_BV_001',int(city_id),round(100.0*eresult1/(presult1+presult2+eresult1+eresult2),4)
     #    from bass2.stat_ecustsvc_0022 where time_id=$date_optime and region_id='0'
     #    "
     #    puts $sql_buf
     #
     #if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   #	     trace_sql $errmsg 1300
	   #	     puts "errmsg:$errmsg"
	   #	     return -1
	   #}

     #MARKET_BV_002	ʵ��������Ծ����������
     #TYPE:
     #1-�źŻ�Ծ��������
     #2-�ɷѻ�Ծ��������
     #0-ָ��רӪ������
	   set sql_buf "declare global temporary table session.St_tb_market_month_chl_tmp (
                   type                     smallint,
                   area_code                integer,
                   chl_cnt                  integer
        )
        partitioning key
        	(type,area_code
        	) using hashing
        with replace on commit preserve rows not logged in tbs_user_temp"

		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
		 	trace_sql $errmsg 1300
		 	puts "errmsg:$errmsg"
		 	return -1
		 }
	   set sql_buf "declare global temporary table session.St_tb_market_month_chl_id_tmp (
                   type                     smallint,
                   area_code                integer,
                   channel_id               bigint
        )
        partitioning key
        	(type,area_code
        	) using hashing
        with replace on commit preserve rows not logged in tbs_user_temp"

		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
		 	trace_sql $errmsg 1300
		 	puts "errmsg:$errmsg"
		 	return -1
		 }
		 #�źŻ�Ծ����
     set sql_buf "insert into session.St_tb_market_month_chl_id_tmp (type,area_code,channel_id)
         select 1,int(city_id),channel_id
         from $dw_res_msisdn_yyyymm where sts=3 and wholesale_date>=$date_optime and wholesale_date<'$next_month_day'
         group by int(city_id),channel_id
         having count(distinct sim_id)>31
         "
         puts $sql_buf
     if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 1300
       puts "errmsg:$errmsg"
       return -1
     }
     #�ɷѻ�Ծ����
     set sql_buf "insert into session.St_tb_market_month_chl_id_tmp (type,area_code,channel_id)
         select 2,int(so_city_id),so_channel_id
         from $dw_acct_payitem_yyyymm
         where rec_sts=0 and so_city_id in ('891','892','893','894','895','896','897')
         group by int(so_city_id),so_channel_id
         having sum(recv_cash)>=20000
         "
         puts $sql_buf
     if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 1300
       puts "errmsg:$errmsg"
       return -1
     }
		 #�ź�/�ɷѻ�Ծ��������
     set sql_buf "insert into session.St_tb_market_month_chl_tmp (type,area_code,chl_cnt)
         select a.type,case when grouping(a.area_code)=1 then 890 else a.area_code end,count(1)
         from (select a.type,a.area_code,a.channel_id
               from session.St_tb_market_month_chl_id_tmp a
               where a.channel_id in (select channel_id
                                      from $dim_channel_info
                                      where state=0 and channel_type in ($channel_type) and date(create_date)<'$next_month_day'
                                      group by channel_id
                                     )
              )a
         group by a.type,cube(a.area_code)
         "
         puts $sql_buf
     if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 1300
       puts "errmsg:$errmsg"
       return -1
     }
     #ָ��רӪ������
     set sql_buf "insert into session.St_tb_market_month_chl_tmp (type,area_code,chl_cnt)
         select 0,case when grouping(int(region_code))=1 then 890 else int(region_code) end,count(1)
         from $dim_channel_info
         where state=0 and channel_type in ($channel_type) and date(create_date)<'$next_month_day'
         group by cube(int(region_code))
         "
         puts $sql_buf
     if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 1300
       puts "errmsg:$errmsg"
       return -1
     }
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'MARKET_BV_002',a.area_code,case when b.value=0 then 0 else round(double(a.value)/b.value,4) end
         from (select area_code,chl_cnt as value from session.St_tb_market_month_chl_tmp
               where type<>0
              )a,
              (select area_code,chl_cnt*2 as value from session.St_tb_market_month_chl_tmp
               where type=0
              )b
         where a.area_code=b.area_code
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }


     #Step6.���������е�������
	   set sql_buf "delete from $St_tb_market_month where time_id = $year$month AND item_type='MK'"
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

     #Step7.��ʱ���н����������
	   set sql_buf "insert into $St_tb_market_month (item_type,item_id,area_code,time_id,item_value)
         select 'MK',
               item_id,
               area_code,
               $year$month,
               sum(item_value)
         from session.St_tb_market_month_tmp
         group by item_id,
               area_code
        "
        puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #MARKET_USER_012	������
	   set sql_buf "update $St_tb_market_month a
	       set item_value=(select case when sum(b.item_value)=0 then 0 else round(double(a.item_value)/sum(b.item_value),4) end
	                       from session.St_tb_market_month_tmp b
	                       where a.area_code=b.area_code and b.item_id='MARKET_USER_004'
	                       )
	       where a.item_id='MARKET_USER_012' and a.time_id=$year$month
	      "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

 	   puts "I.�г���Ended..."

	return 0
}

 proc St_tb_market_month_jk {p_optime} {
 	   global conn
 	   global handle

 	   source	report.cfg

 	   set	  date_optime	[ai_to_date $p_optime]
 	   scan   $p_optime "%04s-%02s-%02s" year month day

	   aidb_close $handle
	   if [catch {set handle [aidb_open $conn]} errmsg] {
	   	      trace_sql $errmsg 1100
	   	      return -1
	   }

	   #���Լ���
	   set enterprise_test_list "'89100000000682','89100000000659','89100000000656','89100000000651'"

     #���ڴ���
     set lastyear [expr $year -1 ]
     puts $lastyear
     set lastmonth [GetLastMonth $year$month]
     puts $lastmonth
     set  last_month  [string range [clock format [ clock scan "${year}${month}${day} - 1 month" ] -format "%Y-%m-%d"] 0 9]
     puts $last_month
     set  next_month_day  [string range [clock format [ clock scan "${year}${month}${day} + 1 month" ] -format "%Y-%m-%d"] 0 9]
     puts $next_month_day

     #Դ��
     set   kpi_values                          "bass2.kpi_values"
     set   stat_enterprise_0055_b              "bass2.stat_enterprise_0055_b"
     set   stat_enterprise_0053                "bass2.stat_enterprise_0053"
     set   dw_enterprise_member_mid_yyyymm     "bass2.dw_enterprise_member_mid_$year$month"
     set   dw_enterprise_member_mid_yyyymm1    "bass2.dw_enterprise_member_mid_$lastmonth"
     set   dw_product_yyyymm                   "bass2.dw_product_$year$month"
     set   dw_product_ins_off_ins_prod_yyyymm  "bass2.dw_product_ins_off_ins_prod_$year$month"
     set   dw_enterprise_new_unipay_yyyymm     "bass2.dw_enterprise_new_unipay_$year$month"
     set   dw_enterprise_msg_yyyymm            "bass2.dw_enterprise_msg_$year$month"
     set   dw_product_sprom_yyyymm             "bass2.dw_product_sprom_$year$month"

     #set   dw_enterprise_snapshot_yyyy         "bass2.dw_enterprise_snapshot_$year"
     #set   dw_enterprise_member_snapshot_yyyy  "bass2.dw_enterprise_member_snapshot_$year"

     set   stat_enterprise_0054_ent_snapshot   "bass2.stat_enterprise_0054_ent_snapshot"
     set   stat_enterprise_0054_mem_snapshot   "bass2.stat_enterprise_0054_mem_snapshot"

     #ά��
     set   city_lkp                            "bass2.dim_pub_city"
     set   dim_tb_market_define                "bass2.dim_tb_market_define"

     #�����
     set   St_tb_market_month                  "bass2.St_tb_market_month"

     #Step1.������ʱ�����
	   set sql_buf "declare global temporary table session.St_tb_market_month_tmp (
                   item_id                  VARCHAR(20),
                   area_code                integer,
                   item_value               decimal(18,4)
        )
        partitioning key
        	(item_id,area_code
        	) using hashing
        with replace on commit preserve rows not logged in tbs_user_temp"

		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
		 	trace_sql $errmsg 1300
		 	puts "errmsg:$errmsg"
		 	return -1
		 }
     #Step2.��ʼ����ʱ�����
     set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select b.item_id,int(a.city_id),0
         from $city_lkp a,$dim_tb_market_define b
         where b.item_id like 'CORP%'
         "
         puts $sql_buf

		 if [catch {aidb_sql $handle $sql_buf} errmsg] {
		 	trace_sql $errmsg 1300
		 	puts "errmsg:$errmsg"
		 	return -1
		 }
     #Step3.����ʱ������в���*ֱ�ӻ�ȡ��*����
     #3-1.��Դ<KPI>
     #CORP_REVE_001	���ſͻ���������
     #CORP_REVE_003	���ų�ԱARPU
     #CORP_USER_004	���ſͻ�������(KPI��)
     #CORP_USER_005	���ſͻ���Ա������
     #CORP_USER_006	���ų�Ա������
     #CORP_USER_008	���ų�Ա������
     #CORP_BV_001	���ų�ԱMOU
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select case when kpi_id=3010 then 'CORP_REVE_001'
                     when kpi_id=3015 then 'CORP_REVE_003'
                     when kpi_id=3006 then 'CORP_USER_005'
                     when kpi_id=3005 then 'CORP_USER_006'
                     when kpi_id=3007 then 'CORP_USER_008'
                     when kpi_id=3036 then 'CORP_BV_001'
                end,city_id,kpi_value
         from $kpi_values
         where kpi_type=2 and brand_id=104 and kpi_date=$date_optime
            and kpi_id in (3010,3015,3006,3005,3007,3036)
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'CORP_USER_004',case when grouping(int(ent_city_id))=1 then 890 else int(ent_city_id) end
            ,count(distinct enterprise_id)
         from $dw_enterprise_msg_yyyymm
         where ent_status_id = 0 and enterprise_id not in ($enterprise_test_list)
         group by cube(int(ent_city_id))
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

	   #3-2.2011��ͳ�ƿھ�
	   #3-2-1.CORP_REVE_002	������Ϣ������
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'CORP_REVE_002',case when int(city_id)=888 then 891 else int(city_id) end,sum(RESULT)
         from $stat_enterprise_0055_b
         where op_time=$year$month and s_index_id in (18)
         group by case when int(city_id)=888 then 891 else int(city_id) end
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #3-2-2.CORP_USER_007	���ų�Ա��ʧ��
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
	       select 'CORP_USER_007',case when grouping(a.city_id)=1 then 890 else a.city_id end,sum(a.cnt)
	       from (select value(int(b.ent_city_id),int(d.ent_city_id)) as city_id,count(distinct b.user_id) as cnt
               from $dw_enterprise_member_mid_yyyymm1 b
                    left join $dw_enterprise_member_mid_yyyymm d on b.user_id = d.user_id,
                    $dw_product_yyyymm c
               where c.user_id = b.user_id and d.user_id is null
                  and b.dummy_mark = $rep_false and c.month_off_mark = $rep_false
                  and b.enterprise_id not in ($enterprise_test_list)
               group by value(int(b.ent_city_id),int(d.ent_city_id))
              )a
         group by cube(city_id)
        "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #3-2-3.CORP_BV_002	����ר�ߴﵽ��
	   #3-2-4.CORP_BV_003	����ר������
	   #3-2-5.CORP_BV_004	M2M�ն˵�����
     #3-2-6.CORP_BV_005	M2M����
     #3-2-7.CORP_BV_006	����ͨ���·�չ�ն���
     #3-2-7.CORP_BV_007	����ͨ����
     #3-2-8.CORP_BV_008	���Ų���ͳ������
     #3-2-9.CORP_BV_009	���Ų�������
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select item_id,
                case when grouping(int(region_id))=1 then 890 else int(region_id) end,
                count(distinct cust_party_role_id)
         from (select case when offer_id in (112091201002,112091201001) then 'CORP_BV_002'
                         else 'CORP_BV_004'
                      end as item_id,region_id,cust_party_role_id
               from $dw_product_ins_off_ins_prod_yyyymm
               where offer_id in (112091201002,112091201001,112094600001,112094900001,112094700001,112094401001,112001005702,112001005701,112091601001)
                   and date(expire_date)>='$next_month_day'
              )a
         group by item_id,cube(int(region_id))
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'CORP_BV_006',
                case when grouping(int(region_id))=1 then 890 else int(region_id) end,
                count(1)
         from $dw_product_ins_off_ins_prod_yyyymm
         where offer_id in (112001005702,112001005701) and CREATE_DATE>=$date_optime and CREATE_DATE<'$next_month_day'
            and date(expire_date)>='$next_month_day'
         group by cube(int(region_id))
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select case when service_id in (912,912001,912002) then 'CORP_BV_003'
                     when service_id=942 then 'CORP_BV_007'
                     when service_id=933 then 'CORP_BV_009'
                     else 'CORP_BV_005'
                end,
                case when grouping(int(city_id))=1 then 890 else int(city_id) end,
                sum(UNIPAY_FEE+NON_UNIPAY_FEE)
         from $dw_enterprise_new_unipay_yyyymm a
         where service_id in (912,912001,912002,942,933,949,942,946,944,947,916)
         group by case when service_id in (912,912001,912002) then 'CORP_BV_003'
                     when service_id=942 then 'CORP_BV_007'
                     when service_id=933 then 'CORP_BV_009'
                     else 'CORP_BV_005'
                end,cube(int(city_id))
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'CORP_BV_008',
                case when grouping(int(city_id))=1 then 890 else int(city_id) end,
                sum(UNIPAY_FEE)
         from $dw_enterprise_new_unipay_yyyymm a
         where service_id in (933)
         group by cube(int(city_id))
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   ##2011ǰͳ�ƿھ�
     #set stat_enterprise_0032                  "bass2.stat_enterprise_0032_2010"
     #set stat_enterprise_0035                  "bass2.stat_enterprise_0035_2010"
     #set dw_enterprise_sub_yyyymm              "bass2.dw_enterprise_sub_$year$month"
	   #set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
     #    select case when s_index_id in ('J070') then 'CORP_REVE_002'
     #                when s_index_id in ('J052') then 'CORP_USER_007'
     #           end,case when city_id=888 then 891 else city_id end,sum(RESULT)
     #    from $stat_enterprise_0032
     #    where time_id=$year$month and s_index_id in ('J070','J052')
     #    group by case when s_index_id in ('J070') then 'CORP_REVE_002'
     #                when s_index_id in ('J052') then 'CORP_USER_007'
     #           end,case when city_id=888 then 891 else city_id end
     #    "
     #    puts $sql_buf
     #
     #if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   #	     trace_sql $errmsg 1300
	   #	     puts "errmsg:$errmsg"
	   # 	     return -1
	   #}
	   #set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
     #    select case when s_index_id in ('C151','C155') then 'CORP_BV_002'
     #                when s_index_id in ('C153','C157') then 'CORP_BV_003'
     #                when s_index_id in ('C076')        then 'CORP_BV_004'
     #                when s_index_id in ('C094')        then 'CORP_BV_005'
     #                when s_index_id in ('C105')        then 'CORP_BV_007'
     #                when s_index_id in ('C014')        then 'CORP_BV_008'
     #                when s_index_id in ('C013')        then 'CORP_BV_009'
     #           end,case when city_id=888 then 891 else city_id end,sum(RESULT)
     #    from $stat_enterprise_0035
     #    where time_id=$year$month and s_index_id in ('C151','C155','C153','C157','C076','C094','C105','C014','C013')
     #    group by case when s_index_id in ('C151','C155') then 'CORP_BV_002'
     #                when s_index_id in ('C153','C157') then 'CORP_BV_003'
     #                when s_index_id in ('C076')        then 'CORP_BV_004'
     #                when s_index_id in ('C094')        then 'CORP_BV_005'
     #                when s_index_id in ('C105')        then 'CORP_BV_007'
     #                when s_index_id in ('C014')        then 'CORP_BV_008'
     #                when s_index_id in ('C013')        then 'CORP_BV_009'
     #           end,case when city_id=888 then 891 else city_id end
     #    "
     #    puts $sql_buf
     #
     #if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   #	     trace_sql $errmsg 1300
	   #	     puts "errmsg:$errmsg"
	   # 	     return -1
	   #}
	   #set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
     #    select 'CORP_BV_006',case when grouping(int(city_id))=1 then 890 else int(city_id) end,count(1)
     #    from $dw_enterprise_sub_yyyymm
     #    where service_id='942' and rec_status=1 and CREATE_DATE>=$date_optime and CREATE_DATE<'$next_month_day'
     #    group by cube(int(city_id))
     #    "
     #    puts $sql_buf
     #
     #if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   #	     trace_sql $errmsg 1300
	   #	     puts "errmsg:$errmsg"
	   #	     return -1
	   #}
	   ##201001-201004����Ч(��201005��֮��KPI��,���ų�ԱMOU�ھ����Ϊ��ȷ����)
	   #set sql_buf "delete from session.St_tb_market_month_tmp
	   #    where item_id='CORP_BV_001' and item_value>0
     #    "
     #    puts $sql_buf
     #
     #if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   #	     trace_sql $errmsg 1300
	   #	     puts "errmsg:$errmsg"
	   #	     return -1
	   #}
	   #set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
     #    select 'CORP_BV_001',case when grouping(city_id)=1 then 890 else city_id end
     #       ,case when sum(a.value2)=0 then 0 else round(1.0*sum(a.value1)/sum(a.value2),2) end
     #    from (select int(ent_city_id) as city_id,sum(call_duration_m) as value1
     #            ,count(distinct user_id) as value2
     #          from $dw_enterprise_member_mid_yyyymm
     #          where dummy_mark = $rep_false and enterprise_id not in ($enterprise_test_list)
     #          group by int(ent_city_id)
     #         )a
     #    group by cube(city_id)
     #    "
     #    puts $sql_buf
     #
     #if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   #	     trace_sql $errmsg 1300
	   #	     puts "errmsg:$errmsg"
	   #	     return -1
	   #}

     #Step4.����ʱ������в���*��ֱ�ӻ�ȡ(ͳ��)��*����
     #CORP_USER_001	��Ҫ���ſͻ��������
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'CORP_USER_001',city_id,case when city_id=891 then ROUND(double(per_cover)/200.0,4) else ROUND(double(per_cover)/100.0,4) end
         from (select case when int(city_id)=888 then 891 else int(city_id) end as city_id,sum(per_cover) as per_cover
               from $stat_enterprise_0053
               where time_id=$date_optime
               group by case when int(city_id)=888 then 891 else int(city_id) end
              )a
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
     #CORP_USER_002	�ص㼯�ſͻ�����
	   #set sql_buf "select count(1) from syscat.tables
	   #    where tabname = 'DW_ENTERPRISE_SNAPSHOT_${year}' and tabschema = 'BASS2'
	   #    "
     #
     #puts $sql_buf
     #if [catch {aidb_sql $handle $sql_buf} errmsg] {
     #  trace_sql $errmsg 1300
     #  puts "errmsg:$errmsg"
     #  return -1
     #}
     #
     #while {[set this_row [aidb_fetch $handle]] != ""} {
     #   set ent_exist_flag [lindex $this_row 0]
     #}
	   #aidb_close $handle
	   #if [catch {set handle [aidb_open $conn]} errmsg] {
	   #	      trace_sql $errmsg 1100
	   #	      return -1
	   #}
     #if { $ent_exist_flag != 1 } {
     #    set   dw_enterprise_snapshot_yyyy         "bass2.dw_enterprise_snapshot_$lastyear"
     #    #set   dw_enterprise_snapshot_yyyy         "bass2.dw_enterprise_snapshot"
     #}
	   #set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
	   #    select 'CORP_USER_002',city_id,case when a.value2=0 then 0 else 1-round(double(a.value1)/a.value2,4) end
	   #    from (select case when grouping(int(a.ent_city_id))=1 then 890 else int(a.ent_city_id) end as city_id,
     #             sum(case when a.ent_status_id <>0 then a.cnt else 0 end) as value1,sum(a.cnt) as value2
     #          from (select b.ent_city_id,b.ent_status_id,count(distinct a.enterprise_id) as cnt
     #                from $dw_enterprise_snapshot_yyyy a,$dw_enterprise_msg_yyyymm b
     #                where a.enterprise_id=b.enterprise_id and b.level_def_mode=1
     #                group by b.ent_city_id,b.ent_status_id
     #               )a
     #          group by cube(int(a.ent_city_id))
     #         )a
     #    "
     #    puts $sql_buf
     #
     #if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   #	     trace_sql $errmsg 1300
	   #	     puts "errmsg:$errmsg"
	   #	     return -1
	   #}
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
	       select 'CORP_USER_002',city_id,case when a.value2=0 then 0 else 1-round(double(a.value1)/a.value2,4) end
	       from (select case when grouping(int(a.ent_city_id))=1 then 890 else int(a.ent_city_id) end as city_id,
                  sum(case when a.ent_status_id <>0 then a.cnt else 0 end) as value1,sum(a.cnt) as value2
               from (select b.ent_city_id,b.ent_status_id,count(distinct a.enterprise_id) as cnt
                     from $stat_enterprise_0054_ent_snapshot a,$dw_enterprise_msg_yyyymm b
                     where a.enterprise_id=b.enterprise_id and a.city_id='888'
                     group by b.ent_city_id,b.ent_status_id
                    )a
               group by cube(int(a.ent_city_id))
              )a
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
     #CORP_USER_003	���պ���Ӳ��������
	   #set sql_buf "select count(1) from syscat.tables
	   #    where tabname = 'DW_ENTERPRISE_MEMBER_SNAPSHOT_${year}' and tabschema = 'BASS2'
	   #    "
     #
     #puts $sql_buf
     #if [catch {aidb_sql $handle $sql_buf} errmsg] {
     #  trace_sql $errmsg 1300
     #  puts "errmsg:$errmsg"
     #  return -1
     #}
     #
     #while {[set this_row [aidb_fetch $handle]] != ""} {
     #   set mem_exist_flag [lindex $this_row 0]
     #}
	   #aidb_close $handle
	   #if [catch {set handle [aidb_open $conn]} errmsg] {
	   #	      trace_sql $errmsg 1100
	   #	      return -1
	   #}
     #if { $mem_exist_flag != 1 } {
     #    set   dw_enterprise_member_snapshot_yyyy  "bass2.dw_enterprise_member_snapshot_$lastyear"
     #    #set   dw_enterprise_member_snapshot_yyyy  "bass2.dw_enterprise_member_snapshot_2011"
     #}
	   #set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
     #    select 'CORP_USER_003',case when grouping(a.city_id)=1 then 890 else a.city_id end,
     #       case when sum(a.value2)=0 then 0 else round(double(sum(a.value1))/sum(a.value2),4) end
     #    from (select int(a.city_id) as city_id,count(distinct a.user_id) as value2,
     #             count(distinct (case when b.user_id is not null then a.user_id end)) as value1
     #          from $dw_enterprise_member_snapshot_yyyy a left join $dw_product_sprom_yyyymm b
     #               on a.user_id=b.user_id and b.expire_date>='$next_month_day'
     #          group by int(a.city_id)
     #         )a
     #    group by cube(a.city_id)
     #    "
     #    puts $sql_buf
     #
     #if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   #	     trace_sql $errmsg 1300
	   #	     puts "errmsg:$errmsg"
	   #	     return -1
	   #}
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'CORP_USER_003',case when grouping(a.city_id)=1 then 890 else a.city_id end,
            case when sum(a.value2)=0 then 0 else round(double(sum(a.value1))/sum(a.value2),4) end
         from (select int(value(b.city_id,'891')) as city_id,count(distinct a.user_id) as value2,
                  count(distinct (case when b.user_id is not null then a.user_id end)) as value1
               from $stat_enterprise_0054_mem_snapshot a left join $dw_product_sprom_yyyymm b
                    on a.user_id=b.user_id and b.expire_date>='$next_month_day'
               group by int(value(b.city_id,'891'))
              )a
         group by cube(a.city_id)
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

     #CORP_BV_010	���Ų���ͳ��ռ��
	   set sql_buf "insert into session.St_tb_market_month_tmp (item_id,area_code,item_value)
         select 'CORP_BV_010',a.area_code,case when b.value=0 then 0 else round(double(a.value)/b.value,4) end
         from (select area_code,sum(item_value) as value from session.St_tb_market_month_tmp
               where item_id='CORP_BV_008' group by area_code
              )a,
              (select area_code,sum(item_value) as value from session.St_tb_market_month_tmp
               where item_id in ('CORP_BV_009') group by area_code
              )b
         where a.area_code=b.area_code
         "
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }


     #Step6.���������е�������
	   set sql_buf "delete from $St_tb_market_month where time_id = $year$month AND item_type='JK'"
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

     #Step7.��ʱ���н����������
	   set sql_buf "insert into $St_tb_market_month (item_type,item_id,area_code,time_id,item_value)
         select 'JK',
               item_id,
               area_code,
               $year$month,
               sum(item_value)
         from session.St_tb_market_month_tmp
         group by item_id,
               area_code
        "
        puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }


 	   puts "II.������Ended..."

	return 0
}