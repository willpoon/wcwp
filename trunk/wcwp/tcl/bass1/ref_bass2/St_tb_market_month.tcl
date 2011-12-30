#======================================================================================
#版权归属：Copyright (c) 2011,AsiaInfo.Report.System
#程序名称: St_tb_market_month.tcl
#程序功能: 市场运营监控-业绩报告-市场/集客指标(不含衍生指标)月接口数据生成
#分析目标: 略
#分析指标: 见结果表
#分析维度: 分日期/地市
#运行粒度: 月
#运行示例: crt_basetab -SSt_tb_market_month.tcl -D2011-06-01
#创建时间: 2011-7-13
#创 建 人: AsiaInfo	Liwei
#存在问题: 略
#修改历史: 1.2011-7-20 By Liwei 新增集客类指标
#          2.2011-7-28 By Liwei 新增补跑数据说明
#          3.2011-8-2 By Liwei 完善程序(去掉相关注释)等
#          4.2011-8-4 By Liwei 修改CORP_USER_002/CORP_USER_003统计口径(原从拍照集团/集团成员出,但无重要集团数据)
#注意事项: 1.补跑相关周期(割接前后)数据时,注意注释掉相应的代码段,具体如下:
#            MK:MARKET_USER_016	宽带客户净增数/MARKET_BV_001	电子渠道业务占比
#            Jk:CORP_REVE_002	集团信息化收入等(具体见相关代码段-分2011年前/后口径)
#            拍照集团客户/集团成员表- 每年手工录入数据生成
#            CORP_BV_001:因201005月KPI中,集团成员MOU口径才变更为正确,201001-201004月须另外统计
#=======================================================================================
 proc deal {p_optime p_timestamp} {

 	   global conn
 	   global handle

 	   if [catch {set handle [aidb_open $conn]} errmsg] {
 	   	trace_sql $errmsg 1000
 	   	return -1
 	   }

 	   #I.市场类
 	   puts "I.市场类Beginnig..."
 	   if {[St_tb_market_month_mk $p_optime]  != 0} {
 	   	  aidb_roll $conn
 	   	  aidb_close $handle
 	   	 return -1
 	   }
 	   #II.集客类
 	   puts "II.集客类Beginnig..."
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
	   #专营店类别
	   set channel_type "90881"

     #日期处理
     set lastmonth [GetLastMonth $year$month]
     puts $lastmonth
     set  last_month  [string range [clock format [ clock scan "${year}${month}${day} - 1 month" ] -format "%Y-%m-%d"] 0 9]
     puts $last_month
     set  next_month_day  [string range [clock format [ clock scan "${year}${month}${day} + 1 month" ] -format "%Y-%m-%d"] 0 9]
     puts $next_month_day

     #源表
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

     #维表
     set   city_lkp                            "bass2.dim_pub_city"
     set   dim_tb_market_define                "bass2.dim_tb_market_define"

     #结果表
     set   St_tb_market_month                  "bass2.St_tb_market_month"

     #Step1.创建临时结果表
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
     #Step2.初始化临时结果表
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
     #Step3.向临时结果表中插入*直接获取类*数据
     #来源<KPI>-------------------->
     #MARKET_REVE_001	运营收入（万元）
     #MARKET_REVE_003	语音收入
     #MARKET_REVE_004	ARPU
     #MARKET_USER_004	移动到达用户数
     #MARKET_USER_005	电信到达用户数
     #MARKET_USER_006	联通到达用户数
	   #MARKET_USER_007	TD客户到达数
     #MARKET_USER_010	新增客户数
     #MARKET_USER_013	活动用户数
     #MARKET_BV_003	计费时长
     #MARKET_USER_012	离网率
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
	   #MARKET_REVE_002 数据业务下账收入（万元）
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
	   #MARKET_USER_009	净增客户数
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
	   #MARKET_USER_014	中高端客户数
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

	   #MARKET_USER_017	净增TD客户数（户）
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
     #Step4.向临时结果表中插入*非直接获取(统计)类*数据
     #MARKET_REVE_006	坏账金额(按市场部月度经分会议口径出-不含"远教项目"(80000618	ICT业务集成费收入))
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
	   #MARKET_USER_008	TD活跃客户数
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
     #MARKET_USER_016	宽带客户净增数
     #1.割接后统计口径
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
     ##2.割接前统计口径
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

     #Step6.向临时结果表中插入*占比类*数据
     #MARKET_REVE_007	坏账率
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
	   #MARKET_USER_001	移动市场份额
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
     #MARKET_USER_002	电信市场份额
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
     #MARKET_USER_003	联通市场份额
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
	   #MARKET_USER_011	新增净增比
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
	   #MARKET_USER_015	中高端用户占比
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
     #MARKET_BV_001	电子渠道业务占比
     #1.割接后统计口径
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
		 #不含缴费类
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
		 #缴费类
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
     ##2.割接前统计口径
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

     #MARKET_BV_002	实体渠道活跃度渠道数量
     #TYPE:
     #1-放号活跃网点数量
     #2-缴费活跃网点数量
     #0-指定专营店数量
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
		 #放号活跃网点
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
     #缴费活跃网点
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
		 #放号/缴费活跃网点数量
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
     #指定专营店数量
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


     #Step6.清除结果表中当月数据
	   set sql_buf "delete from $St_tb_market_month where time_id = $year$month AND item_type='MK'"
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

     #Step7.临时表中结果插入结果表
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
	   #MARKET_USER_012	离网率
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

 	   puts "I.市场类Ended..."

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

	   #测试集团
	   set enterprise_test_list "'89100000000682','89100000000659','89100000000656','89100000000651'"

     #日期处理
     set lastyear [expr $year -1 ]
     puts $lastyear
     set lastmonth [GetLastMonth $year$month]
     puts $lastmonth
     set  last_month  [string range [clock format [ clock scan "${year}${month}${day} - 1 month" ] -format "%Y-%m-%d"] 0 9]
     puts $last_month
     set  next_month_day  [string range [clock format [ clock scan "${year}${month}${day} + 1 month" ] -format "%Y-%m-%d"] 0 9]
     puts $next_month_day

     #源表
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

     #维表
     set   city_lkp                            "bass2.dim_pub_city"
     set   dim_tb_market_define                "bass2.dim_tb_market_define"

     #结果表
     set   St_tb_market_month                  "bass2.St_tb_market_month"

     #Step1.创建临时结果表
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
     #Step2.初始化临时结果表
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
     #Step3.向临时结果表中插入*直接获取类*数据
     #3-1.来源<KPI>
     #CORP_REVE_001	集团客户整体收入
     #CORP_REVE_003	集团成员ARPU
     #CORP_USER_004	集团客户到达数(KPI无)
     #CORP_USER_005	集团客户成员到达数
     #CORP_USER_006	集团成员新增数
     #CORP_USER_008	集团成员离网数
     #CORP_BV_001	集团成员MOU
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

	   #3-2.2011后统计口径
	   #3-2-1.CORP_REVE_002	集团信息化收入
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
	   #3-2-2.CORP_USER_007	集团成员流失数
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
	   #3-2-3.CORP_BV_002	集团专线达到数
	   #3-2-4.CORP_BV_003	集团专线收入
	   #3-2-5.CORP_BV_004	M2M终端到达数
     #3-2-6.CORP_BV_005	M2M收入
     #3-2-7.CORP_BV_006	车务通当月发展终端数
     #3-2-7.CORP_BV_007	车务通收入
     #3-2-8.CORP_BV_008	集团彩铃统付收入
     #3-2-9.CORP_BV_009	集团彩铃收入
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
	   ##2011前统计口径
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
	   ##201001-201004月有效(因201005月之后KPI中,集团成员MOU口径变更为正确的了)
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

     #Step4.向临时结果表中插入*非直接获取(统计)类*数据
     #CORP_USER_001	重要集团客户覆盖情况
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
     #CORP_USER_002	重点集团客户保有
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
     #CORP_USER_003	拍照号码硬捆绑达标率
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

     #CORP_BV_010	集团彩铃统付占比
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


     #Step6.清除结果表中当月数据
	   set sql_buf "delete from $St_tb_market_month where time_id = $year$month AND item_type='JK'"
         puts $sql_buf

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

     #Step7.临时表中结果插入结果表
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


 	   puts "II.集客类Ended..."

	return 0
}