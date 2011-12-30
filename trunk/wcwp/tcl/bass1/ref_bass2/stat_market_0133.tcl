#======================================================================================
#版权归属：Copyright (c) 2010,AsiaInfo.Report.System
#程序名称: stat_market_0133.tcl
#程序功能: 参与营销活动协议在网客户构成
#分析目标: 只抓取在网的用户情况
#1      全区	
#2      ARPU<30
#3 	30≤ARPU<50
#4 	50≤ARPU<80
#5 	80≤ARPU<100
#6 	100≤ARPU<120
#7 	120≤ARPU<150
#8 	150≤ARPU<200
#9 	200≤ARPU<300
#10	300≤ARPU<500
#11	500≤ARPU<800
#12	800≤ARPU<1500
#13	1500≤ARPU
#14 本月内失效
#15	1-3月失效
#16	3-5月失效
#17	5-8月失效
#18	8-12月失效
#19	12月以上失效
#分析维度: 全区 地市 ARPU分档
#运行粒度: 月
#运行示例: crt_basetab.sh stat_market_0133.tcl 2011-04-01
#创建时间: 2011-4-26
#创 建 人: Asiainfo-Linkage
#存在问题:
#修改历史:
#=======================================================================================
proc deal {p_optime p_timestamp} {
    global conn
    global handle

    if [catch {set handle [aidb_open $conn]} errmsg] {
        trace_sql $errmsg 1000
        return -1
    }

    if {[stat_market_0133 $p_optime] != 0} {
        aidb_roll $conn
        aidb_close $handle
        return -1
    }

    aidb_commit $conn
    aidb_close $handle

    return 0
 }

proc stat_market_0133 {p_optime} {
    global conn
    global handle

    source stat_insert_index.tcl
    source report.cfg

    #日期处理
    set    date_optime  [ai_to_date $p_optime]
    scan   $p_optime    "%04s-%02s-%02s" year month day
	  #获取数据月份yyyymm
	  set op_month [ string range $p_optime 0 3][string range $p_optime 5 6 ]
	  #获取数据月份1号yyyy-mm-01
	  set op_month_01 [ string range $op_month 0 3 ]-[ string range $op_month 4 5 ]-01
	  #获取数据月份下月1号yyyy-mm-01
	  set nt_month_01 [ clock format [ clock scan "${op_month_01} + 1 months" ] -format "%Y-%m-01" ]


	  #删除在网用户临时表
		set sql_buf "alter table bass2.stat_market_qu_0133_t1 activate not logged initially with empty table"
		puts ${sql_buf}
		if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
			trace_sql $errmsg 1008
			puts "errmsg:$errmsg"
			return -1
		}
		aidb_commit $conn

    #插入临时表,抓取捆绑了活动的在网用户资料
    set sql_buf "
    insert into bass2.stat_market_qu_0133_t1
    (   
		  user_id     
		  ,product_no  
		  ,brand_id    
		  ,fact_fee
    )
			select distinct a.user_id,a.product_no,a.brand_id,a.fact_fee
			from bass2.dw_product_${op_month} a,
			     bass2.dw_product_user_promo_$op_month b
			where a.user_id=b.user_id
			  and date(b.expire_date)>=date('${nt_month_01}')
				and usertype_id in (1,2,9) 
				and userstatus_id in (1,2,3,6,8)
				and test_mark<>1
				and brand_id in (1,3,4,5,7)
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }

	##删除结果集当月用户号码清单数据
		set sql_buf "alter table bass2.stat_market_product_0133_t1 activate not logged initially with empty table"
		puts ${sql_buf}
		if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
			trace_sql $errmsg 1008
			puts "errmsg:$errmsg"
			return -1
		}
		aidb_commit $conn

	#抓取当月在网用户清单号码，并且限制3大品牌
    set sql_buf "
    insert into bass2.stat_market_product_0133_t1
    (   product_no
				,brand_id
				,INDEX_T1
    )
    select 
           product_no,
					case when brand_id=1 then 1
					     when brand_id=4 then 2
					     when brand_id in (3,5,7) then 3
					end,
					1
		  from bass2.stat_market_qu_0133_t1
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


	##删除结果集当月用户号码清单数据
		set sql_buf "alter table bass2.stat_market_zf_0133_t1 activate not logged initially with empty table"
		puts ${sql_buf}
		if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
			trace_sql $errmsg 1008
			puts "errmsg:$errmsg"
			return -1
		}
		aidb_commit $conn

	#抓取当月在网用户清单号码，并且限制138资费
    set sql_buf "
    insert into bass2.stat_market_zf_0133_t1
    (   product_no
				,brand_id
				,INDEX_T1
    )
    select 
           a.product_no,
					case when a.brand_id=1 then 1
					     when a.brand_id=4 then 2
					     when a.brand_id in (3,5,7) then 3
					end,
					1
		  from bass2.stat_market_qu_0133_t1 a,
				(
				select distinct b.user_id from bass2.dw_product_sprom_${op_month} b,bass2.dim_product_item c
				where b.sprom_id=c.prod_id and c.prod_name like '%138资费%' and b.active_mark = 1
				) b 
			where a.user_id=b.user_id		 
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



	#===============================================================================================
	  set sql_buf "alter table bass2.stat_market_prom_0133_t1 activate not logged initially with empty table"
		puts ${sql_buf}
		if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
			trace_sql $errmsg 1008
			puts "errmsg:$errmsg"
			return -1
		}
		aidb_commit $conn


	##=====================================================
	##抓取当月在网用户活动列表
	##=====================================================
    set sql_buf "
    insert into bass2.stat_market_prom_0133_t1
    (
				time_id
				,prom_id
				,prom_name
				,index_t1
    )
		select distinct $op_month,b.cond_id,b.cond_name,1
			from bass2.stat_market_qu_0133_t1 a,
			     (select distinct user_id,promo_id,promo_name,cond_id,cond_name,expire_date
			        from bass2.dw_product_user_promo_${op_month} where date(expire_date)>=date('${nt_month_01}')) b
			where a.user_id=b.user_id
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



    #Step4.创建结果临时表
    set sql_buf "
    declare global temporary table session.stat_market_qu_0133_t2
    (
			city_id          varchar(7),
			index            bigint,
			qqt_cnts         integer,
			dgdd_cnts        integer,
			szx_cnts         integer,
			zf_cnts          integer,
			prod_cnts        integer
    )
    partitioning key(index) using hashing
    with replace on commit preserve rows not logged in tbs_user_temp
    "
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1010
        puts "errmsg:$errmsg"
        return -1
    }

	##=====================================================
	##提取全区的3大品牌用户数情况
	##=====================================================
    set sql_buf "
    insert into session.stat_market_qu_0133_t2
    (
				city_id
				,index
				,qqt_cnts
				,dgdd_cnts
				,szx_cnts
				,zf_cnts
    )
    select '890',
           1,
					sum(case when a.brand_id=1 then 1 else 0 end),
					sum(case when a.brand_id=4 then 1 else 0 end),
					sum(case when a.brand_id in (3,5,7) then 1 else 0 end),
					sum(case when b.user_id is not null then 1 else 0 end)
		from stat_market_qu_0133_t1 a
		left join
				(
				select distinct b.user_id from bass2.dw_product_sprom_${op_month} b,bass2.dim_product_item c
				where b.sprom_id=c.prod_id and c.prod_name like '%138资费%' and b.active_mark = 1
				) b on a.user_id=b.user_id
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


	##=====================================================
	## 提取全区的活动数
	##=====================================================
    set sql_buf "
    insert into session.stat_market_qu_0133_t2
    (
				city_id
				,index
				,prod_cnts
    )
    select '890',1,count(distinct prom_name) from bass2.stat_market_prom_0133_t1
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


	##=====================================================
	##提取fact_fee分档全区的3大品牌用户数情况
	##=====================================================
    set sql_buf "
    insert into session.stat_market_qu_0133_t2
    (
				city_id
				,index
				,qqt_cnts
				,dgdd_cnts
				,szx_cnts
				,zf_cnts
    )
    select 
           '890',
					case when fact_fee<30 then 2
					     when fact_fee>=30 and fact_fee<50 then 3
					     when fact_fee>=50 and fact_fee<80 then 4
					     when fact_fee>=80 and fact_fee<100 then 5
					     when fact_fee>=100 and fact_fee<120 then 6
					     when fact_fee>=120 and fact_fee<150 then 7
					     when fact_fee>=150 and fact_fee<200 then 8
					     when fact_fee>=200 and fact_fee<300 then 9
					     when fact_fee>=300 and fact_fee<500 then 10
					     when fact_fee>=500 and fact_fee<800 then 11
					     when fact_fee>=800 and fact_fee<1500 then 12
					     when fact_fee>=1500 then 13
					 end,
					sum(case when a.brand_id=1 then 1 else 0 end),
					sum(case when a.brand_id=4 then 1 else 0 end),
					sum(case when a.brand_id in (3,5,7) then 1 else 0 end),
					sum(case when b.user_id is not null then 1 else 0 end)
		from stat_market_qu_0133_t1 a
		left join
				(
				select distinct b.user_id from bass2.dw_product_sprom_${op_month} b,bass2.dim_product_item c
				where b.sprom_id=c.prod_id and c.prod_name like '%138资费%' and b.active_mark = 1
				) b on a.user_id=b.user_id
   group by 
					case when fact_fee<30 then 2
					     when fact_fee>=30 and fact_fee<50 then 3
					     when fact_fee>=50 and fact_fee<80 then 4
					     when fact_fee>=80 and fact_fee<100 then 5
					     when fact_fee>=100 and fact_fee<120 then 6
					     when fact_fee>=120 and fact_fee<150 then 7
					     when fact_fee>=150 and fact_fee<200 then 8
					     when fact_fee>=200 and fact_fee<300 then 9
					     when fact_fee>=300 and fact_fee<500 then 10
					     when fact_fee>=500 and fact_fee<800 then 11
					     when fact_fee>=800 and fact_fee<1500 then 12
					     when fact_fee>=1500 then 13
					 end   
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


	##=====================================================
	## 提取全区fact_fee分档的活动数
	##=====================================================
    set sql_buf "
    insert into session.stat_market_qu_0133_t2
    (
				city_id
				,index
				,prod_cnts
    )
		select '890',
			case when a.fact_fee<30 then 2
		     when a.fact_fee>=30 and fact_fee<50 then 3
		     when a.fact_fee>=50 and fact_fee<80 then 4
		     when a.fact_fee>=80 and fact_fee<100 then 5
		     when a.fact_fee>=100 and fact_fee<120 then 6
		     when a.fact_fee>=120 and fact_fee<150 then 7
		     when a.fact_fee>=150 and fact_fee<200 then 8
		     when a.fact_fee>=200 and fact_fee<300 then 9
		     when a.fact_fee>=300 and fact_fee<500 then 10
		     when a.fact_fee>=500 and fact_fee<800 then 11
		     when a.fact_fee>=800 and fact_fee<1500 then 12
		     when a.fact_fee>=1500 then 13
		    end,
		    count(distinct b.cond_name)
			from bass2.stat_market_qu_0133_t1 a,
			     (select distinct user_id,promo_id,promo_name,cond_id,cond_name,expire_date
			        from bass2.dw_product_user_promo_${op_month} where date(expire_date)>=date('${nt_month_01}')) b
			where a.user_id=b.user_id
		group by 
		case when a.fact_fee<30 then 2
		     when a.fact_fee>=30 and fact_fee<50 then 3
		     when a.fact_fee>=50 and fact_fee<80 then 4
		     when a.fact_fee>=80 and fact_fee<100 then 5
		     when a.fact_fee>=100 and fact_fee<120 then 6
		     when a.fact_fee>=120 and fact_fee<150 then 7
		     when a.fact_fee>=150 and fact_fee<200 then 8
		     when a.fact_fee>=200 and fact_fee<300 then 9
		     when a.fact_fee>=300 and fact_fee<500 then 10
		     when a.fact_fee>=500 and fact_fee<800 then 11
		     when a.fact_fee>=800 and fact_fee<1500 then 12
		     when a.fact_fee>=1500 then 13
		    end
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



	##删除结果集fact_fee分档用户清单临时表
		set sql_buf "alter table bass2.stat_market_product_0133_t2 activate not logged initially with empty table"
		puts ${sql_buf}
		if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
			trace_sql $errmsg 1008
			puts "errmsg:$errmsg"
			return -1
		}
		aidb_commit $conn

	#抓取当月在网用户fact_fee分档结果
    set sql_buf "
    insert into bass2.stat_market_product_0133_t2
    (   product_no
				,brand_id
				,INDEX_T2
    )
    select product_no,
					case when brand_id=1 then 1
					     when brand_id=4 then 2
					     when brand_id in (3,5,7) then 3
					end,
					case when fact_fee<30 then 2
					     when fact_fee>=30 and fact_fee<50 then 3
					     when fact_fee>=50 and fact_fee<80 then 4
					     when fact_fee>=80 and fact_fee<100 then 5
					     when fact_fee>=100 and fact_fee<120 then 6
					     when fact_fee>=120 and fact_fee<150 then 7
					     when fact_fee>=150 and fact_fee<200 then 8
					     when fact_fee>=200 and fact_fee<300 then 9
					     when fact_fee>=300 and fact_fee<500 then 10
					     when fact_fee>=500 and fact_fee<800 then 11
					     when fact_fee>=800 and fact_fee<1500 then 12
					     when fact_fee>=1500 then 13
					 end
		  from bass2.stat_market_qu_0133_t1
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



	##删除结果集fact_fee分档用户清单临时表
		set sql_buf "alter table bass2.stat_market_zf_0133_t2 activate not logged initially with empty table"
		puts ${sql_buf}
		if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
			trace_sql $errmsg 1008
			puts "errmsg:$errmsg"
			return -1
		}
		aidb_commit $conn

	#抓取当月在网用户fact_fee分档结果
    set sql_buf "
    insert into bass2.stat_market_zf_0133_t2
    (   product_no
				,brand_id
				,INDEX_T2
    )
    select a.product_no,
					case when a.brand_id=1 then 1
					     when a.brand_id=4 then 2
					     when a.brand_id in (3,5,7) then 3
					end,
					case when a.fact_fee<30 then 2
					     when a.fact_fee>=30 and a.fact_fee<50 then 3
					     when a.fact_fee>=50 and a.fact_fee<80 then 4
					     when a.fact_fee>=80 and a.fact_fee<100 then 5
					     when a.fact_fee>=100 and a.fact_fee<120 then 6
					     when a.fact_fee>=120 and a.fact_fee<150 then 7
					     when a.fact_fee>=150 and a.fact_fee<200 then 8
					     when a.fact_fee>=200 and a.fact_fee<300 then 9
					     when a.fact_fee>=300 and a.fact_fee<500 then 10
					     when a.fact_fee>=500 and a.fact_fee<800 then 11
					     when a.fact_fee>=800 and a.fact_fee<1500 then 12
					     when a.fact_fee>=1500 then 13
					 end
		  from bass2.stat_market_qu_0133_t1 a,
				(
				select distinct b.user_id from bass2.dw_product_sprom_${op_month} b,bass2.dim_product_item c
				where b.sprom_id=c.prod_id and c.prod_name like '%138资费%' and b.active_mark = 1
				) b 
		  where a.user_id=b.user_id		 
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



	#===============================================================================================
	  set sql_buf "alter table bass2.stat_market_prom_0133_t2 activate not logged initially with empty table"
		puts ${sql_buf}
		if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
			trace_sql $errmsg 1008
			puts "errmsg:$errmsg"
			return -1
		}
		aidb_commit $conn


	##=====================================================
	##抓取当月在网用户活动列表
	##=====================================================
    set sql_buf "
    insert into bass2.stat_market_prom_0133_t2
    (
				time_id
				,prom_id
				,prom_name
				,index_t2
    )
		select distinct $op_month,b.cond_id,b.cond_name,
		      case when a.fact_fee<30 then 2
					     when a.fact_fee>=30 and fact_fee<50 then 3
					     when a.fact_fee>=50 and fact_fee<80 then 4
					     when a.fact_fee>=80 and fact_fee<100 then 5
					     when a.fact_fee>=100 and fact_fee<120 then 6
					     when a.fact_fee>=120 and fact_fee<150 then 7
					     when a.fact_fee>=150 and fact_fee<200 then 8
					     when a.fact_fee>=200 and fact_fee<300 then 9
					     when a.fact_fee>=300 and fact_fee<500 then 10
					     when a.fact_fee>=500 and fact_fee<800 then 11
					     when a.fact_fee>=800 and fact_fee<1500 then 12
					     when a.fact_fee>=1500 then 13
					 end
			from bass2.stat_market_qu_0133_t1 a,
			     bass2.dw_product_user_promo_$op_month b
			where a.user_id=b.user_id
			  and date(b.expire_date)>=date('${nt_month_01}')
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


	##=====================================================
	##提取捆绑到期的区间个品牌用户的分布情况
	##=====================================================
    set sql_buf "
    insert into session.stat_market_qu_0133_t2
    (
				city_id
				,index
				,qqt_cnts
				,dgdd_cnts
				,szx_cnts
				,zf_cnts
				,prod_cnts
    )
    select '890',
      case when (days(date(a.expire_date))-days(date('${nt_month_01}')))/30<1 then 14
           when (days(date(a.expire_date))-days(date('${nt_month_01}')))/30>=1 and (days(date(a.expire_date))-days(date('${nt_month_01}')))/30<3 then 15
           when (days(date(a.expire_date))-days(date('${nt_month_01}')))/30>=3 and (days(date(a.expire_date))-days(date('${nt_month_01}')))/30<5 then 16
           when (days(date(a.expire_date))-days(date('${nt_month_01}')))/30>=5 and (days(date(a.expire_date))-days(date('${nt_month_01}')))/30<8 then 17
           when (days(date(a.expire_date))-days(date('${nt_month_01}')))/30>=8 and (days(date(a.expire_date))-days(date('${nt_month_01}')))/30<12 then 18
           when (days(date(a.expire_date))-days(date('${nt_month_01}')))/30>=12 then 19
      end,
					count(distinct case when a.brand_id=1 then a.user_id end),
					count(distinct case when a.brand_id=4 then a.user_id end),
					count(distinct case when a.brand_id in (3,5,7) then a.user_id end),
					count(distinct case when c.user_id is not null then a.user_id end),
					count(distinct a.cond_name)
     from
	    (
	    select distinct 
	         a.user_id,a.brand_id,b.cond_id,b.cond_name,b.expire_date
			from bass2.stat_market_qu_0133_t1 a,
			     (select distinct user_id,promo_id,promo_name,cond_id,cond_name,expire_date
			        from bass2.dw_product_user_promo_${op_month} where date(expire_date)>=date('${nt_month_01}')) b
			where a.user_id=b.user_id
			 ) a
		left join
				(
				select distinct b.user_id from bass2.dw_product_sprom_${op_month} b,bass2.dim_product_item c
				where b.sprom_id=c.prod_id and c.prod_name like '%138资费%' and b.active_mark = 1
				) c on a.user_id=c.user_id
   group by 
      case when (days(date(a.expire_date))-days(date('${nt_month_01}')))/30<1 then 14
           when (days(date(a.expire_date))-days(date('${nt_month_01}')))/30>=1 and (days(date(a.expire_date))-days(date('${nt_month_01}')))/30<3 then 15
           when (days(date(a.expire_date))-days(date('${nt_month_01}')))/30>=3 and (days(date(a.expire_date))-days(date('${nt_month_01}')))/30<5 then 16
           when (days(date(a.expire_date))-days(date('${nt_month_01}')))/30>=5 and (days(date(a.expire_date))-days(date('${nt_month_01}')))/30<8 then 17
           when (days(date(a.expire_date))-days(date('${nt_month_01}')))/30>=8 and (days(date(a.expire_date))-days(date('${nt_month_01}')))/30<12 then 18
           when (days(date(a.expire_date))-days(date('${nt_month_01}')))/30>=12 then 19
      end  
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



	##删除结果集分档用户清单临时表
		set sql_buf "alter table bass2.stat_market_product_0133_t3 activate not logged initially with empty table"
		puts ${sql_buf}
		if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
			trace_sql $errmsg 1008
			puts "errmsg:$errmsg"
			return -1
		}
		aidb_commit $conn

	#抓取当月在网用户分档结果
    set sql_buf "
    insert into bass2.stat_market_product_0133_t3
    (   product_no
				,brand_id
				,INDEX_T3
    )
    select product_no,
					case when brand_id=1 then 1
					     when brand_id=4 then 2
					     when brand_id in (3,5,7) then 3
					end,
	      case when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30<1 then 14
	           when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30>=1 and (days(date(b.expire_date))-days(date('${nt_month_01}')))/30<3 then 15
	           when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30>=3 and (days(date(b.expire_date))-days(date('${nt_month_01}')))/30<5 then 16
	           when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30>=5 and (days(date(b.expire_date))-days(date('${nt_month_01}')))/30<8 then 17
	           when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30>=8 and (days(date(b.expire_date))-days(date('${nt_month_01}')))/30<12 then 18
	           when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30>=12 then 19
	      end
			from bass2.stat_market_qu_0133_t1 a,
			     (select distinct user_id,promo_id,promo_name,cond_id,cond_name,expire_date
			        from bass2.dw_product_user_promo_${op_month} where date(expire_date)>=date('${nt_month_01}')) b
			where a.user_id=b.user_id
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



	##删除结果集分档用户清单临时表
		set sql_buf "alter table bass2.stat_market_zf_0133_t3 activate not logged initially with empty table"
		puts ${sql_buf}
		if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
			trace_sql $errmsg 1008
			puts "errmsg:$errmsg"
			return -1
		}
		aidb_commit $conn

	#抓取当月在网用户分档结果
    set sql_buf "
    insert into bass2.stat_market_zf_0133_t3
    (   product_no
				,brand_id
				,INDEX_T3
    )
    select product_no,
					case when brand_id=1 then 1
					     when brand_id=4 then 2
					     when brand_id in (3,5,7) then 3
					end,
	      case when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30<1 then 14
	           when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30>=1 and (days(date(b.expire_date))-days(date('${nt_month_01}')))/30<3 then 15
	           when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30>=3 and (days(date(b.expire_date))-days(date('${nt_month_01}')))/30<5 then 16
	           when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30>=5 and (days(date(b.expire_date))-days(date('${nt_month_01}')))/30<8 then 17
	           when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30>=8 and (days(date(b.expire_date))-days(date('${nt_month_01}')))/30<12 then 18
	           when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30>=12 then 19
	      end
			from bass2.stat_market_qu_0133_t1 a,
			     (select distinct user_id,promo_id,promo_name,cond_id,cond_name,expire_date
			        from bass2.dw_product_user_promo_${op_month} where date(expire_date)>=date('${nt_month_01}')) b,
				(
				select distinct b.user_id from bass2.dw_product_sprom_${op_month} b,bass2.dim_product_item c
				where b.sprom_id=c.prod_id and c.prod_name like '%138资费%' and b.active_mark = 1
				) c 			     
			where a.user_id=b.user_id
			  and a.user_id=c.user_id
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }




#===============================================================================================
	  set sql_buf "alter table bass2.stat_market_prom_0133_t3 activate not logged initially with empty table"
		puts ${sql_buf}
		if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
			trace_sql $errmsg 1008
			puts "errmsg:$errmsg"
			return -1
		}
		aidb_commit $conn


	##=====================================================
	##抓取当月在网用户活动列表
	##=====================================================
    set sql_buf "
    insert into bass2.stat_market_prom_0133_t3
    (
				time_id
				,prom_id
				,prom_name
				,index_t3
    )
		select distinct $op_month,b.cond_id,b.cond_name,
	      case when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30<1 then 14
	           when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30>=1 and (days(date(b.expire_date))-days(date('${nt_month_01}')))/30<3 then 15
	           when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30>=3 and (days(date(b.expire_date))-days(date('${nt_month_01}')))/30<5 then 16
	           when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30>=5 and (days(date(b.expire_date))-days(date('${nt_month_01}')))/30<8 then 17
	           when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30>=8 and (days(date(b.expire_date))-days(date('${nt_month_01}')))/30<12 then 18
	           when (days(date(b.expire_date))-days(date('${nt_month_01}')))/30>=12 then 19
	      end
			from bass2.stat_market_qu_0133_t1 a,
			     (select distinct user_id,promo_id,promo_name,cond_id,cond_name,expire_date
			        from bass2.dw_product_user_promo_${op_month} where date(expire_date)>=date('${nt_month_01}')) b
			where a.user_id=b.user_id
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



	# #===============================================================================================
	# #删除结果集当月数据
	# #===============================================================================================
		set sql_buf "delete from bass2.stat_market_qu_0133 where time_id = ${op_month}"
		puts ${sql_buf}
		if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
			trace_sql $errmsg 1008
			puts "errmsg:$errmsg"
			return -1
		}
		aidb_commit $conn



	##=================================================
	## 生成结果数据 1
	##=================================================
    set sql_buf "
    insert into bass2.stat_market_qu_0133
    (
				time_id
				,city_id
				,index
				,qqt_cnts
				,dgdd_cnts
				,szx_cnts
				,zf_cnts
				,prod_cnts
    )
    select $op_month,
					'890',
					index,
          sum(qqt_cnts),
          sum(dgdd_cnts),
          sum(szx_cnts),
          sum(zf_cnts),
          sum(prod_cnts)
		  from session.stat_market_qu_0133_t2
		 group by index
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



  #收集信息
	exec db2 "connect to bassdb user bass2 using bass2"
	exec db2 "runstats on table bass2.stat_market_product_0133_t1 with distribution and detailed indexes all"
	exec db2 "runstats on table bass2.stat_market_product_0133_t2 with distribution and detailed indexes all"
	exec db2 "runstats on table bass2.stat_market_product_0133_t3 with distribution and detailed indexes all"
  exec db2 "runstats on table bass2.stat_market_prom_0133_t1 with distribution and detailed indexes all"
	exec db2 "runstats on table bass2.stat_market_prom_0133_t2 with distribution and detailed indexes all"
	exec db2 "runstats on table bass2.stat_market_prom_0133_t3 with distribution and detailed indexes all"
	exec db2 "terminate"


  	set sql_buf "drop table bass2.stat_market_product_0133_${op_month}"
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   }
	   aidb_commit $conn

	  set sql_buf "create table bass2.stat_market_product_0133_${op_month} like bass2.stat_market_product_0133_yyyymm
		             distribute by hash(product_no,brand_id)
		             in tbs_report index in tbs_index not logged initially"
	  puts ${sql_buf}
	  if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	  	puts "errmsg:$errmsg"
	  }
	  aidb_commit $conn


	##=================================================
	## 生成结果数据 2，用于检索号码清单
	##=================================================
    set sql_buf "
    insert into bass2.stat_market_product_0133_$op_month
    (
				product_no
				,brand_id
				,index_1
				,index_2
				,index_3
				,index_4
				,index_5
				,index_6
				,index_7
				,index_8
				,index_9
				,index_10
				,index_11
				,index_12
				,index_13
				,index_14
				,index_15
				,index_16
				,index_17
				,index_18
				,index_19
    )
		select distinct a.product_no,a.brand_id,
				index_t1,
				case when b.index_t2=2 then 2 else 0 end,
				case when b.index_t2=3 then 3 else 0 end,
				case when b.index_t2=4 then 4 else 0 end,
				case when b.index_t2=5 then 5 else 0 end,
				case when b.index_t2=6 then 6 else 0 end,
				case when b.index_t2=7 then 7 else 0 end,
				case when b.index_t2=8 then 8 else 0 end,
				case when b.index_t2=9 then 9 else 0 end,
				case when b.index_t2=10 then 10 else 0 end,
				case when b.index_t2=11 then 11 else 0 end,
				case when b.index_t2=12 then 12 else 0 end,
				case when b.index_t2=13 then 13 else 0 end,
				case when c.index_t3=14 then 14 else 0 end,
				case when c.index_t3=15 then 15 else 0 end,
				case when c.index_t3=16 then 16 else 0 end,
				case when c.index_t3=17 then 17 else 0 end,
				case when c.index_t3=18 then 18 else 0 end,
				case when c.index_t3=19 then 19 else 0 end
		from bass2.stat_market_product_0133_t1 a
		left join bass2.stat_market_product_0133_t2 b on a.product_no=b.product_no
		left join bass2.stat_market_product_0133_t3 c on a.product_no=c.product_no
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


  	set sql_buf "drop table bass2.stat_market_zf_0133_${op_month}"
	   puts ${sql_buf}
	   if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	   	puts "errmsg:$errmsg"
	   }
	   aidb_commit $conn

	  set sql_buf "create table bass2.stat_market_zf_0133_${op_month} like bass2.stat_market_zf_0133_yyyymm
		             distribute by hash(product_no,brand_id)
		             in tbs_report index in tbs_index not logged initially"
	  puts ${sql_buf}
	  if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
	  	puts "errmsg:$errmsg"
	  }
	  aidb_commit $conn

	##=================================================
	## 生成结果数据 3，用于检索号码清单
	##=================================================
    set sql_buf "
    insert into bass2.stat_market_zf_0133_$op_month
    (
				product_no
				,brand_id
				,index_1
				,index_2
				,index_3
				,index_4
				,index_5
				,index_6
				,index_7
				,index_8
				,index_9
				,index_10
				,index_11
				,index_12
				,index_13
				,index_14
				,index_15
				,index_16
				,index_17
				,index_18
				,index_19
    )
		select distinct a.product_no,a.brand_id,
				index_t1,
				case when b.index_t2=2 then 2 else 0 end,
				case when b.index_t2=3 then 3 else 0 end,
				case when b.index_t2=4 then 4 else 0 end,
				case when b.index_t2=5 then 5 else 0 end,
				case when b.index_t2=6 then 6 else 0 end,
				case when b.index_t2=7 then 7 else 0 end,
				case when b.index_t2=8 then 8 else 0 end,
				case when b.index_t2=9 then 9 else 0 end,
				case when b.index_t2=10 then 10 else 0 end,
				case when b.index_t2=11 then 11 else 0 end,
				case when b.index_t2=12 then 12 else 0 end,
				case when b.index_t2=13 then 13 else 0 end,
				case when c.index_t3=14 then 14 else 0 end,
				case when c.index_t3=15 then 15 else 0 end,
				case when c.index_t3=16 then 16 else 0 end,
				case when c.index_t3=17 then 17 else 0 end,
				case when c.index_t3=18 then 18 else 0 end,
				case when c.index_t3=19 then 19 else 0 end
		from bass2.stat_market_zf_0133_t1 a
		left join bass2.stat_market_zf_0133_t2 b on a.product_no=b.product_no
		left join bass2.stat_market_zf_0133_t3 c on a.product_no=c.product_no
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


	# #===============================================================================================
	# # : 删除结果集当月数据
	# #===============================================================================================
		set sql_buf "delete from bass2.stat_market_prom_0133 where time_id = ${op_month}"
		puts ${sql_buf}
		if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
			trace_sql $errmsg 1008
			puts "errmsg:$errmsg"
			return -1
		}
		aidb_commit $conn



	##=================================================
	## 生成结果数据 4，用于检索活动清单
	##=================================================
    set sql_buf "
    insert into bass2.stat_market_prom_0133
    (
				time_id   
				,prom_name 
				,index_1   
				,index_2   
				,index_3   
				,index_4   
				,index_5   
				,index_6   
				,index_7   
				,index_8   
				,index_9   
				,index_10  
				,index_11  
				,index_12  
				,index_13  
				,index_14  
				,index_15  
				,index_16  
				,index_17  
				,index_18  
				,index_19  
    )
		select distinct $op_month,a.prom_name,
				index_t1,
				case when b.index_t2=2 then 2 else 0 end,
				case when b.index_t2=3 then 3 else 0 end,
				case when b.index_t2=4 then 4 else 0 end,
				case when b.index_t2=5 then 5 else 0 end,
				case when b.index_t2=6 then 6 else 0 end,
				case when b.index_t2=7 then 7 else 0 end,
				case when b.index_t2=8 then 8 else 0 end,
				case when b.index_t2=9 then 9 else 0 end,
				case when b.index_t2=10 then 10 else 0 end,
				case when b.index_t2=11 then 11 else 0 end,
				case when b.index_t2=12 then 12 else 0 end,
				case when b.index_t2=13 then 13 else 0 end,
				case when c.index_t3=14 then 14 else 0 end,
				case when c.index_t3=15 then 15 else 0 end,
				case when c.index_t3=16 then 16 else 0 end,
				case when c.index_t3=17 then 17 else 0 end,
				case when c.index_t3=18 then 18 else 0 end,
				case when c.index_t3=19 then 19 else 0 end
		from bass2.stat_market_prom_0133_t1 a
		left join bass2.stat_market_prom_0133_t2 b on a.prom_id=b.prom_id
		left join bass2.stat_market_prom_0133_t3 c on a.prom_id=c.prom_id
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }




  #收集信息
	exec db2 "connect to bassdb user bass2 using bass2"
	exec db2 "runstats on table bass2.stat_market_qu_0133 with distribution and detailed indexes all"
	exec db2 "runstats on table bass2.stat_market_product_0133_$op_month with distribution and detailed indexes all"
	exec db2 "runstats on table bass2.stat_market_zf_0133_$op_month with distribution and detailed indexes all"
	exec db2 "runstats on table bass2.stat_market_prom_0133 with distribution and detailed indexes all"
	exec db2 "terminate"

    return 0
}