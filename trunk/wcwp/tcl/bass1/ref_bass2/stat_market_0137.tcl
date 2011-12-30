#======================================================================================
#版权归属：Copyright (c) 2010,AsiaInfo.Report.System
#程序名称: stat_market_0137.tcl
#程序功能: 日/周报表：全球通全网统一套餐客户办理情况
#分析目标: 只抓取在网的用户情况
#全球通全网统一套餐客户办理量（单位个） 
#其中新增客户办理量（单位个）           
#其中存量客户办理量（单位个）           
#其他全球通套餐客户办理量（单位个）     
#其中新增客户办理量（单位个）           
#其中存量客户办理量（单位个）           
#通过短信办理数量（单位个）             
#通过10086热线办理数量（单位个）        
#通过营业厅办理数量（单位个）           
#通过WAP掌上营业厅办理数量（单位个）    
#通过网站办理数量（单位个）             
#上网套餐-58元 办理数量                 
#上网套餐-88元 办理数量                 
#上网套餐-128元 办理数量                
#商旅套餐-58元 办理数量                 
#商旅套餐-88元 办理数量                 
#商旅套餐-128元 办理数量                
#商旅套餐-158元 办理数量                
#商旅套餐-188元 办理数量                
#商旅套餐-288元 办理数量                
#商旅套餐-388元 办理数量                
#商旅套餐-588元 办理数量                
#商旅套餐-888元 办理数量                
#本地套餐-58元 办理数量                 
#本地套餐-88元 办理数量                 
#本地套餐-128元 办理数量                
#短信包 办理数量                        
#彩信包 办理数量                        
#全球通尊享包 办理数量                  
#全球通阅读包 办理数量                  
#全球通音乐包 办理数量                  
#全球通凤凰资讯包 办理数量              
#分析维度: 全区
#运行粒度: 日
#运行示例: crt_basetab.sh stat_market_0137.tcl 2011-06-12
#创建时间: 2011-5-31
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

    if {[stat_market_0137 $p_optime] != 0} {
        aidb_roll $conn
        aidb_close $handle
        return -1
    }

    aidb_commit $conn
    aidb_close $handle

    return 0
 }

proc stat_market_0137 {p_optime} {
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


    #Step1.创建结果临时表
    set sql_buf "
    declare global temporary table session.stat_market_0137_mid
    (
			time_id          date,
			city_id          varchar(7),
			result1          integer,
			result2          integer,
			result3          integer,
			result4          integer,
			result5          integer,
			result6          integer,
			result7          integer,
			result8          integer,
			result9          integer,
			result10         integer,
			result11         integer,
			result12         integer,
			result13         integer,
			result14         integer,
			result15         integer,
			result16         integer,
			result17         integer,
			result18         integer,
			result19         integer,
			result20         integer,
			result21         integer,
			result22         integer,
			result23         integer,
			result24         integer,
			result25         integer,
			result26         integer,
			result27         integer,
			result28         integer,
			result29         integer,
			result30         integer,
			result31         integer,
			result32         integer
    )
    partitioning key(time_id) using hashing
    with replace on commit preserve rows not logged in tbs_user_temp
    "
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1010
        puts "errmsg:$errmsg"
        return -1
    }


    #抓取每个指标值插入中间表
    #1、全球通全网统一套餐客户办理量（单位个）
    #2、其中新增客户办理量（单位个）
    #3、其中存量客户办理量（单位个）
    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result1
				,result2
				,result3
    )
    select $date_optime,
           c.city_id,
		       count(distinct product_instance_id),
		       count(distinct case when a.valid_type=1 then product_instance_id  end),
		       count(distinct product_instance_id)-count(distinct case when a.valid_type=1 then product_instance_id  end)
		from 
		   (
        select a.*
					,row_number()over(partition by a.product_instance_id order by a.offer_id ) rn 
					from bass2.dw_product_ins_off_ins_prod_ds a,		
		           bass2.dim_qqt_offer_id b
					where a.offer_id=b.offer_id
					  and b.index=1
					  and date(a.create_date)=${date_optime}
					  and date(a.expire_date)>${date_optime}
			  ) a,
			  bass2.dw_product_$year$month$day c
		where a.product_instance_id=c.user_id
		  and a.rn=1
	group by c.city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    #4、其他全球通套餐客户办理量（单位个）
    #5、其中新增客户办理量（单位个）
    #6、其中存量客户办理量（单位个）
    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result4
				,result5
				,result6
    )
    select $date_optime,
           c.city_id,
		       count(distinct product_instance_id),
		       count(distinct case when a.valid_type=1 then product_instance_id  end),
		       count(distinct product_instance_id)-count(distinct case when a.valid_type=1 then product_instance_id  end)
		from bass2.dw_product_ins_off_ins_prod_ds a,
		     bass2.dim_qqt_offer_id b,
		     bass2.dw_product_$year$month$day c
		where a.product_instance_id=c.user_id
		  and a.offer_id=b.offer_id
		  and b.index=2
		  and date(a.create_date)=${date_optime}
		  and date(a.expire_date)>${date_optime}
	group by c.city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    #7、通过短信办理数量（单位个）                
    #8、通过10086热线办理数量（单位个）           
    #9、通过营业厅办理数量（单位个）              
    #10、通过WAP掌上营业厅办理数量（单位个）       
    #11、通过网站办理数量（单位个）  
    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result7
				,result8
				,result9
				,result10
				,result11
    )
    select $date_optime,
           e.city_id,
			     count(distinct case when a.op_id = 10000047 then a.product_instance_id end),
			     count(distinct case when a.org_id=11111124 then a.product_instance_id end),
			     count(distinct case when a.op_id not in  (10000047,10000475) and a.org_id<>11111124 then a.product_instance_id end),
			     0,
			     count(distinct case when a.op_id = 10000475 then a.product_instance_id end)
			from bass2.dw_product_ord_cust_dm_${op_month} a,
			     bass2.dw_product_ins_off_ins_prod_ds b,
			     bass2.dw_product_ord_offer_dm_${op_month} c,
			     bass2.dim_qqt_offer_id d,
			     bass2.dw_product_$year$month$day e
			where a.product_instance_id=e.user_id
			  and a.product_instance_id=b.product_instance_id
			  and a.customer_order_id=c.customer_order_id
			  and b.offer_id=d.offer_id
			  and d.offer_id=c.offer_id
			  and date(b.create_date)=${date_optime}
			  and a.op_time=${date_optime}
			  and c.op_time=${date_optime}
			  and c.state=1
			  and d.index=1
		 group by e.city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



    #12、上网套餐-58元 办理数量
    #13、上网套餐-88元 办理数量
    #14、上网套餐-128元 办理数量
    #15、商旅套餐-58元 办理数量  
		#16、商旅套餐-88元 办理数量  
		#17、商旅套餐-128元 办理数量 
		#18、商旅套餐-158元 办理数量 
		#19、商旅套餐-188元 办理数量 
		#20、商旅套餐-288元 办理数量 
		#21、商旅套餐-388元 办理数量 
		#22、商旅套餐-588元 办理数量 
		#23、商旅套餐-888元 办理数量 
    #24、本地套餐-58元 办理数量    
    #25、本地套餐-88元 办理数量    
    #26、本地套餐-128元 办理数量     		
    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result12
				,result13
				,result14
				,result15
				,result16
				,result17
				,result18
				,result19
				,result20
				,result21
				,result22
				,result23
				,result24
				,result25
				,result26
    )
			select $date_optime,
			       a.city_id,
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
            count(distinct case when  a.offer_id in (111090001345,111090001362) then  a.product_instance_id end)
			 from (
			 select a.product_instance_id,c.city_id,a.offer_id,a.create_date,a.expire_date,a.valid_date
             ,row_number() over(partition by a.product_instance_id order by a.create_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_ds a,
              bass2.dim_qqt_offer_id b,
              bass2.dw_product_$year$month$day c
        where a.product_instance_id=c.user_id
          and a.offer_id=b.offer_id
          and b.index=1
          and date(a.create_date)=${date_optime}
			    and date(a.expire_date)>${date_optime}
            ) a
			where a.row_id=1
	  group by a.city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }






    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result27
				,result28
				,result29
				,result30
				,result31
				,result32
    )
			select $date_optime,
			       a.city_id,
            count(distinct case when  a.offer_id in (111090001363) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001364) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001365) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001366) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001367) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001368) then  a.product_instance_id end)
			 from (
			 select a.product_instance_id,c.city_id,a.offer_id,a.create_date,a.expire_date,a.valid_date
             ,row_number() over(partition by a.product_instance_id order by a.create_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_ds a,
              bass2.dw_product_$year$month$day c
        where a.product_instance_id=c.user_id
          and c.test_mark<>1
          and date(a.create_date)=${date_optime}
			    and date(a.expire_date)>${date_optime}
            ) a
			where a.row_id=1
	  group by a.city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }







###生成一条全区的数据记录---------------------
    #抓取每个指标值插入中间表
    #1、全球通全网统一套餐客户办理量（单位个）
    #2、其中新增客户办理量（单位个）
    #3、其中存量客户办理量（单位个）
    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result1
				,result2
				,result3
    )
    select $date_optime,
           '890',
		       count(distinct a.product_instance_id),
		       count(distinct case when a.valid_type=1 then a.product_instance_id  end),
		       count(distinct a.product_instance_id)-count(distinct case when a.valid_type=1 then a.product_instance_id  end)
		from 
		  (
        select a.*
					,row_number()over(partition by a.product_instance_id order by a.offer_id ) rn 
					from bass2.dw_product_ins_off_ins_prod_ds a,		
		           bass2.dim_qqt_offer_id b
					where a.offer_id=b.offer_id
					  and b.index=1
					  and date(a.create_date)=${date_optime}
					  and date(a.expire_date)>${date_optime}
			  ) a
		where a.rn=1
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    #4、其他全球通套餐客户办理量（单位个）
    #5、其中新增客户办理量（单位个）
    #6、其中存量客户办理量（单位个）
    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result4
				,result5
				,result6
    )
    select $date_optime,
           '890',
		       count(distinct a.product_instance_id),
		       count(distinct case when a.valid_type=1 then a.product_instance_id  end),
		       count(distinct a.product_instance_id)-count(distinct case when a.valid_type=1 then a.product_instance_id  end)
		from (
        select a.*
					,row_number()over(partition by a.product_instance_id order by a.offer_id ) rn 
					from bass2.dw_product_ins_off_ins_prod_ds a,		
		           bass2.dim_qqt_offer_id b
					where a.offer_id=b.offer_id
					  and b.index=2
					  and date(a.create_date)=${date_optime}
					  and date(a.expire_date)>${date_optime}
			  ) a
		where a.rn=1
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    #7、通过短信办理数量（单位个）                
    #8、通过10086热线办理数量（单位个）           
    #9、通过营业厅办理数量（单位个）              
    #10、通过WAP掌上营业厅办理数量（单位个）       
    #11、通过网站办理数量（单位个）  
    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result7
				,result8
				,result9
				,result10
				,result11
    )
    select $date_optime,
           '890',
			     count(distinct case when a.op_id = 10000047 then a.product_instance_id end),
			     count(distinct case when a.org_id=11111124 then a.product_instance_id end),
			     count(distinct case when a.op_id not in  (10000047,10000475) and a.org_id<>11111124 then a.product_instance_id end),
			     0,
			     count(distinct case when a.op_id = 10000475 then a.product_instance_id end)
			from bass2.dw_product_ord_cust_dm_${op_month} a,
			     bass2.dw_product_ins_off_ins_prod_ds b,
			     bass2.dw_product_ord_offer_dm_${op_month} c,
			     bass2.dim_qqt_offer_id d
			where a.product_instance_id=b.product_instance_id
			  and a.customer_order_id=c.customer_order_id
			  and b.offer_id=d.offer_id
			  and d.offer_id=c.offer_id
			  and date(b.create_date)=${date_optime}
			  and a.op_time=${date_optime}
			  and c.op_time=${date_optime}
			  and c.state=1
			  and d.index=1
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



    #12、上网套餐-58元 办理数量
    #13、上网套餐-88元 办理数量
    #14、上网套餐-128元 办理数量
    #15、商旅套餐-58元 办理数量  
		#16、商旅套餐-88元 办理数量  
		#17、商旅套餐-128元 办理数量 
		#18、商旅套餐-158元 办理数量 
		#19、商旅套餐-188元 办理数量 
		#20、商旅套餐-288元 办理数量 
		#21、商旅套餐-388元 办理数量 
		#22、商旅套餐-588元 办理数量 
		#23、商旅套餐-888元 办理数量 
    #24、本地套餐-58元 办理数量    
    #25、本地套餐-88元 办理数量    
    #26、本地套餐-128元 办理数量     		
    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result12
				,result13
				,result14
				,result15
				,result16
				,result17
				,result18
				,result19
				,result20
				,result21
				,result22
				,result23
				,result24
				,result25
				,result26
    )
			select $date_optime,
			       '890',
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
            count(distinct case when  a.offer_id in (111090001345,111090001362) then  a.product_instance_id end)
			 from (
			 select a.product_instance_id,a.offer_id,a.create_date,a.expire_date,a.valid_date
             ,row_number() over(partition by a.product_instance_id order by a.create_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_ds a,
              bass2.dim_qqt_offer_id b
        where a.offer_id=b.offer_id
          and b.index=1
          and date(a.create_date)=${date_optime}
			    and date(a.expire_date)>${date_optime}
            ) a
			where a.row_id=1
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }





    set sql_buf "
    insert into session.stat_market_0137_mid
    (
				time_id
				,city_id
				,result27
				,result28
				,result29
				,result30
				,result31
				,result32
    )
			select $date_optime,
			       '890',
            count(distinct case when  a.offer_id in (111090001363) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001364) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001365) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001366) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001367) then  a.product_instance_id end),
            count(distinct case when  a.offer_id in (111090001368) then  a.product_instance_id end)
			 from (
			 select a.product_instance_id,c.city_id,a.offer_id,a.create_date,a.expire_date,a.valid_date
             ,row_number() over(partition by a.product_instance_id order by a.create_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_ds a,
              bass2.dw_product_$year$month$day c
        where a.product_instance_id=c.user_id
          and c.test_mark<>1
          and date(a.create_date)=${date_optime}
			    and date(a.expire_date)>${date_optime}
            ) a
			where a.row_id=1
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


	# #===============================================================================================
	# # step : 删除结果集当日数据
	# #===============================================================================================
		set sql_buf "delete from bass2.stat_market_0137 where time_id = ${date_optime}"
		puts ${sql_buf}
		if [ catch { aidb_sql $handle $sql_buf } errmsg ] {
			trace_sql $errmsg 1008
			puts "errmsg:$errmsg"
			return -1
		}
		aidb_commit $conn



	##=====================================================
	## step 2: 生成结果数据 完成全区各品牌的抓取
	##=====================================================
    set sql_buf "
    insert into bass2.stat_market_0137
    select 
				$date_optime,
				 city_id
				,sum(value(result1,0))
				,sum(value(result2,0))
				,sum(value(result3,0))
				,sum(value(result4,0))
				,sum(value(result5,0))
				,sum(value(result6,0))
				,sum(value(result7,0))
				,sum(value(result8,0))
				,sum(value(result9,0))
				,sum(value(result10,0))
				,sum(value(result11,0))
				,sum(value(result12,0))
				,sum(value(result13,0))
				,sum(value(result14,0))
				,sum(value(result15,0))
				,sum(value(result16,0))
				,sum(value(result17,0))
				,sum(value(result18,0))
				,sum(value(result19,0))
				,sum(value(result20,0))
				,sum(value(result21,0))
				,sum(value(result22,0))
				,sum(value(result23,0))
				,sum(value(result24,0))
				,sum(value(result25,0))
				,sum(value(result26,0))
				,sum(value(result27,0))
				,sum(value(result28,0))
				,sum(value(result29,0))
				,sum(value(result30,0))
				,sum(value(result31,0))
				,sum(value(result32,0))
		  from session.stat_market_0137_mid
		 group by city_id
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



  #Step.收集信息
	exec db2 "connect to bassdb user bass2 using bass2"
	exec db2 "runstats on table bass2.stat_market_0137 with distribution and detailed indexes all"
	exec db2 "terminate"

    return 0
}


