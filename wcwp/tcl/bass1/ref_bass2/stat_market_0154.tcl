#======================================================================================
#版权归属：Copyright (c) 2010,AsiaInfo.Report.System
#程序名称: stat_market_0154.tcl
#程序功能: 月报表：全球通全网统一套餐客户办理情况
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
#运行粒度: 月
#运行示例: crt_basetab.sh stat_market_0154.tcl 2011-07-01
#创建时间: 2011-7-20
#创 建 人: Asiainfo-Linkage
#存在问题:
#修改历史:短信包/彩信包...此代码逻辑上存在问题，通过表哥容许进行修正 2011-8-11 加上offer_id限制条件
#=======================================================================================
proc deal {p_optime p_timestamp} {
    global conn
    global handle

    if [catch {set handle [aidb_open $conn]} errmsg] {
        trace_sql $errmsg 1000
        return -1
    }

    if {[stat_market_0154 $p_optime] != 0} {
        aidb_roll $conn
        aidb_close $handle
        return -1
    }

    aidb_commit $conn
    aidb_close $handle

    return 0
 }

proc stat_market_0154 {p_optime} {
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
	  #获取数据月份当月末日yyyymm31
	  set last_month_date [ clock format [ clock scan "${nt_month_01} - 1 days" ] -format "%Y-%m-%d" ]



    #1/清空全球通全网统一套餐客户办理用户清单临时表
    #stat_market_0155报表将会用到这些清单进行收入统计
    set sql_buf "alter table bass2.stat_market_0154_user_01 activate not logged initially with empty table;"
    
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    #插入用户清单列表
    set sql_buf "
    insert into BASS2.stat_market_0154_user_01
     (
     	user_id,
     	city_id,
     	offer_id,
     	fact_fee,
     	call_duration_m
     )
    select distinct 
           a.product_instance_id,
		       c.city_id,
		       a.offer_id,
		       c.fact_fee,
		       c.call_duration_m
		  from
		     (
		   select a.*
					,row_number()over(partition by a.product_instance_id order by a.offer_id ) rn
					from bass2.dw_product_ins_off_ins_prod_$op_month a,
		           bass2.dim_qqt_offer_id b
				where a.offer_id=b.offer_id
				  and b.index=1
				  and date(a.create_date)<=date('${last_month_date}')
				  and date(a.expire_date)>=${date_optime}
				 ) a,
				 bass2.dw_product_$op_month c
	where a.product_instance_id=c.user_id
		and a.rn=1
		and c.usertype_id in (1,2,9)
		and c.userstatus_id in (1,2,3,6,8)
	  and c.test_mark<>1
  ;"
    
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }

    
    #2/清空全球通全网统一套餐存量客户办理用户清单临时表
    #stat_market_0155报表将会用到这些清单进行收入统计
    set sql_buf "alter table bass2.stat_market_0154_user_02 activate not logged initially with empty table;"
    
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    #插入用户清单列表
    set sql_buf "
    insert into BASS2.stat_market_0154_user_02
     (
     	user_id,
     	city_id,
     	offer_id,
     	fact_fee,
     	call_duration_m
     )
    select distinct 
           a.product_instance_id,
		       c.city_id,
		       a.offer_id,
		       c.fact_fee,
		       c.call_duration_m
		  from
		     (
		   select a.*
					,row_number()over(partition by a.product_instance_id order by a.offer_id ) rn 
					from bass2.dw_product_ins_off_ins_prod_$op_month a,
		           bass2.dim_qqt_offer_id b
				where a.offer_id=b.offer_id
				  and b.index=1
				  and date(a.create_date)<=date('${last_month_date}')
				  and date(a.expire_date)>=${date_optime}
				 ) a,
				 bass2.dw_product_$op_month c,
				 bass2.dim_qqt_month d
	where a.product_instance_id=c.user_id
		and a.rn=1
		and a.valid_date=d.month_id
		and a.create_date<>a.valid_date
		and c.usertype_id in (1,2,9)
		and c.userstatus_id in (1,2,3,6,8)
	  and c.test_mark<>1		
  ;"
    
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



    #3/清空其他全球通套餐客户办理用户清单临时表
    #stat_market_0155报表将会用到这些清单进行收入统计
    set sql_buf "alter table bass2.stat_market_0154_user_03 activate not logged initially with empty table;"
    
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    #插入用户清单列表
    set sql_buf "
    insert into BASS2.stat_market_0154_user_03
     (
     	user_id,
     	city_id,
     	offer_id,
     	fact_fee,
     	call_duration_m
     )
    select distinct 
           a.product_instance_id,
		       c.city_id,
		       a.offer_id,
		       c.fact_fee,
		       c.call_duration_m
		  from
		     (
		   select a.*
					,row_number()over(partition by a.product_instance_id order by a.offer_id ) rn 
					from bass2.dw_product_ins_off_ins_prod_$op_month a,
		           bass2.dim_qqt_offer_id b
				where a.offer_id=b.offer_id
				  and b.index=2
				  and date(a.create_date)<=date('${last_month_date}')
				  and date(a.expire_date)>=${date_optime}
				 ) a,
				 bass2.dw_product_$op_month c
	where a.product_instance_id=c.user_id
		and a.rn=1
		and c.usertype_id in (1,2,9)
		and c.userstatus_id in (1,2,3,6,8)
	  and c.test_mark<>1
  ;"
    
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



    #4/清空其它全球通全网统一套餐存量客户办理用户清单临时表
    #stat_market_0155报表将会用到这些清单进行收入统计
    set sql_buf "alter table bass2.stat_market_0154_user_04 activate not logged initially with empty table;"
    
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    #插入用户清单列表
    set sql_buf "
    insert into BASS2.stat_market_0154_user_04
     (
     	user_id,
     	city_id,
     	offer_id,
     	fact_fee,
     	call_duration_m
     )
    select distinct 
           a.product_instance_id,
		       c.city_id,
		       a.offer_id,
		       c.fact_fee,
		       c.call_duration_m
		  from
		     (
		   select a.*
					,row_number()over(partition by a.product_instance_id order by a.offer_id ) rn
					from bass2.dw_product_ins_off_ins_prod_$op_month a,
		           bass2.dim_qqt_offer_id b
				where a.offer_id=b.offer_id
				  and b.index=2
				  and date(a.create_date)<=date('${last_month_date}')
				  and date(a.expire_date)>=${date_optime}
				 ) a,
				 bass2.dw_product_$op_month c,
				 bass2.dim_qqt_month d
	where a.product_instance_id=c.user_id
		and a.rn=1
		and a.valid_date=d.month_id
		and a.create_date<>a.valid_date
		and c.usertype_id in (1,2,9)
		and c.userstatus_id in (1,2,3,6,8)
	  and c.test_mark<>1
  ;"
    
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }



    #创建结果临时表
    set sql_buf "
    declare global temporary table session.stat_market_0154_mid
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
			result27         integer
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


    
    #------------------------------------------------------------------------------
    #1、全球通全网统一套餐客户办理量（单位个）
    #2、其中新增客户办理量（单位个）
    #3、其中存量客户办理量（单位个）
    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result1
    )
    select $date_optime,
           city_id,
		       count(user_id)
		from BASS2.stat_market_0154_user_01
	group by city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result3
    )
    select $date_optime,
           city_id,
		       count(user_id)
		from BASS2.stat_market_0154_user_02
	group by city_id
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
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result4
    )
    select $date_optime,
           city_id,
		       count(user_id)
		from BASS2.stat_market_0154_user_03
	group by city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result6
    )
    select $date_optime,
           city_id,
		       count(user_id)
		from BASS2.stat_market_0154_user_04
	group by city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }




    #07、上网套餐-58元 办理数量
    #08、上网套餐-88元 办理数量
    #09、上网套餐-128元 办理数量
    #10、商旅套餐-58元 办理数量  
		#11、商旅套餐-88元 办理数量  
		#12、商旅套餐-128元 办理数量 
		#13、商旅套餐-158元 办理数量 
		#14、商旅套餐-188元 办理数量 
		#15、商旅套餐-288元 办理数量 
		#16、商旅套餐-388元 办理数量 
		#17、商旅套餐-588元 办理数量 
		#18、商旅套餐-888元 办理数量 
    #19、本地套餐-58元 办理数量    
    #20、本地套餐-88元 办理数量    
    #21、本地套餐-128元 办理数量

    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result7
				,result8
				,result9
				,result10
				,result11
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
    )
   select $date_optime,
          city_id,
          count(distinct case when  offer_id in (111090001348,111090001331) then  user_id end),
          count(distinct case when  offer_id in (111090001349,111090001332) then  user_id end),
          count(distinct case when  offer_id in (111090001350,111090001333) then  user_id end),
          count(distinct case when  offer_id in (111090001334,111090001351) then  user_id end),
          count(distinct case when  offer_id in (111090001335,111090001352) then  user_id end),
          count(distinct case when  offer_id in (111090001336,111090001353) then  user_id end),
          count(distinct case when  offer_id in (111090001337,111090001354) then  user_id end),
          count(distinct case when  offer_id in (111090001338,111090001355) then  user_id end),
          count(distinct case when  offer_id in (111090001339,111090001356) then  user_id end),
          count(distinct case when  offer_id in (111090001340,111090001357) then  user_id end),
          count(distinct case when  offer_id in (111090001341,111090001358) then  user_id end),
          count(distinct case when  offer_id in (111090001342,111090001359) then  user_id end),
          count(distinct case when  offer_id in (111090001343,111090001360) then  user_id end),
          count(distinct case when  offer_id in (111090001344,111090001361) then  user_id end),
          count(distinct case when  offer_id in (111090001345,111090001362) then  user_id end)
		from BASS2.stat_market_0154_user_01
	group by city_id
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


		#22.短信包
		#23.彩信包
		#24.全球通尊享包
		#25.全球通阅读包
		#26.全球通音乐包
		#27.全球通凤凰资讯包
    set sql_buf "
    insert into session.stat_market_0154_mid
     (
				time_id
				,city_id
				,result22
				,result23
				,result24
				,result25
				,result26
				,result27
      )
			select $date_optime,
			       a.city_id,
            count(distinct case when  offer_id in (111090001363) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001364) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001365) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001366) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001367) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001368) then  product_instance_id end)
			 from (
			 select product_instance_id,c.city_id,a.offer_id,a.create_date,a.expire_date,a.valid_date
             ,row_number() over(partition by a.product_instance_id order by a.create_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_$op_month a,
              bass2.dw_product_$op_month c
        where a.product_instance_id=c.user_id
				  and date(a.create_date)<=date('${last_month_date}')
				  and date(a.expire_date)>=${date_optime}
			    and c.test_mark<>1
			    and a.offer_id in (111090001363,111090001364,111090001365,111090001366,111090001367,111090001368)
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



####生成全区的一条记录

   
    #------------------------------------------------------------------------------
    #1、全球通全网统一套餐客户办理量（单位个）
    #2、其中新增客户办理量（单位个）
    #3、其中存量客户办理量（单位个）
    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result1
    )
    select $date_optime,
           '890',
		       count(user_id)
		from BASS2.stat_market_0154_user_01
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result3
    )
    select $date_optime,
           '890',
		       count(user_id)
		from BASS2.stat_market_0154_user_02
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
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result4
    )
    select $date_optime,
           '890',
		       count(user_id)
		from BASS2.stat_market_0154_user_03
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result6
    )
    select $date_optime,
           '890',
		       count(user_id)
		from BASS2.stat_market_0154_user_04
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }




    #07、上网套餐-58元 办理数量
    #08、上网套餐-88元 办理数量
    #09、上网套餐-128元 办理数量
    #10、商旅套餐-58元 办理数量  
		#11、商旅套餐-88元 办理数量  
		#12、商旅套餐-128元 办理数量 
		#13、商旅套餐-158元 办理数量 
		#14、商旅套餐-188元 办理数量 
		#15、商旅套餐-288元 办理数量 
		#16、商旅套餐-388元 办理数量 
		#17、商旅套餐-588元 办理数量 
		#18、商旅套餐-888元 办理数量 
    #19、本地套餐-58元 办理数量    
    #20、本地套餐-88元 办理数量    
    #21、本地套餐-128元 办理数量

    set sql_buf "
    insert into session.stat_market_0154_mid
    (
				time_id
				,city_id
				,result7
				,result8
				,result9
				,result10
				,result11
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
    )
   select $date_optime,
          '890',
          count(distinct case when  offer_id in (111090001348,111090001331) then  user_id end),
          count(distinct case when  offer_id in (111090001349,111090001332) then  user_id end),
          count(distinct case when  offer_id in (111090001350,111090001333) then  user_id end),
          count(distinct case when  offer_id in (111090001334,111090001351) then  user_id end),
          count(distinct case when  offer_id in (111090001335,111090001352) then  user_id end),
          count(distinct case when  offer_id in (111090001336,111090001353) then  user_id end),
          count(distinct case when  offer_id in (111090001337,111090001354) then  user_id end),
          count(distinct case when  offer_id in (111090001338,111090001355) then  user_id end),
          count(distinct case when  offer_id in (111090001339,111090001356) then  user_id end),
          count(distinct case when  offer_id in (111090001340,111090001357) then  user_id end),
          count(distinct case when  offer_id in (111090001341,111090001358) then  user_id end),
          count(distinct case when  offer_id in (111090001342,111090001359) then  user_id end),
          count(distinct case when  offer_id in (111090001343,111090001360) then  user_id end),
          count(distinct case when  offer_id in (111090001344,111090001361) then  user_id end),
          count(distinct case when  offer_id in (111090001345,111090001362) then  user_id end)
		from BASS2.stat_market_0154_user_01
 ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }


		#22.短信包
		#23.彩信包
		#24.全球通尊享包
		#25.全球通阅读包
		#26.全球通音乐包
		#27.全球通凤凰资讯包
    set sql_buf "
    insert into session.stat_market_0154_mid
     (
				time_id
				,city_id
				,result22
				,result23
				,result24
				,result25
				,result26
				,result27
      )
			select $date_optime,
			       '890',
            count(distinct case when  offer_id in (111090001363) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001364) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001365) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001366) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001367) then  product_instance_id end),
            count(distinct case when  offer_id in (111090001368) then  product_instance_id end)
			 from (
			 select product_instance_id,c.city_id,a.offer_id,a.create_date,a.expire_date,a.valid_date
             ,row_number() over(partition by a.product_instance_id order by a.create_date desc) row_id
         from bass2.dw_product_ins_off_ins_prod_$op_month a,
              bass2.dw_product_$op_month c
        where a.product_instance_id=c.user_id
				  and date(a.create_date)<=date('${last_month_date}')
				  and date(a.expire_date)>=${date_optime}
			    and c.test_mark<>1
			    and a.offer_id in (111090001363,111090001364,111090001365,111090001366,111090001367,111090001368)
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
		set sql_buf "delete from bass2.stat_market_0154 where time_id = ${date_optime}"
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
    insert into bass2.stat_market_0154
    select 
				$date_optime
				,city_id
				,sum(value(result1,0))
				,sum(value(result1,0))-sum(value(result3,0))
				,sum(value(result3,0))
				,sum(value(result4,0))
				,sum(value(result4,0))-sum(value(result6,0))
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
		  from session.stat_market_0154_mid
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
	exec db2 "runstats on table bass2.stat_market_0154 with distribution and detailed indexes all"
	exec db2 "terminate"

    return 0
}


