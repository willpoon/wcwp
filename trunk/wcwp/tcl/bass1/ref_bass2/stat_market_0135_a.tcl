#======================================================================================
#版权归属：Copyright (c) 2011,AsiaInfo.Report.System
#程序名称: stat_market_0135_a.tcl
#程序功能: 违规预警模型-窜卡
#分析目标: 略
#分析指标: 见维表
#分析维度: 全区 地市 区县 渠道
#运行粒度: 月
#运行示例: crt_basetab.sh stat_market_0135_a.tcl 2011-04-01
#创建时间: 2011-5-10
#创 建 人: Asiainfo-Linkage HuangBo
#存在问题:
#修改历史: 1、2011-6-23 增加明细数据链接
#=======================================================================================
proc deal {p_optime p_timestamp} {
    global conn
    global handle

    if [catch {set handle [aidb_open $conn]} errmsg] {
        trace_sql $errmsg 1000
        return -1
    }

    if {[stat_market_0135_a $p_optime] != 0} {
        aidb_roll $conn
        aidb_close $handle
        return -1
    }

    aidb_commit $conn
    aidb_close $handle

    return 0
}

proc stat_market_0135_a {p_optime} {
    global conn
    global handle

    source stat_insert_index.tcl
    source report.cfg

    #日期处理
    set    date_optime      [ai_to_date $p_optime]
    scan   $p_optime        "%04s-%02s-%02s" year month day
    set    month_start      "$year-$month-01"
    set    month_end        [clock format [clock scan "$month_start + 1month - 1day"] -format "%Y-%m-%d"]
    set    last2month       [clock format [clock scan "$month_start - 2month"] -format "%Y-%m-%d"]
    set    last1month       [clock format [clock scan "$month_start - 1month"] -format "%Y-%m-%d"]
    scan   $month_end       "%04s-%02s-%02s" meyear memonth meday
    scan   $last1month      "%04s-%02s-%02s" l1myear l1mmonth l1mday

    #源表
    set dw_product_yyyymm           "bass2.dw_product_$year$month"
    set dw_call_yyyymm              "bass2.dw_call_$year$month"
    set dw_thorpe_user_detail_mm    "bass2.dw_thorpe_user_detail_mm"

    #目标表
    set stat_market_0135_a          "bass2.stat_market_0135_a"
    set stat_market_0135_a_result   "bass2.stat_market_0135_a_result"
    set stat_market_0135_a_final    "bass2.stat_market_0135_a_final"
    set stat_market_0135_a_link1    "bass2.stat_market_0135_a_link1"
    set stat_market_0135_a_link2    "bass2.stat_market_0135_a_link2"

    #维表
    set dim_pub_region              "bassweb.dim_pub_region"
    set dwd_channel_dept_yyyymmdd   "bass2.dwd_channel_dept_$meyear$memonth$meday"
    set dim_pub_channel_ext         "bassweb.dim_pub_channel_ext"
    set dim_bboss_channel_type      "bass2.dim_bboss_channel_type"
    set dim_pub_city                "bassweb.dim_pub_city"
    set dim_user_status             "bass2.dim_user_status"

    #临时表
    set stat_market_0135_a_t1       "bass2.stat_market_0135_a_t1"
    set stat_market_0135_a_t2       "bass2.stat_market_0135_a_t2"
    set stat_market_0135_a_t3       "bass2.stat_market_0135_a_t3"
    set stat_market_0135_a_t4       "bass2.stat_market_0135_a_t4"
    set stat_market_0135_a_t5       "bass2.stat_market_0135_a_t5"
    set stat_market_0135_a_t6       "bass2.stat_market_0135_a_t6"
    set stat_market_0135_a_t7       "bass2.stat_market_0135_a_t7"
    set stat_market_0135_a_t8       "bass2.stat_market_0135_a_t8"

    #Step1.创建结果临时表
    set sql_buf "drop table $stat_market_0135_a_t1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_a_t2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_a_t3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_a_t4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_a_t5;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_a_t6;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_a_t7;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_a_t8;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    set sql_buf "
    create table $stat_market_0135_a_t1
    (
           channel_id       bigint,
           month_new        integer,
           diffarea_1       integer,
           diffarea_2       integer,
           diffarea_3       integer,
           avg_active_cycle integer,
           increase_rate    decimal(6,2)
    )
    in tbs_ods_other index in tbs_index partitioning key (channel_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1010
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_market_0135_a_t2
    (
           user_id          varchar(20),
           channel_id       bigint,
           county_id        varchar(20),
           month_new_mark   smallint
    )
    in tbs_ods_other index in tbs_index partitioning key (user_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1020
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_market_0135_a_t3
    (
           user_id          varchar(20),
           channel_id       bigint,
           active_cycle     integer
    )
    in tbs_ods_other index in tbs_index partitioning key (user_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1030
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_market_0135_a_t4
    (
           channel_id       bigint,
           time_order       smallint,
           month_new        integer
    )
    in tbs_ods_other index in tbs_index partitioning key (channel_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1040
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_market_0135_a_t5
    (
           entity_id        bigint,
           entity_type      smallint,
           month_new_rate   decimal(9,2)
    )
    in tbs_ods_other index in tbs_index partitioning key (entity_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1050
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_market_0135_a_t6
    (
           entity_id        bigint,
           entity_type      smallint,
           bn_value         decimal(9,2)
    )
    in tbs_ods_other index in tbs_index partitioning key (entity_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1060
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_market_0135_a_t7
    (
           channel_id   bigint,
           user_id      varchar(20)
    )
    in tbs_ods_other index in tbs_index partitioning key (user_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1070
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_market_0135_a_t8
    (
           channel_id   bigint,
           user_id      varchar(20),
           diffarea     varchar(8)
    )
    in tbs_ods_other index in tbs_index partitioning key (user_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1080
        puts "errmsg:$errmsg"
        return -1
    }

    #Step2.生成临时表数据
    set sql_buf "
    insert into $stat_market_0135_a_t2
    (
           user_id,
           channel_id,
           county_id,
           month_new_mark
    )
    select user_id,
           channel_id,
           county_id,
           month_new_mark
      from $dw_product_yyyymm a
     where userstatus_id in (1,2,3,6,8)
       and usertype_id in (1,2,9)
       and test_mark = 0
       and month_new_mark = 1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2010
        puts "errmsg:$errmsg"
        return -1
    }

    ##月发展用户数
    ##网点上月发展用户数量
    set sql_buf "
    insert into $stat_market_0135_a_t1
    (
           channel_id,
           month_new
    )
    select channel_id,
           count(*)
      from $stat_market_0135_a_t2
     group by
           channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2030
        puts "errmsg:$errmsg"
        return -1
    }

    ##异区通话用户
    ##用户通话区域不在该号码开户网点所属区域的通话时长占比
    set sql_buf "
    insert into $stat_market_0135_a_t1
    (
           channel_id,
           diffarea_1
    )
    select a.channel_id,
           count(*)
      from (
            select b.user_id,
                   b.channel_id,
                   sum(case when c.county_id <> b.county_id then a.call_duration else 0 end) * 100.00
                    / sum(a.call_duration) as call_rate
              from $dw_thorpe_user_detail_mm a,
                   $stat_market_0135_a_t2 b,
                   $dim_pub_region c
             where a.user_id = b.user_id
               and a.thorpe_id = c.region_id
               and a.op_time = $date_optime
             group by
                   b.user_id,
                   b.channel_id
           ) a
     where a.call_rate >= 70
     group by
           a.channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2040
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_a_t1
    (
           channel_id,
           diffarea_2,
           diffarea_3
    )
    select b.channel_id,
           count(case when call_rate_1 >= 30 then a.user_id end),
           count(case when call_rate_2 >= 20 then a.user_id end)
      from (
            select user_id,
                   case when sum(case when roamtype_id in (0,1,6) then call_duration else 0 end) = 0
                        then 0
                        else sum(case when roamtype_id in (1,6) then call_duration else 0 end) * 100.00
                                / sum(case when roamtype_id in (0,1,6) then call_duration else 0 end)
                   end as call_rate_1,
                   sum(case when roamtype_id not in (0,1,6) then call_duration else 0 end) * 100.00
                    / sum(call_duration) as call_rate_2
              from $dw_call_yyyymm a
             group by
                   user_id
           ) a,
           $stat_market_0135_a_t2 b
     where a.user_id = b.user_id
     group by
           b.channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2050
        puts "errmsg:$errmsg"
        return -1
    }

    ####异区通话用户明细临时表
    set sql_buf "
    insert into $stat_market_0135_a_t7
    (
           channel_id,
           user_id
    )
    select b.channel_id,
           a.user_id
      from (
            select user_id,
                   sum(case when roamtype_id in (1,6) then call_duration else 0 end) * 100.00
                    / sum(call_duration) as call_rate_1
              from $dw_call_yyyymm a
             where a.roamtype_id in (0,1,6)
             group by
                   user_id
           ) a,
           $stat_market_0135_a_t2 b
     where a.user_id = b.user_id
       and a.call_rate_1 >= 30;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2050
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_a_t8
    (
           channel_id,
           user_id,
           diffarea
    )
    select b.channel_id,
           a.user_id,
           a.roam_city_id
      from (
            select user_id,
                   a.roam_city_id
              from $dw_call_yyyymm a
             where a.roamtype_id in (1,6)
             group by
                   user_id,
                   a.roam_city_id
           ) a,
           $stat_market_0135_a_t7 b
     where a.user_id = b.user_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2050
        puts "errmsg:$errmsg"
        return -1
    }

    ##平均激活周期
    ##网点上月发展用户开户时间与首次通话时间的时间差的平均值
  for {set loopmonth $month_start} {$loopmonth <= $month_end} {
          set loopmonth [clock format [clock scan "${loopmonth} + 1day" ] -format "%Y-%m-%d" ]} {
  #循环开始
    puts $loopmonth
    scan $loopmonth "%04s-%02s-%02s" lpyear lpmonth lpday

    set date_lpmonth    [ai_to_date $loopmonth]

    set dw_product_lpyyyymmdd   "bass2.dw_product_$lpyear$lpmonth$lpday"

    set sql_buf "
    insert into $stat_market_0135_a_t3
    (
           user_id,
           channel_id,
           active_cycle
    )
    select user_id,
           channel_id,
           $date_lpmonth - create_date
      from $dw_product_lpyyyymmdd
     where day_call_mark = 1
       and month_new_mark = 1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }

  #循环结束
  }

    set sql_buf "
    insert into $stat_market_0135_a_t1
    (
           channel_id,
           avg_active_cycle
    )
    select a.channel_id,
           avg(active_cycle)
      from (
            select user_id,
                   channel_id,
                   min(active_cycle) as active_cycle
              from $stat_market_0135_a_t3
             group by
                   user_id,
                   channel_id
           ) a
     group by
           a.channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2060
        puts "errmsg:$errmsg"
        return -1
    }

    ##放号量增幅
    ##网点上月发展用户数量除以前三个月（N，N-1，N-2）发展用户数量平均值
  for {set loopmonth $month_start;set time_order "1"} {$time_order <= 3} {
          set loopmonth [clock format [clock scan "${loopmonth} - 1month"] -format "%Y-%m-%d"];incr time_order +1} {
  #循环开始
    puts $loopmonth
    scan $loopmonth "%04s-%02s-%02s" lpyear lpmonth lpday

    set date_lpmonth    [ai_to_date $loopmonth]

    set dw_product_lpyyyymm   "bass2.dw_product_$lpyear$lpmonth"

    set sql_buf "
    insert into $stat_market_0135_a_t4
    (
           channel_id,
           time_order,
           month_new
    )
    select channel_id,
           $time_order,
           count(*)
      from $dw_product_lpyyyymm
     where month_new_mark = 1
       and userstatus_id in (1,2,3,6,8)
       and usertype_id in (1,2,9)
       and test_mark = 0
     group by
           channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2070
        puts "errmsg:$errmsg"
        return -1
    }

  #循环结束
  }

    set sql_buf "
    insert into $stat_market_0135_a_t1
    (
           channel_id,
           increase_rate
    )
    select a.channel_id,
           sum(case when time_order = 1 then month_new else 0 end) * 100.00
            / avg(month_new)
      from $stat_market_0135_a_t4 a
     group by
           a.channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2080
        puts "errmsg:$errmsg"
        return -1
    }

    #Step3.清除结果表历史数据
    set sql_buf "delete from $stat_market_0135_a where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3010
        puts "errmsg:$errmsg"
        return -1
    }

    #Step4.生成结果表数据
    set sql_buf "
    insert into $stat_market_0135_a
    (
           op_time          ,
           channel_id       ,
           county_id        ,
           city_id          ,
           month_new        ,
           diffarea_1       ,
           diffarea_2       ,
           diffarea_3       ,
           avg_active_cycle ,
           increase_rate
    )
    select $year$month,
           b.channel_id,
           char(a.region_type),
           char(a.channel_city),
           sum(value(b.month_new,0)),
           sum(value(b.diffarea_1,0)),
           sum(value(b.diffarea_2,0)),
           sum(value(b.diffarea_3,0)),
           sum(value(b.avg_active_cycle,0)),
           sum(value(b.increase_rate,0))
      from $dwd_channel_dept_yyyymmdd a,
           $stat_market_0135_a_t1 b
     where a.organize_id = b.channel_id
     group by
           b.channel_id,
           char(a.region_type),
           char(a.channel_city);"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 4010
        puts "errmsg:$errmsg"
        return -1
    }

    exec db2 "connect to bassdb user bass2 using bass2"
    exec db2 "runstats on table $stat_market_0135_a with distribution and indexes all"
    exec db2 "terminate"

    #Step5.窜卡流程
    ##区域内新增用户增幅
    ####渠道
    set sql_buf "
    insert into $stat_market_0135_a_t5
    (
           entity_id,
           entity_type,
           month_new_rate
    )
    select value(a.channel_id,b.channel_id),
           1,
           case when a.channel_id is null then 0
                when b.channel_id is null then 100
                when b.month_new = 0 then 100
                else a.month_new * 100.00 / b.month_new
           end
      from (
            select a.channel_id,
                   sum(b.month_new) month_new
              from $dwd_channel_dept_yyyymmdd a,
                   $stat_market_0135_a b,
                   $dim_bboss_channel_type c
             where a.organize_id = b.channel_id
               and a.channel_type = c.code_type
               and a.dept_type_dtl = c.code_id
               and c.code_name in ('特约代理点','指定专营店')
               and a.rec_status = 0
               and b.op_time = $year$month
             group by
                   a.channel_id
           ) a
      full join
           (
            select a.channel_id,
                   sum(b.month_new) month_new
              from $dwd_channel_dept_yyyymmdd a,
                   $stat_market_0135_a b,
                   $dim_bboss_channel_type c
             where a.organize_id = b.channel_id
               and a.channel_type = c.code_type
               and a.dept_type_dtl = c.code_id
               and c.code_name in ('特约代理点','指定专营店')
               and a.rec_status = 0
               and b.op_time = $l1myear$l1mmonth
             group by
                   a.channel_id
           ) b
        on a.channel_id = b.channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5010
        puts "errmsg:$errmsg"
        return -1
    }

    ####地市
    set sql_buf "
    insert into $stat_market_0135_a_t5
    (
           entity_id,
           entity_type,
           month_new_rate
    )
    select int(value(a.region_type,b.region_type)),
           2,
           case when a.region_type is null then 0
                when b.region_type is null then 100
                when b.month_new = 0 then 100
                else a.month_new * 100.00 / b.month_new
           end
      from (
            select a.region_type,
                   sum(b.month_new) month_new
              from $dwd_channel_dept_yyyymmdd a,
                   $stat_market_0135_a b,
                   $dim_bboss_channel_type c
             where a.organize_id = b.channel_id
               and a.channel_type = c.code_type
               and a.dept_type_dtl = c.code_id
               and c.code_name in ('特约代理点','指定专营店')
               and a.rec_status = 0
               and b.op_time = $year$month
             group by
                   a.region_type
           ) a
      full join
           (
            select a.region_type,
                   sum(b.month_new) month_new
              from $dwd_channel_dept_yyyymmdd a,
                   $stat_market_0135_a b,
                   $dim_bboss_channel_type c
             where a.organize_id = b.channel_id
               and a.channel_type = c.code_type
               and a.dept_type_dtl = c.code_id
               and c.code_name in ('特约代理点','指定专营店')
               and a.rec_status = 0
               and b.op_time = $l1myear$l1mmonth
             group by
                   a.region_type
           ) b
        on a.region_type = b.region_type;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5020
        puts "errmsg:$errmsg"
        return -1
    }

    ####全区
    set sql_buf "
    insert into $stat_market_0135_a_t5
    (
           entity_id,
           entity_type,
           month_new_rate
    )
    select int(value(a.channel_city,b.channel_city)),
           3,
           case when a.channel_city is null then 0
                when b.channel_city is null then 100
                when b.month_new = 0 then 100
                else a.month_new * 100.00 / b.month_new
           end
      from (
            select a.channel_city,
                   sum(b.month_new) month_new
              from $dwd_channel_dept_yyyymmdd a,
                   $stat_market_0135_a b,
                   $dim_bboss_channel_type c
             where a.organize_id = b.channel_id
               and a.channel_type = c.code_type
               and a.dept_type_dtl = c.code_id
               and c.code_name in ('特约代理点','指定专营店')
               and a.rec_status = 0
               and b.op_time = $year$month
             group by
                   a.channel_city
           ) a
      full join
           (
            select a.channel_city,
                   sum(b.month_new) month_new
              from $dwd_channel_dept_yyyymmdd a,
                   $stat_market_0135_a b,
                   $dim_bboss_channel_type c
             where a.organize_id = b.channel_id
               and a.channel_type = c.code_type
               and a.dept_type_dtl = c.code_id
               and c.code_name in ('特约代理点','指定专营店')
               and a.rec_status = 0
               and b.op_time = $l1myear$l1mmonth
             group by
                   a.channel_city
           ) b
        on a.channel_city = b.channel_city;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5030
        puts "errmsg:$errmsg"
        return -1
    }

    ####全国
    set sql_buf "
    insert into $stat_market_0135_a_t5
    (
           entity_id,
           entity_type,
           month_new_rate
    )
    select 890,
           4,
           a.month_new * 100.00 / b.month_new
      from (
            select sum(b.month_new) month_new
              from $dwd_channel_dept_yyyymmdd a,
                   $stat_market_0135_a b,
                   $dim_bboss_channel_type c
             where a.organize_id = b.channel_id
               and a.channel_type = c.code_type
               and a.dept_type_dtl = c.code_id
               and c.code_name in ('特约代理点','指定专营店')
               and a.rec_status = 0
               and b.op_time = $year$month
           ) a,
           (
            select sum(b.month_new) month_new
              from $dwd_channel_dept_yyyymmdd a,
                   $stat_market_0135_a b,
                   $dim_bboss_channel_type c
             where a.organize_id = b.channel_id
               and a.channel_type = c.code_type
               and a.dept_type_dtl = c.code_id
               and c.code_name in ('特约代理点','指定专营店')
               and a.rec_status = 0
               and b.op_time = $l1myear$l1mmonth
           ) b;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5040
        puts "errmsg:$errmsg"
        return -1
    }

    ##进入预警流程的渠道
    ####地市 B(0)=网点当月新增用户数增长幅度高于区域内整体增长幅度的150%
    set sql_buf "
    insert into $stat_market_0135_a_t6
    (
           entity_id,
           entity_type,
           bn_value
    )
    select a.entity_id,
           b.entity_type,
           case when a.month_new_rate = 0 then 0
                when b.month_new_rate = 0 then 100
                else a.month_new_rate / b.month_new_rate
           end
      from $stat_market_0135_a_t5 a,
           $dwd_channel_dept_yyyymmdd c,
           $stat_market_0135_a_t5 b
     where a.month_new_rate / b.month_new_rate > 1.5
       and b.month_new_rate > 0
       and a.entity_id = c.organize_id
       and int(c.region_type) = b.entity_id
       and a.entity_type = 1
       and b.entity_type = 2
       and c.rec_status = 0;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5050
        puts "errmsg:$errmsg"
        return -1
    }

    ####全区 B(0)=网点当月新增用户数增长幅度高于区域内整体增长幅度的150%
    set sql_buf "
    insert into $stat_market_0135_a_t6
    (
           entity_id,
           entity_type,
           bn_value
    )
    select a.entity_id,
           b.entity_type,
           case when a.month_new_rate = 0 then 0
                when b.month_new_rate = 0 then 100
                else a.month_new_rate / b.month_new_rate
           end
      from $stat_market_0135_a_t5 a,
           $dwd_channel_dept_yyyymmdd c,
           $stat_market_0135_a_t5 b
     where a.month_new_rate / b.month_new_rate > 1.5
       and b.month_new_rate > 0
       and a.entity_id = c.organize_id
       and int(c.channel_city) = b.entity_id
       and a.entity_type = 1
       and b.entity_type = 3
       and c.rec_status = 0;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5060
        puts "errmsg:$errmsg"
        return -1
    }

    ####全国 B(0)=网点当月新增用户数增长幅度高于区域内整体增长幅度的150%
    set sql_buf "
    insert into $stat_market_0135_a_t6
    (
           entity_id,
           entity_type,
           bn_value
    )
    select a.entity_id,
           b.entity_type,
           case when a.month_new_rate = 0 then 0
                when b.month_new_rate = 0 then 100
                else a.month_new_rate / b.month_new_rate
           end
      from $stat_market_0135_a_t5 a,
           $stat_market_0135_a_t5 b
     where a.month_new_rate / b.month_new_rate > 1.5
       and b.month_new_rate > 0
       and a.entity_type = 1
       and b.entity_type = 4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5060
        puts "errmsg:$errmsg"
        return -1
    }

    ##清除结果表历史数据
    set sql_buf "delete from $stat_market_0135_a_result where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5070
        puts "errmsg:$errmsg"
        return -1
    }

    ##窜卡预警阀值
    ####地市
    set sql_buf "
    insert into $stat_market_0135_a_result
    (
           op_time,
           channel_id,
           county_id,
           city_id,
           entity_type,
           warn_threshold,
           pd_order,
           pj_order,
           pf_order
    )
    select $year$month,
           a.channel_id,
           a.county_id,
           a.city_id,
           b.entity_type,
           a.diffarea_1 / 40 * 70 + a.avg_active_cycle / 1 * 10 + a.increase_rate / 150 * 20,
           row_number() over(partition by a.county_id order by a.diffarea_1 desc),
           row_number() over(partition by a.county_id order by a.avg_active_cycle desc),
           row_number() over(partition by a.county_id order by a.increase_rate desc)
      from stat_market_0135_a a,
           stat_market_0135_a_t6 b
     where a.channel_id = b.entity_id
       and a.op_time = $year$month
       and b.entity_type = 2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5080
        puts "errmsg:$errmsg"
        return -1
    }

    ####全区
    set sql_buf "
    insert into $stat_market_0135_a_result
    (
           op_time,
           channel_id,
           county_id,
           city_id,
           entity_type,
           warn_threshold,
           pd_order,
           pj_order,
           pf_order
    )
    select $year$month,
           a.channel_id,
           a.county_id,
           a.city_id,
           b.entity_type,
           a.diffarea_2 / 30 * 70 + a.avg_active_cycle / 2 * 10 + a.increase_rate / 150 * 20,
           row_number() over(partition by a.city_id order by a.diffarea_2 desc),
           row_number() over(partition by a.city_id order by a.avg_active_cycle desc),
           row_number() over(partition by a.city_id order by a.increase_rate desc)
      from stat_market_0135_a a,
           stat_market_0135_a_t6 b
     where a.channel_id = b.entity_id
       and a.op_time = $year$month
       and b.entity_type = 3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5090
        puts "errmsg:$errmsg"
        return -1
    }

    ####全国
    set sql_buf "
    insert into $stat_market_0135_a_result
    (
           op_time,
           channel_id,
           county_id,
           city_id,
           entity_type,
           warn_threshold,
           pd_order,
           pj_order,
           pf_order
    )
    select $year$month,
           a.channel_id,
           a.county_id,
           a.city_id,
           b.entity_type,
           a.diffarea_3 / 10 * 70 + a.avg_active_cycle / 5 * 10 + a.increase_rate / 150 * 20,
           row_number() over(order by a.diffarea_3 desc),
           row_number() over(order by a.avg_active_cycle desc),
           row_number() over(order by a.increase_rate desc)
      from stat_market_0135_a a,
           stat_market_0135_a_t6 b
     where a.channel_id = b.entity_id
       and a.op_time = $year$month
       and b.entity_type = 4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5100
        puts "errmsg:$errmsg"
        return -1
    }

    ##清除结果表历史数据
    set sql_buf "delete from $stat_market_0135_a_final where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5110
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "delete from $stat_market_0135_a_link1 where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5120
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "delete from $stat_market_0135_a_link2 where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5130
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_a_final
    (
           op_time,
           city_id,
           region_id,
           channel_id,
           channel_name,
           channel_type,
           channel_dtl_type,
           warn_level,
           month_new,
           diffarea_2,
           pd_order,
           avg_active_cycle,
           pj_order,
           increase_rate,
           pf_order,
           warn_mark,
           process_result
    )
    select $year$month,
           a.city_id,
           d.region_id,
           a.channel_id,
           c.channel_name,
           c.channel_type,
           c.dept_type_dtl,
           e.warn_level,
           a.month_new,
           a.diffarea_2,
           b.pd_order,
           a.avg_active_cycle,
           b.pj_order,
           a.increase_rate,
           b.pf_order,
           1,
           ''
      from $stat_market_0135_a a,
           $stat_market_0135_a_result b,
           (
            select a.channel_id,
                   max(a.warn_level) as warn_level
              from (
                    select channel_id,
                           case when pd_order <= 20 and pj_order <= 20 and pf_order <= 20
                                then 3
                                when (pd_order <= 20 and pj_order <= 20 and pf_order > 20)
                                  or (pd_order <= 20 and pj_order > 20 and pf_order <= 20)
                                  or (pd_order > 20 and pj_order <= 20 and pf_order <= 20)
                                then 2
                                when (pd_order <= 20 and pj_order > 20 and pf_order > 20)
                                  or (pd_order > 20 and pj_order <= 20 and pf_order >20)
                                  or (pd_order > 20 and pj_order > 20 and pf_order <= 20)
                                then 1
                                else 0
                           end as warn_level
                      from $stat_market_0135_a_result
                     where entity_type = 3
                       and op_time = $year$month
                     union all
                    select channel_id,
                           case when b.warn_threshold >= 200 and b.warn_threshold < 300 then 1
                                when b.warn_threshold >= 300 and b.warn_threshold < 400 then 2
                                when b.warn_threshold >= 400 then 3
                                else 0
                           end as warn_level
                      from $stat_market_0135_a_result b
                     where entity_type = 3
                       and op_time = $year$month
                   ) a
             group by
                   a.channel_id
           ) e,
           $dwd_channel_dept_yyyymmdd c
      left join
           $dim_pub_channel_ext d
        on c.channel_id = int(d.channel_id)
     where a.channel_id = b.channel_id
       and a.channel_id = c.organize_id
       and a.channel_id = e.channel_id
       and b.entity_type = 3
       and a.op_time = $year$month
       and b.op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5140
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_a_link1
    (
           op_time,
           channel_id,
           product_no,
           diffarea,
           userstatus
    )
    select $year$month,
           a.channel_id,
           c.product_no,
           (select city_name from $dim_pub_city d where b.diffarea = d.city_id),
           (select userstatus_name from $dim_user_status e where c.userstatus_id = e.userstatus_id)
      from $stat_market_0135_a_final a,
           $stat_market_0135_a_t8 b,
           $dw_product_yyyymm c
     where a.channel_id = b.channel_id
       and b.user_id = c.user_id
       and a.op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5150
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_a_link2
    (
           op_time,
           channel_id,
           product_no,
           active_cycle
    )
    select $year$month,
           a.channel_id,
           c.product_no,
           min(b.active_cycle)
      from $stat_market_0135_a_final a,
           $stat_market_0135_a_t3 b,
           $dw_product_yyyymm c
     where a.channel_id = b.channel_id
       and b.user_id = c.user_id
       and a.op_time = $year$month
     group by
           a.channel_id,
           c.product_no;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5160
        puts "errmsg:$errmsg"
        return -1
    }

### 20110803 新增中间表供一经调用 

set sql_buf "
	    delete from bass1.G_S_22059_MONTH_MRKT_0135A where OP_TIME = $year$month
	    "
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2050
        puts "errmsg:$errmsg"
        return -1
    }

set sql_buf "    
    insert into bass1.G_S_22059_MONTH_MRKT_0135A 
    (
     OP_TIME
    ,CHANNEL_ID
    ,BRAND_ID
    ,USER_CNT
    )
    select  $year$month
	   ,a.channel_id
	   ,case 
	    when b.brand_id in (1) then '1'
	    when b.brand_id in (0,3,5,6,7,9999) then '2'
	    when b.brand_id in (4) then '3'
	    else '2' end brand_id
           ,count(0) user_cnt
      from $stat_market_0135_a_t7 a
        ,  $dw_product_yyyymm b 
    where a.user_id = b.user_id 
    group by a.channel_id
	    ,case 
	    when b.brand_id in (1) then '1'
	    when b.brand_id in (0,3,5,6,7,9999) then '2'
	    when b.brand_id in (4) then '3'
	    else '2' end 
"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2050
        puts "errmsg:$errmsg"
        return -1
    }
    #Step5.清空临时表
    set sql_buf "drop table $stat_market_0135_a_t1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_a_t2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_a_t3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_a_t4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_a_t5;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_a_t6;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    return 0
}
