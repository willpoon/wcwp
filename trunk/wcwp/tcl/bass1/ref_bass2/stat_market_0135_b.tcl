#======================================================================================
#版权归属：Copyright (c) 2011,AsiaInfo.Report.System
#程序名称: stat_market_0135_b.tcl
#程序功能: 违规预警模型-养卡
#分析目标: 略
#分析指标: 见维表
#分析维度: 全区 地市 区县 渠道
#运行粒度: 月
#运行示例: crt_basetab.sh stat_market_0135_b.tcl 2011-04-01
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

    if {[stat_market_0135_b $p_optime] != 0} {
        aidb_roll $conn
        aidb_close $handle
        return -1
    }

    aidb_commit $conn
    aidb_close $handle

    return 0
}

proc stat_market_0135_b {p_optime} {
    global conn
    global handle

    source stat_insert_index.tcl
    source report.cfg

    #日期处理
    set    date_optime      [ai_to_date $p_optime]
    scan   $p_optime        "%04s-%02s-%02s" year month day
    set    month_start      "$year-$month-01"
    set    month_end        [clock format [clock scan "$month_start + 1month - 1day"] -format "%Y-%m-%d"]
    scan   $month_end       "%04s-%02s-%02s" meyear memonth meday
    set    last_month       [clock format [clock scan "$month_start - 1month"] -format "%Y-%m-%d"]
    scan   $last_month      "%04s-%02s-%02s" lmyear lmmonth lmday
    set    last2_month      [clock format [clock scan "$month_start - 2month"] -format "%Y-%m-%d"]
    scan   $last2_month     "%04s-%02s-%02s" l2myear l2mmonth l2mday

    #源表
    set dw_product_yyyymm           "bass2.dw_product_$year$month"
    set dw_product_lmyyyymm         "bass2.dw_product_$lmyear$lmmonth"
    set dw_product_l2myyyymm        "bass2.dw_product_$l2myear$l2mmonth"
    set dw_product_imei_rela_yyyymm "bass2.dw_product_imei_rela_$lmyear$lmmonth"
    set dw_call_cell_yyyymm         "bass2.dw_call_cell_$year$month"

    #目标表
    set stat_market_0135_b          "bass2.stat_market_0135_b"
    set stat_market_0135_b_result   "bass2.stat_market_0135_b_result"
    set stat_market_0135_b_final    "bass2.stat_market_0135_b_final"
    set stat_market_0135_b_link1    "bass2.stat_market_0135_b_link1"
    set stat_market_0135_b_link2    "bass2.stat_market_0135_b_link2"
    set stat_market_0135_b_link3    "bass2.stat_market_0135_b_link3"
    set stat_market_0135_b_link4    "bass2.stat_market_0135_b_link4"
    set stat_market_0135_b_link5    "bass2.stat_market_0135_b_link5"
    set stat_market_0135_b_link6    "bass2.stat_market_0135_b_link6"
    set stat_market_0135_b_link7    "bass2.stat_market_0135_b_link7"

    #维表
    set dim_pub_cell_ext            "bassweb.dim_pub_cell_ext"
    set dwd_channel_dept_yyyymmdd   "bass2.dwd_channel_dept_$meyear$memonth$meday"
    set dim_pub_channel_ext         "bassweb.dim_pub_channel_ext"
    set dim_bboss_channel_type      "bass2.dim_bboss_channel_type"
    set dim_pub_bts                 "bassweb.dim_pub_bts"
    set dim_user_status             "bass2.dim_user_status"

    #临时表
    set stat_market_0135_b_t1       "bass2.stat_market_0135_b_t1"
    set stat_market_0135_b_t2       "bass2.stat_market_0135_b_t2"
    set stat_market_0135_b_t3       "bass2.stat_market_0135_b_t3"
    set stat_market_0135_b_t4       "bass2.stat_market_0135_b_t4"
    set stat_market_0135_b_t5       "bass2.stat_market_0135_b_t5"
    set stat_market_0135_b_t6       "bass2.stat_market_0135_b_t6"

    #Step1.创建结果临时表
    set sql_buf "drop table $stat_market_0135_b_t1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_b_t2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_b_t3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_b_t4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_b_t5;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_b_t6;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    set sql_buf "
    create table $stat_market_0135_b_t1
    (
           channel_id       bigint,
           month_new        integer,
           month_new_loss1  integer,
           month_new_loss2  integer,
           low_arpu         integer,
           low_mou          integer,
           same_imei        integer,
           imeis            integer,
           zero             integer,
           bts              integer
    )
    in tbs_ods_other index in tbs_index partitioning key (channel_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1010
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_market_0135_b_t2
    (
           user_id         varchar(20),
           channel_id      bigint,
           fact_fee        decimal(9,2),
           call_duration_m integer
    )
    in tbs_ods_other index in tbs_index partitioning key (user_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1020
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_market_0135_b_t3
    (
           user_id         varchar(20),
           imei            varchar(18),
           channel_id      bigint
    )
    in tbs_ods_other index in tbs_index partitioning key (user_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1020
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_market_0135_b_t4
    (
           entity_id        bigint,
           entity_type      smallint,
           month_new_rate   decimal(9,2)
    )
    in tbs_ods_other index in tbs_index partitioning key (entity_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1020
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_market_0135_b_t5
    (
           entity_id        bigint,
           entity_type      smallint,
           bn_value         decimal(9,2)
    )
    in tbs_ods_other index in tbs_index partitioning key (entity_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1020
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_market_0135_b_t6
    (
           user_id  varchar(20),
           bts_id   varchar(10)
    )
    in tbs_ods_other index in tbs_index partitioning key (user_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1020
        puts "errmsg:$errmsg"
        return -1
    }
    

    #Step2.生成临时表数据
    set sql_buf "
    insert into $stat_market_0135_b_t2
    (
           user_id          ,
           channel_id       ,
           fact_fee         ,
           call_duration_m
    )
    select b.user_id,
           b.channel_id,
           a.fact_fee,
           a.call_duration_m
      from $dw_product_yyyymm a,
           $dw_product_lmyyyymm b
     where a.user_id = b.user_id
       and b.userstatus_id in (1,2,3,6,8)
       and b.usertype_id in (1,2,9)
       and b.test_mark = 0
       and b.month_new_mark = 1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2010
        puts "errmsg:$errmsg"
        return -1
    }

    ##用户基站通话信息
    set sql_buf "
    insert into $stat_market_0135_b_t6
    (
           user_id,
           bts_id
    )
    select a.user_id,
           c.bts_id
      from $dw_call_cell_yyyymm a,
           $dim_pub_cell_ext c
     where a.lac_id = c.lac_id
       and a.cell_id = c.cell_id
     group by
           a.user_id,
           c.bts_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2010
        puts "errmsg:$errmsg"
        return -1
    }

    ##月发展用户数
    ##网点上月发展用户数量
    ##低ARPU客户数
    ##网点上月发展用户中ARPU值低于ARPU阀值的用户数
    ##低MOU客户数
    ##网点上月发展用户中MOU值低于MOU阀值的用户数
    ##新增零次户数
    ##网点上月发展用户中MOU值等于0的用户数
    ##临近基站用户数
    ##网点上月发展用户中仅在一个或相邻的3个基站下通话的客户数
    set sql_buf "
    insert into $stat_market_0135_b_t1
    (
           channel_id,
           month_new,
           month_new_loss1,
           low_arpu,
           low_mou,
           zero,
           bts
    )
    select channel_id,
           count(a.user_id) as month_new,
           count(c.user_id) as month_new_loss1,
           count(case when a.fact_fee < 10 then a.user_id end) as low_arpu,
           count(case when a.call_duration_m < 5 then a.user_id end) as low_mou,
           count(case when a.call_duration_m = 0 then a.user_id end) as zero,
           count(b.user_id) as bts
      from $stat_market_0135_b_t2 a
      left join
           (
            select a.user_id
              from $stat_market_0135_b_t6 a
             group by
                   a.user_id
            having count(distinct a.bts_id) = 1
           ) b
        on a.user_id = b.user_id
      left join
           (
            select a.user_id
              from $dw_product_yyyymm a,
                   $dw_product_lmyyyymm b
             where a.user_id = b.user_id
               and a.month_off_mark = 1
               and b.userstatus_id in (1,2,3,6,8)
               and b.usertype_id in (1,2,9)
               and b.test_mark = 0
               and b.month_new_mark = 1
           ) c
        on a.user_id = c.user_id
     group by
           channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_b_t1
    (
           channel_id,
           month_new_loss2
    )
    select a.channel_id,
           count(*)
      from $dw_product_yyyymm a,
           $dw_product_l2myyyymm b
     where a.user_id = b.user_id
       and a.month_off_mark = 1
       and b.userstatus_id in (1,2,3,6,8)
       and b.usertype_id in (1,2,9)
       and b.test_mark = 0
       and b.month_new_mark = 1
     group by
           a.channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }

    ##相同IMEI激活用户
    ##在网点上月发展的用户中如果有两个以上用户使用同一IMEI激活则这些用户都算作相同IMEI激活用户
    set sql_buf "
    insert into $stat_market_0135_b_t3
    (
           user_id,
           imei,
           channel_id
    )
    select a.user_id,
           a.imei,
           a.channel_id
      from (
            select a.imei,
                   a.user_id,
                   b.channel_id,
                   row_number() over(partition by a.user_id order by first_use_date) imei_order
              from $dw_product_imei_rela_yyyymm a,
                   $stat_market_0135_b_t2 b
             where a.user_id = b.user_id
               and a.first_use_date >= date('$last_month')
           ) a
     where imei_order = 1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2030
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_b_t1
    (
           channel_id,
           same_imei,
           imeis
    )
    select a.channel_id,
           count(distinct a.user_id) as same_imei,
           count(distinct a.imei) as imeis
      from $stat_market_0135_b_t3 a,
           (
            select imei,
                   channel_id
              from $stat_market_0135_b_t3
             group by
                   imei,
                   channel_id
            having count(distinct user_id) >= 3
           ) b
     where a.imei = b.imei
       and a.channel_id = b.channel_id
     group by
           a.channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2030
        puts "errmsg:$errmsg"
        return -1
    }

    #Step3.清除结果表历史数据
    set sql_buf "delete from $stat_market_0135_b where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3010
        puts "errmsg:$errmsg"
        return -1
    }

    #Step4.生成结果表数据
    set sql_buf "
    insert into $stat_market_0135_b
    (
           op_time          ,
           channel_id       ,
           month_new        ,
           month_new_loss1  ,
           month_new_loss2  ,
           low_arpu_rate    ,
           low_mou_rate     ,
           same_imei        ,
           avg_same_imei    ,
           same_imei_rate   ,
           zero_rate        ,
           bts_rate
    )
    select $year$month,
           b.channel_id,
           sum(value(b.month_new,0)),
           sum(value(b.month_new_loss1,0)),
           sum(value(b.month_new_loss2,0)),
           case when sum(value(b.month_new,0)) = 0 then 0
                else sum(value(b.low_arpu,0)) * 100.00 / sum(value(b.month_new,0))
           end,
           case when sum(value(b.month_new,0)) = 0 then 0
                else sum(value(b.low_mou,0)) * 100.00 / sum(value(b.month_new,0))
           end,
           sum(value(b.same_imei,0)),
           case when sum(value(b.imeis,0)) = 0 then 0
                else sum(value(b.same_imei,0)) * 1.00 / sum(value(b.imeis,0))
           end,
           case when sum(value(b.month_new,0)) = 0 then 0
                else sum(value(b.same_imei,0)) * 100.00 / sum(value(b.month_new,0))
           end,
           case when sum(value(b.month_new,0)) = 0 then 0
                else sum(value(b.zero,0)) * 100.00 / sum(value(b.month_new,0))
           end,
           case when sum(value(b.month_new,0)) = 0 then 0
                else sum(value(b.bts,0)) * 100.00 / sum(value(b.month_new,0))
           end
      from $dwd_channel_dept_yyyymmdd a,
           $stat_market_0135_b_t1 b
     where a.organize_id = b.channel_id
     group by
           b.channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 4010
        puts "errmsg:$errmsg"
        return -1
    }

    exec db2 "connect to bassdb user bass2 using bass2"
    exec db2 "runstats on table $stat_market_0135_b with distribution and indexes all"
    exec db2 "terminate"

    #Step5.养卡流程
    ##区域内新增用户增幅
    ####渠道
    set sql_buf "
    insert into $stat_market_0135_b_t4
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
                   $stat_market_0135_b b,
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
                   $stat_market_0135_b b,
                   $dim_bboss_channel_type c
             where a.organize_id = b.channel_id
               and a.channel_type = c.code_type
               and a.dept_type_dtl = c.code_id
               and c.code_name in ('特约代理点','指定专营店')
               and a.rec_status = 0
               and b.op_time = $lmyear$lmmonth
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

    ####全区
    set sql_buf "
    insert into $stat_market_0135_b_t4
    (
           entity_id,
           entity_type,
           month_new_rate
    )
    select value(a.channel_city,b.channel_city),
           2,
           case when a.channel_city is null then 0
                when b.channel_city is null then 100
                when b.month_new = 0 then 100
                else a.month_new * 100.00 / b.month_new
           end
      from (
            select a.channel_city,
                   sum(b.month_new) month_new
              from $dwd_channel_dept_yyyymmdd a,
                   $stat_market_0135_b b,
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
                   $stat_market_0135_b b,
                   $dim_bboss_channel_type c
             where a.organize_id = b.channel_id
               and a.channel_type = c.code_type
               and a.dept_type_dtl = c.code_id
               and c.code_name in ('特约代理点','指定专营店')
               and a.rec_status = 0
               and b.op_time = $lmyear$lmmonth
             group by
                   a.channel_city
           ) b
        on a.channel_city = b.channel_city;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5020
        puts "errmsg:$errmsg"
        return -1
    }

    ##进入预警流程的渠道
    ####B(0)=网点当月新增用户数增长幅度高于区域内整体增长幅度的150%
    set sql_buf "
    insert into $stat_market_0135_b_t5
    (
           entity_id,
           entity_type,
           bn_value
    )
    select a.entity_id,
           b.entity_type,
           a.month_new_rate / b.month_new_rate
      from $stat_market_0135_b_t4 a,
           $dwd_channel_dept_yyyymmdd c,
           $stat_market_0135_b_t4 b
     where a.month_new_rate / b.month_new_rate > 1.5
       and a.entity_id = c.organize_id
       and int(c.channel_city) = b.entity_id
       and a.entity_type = 1
       and b.entity_type = 2
       and c.rec_status = 0;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5030
        puts "errmsg:$errmsg"
        return -1
    }

    ##养卡预警阀值
    set sql_buf "delete from $stat_market_0135_b_result where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5040
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_b_result
    (
           op_time          ,
           channel_id       ,
           county_id        ,
           city_id          ,
           entity_type      ,
           warn_threshold   ,
           pa_order         ,
           pm_order         ,
           pp1_order        ,
           px_order
    )
    select $year$month,
           a.channel_id,
           char(c.region_type),
           char(c.channel_city),
           b.entity_type,
           a.low_arpu_rate / 30 * 10 + a.low_mou_rate / 10 * 30 + a.same_imei / 10 * 10 + a.avg_same_imei / 5 * 10
            + a.same_imei_rate / 20 * 10 + a.zero_rate / 10 * 20 + a.bts_rate / 10 * 10,
           row_number() over(partition by c.channel_city order by a.low_arpu_rate desc),
           row_number() over(partition by c.channel_city order by a.low_mou_rate desc),
           row_number() over(partition by c.channel_city order by a.avg_same_imei desc),
           row_number() over(partition by c.channel_city order by a.zero_rate desc)
      from $stat_market_0135_b a,
           $stat_market_0135_b_t5 b,
           $dwd_channel_dept_yyyymmdd c
     where a.channel_id = b.entity_id
       and a.channel_id = c.organize_id
       and b.entity_type = 2
       and a.op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5050
        puts "errmsg:$errmsg"
        return -1
    }

    ##清除结果表历史数据
    set sql_buf "delete from $stat_market_0135_b_final where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5060
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "delete from $stat_market_0135_b_link1 where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5070
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "delete from $stat_market_0135_b_link2 where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5080
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "delete from $stat_market_0135_b_link3 where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5090
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "delete from $stat_market_0135_b_link4 where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5100
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "delete from $stat_market_0135_b_link5 where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5110
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "delete from $stat_market_0135_b_link6 where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5120
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "delete from $stat_market_0135_b_link7 where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5130
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_b_final
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
           month_new_loss1,
           month_new_loss2,
           low_arpu_rate,
           pa_order,
           low_mou_rate,
           pm_order,
           same_imei,
           avg_same_imei,
           pp1_order,
           same_imei_rate,
           zero_rate,
           px_order,
           bts_rate,
           warn_mark,
           process_result
    )
    select $year$month,
           b.city_id,
           d.region_id,
           a.channel_id,
           c.channel_name,
           c.channel_type,
           c.dept_type_dtl,
           e.warn_level,
           a.month_new,
           a.month_new_loss1,
           a.month_new_loss2,
           a.low_arpu_rate,
           b.pa_order,
           a.low_mou_rate,
           b.pm_order,
           a.same_imei,
           a.avg_same_imei,
           b.pp1_order,
           a.same_imei_rate,
           a.zero_rate,
           b.px_order,
           a.bts_rate,
           1,
           ''
      from $stat_market_0135_b a,
           $stat_market_0135_b_result b,
           (
            select a.channel_id,
                   max(a.warn_level) as warn_level
              from (
                    select channel_id,
                           case when (pa_order <= 20 and pm_order <= 20 and pp1_order <= 20 and px_order <= 20)
                                  or (pa_order > 20 and pm_order <= 20 and pp1_order <= 20 and px_order <= 20)
                                  or (pa_order <= 20 and pm_order > 20 and pp1_order <= 20 and px_order <= 20)
                                  or (pa_order <= 20 and pm_order <= 20 and pp1_order > 20 and px_order <= 20)
                                  or (pa_order <= 20 and pm_order <= 20 and pp1_order <= 20 and px_order > 20)
                                then 3
                                when (pa_order <= 20 and pm_order <= 20)
                                  or (pa_order <= 20 and pp1_order <= 20)
                                  or (pa_order <= 20 and px_order <= 20)
                                  or (pm_order <= 20 and pp1_order <= 20)
                                  or (pm_order <= 20 and px_order <= 20)
                                  or (pp1_order <= 20 and px_order <= 20)
                                then 2
                                when pa_order <= 20
                                  or pm_order <= 20
                                  or pp1_order <= 20
                                  or px_order <= 20
                                then 1
                                else 0
                           end as warn_level
                      from $stat_market_0135_b_result
                     where entity_type = 2
                       and op_time = $year$month
                     union all
                    select channel_id,
                           case when b.warn_threshold >= 200 and b.warn_threshold < 300 then 1
                                when b.warn_threshold >= 300 and b.warn_threshold < 400 then 2
                                when b.warn_threshold >= 400 then 3
                                else 0
                           end as warn_level
                      from $stat_market_0135_b_result b
                     where entity_type = 2
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
       and b.entity_type = 2
       and a.op_time = $year$month
       and b.op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5140
        puts "errmsg:$errmsg"
        return -1
    }

    ##新增用户次月拆机号码
    set sql_buf "
    insert into $stat_market_0135_b_link1
    (
           op_time,
           channel_id,
           product_no,
           join_date,
           loss_date
    )
    select $year$month,
           b.channel_id,
           a.product_no,
           b.create_date,
           a.expire_date
      from $dw_product_yyyymm a,
           $dw_product_lmyyyymm b
     where a.user_id = b.user_id
       and a.month_off_mark = 1
       and b.userstatus_id in (1,2,3,6,8)
       and b.usertype_id in (1,2,9)
       and b.test_mark = 0
       and b.month_new_mark = 1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5150
        puts "errmsg:$errmsg"
        return -1
    }

    ##新增用户第三月拆机号码
    set sql_buf "
    insert into $stat_market_0135_b_link2
    (
           op_time,
           channel_id,
           product_no,
           join_date,
           loss_date
    )
    select $year$month,
           b.channel_id,
           a.product_no,
           b.create_date,
           a.expire_date
      from $dw_product_yyyymm a,
           $dw_product_l2myyyymm b
     where a.user_id = b.user_id
       and a.month_off_mark = 1
       and b.userstatus_id in (1,2,3,6,8)
       and b.usertype_id in (1,2,9)
       and b.test_mark = 0
       and b.month_new_mark = 1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5160
        puts "errmsg:$errmsg"
        return -1
    }

    ##低ARPU值客户
    set sql_buf "
    insert into $stat_market_0135_b_link3
    (
           op_time,
           channel_id,
           product_no,
           join_date,
           arpu,
           userstatus
    )
    select $year$month,
           b.channel_id,
           b.product_no,
           b.create_date,
           a.fact_fee,
           (select userstatus_name from $dim_user_status c where c.userstatus_id = a.userstatus_id)
      from $dw_product_yyyymm a,
           $dw_product_lmyyyymm b
     where a.user_id = b.user_id
       and a.fact_fee < 10
       and b.userstatus_id in (1,2,3,6,8)
       and b.usertype_id in (1,2,9)
       and b.test_mark = 0
       and b.month_new_mark = 1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5170
        puts "errmsg:$errmsg"
        return -1
    }

    ##低MOU值客户
    set sql_buf "
    insert into $stat_market_0135_b_link4
    (
           op_time,
           channel_id,
           product_no,
           join_date,
           mou,
           userstatus
    )
    select $year$month,
           b.channel_id,
           b.product_no,
           b.create_date,
           a.call_duration_m,
           (select userstatus_name from $dim_user_status c where c.userstatus_id = a.userstatus_id)
      from $dw_product_yyyymm a,
           $dw_product_lmyyyymm b
     where a.user_id = b.user_id
       and a.call_duration_m < 5
       and b.userstatus_id in (1,2,3,6,8)
       and b.usertype_id in (1,2,9)
       and b.test_mark = 0
       and b.month_new_mark = 1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5180
        puts "errmsg:$errmsg"
        return -1
    }

    ##新增零次户
    set sql_buf "
    insert into $stat_market_0135_b_link6
    (
           op_time,
           channel_id,
           product_no,
           join_date,
           userstatus
    )
    select $year$month,
           b.channel_id,
           b.product_no,
           b.create_date,
           (select userstatus_name from $dim_user_status c where c.userstatus_id = a.userstatus_id)
      from $dw_product_yyyymm a,
           $dw_product_lmyyyymm b
     where a.user_id = b.user_id
       and a.call_duration_m = 0
       and b.userstatus_id in (1,2,3,6,8)
       and b.usertype_id in (1,2,9)
       and b.test_mark = 0
       and b.month_new_mark = 1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5190
        puts "errmsg:$errmsg"
        return -1
    }

    ##相同IMEI激活用户
    set sql_buf "
    insert into $stat_market_0135_b_link5
    (
           op_time,
           channel_id,
           product_no,
           join_date,
           imei
    )
    select $year$month,
           a.channel_id,
           c.product_no,
           c.create_date,
           a.imei
      from $dw_product_yyyymm c,
           $stat_market_0135_b_t3 a,
           (
            select imei,
                   channel_id
              from $stat_market_0135_b_t3
             group by
                   imei,
                   channel_id
            having count(distinct user_id) >= 3
           ) b
     where a.user_id = c.user_id
       and a.imei = b.imei
       and a.channel_id = b.channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5200
        puts "errmsg:$errmsg"
        return -1
    }

    ##临近基站通话用户
    set sql_buf "
    insert into $stat_market_0135_b_link7
    (
           op_time,
           channel_id,
           product_no,
           join_date,
           bts_name
    )
    select $year$month,
           a.channel_id,
           a.product_no,
           a.create_date,
           d.bts_name
      from $dw_product_lmyyyymm a,
           $stat_market_0135_b_t6 b
      left join
           (
            select bts_id,
                   max(bts_name) as bts_name
              from $dim_pub_bts
             group by
                   bts_id
           ) d
        on b.bts_id = d.bts_id,
           (
            select a.user_id
              from $stat_market_0135_b_t6 a
             group by
                   a.user_id
            having count(distinct a.bts_id) = 1
           ) c
     where a.user_id = b.user_id
       and a.user_id = c.user_id
       and a.userstatus_id in (1,2,3,6,8)
       and a.usertype_id in (1,2,9)
       and a.test_mark = 0
       and a.month_new_mark = 1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5210
        puts "errmsg:$errmsg"
        return -1
    }

    #Step6.清空临时表
    set sql_buf "drop table $stat_market_0135_b_t1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_b_t2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_b_t3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_b_t4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_b_t5;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_b_t6;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    return 0
}