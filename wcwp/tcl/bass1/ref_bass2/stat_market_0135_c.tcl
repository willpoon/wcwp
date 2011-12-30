#======================================================================================
#版权归属：Copyright (c) 2011,AsiaInfo.Report.System
#程序名称: stat_market_0135_c.tcl
#程序功能: 违规预警模型-酬金异动
#分析目标: 略
#分析指标: 见维表
#分析维度: 全区 地市 区县 渠道
#运行粒度: 月
#运行示例: crt_basetab.sh stat_market_0135_c.tcl 2011-04-01
#创建时间: 2011-5-12
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

    if {[stat_market_0135_c $p_optime] != 0} {
        aidb_roll $conn
        aidb_close $handle
        return -1
    }

    aidb_commit $conn
    aidb_close $handle

    return 0
}

proc stat_market_0135_c {p_optime} {
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
    set    ls1month         [clock format [clock scan "$month_start - 1month"] -format "%Y-%m-%d"]
    set    ls2month         [clock format [clock scan "$month_start - 2month"] -format "%Y-%m-%d"]
    set    ls3month         [clock format [clock scan "$month_start - 3month"] -format "%Y-%m-%d"]
    set    ls5month         [clock format [clock scan "$month_start - 5month"] -format "%Y-%m-%d"]
    scan   $ls1month        "%04s-%02s-%02s" ls1myear ls1mmonth ls1mday
    scan   $ls2month        "%04s-%02s-%02s" ls2myear ls2mmonth ls2mday
    scan   $ls3month        "%04s-%02s-%02s" ls3myear ls3mmonth ls3mday
    scan   $ls5month        "%04s-%02s-%02s" ls5myear ls5mmonth ls5mday

    #源表
    set dw_product_yyyymm                   "bass2.dw_product_$year$month"
    set dw_product_ls1yyyymm                "bass2.dw_product_$ls1myear$ls1mmonth"
    set stat_channel_reward_detail_yyyymm   "bass2.stat_channel_reward_detail_$year$month"
    set stat_channel_reward_detail_ls1yyyymm    "bass2.stat_channel_reward_detail_$ls1myear$ls1mmonth"
    set stat_channel_reward_detail_ls2yyyymm    "bass2.stat_channel_reward_detail_$ls2myear$ls2mmonth"
    set stat_channel_reward_detail_ls3yyyymm    "bass2.stat_channel_reward_detail_$ls3myear$ls3mmonth"
    set channel_nbusi_reward_yyyymm         "bass2.channel_nbusi_reward_$year$month"
    set channel_nbusi_reward_ls1yyyymm      "bass2.channel_nbusi_reward_$ls1myear$ls1mmonth"
    set channel_bbusi_reward_yyyymm         "bass2.channel_bbusi_reward_$year$month"
    set channel_bbusi_reward_ls1yyyymm      "bass2.channel_bbusi_reward_$ls1myear$ls1mmonth"
    set stat_channel_reward_0003            "bass2.stat_channel_reward_0003"
    set stat_channel_reward_0011            "bass2.stat_channel_reward_0011"

    #目标表
    set stat_market_0135_c          "bass2.stat_market_0135_c"
    set stat_market_0135_c_result   "bass2.stat_market_0135_c_result"
    set stat_market_0135_c_final    "bass2.stat_market_0135_c_final"
    set stat_market_0135_c_link1    "bass2.stat_market_0135_c_link1"
    set stat_market_0135_c_link2    "bass2.stat_market_0135_c_link2"
    set stat_market_0135_c_link3    "bass2.stat_market_0135_c_link3"
    set stat_market_0135_c_link4    "bass2.stat_market_0135_c_link4"
    set stat_market_0135_c_link5    "bass2.stat_market_0135_c_link5"

    #维表
    set dwd_channel_dept_yyyymmdd   "bass2.dwd_channel_dept_$meyear$memonth$meday"
    set dim_pub_channel_ext         "bassweb.dim_pub_channel_ext"
    set dim_bboss_channel_type      "bass2.dim_bboss_channel_type"
    set map_busi_code               "bass2.map_busi_code"
    set dim_user_status             "bass2.dim_user_status"

    #临时表
    set stat_market_0135_c_t1       "bass2.stat_market_0135_c_t1"
    set stat_market_0135_c_t2       "bass2.stat_market_0135_c_t2"
    set stat_market_0135_c_t3       "bass2.stat_market_0135_c_t3"
    set stat_market_0135_c_t4       "bass2.stat_market_0135_c_t4"

    #Step1.创建结果临时表
    set sql_buf "drop table $stat_market_0135_c_t1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_c_t2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_c_t3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_c_t4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    set sql_buf "
    create table $stat_market_0135_c_t1
    (
           channel_id       bigint,
           reward           decimal(9,2),
           avg_reward       decimal(9,2),
           month_new        integer,
           last_month_new   integer,
           newbusi          integer,
           last_newbusi     integer,
           normalbusi       integer,
           last_normalbusi  integer,
           duplicate_num    integer,
           total_num        integer,
           sub_reward_2nd1st    integer,
           sub_reward_3rd1st    integer,
           sub_reward_2nd   integer,
           sub_reward_3rd   integer,
           nbusi_reward_2nd1st  integer,
           nbusi_reward_3rd1st  integer,
           nbusi_reward_2nd integer,
           nbusi_reward_3rd integer
    )
    in tbs_ods_other index in tbs_index partitioning key (channel_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1010
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_market_0135_c_t2
    (
           user_id          varchar(20),
           channel_id       bigint,
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
    create table $stat_market_0135_c_t3
    (
           entity_id    bigint,
           entity_type  smallint,
           new_rate     decimal(9,2),
           newbusi_rate decimal(9,2),
           normal_rate  decimal(9,2)
    )
    in tbs_ods_other index in tbs_index partitioning key (entity_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1030
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_market_0135_c_t4
    (
           object_id    bigint,
           phone_id     varchar(15),
           busi_id      bigint,
           busi_date    date
    )
    in tbs_ods_other index in tbs_index partitioning key (phone_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1030
        puts "errmsg:$errmsg"
        return -1
    }

    #Step2.生成临时表数据
    set sql_buf "
    insert into $stat_market_0135_c_t2
    (
           user_id          ,
           channel_id       ,
           month_new_mark
    )
    select a.user_id,
           a.channel_id,
           a.month_new_mark
      from $dw_product_yyyymm a
     where userstatus_id in (1,2,3,6,8)
       and usertype_id in (1,2,9)
       and test_mark = 0;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2010
        puts "errmsg:$errmsg"
        return -1
    }

    ##酬金总额
    set sql_buf "
    insert into $stat_market_0135_c_t1
    (
           channel_id,
           reward
    )
    select channel_id,
           sum(fee)
      from $stat_channel_reward_detail_yyyymm
     group by
           channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_c_t1
    (
           channel_id,
           avg_reward
    )
    select channel_id,
           avg(reward)
      from (
            select channel_id,
                   sum(fee) as reward
              from $stat_channel_reward_detail_ls1yyyymm
             group by
                   channel_id
             union all
            select channel_id,
                   sum(fee) as reward
              from $stat_channel_reward_detail_ls2yyyymm
             group by
                   channel_id
             union all
            select channel_id,
                   sum(fee) as reward
              from $stat_channel_reward_detail_ls3yyyymm
             group by
                   channel_id
           ) a
     group by
           channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2030
        puts "errmsg:$errmsg"
        return -1
    }

    ##放号量
    set sql_buf "
    insert into $stat_market_0135_c_t1
    (
           channel_id,
           month_new
    )
    select channel_id,
           count(*)
      from $dw_product_yyyymm a
     where userstatus_id in (1,2,3,6,8)
       and usertype_id in (1,2,9)
       and test_mark = 0
       and month_new_mark = 1
     group by
           channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2040
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_c_t1
    (
           channel_id,
           last_month_new
    )
    select channel_id,
           count(*)
      from $dw_product_ls1yyyymm a
     where userstatus_id in (1,2,3,6,8)
       and usertype_id in (1,2,9)
       and test_mark = 0
       and month_new_mark = 1
     group by
           channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2050
        puts "errmsg:$errmsg"
        return -1
    }

    ##新业务开通量
    #set sql_buf "
    #insert into $stat_market_0135_c_t1
    #(
    #       channel_id,
    #       newbusi
    #)
    #select b.so_org_id,
    #       count(distinct b.so_nbr)
    #  from $dw_product_busi_msc_dm_yyyymm a,
    #       $dw_product_busi_dm_yyyymm b
    # where a.so_nbr = b.so_nbr
    #   and b.so_mode in ('0','1','2')
    #   and b.busi_code not in (1,4,5,7,1318,1315,201,1009,1017,1327,1101,205,204,2804,2808,2824,2842,2846,1005,2822,2844,2848,1018,205,1020,1010,204,1019,1326,1034,1035,205,2806,201,204,201,2802,2832,1655,1096)
    #   and b.busi_code not between 9000 and 9020
    #   and b.op_id <> 9999
    #   and b.op_id not in (1,800)
    # group by
    #       b.so_org_id;"
    #puts $sql_buf
    #if [catch {aidb_sql $handle $sql_buf} errmsg] {
    #    trace_sql $errmsg 2060
    #    puts "errmsg:$errmsg"
    #    return -1
    #}

    set sql_buf "
    insert into $stat_market_0135_c_t1
    (
           channel_id,
           newbusi
    )
    select a.object_id,
           count(*)
      from $channel_nbusi_reward_yyyymm a
     where a.use_months = 2
     group by
           a.object_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2060
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_c_t1
    (
           channel_id,
           last_newbusi
    )
    select a.object_id,
           count(*)
      from $channel_nbusi_reward_ls1yyyymm a
     where a.use_months = 2
     group by
           a.object_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2070
        puts "errmsg:$errmsg"
        return -1
    }

    ##普通业务办理量
    #set sql_buf "
    #insert into $stat_market_0135_c_t1
    #(
    #       channel_id,
    #       normalbusi
    #)
    #select a.so_org_id,
    #       count(distinct a.so_nbr)
    #  from $dw_product_busi_dm_yyyymm a
    # where a.so_mode in ('0','1','2')
    #   and a.busi_code not in (1,4,5,7,1318,1315,201,1009,1017,1327,1101,205,204,2804,2808,2824,2842,2846,1005,2822,2844,2848,1018,205,1020,1010,204,1019,1326,1034,1035,205,2806,201,204,201,2802,2832,1655,1096)
    #   and a.busi_code not between 9000 and 9020
    #   and a.op_id <> 9999
    #   and a.op_id not in (1,800)
    #   and not exists (
    #        select 1
    #          from $dw_product_busi_msc_dm_yyyymm b
    #         where a.so_nbr = b.so_nbr)
    # group by
    #       a.so_org_id;"
    #puts $sql_buf
    #if [catch {aidb_sql $handle $sql_buf} errmsg] {
    #    trace_sql $errmsg 2080
    #    puts "errmsg:$errmsg"
    #    return -1
    #}

    set sql_buf "
    insert into $stat_market_0135_c_t1
    (
           channel_id,
           normalbusi
    )
    select a.object_id,
           count(*)
      from $channel_bbusi_reward_yyyymm a
     group by
           a.object_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2080
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_c_t1
    (
           channel_id,
           last_normalbusi
    )
    select a.object_id,
           count(*)
      from $channel_bbusi_reward_ls1yyyymm a
     group by
           a.object_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2090
        puts "errmsg:$errmsg"
        return -1
    }

    ##重复号码办理量
    set sql_buf "
    insert into $stat_market_0135_c_t4
    (
           object_id,
           phone_id,
           busi_id,
           busi_date
    )
    select a.object_id,
           a.phone_id,
           a.busi_id,
           so_date
      from $channel_bbusi_reward_yyyymm a
     union all
    select b.object_id,
           b.phone_id,
           b.busi_id,
           ext3
      from $channel_nbusi_reward_yyyymm b
     where use_months = 2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2100
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_c_t1
    (
           channel_id,
           duplicate_num,
           total_num
    )
    select object_id,
           count(case when busi_counts > 5 then phone_id end),
           count(*)
      from (
            select object_id,
                   phone_id,
                   count(distinct busi_id) as busi_counts
              from $stat_market_0135_c_t4 a
             group by
                   object_id,
                   phone_id
           ) a
     group by
           object_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2100
        puts "errmsg:$errmsg"
        return -1
    }

    ##递延酬金情况
    ##网点当月应结递延发放酬金金额
    ##D1(0)=放号第二次酬金金额为0元的号码占比达到第一次结算号码量的30%
    ##D2(0)=放号第三次酬金金额为0元的号码占比达到第一次结算号码量的60%
    set sql_buf "
    insert into $stat_market_0135_c_t1
    (
           channel_id,
           sub_reward_2nd,
           sub_reward_3rd
    )
    select channel_id,
           count(distinct case when user_months = 4 and fee = 0 then product_no end),
           count(distinct case when user_months = 7 and fee = 0 then product_no end)
      from $stat_channel_reward_0003
     where op_time = $year$month
     group by
           channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2110
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_c_t1
    (
           channel_id,
           sub_reward_2nd1st,
           sub_reward_3rd1st
    )
    select channel_id,
           count(distinct case when op_time = $ls2myear$ls2mmonth then product_no end),
           count(distinct case when op_time = $ls5myear$ls5mmonth then product_no end)
      from $stat_channel_reward_0003
     where op_time in ($ls5myear$ls5mmonth,$ls2myear$ls2mmonth)
       and user_months = 2
     group by
           channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2110
        puts "errmsg:$errmsg"
        return -1
    }

    ##D3(0)=新业务第二次酬金金额为0元的号码占比达到第一次结算号码量的20%
    ##D4(0)=新业务第三次酬金金额为0元的号码占比达到第一次结算号码量的250%
    set sql_buf "
    insert into $stat_market_0135_c_t1
    (
           channel_id,
           nbusi_reward_2nd,
           nbusi_reward_3rd
    )
    select channel_id,
           count(distinct case when use_months = 3 and b.op_time = date('$ls1month') then b.product_no end),
           count(distinct case when use_months = 5 and b.op_time = date('$ls3month') then b.product_no end)
      from $stat_channel_reward_0011 a
      left join (
            select product_no,
                   op_time
              from $stat_channel_reward_0011
             where op_time in (date('$ls1month'),date('$ls3month'))
               and use_months = 2
           ) b
        on a.product_no = b.product_no
     where a.op_time = $date_optime
     group by
           channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2120
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_c_t1
    (
           channel_id,
           nbusi_reward_2nd1st,
           nbusi_reward_3rd1st
    )
    select channel_id,
           count(distinct case when op_time = date('$ls1month') then product_no end),
           count(distinct case when op_time = date('$ls3month') then product_no end)
      from $stat_channel_reward_0011
     where op_time in (date('$ls1month'),date('$ls3month'))
       and use_months = 2
     group by
           channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2120
        puts "errmsg:$errmsg"
        return -1
    }

    #Step3.清除结果表历史数据
    set sql_buf "delete from $stat_market_0135_c where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3010
        puts "errmsg:$errmsg"
        return -1
    }

    #Step4.生成结果表数据
    set sql_buf "
    insert into $stat_market_0135_c
    (
           op_time       ,
           channel_id    ,
           reward        ,
           reward_rate   ,
           month_new     ,
           new_rate      ,
           newbusi       ,
           newbusi_rate  ,
           normalbusi    ,
           normal_rate   ,
           duplicate_rate,
           sub_2nd_rate  ,
           sub_3rd_rate  ,
           nbusi_2nd_rate,
           nbusi_3rd_rate
    )
    select $year$month,
           a.channel_id,
           sum(value(reward,0)),
           case when sum(value(avg_reward,0)) = 0
                then case when sum(value(reward,0)) = 0 then 0 else 100 end
                else sum(value(reward,0)) * 100.00 / sum(value(avg_reward,0)) - 100
           end,
           sum(value(month_new,0)),
           case when sum(value(last_month_new,0)) = 0
                then case when sum(value(month_new,0)) = 0 then 0 else 100 end
                else sum(value(month_new,0)) * 100.00 / sum(value(last_month_new,0)) - 100
           end,
           sum(value(newbusi,0)),
           case when sum(value(last_newbusi,0)) = 0
                then case when sum(value(newbusi,0)) = 0 then 0 else 100 end
                else sum(value(newbusi,0)) * 100.00 / sum(value(last_newbusi,0)) - 100
           end,
           sum(value(normalbusi,0)),
           case when sum(value(last_normalbusi,0)) = 0
                then case when sum(value(normalbusi,0)) = 0 then 0 else 100 end
                else sum(value(normalbusi,0)) * 100.00 / sum(value(last_normalbusi,0)) - 100
           end,
           case when sum(value(total_num,0)) = 0
                then 0
                else sum(value(duplicate_num,0)) * 100.00 / sum(value(total_num,0))
           end,
           case when sum(value(sub_reward_2nd1st,0)) = 0
                then 0
                else sum(value(sub_reward_2nd,0)) * 100.00 / sum(value(sub_reward_2nd1st,0))
           end,
           case when sum(value(sub_reward_3rd1st,0)) = 0
                then 0
                else sum(value(sub_reward_3rd,0)) * 100.00 / sum(value(sub_reward_3rd1st,0))
           end,
           case when sum(value(nbusi_reward_2nd1st,0)) = 0
                then 0
                else 100 - sum(value(nbusi_reward_2nd,0)) * 100.00 / sum(value(nbusi_reward_2nd1st,0))
           end,
           case when sum(value(nbusi_reward_3rd1st,0)) = 0
                then case when sum(value(nbusi_reward_3rd,0)) = 0 then 0 else 100 end
                else 100 - sum(value(nbusi_reward_3rd,0)) * 100.00 / sum(value(nbusi_reward_3rd1st,0))
           end
      from $stat_market_0135_c_t1 a
     group by
           a.channel_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 4010
        puts "errmsg:$errmsg"
        return -1
    }

    exec db2 "connect to bassdb user bass2 using bass2"
    exec db2 "runstats on table $stat_market_0135_c with distribution and indexes all"
    exec db2 "terminate"

    #Step5.酬金异动流程
    ##放号量增幅
    ##新业务开通量增幅
    ##普通业务办理量增幅
    set sql_buf "
    insert into $stat_market_0135_c_t3
    (
           entity_id,
           entity_type,
           new_rate,
           newbusi_rate,
           normal_rate
    )
    select int(value(a.region_type,b.region_type)),
           2,
           case when a.region_type is null then -100
                when b.region_type is null then 0
                when b.month_new = 0 then 100
                else a.month_new * 100.00 / b.month_new - 100
           end,
           case when a.region_type is null then -100
                when b.region_type is null then 0
                when b.newbusi = 0 then 100
                else a.newbusi * 100.00 / b.newbusi - 100
           end,
           case when a.region_type is null then -100
                when b.region_type is null then 0
                when b.normalbusi = 0 then 100
                else a.normalbusi * 100.00 / b.normalbusi - 100
           end
      from (
            select a.region_type,
                   sum(b.month_new) month_new,
                   sum(b.newbusi) newbusi,
                   sum(b.normalbusi) normalbusi
              from $dwd_channel_dept_yyyymmdd a,
                   $stat_market_0135_c b,
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
                   sum(b.month_new) month_new,
                   sum(b.newbusi) newbusi,
                   sum(b.normalbusi) normalbusi
              from $dwd_channel_dept_yyyymmdd a,
                   $stat_market_0135_c b,
                   $dim_bboss_channel_type c
             where a.organize_id = b.channel_id
               and a.channel_type = c.code_type
               and a.dept_type_dtl = c.code_id
               and c.code_name in ('特约代理点','指定专营店')
               and a.rec_status = 0
               and b.op_time = $ls1myear$ls1mmonth
             group by
                   a.region_type
           ) b
        on a.region_type = b.region_type;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5010
        puts "errmsg:$errmsg"
        return -1
    }

    ##预警阀值
    set sql_buf "delete from $stat_market_0135_c_result where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5040
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_c_result
    (
           op_time      ,
           channel_id   ,
           county_id    ,
           city_id      ,
           entity_type  ,
           warn_threshold
    )
    select $year$month,
           a.channel_id,
           char(b.region_type),
           char(b.channel_city),
           c.entity_type,
           a.new_rate / c.new_rate * 20
            + a.newbusi_rate / c.newbusi_rate * 10
            + a.normal_rate / c.normal_rate * 15
            + a.duplicate_rate / 10 * 20
            + a.sub_2nd_rate / 30 * 15
            + a.sub_3rd_rate / 60 * 5
            + a.nbusi_2nd_rate / 20 * 10
            + a.nbusi_3rd_rate / 250 * 5
      from $stat_market_0135_c a,
           $dwd_channel_dept_yyyymmdd b,
           $stat_market_0135_c_t3 c
     where a.channel_id = b.organize_id
       and int(b.region_type) = c.entity_id
       and a.reward_rate > 50
       and c.new_rate <> 0
       and c.newbusi_rate <> 0
       and c.normal_rate <> 0
       and b.rec_status = 0
       and c.entity_type = 2
       and a.op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5050
        puts "errmsg:$errmsg"
        return -1
    }

    ##清除结果表历史数据
    set sql_buf "delete from $stat_market_0135_c_final where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5060
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "delete from $stat_market_0135_c_link1 where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5060
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "delete from $stat_market_0135_c_link2 where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5060
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "delete from $stat_market_0135_c_link3 where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5060
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "delete from $stat_market_0135_c_link4 where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5060
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "delete from $stat_market_0135_c_link5 where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5060
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_market_0135_c_final
    (
           op_time,
           city_id,
           region_id,
           channel_id,
           channel_name,
           channel_type,
           channel_dtl_type,
           warn_level,
           reward,
           new_rate,
           newbusi_rate,
           normal_rate,
           duplicate_rate,
           sub_2nd_rate,
           sub_3rd_rate,
           nbusi_2nd_rate,
           nbusi_3rd_rate,
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
           case when b.warn_threshold >= 150 and b.warn_threshold < 225 then 1
                when b.warn_threshold >= 225 and b.warn_threshold < 300 then 2
                when b.warn_threshold >= 300 then 3
                else 0
           end,
           reward,
           new_rate,
           newbusi_rate,
           normal_rate,
           duplicate_rate,
           sub_2nd_rate,
           sub_3rd_rate,
           nbusi_2nd_rate,
           nbusi_3rd_rate,
           1,
           ''
      from $stat_market_0135_c a,
           $stat_market_0135_c_result b,
           $dwd_channel_dept_yyyymmdd c
      left join
           $dim_pub_channel_ext d
        on c.channel_id = int(d.channel_id)
     where a.channel_id = b.channel_id
       and a.channel_id = c.organize_id
       and b.entity_type = 2
       and a.op_time = $year$month
       and b.op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5070
        puts "errmsg:$errmsg"
        return -1
    }

    ##普通业务办理量占比
    set sql_buf "
    insert into $stat_market_0135_c_link1
    (
           op_time,
           channel_id,
           product_no,
           busi_name,
           busi_date
    )
    select $year$month,
           a.object_id,
           a.phone_id,
           c.opt_name,
           a.busi_date
      from $stat_market_0135_c_t4 a
      left join
           $map_busi_code c
        on a.busi_id = c.business_id,
           (
            select a.object_id,
                   a.phone_id,
                   count(distinct a.busi_id) busi_counts
              from $stat_market_0135_c_t4 a
             group by
                   a.object_id,
                   a.phone_id
           ) b
     where a.object_id = b.object_id
       and a.phone_id = b.phone_id
       and b.busi_counts > 5;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5080
        puts "errmsg:$errmsg"
        return -1
    }

    ##放号第二次酬金金额为0元的号码
    set sql_buf "
    insert into $stat_market_0135_c_link2
    (
           op_time,
           channel_id,
           product_no,
           join_date,
           userstatus
    )
    select $year$month,
           a.channel_id,
           a.product_no,
           a.create_date,
           c.userstatus_name
      from $stat_channel_reward_0003 a,
           (
            select product_no,
                   userstatus_id,
                   row_number() over (partition by product_no order by sts_date desc) as sts_order
              from $dw_product_yyyymm b
           ) b
      left join
           $dim_user_status c
        on b.userstatus_id = c.userstatus_id
     where a.product_no = b.product_no
       and b.sts_order = 1
       and a.user_months = 4
       and a.fee = 0
       and a.op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5090
        puts "errmsg:$errmsg"
        return -1
    }

    ##放号第三次酬金金额为0元的号码
    set sql_buf "
    insert into $stat_market_0135_c_link3
    (
           op_time,
           channel_id,
           product_no,
           join_date,
           userstatus
    )
    select $year$month,
           a.channel_id,
           a.product_no,
           a.create_date,
           c.userstatus_name
      from $stat_channel_reward_0003 a,
           (
            select product_no,
                   userstatus_id,
                   row_number() over (partition by product_no order by sts_date desc) as sts_order
              from $dw_product_yyyymm b
           ) b
      left join
           $dim_user_status c
        on b.userstatus_id = c.userstatus_id
     where a.product_no = b.product_no
       and b.sts_order = 1
       and a.user_months = 7
       and a.fee = 0
       and a.op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5100
        puts "errmsg:$errmsg"
        return -1
    }

    ##新业务第二次酬金金额为0元的号码
    set sql_buf "
    insert into $stat_market_0135_c_link4
    (
           op_time,
           channel_id,
           product_no,
           busi_name,
           busi_date,
           userstatus
    )
    select $year$month,
           a.channel_id,
           a.product_no,
           a.busi_name,
           a.join_date,
           d.userstatus_name
      from $stat_channel_reward_0011 a
      left join 
           (
            select product_no
              from $stat_channel_reward_0011
             where op_time = $date_optime
               and use_months = 3
           ) b
        on a.product_no = b.product_no,
           (
            select product_no,
                   userstatus_id,
                   row_number() over (partition by product_no order by sts_date desc) as sts_order
              from $dw_product_yyyymm b
           ) c
      left join
           $dim_user_status d
        on d.userstatus_id = c.userstatus_id
     where a.product_no = c.product_no
       and a.use_months = 2
       and b.product_no is null
       and a.op_time = date('$ls1month');"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5110
        puts "errmsg:$errmsg"
        return -1
    }

    ##新业务第三次酬金金额为0元的号码
    set sql_buf "
    insert into $stat_market_0135_c_link5
    (
           op_time,
           channel_id,
           product_no,
           busi_name,
           busi_date,
           userstatus
    )
    select $year$month,
           a.channel_id,
           a.product_no,
           a.busi_name,
           a.join_date,
           d.userstatus_name
      from $stat_channel_reward_0011 a
      left join 
           (
            select product_no
              from $stat_channel_reward_0011
             where op_time = $date_optime
               and use_months = 5
           ) b
        on a.product_no = b.product_no,
           (
            select product_no,
                   userstatus_id,
                   row_number() over (partition by product_no order by sts_date desc) as sts_order
              from $dw_product_yyyymm b
           ) c
      left join
           $dim_user_status d
        on d.userstatus_id = c.userstatus_id
     where a.product_no = c.product_no
       and a.use_months = 2
       and b.product_no is null
       and a.op_time = date('$ls3month');"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5110
        puts "errmsg:$errmsg"
        return -1
    }

    #Step5.清空临时表
    set sql_buf "drop table $stat_market_0135_c_t1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_c_t2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_c_t3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_market_0135_c_t4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    return 0
}