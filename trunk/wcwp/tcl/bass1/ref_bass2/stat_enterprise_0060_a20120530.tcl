#======================================================================================
#版权归属：Copyright (c) 2010,AsiaInfo.Report.System
#程序名称: stat_enterprise_0060_a.tcl
#程序功能: 2012年集团客户综合统计报表-1、集团客户和成员
#分析目标: 略
#分析指标: 见维表
#分析维度: 全区 地市
#运行粒度: 月
#运行示例: crt_basetab.sh stat_enterprise_0060_a.tcl 2011-11-01
#创建时间: 2011-12-09
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

    if {[stat_enterprise_0060_a $p_optime] != 0} {
        aidb_roll $conn
        aidb_close $handle
        return -1
    }

    aidb_commit $conn
    aidb_close $handle

    return 0
}

proc stat_enterprise_0060_a {p_optime} {
    global conn
    global handle

    source stat_insert_index.tcl
    source report.cfg

    #日期处理
    set    date_optime      [ai_to_date $p_optime]
    scan    $p_optime       "%04s-%02s-%02s" year month day
    puts    $p_optime 
    set    day_n1m_optime   [clock format [ clock scan "${year}${month}${day} + 1month" ] -format "%Y-%m-%d"]
    set    date_n1m_optime  [ai_to_date $day_n1m_optime]
    set    day_me_optime    [clock format [ clock scan "${year}${month}${day} + 1month - 1day" ] -format "%Y-%m-%d"]
    set    date_me_optime   [ai_to_date $day_me_optime]
    scan   $day_me_optime   "%04s-%02s-%02s" meyear memonth meday
    set    date_ye_optime   [ai_to_date "${year}-12-31"]
    set    day_l1m_optime   [clock format [ clock scan "${year}${month}${day} - 1month" ] -format "%Y-%m-%d"]
    scan   $day_l1m_optime  "%04s-%02s-%02s" lmyear lmmonth lmday
    set    day_l2m_optime   [clock format [ clock scan "${year}${month}${day} - 2month" ] -format "%Y-%m-%d"]
    set    day_start_optime "$year-01-01"
    set    day_lsyear_optime    [clock format [ clock scan "${year}${month}${day} - 1year" ] -format "%Y-%m-%d"]
    scan   $day_lsyear_optime   "%04s-%02s-%02s" lyyear lymonth lyday
    set    day_lsyear_endmonth  "${lyyear}-12-01"
    puts   $day_lsyear_endmonth

    #代换字符串
    set test_enterprise_id  "'89100000000682','89100000000659','89100000000656','89100000000651'"

    #源表
    set dw_enterprise_member_mid_yyyymm "bass2.dw_enterprise_member_mid_$year$month"
    set dw_enterprise_member_mid_lmyyyymm "bass2.dw_enterprise_member_mid_$lmyear$lmmonth"
    set dw_enterprise_msg_yyyymm        "bass2.dw_enterprise_msg_$year$month"
    set dw_enterprise_msg_lmyyyymm      "bass2.dw_enterprise_msg_$lmyear$lmmonth"
    set dw_enterprise_extsub_rela_yyyymm  "bass2.dw_enterprise_extsub_rela_$year$month"
    set dw_product_yyyymm               "bass2.dw_product_$year$month"
    set dw_product_td_yyyymm            "bass2.dw_product_td_$year$month"
    set dw_acct_shoulditem_yyyymm       "bass2.dw_acct_shoulditem_$year$month"
    set dw_td_check_user_yyyymm         "bass2.dw_td_check_user_three_$year$month"

    #目标表
    set stat_enterprise_0060_a          "bass2.stat_enterprise_0060_a"

    #维表
    set dim_pub_city                    "bassweb.dim_pub_city"
    set ent_index_s_1                   "bass2.ent_index_s_1"
    set dim_ent_unipay_item             "bass2.dim_ent_unipay_item"

    #临时表
    set stat_enterprise_0060_a_tmp1     "bass2.stat_enterprise_0060_a_tmp1"
    set stat_enterprise_0060_a_tmp2     "bass2.stat_enterprise_0060_a_tmp2"
    set stat_enterprise_0060_a_tmp3     "bass2.stat_enterprise_0060_a_tmp3"
    set stat_enterprise_0060_a_tmp4     "bass2.stat_enterprise_0060_a_tmp4"
    set stat_enterprise_0060_a_tmp5     "bass2.stat_enterprise_0060_a_tmp5"
    set stat_enterprise_0060_a_tmp6     "bass2.stat_enterprise_0060_a_tmp6"

    #Step1.创建结果临时表
    set sql_buf "drop table $stat_enterprise_0060_a_tmp1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_a_tmp2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_a_tmp3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_a_tmp4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_a_tmp5;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_a_tmp6;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    set sql_buf "
    create table $stat_enterprise_0060_a_tmp1
    (
           enterprise_id    varchar(20),
           city_id          varchar(7),
           time_order       smallint,
           group_level      varchar(4),
           month_new_mark   smallint,
           member_counts    integer,
           ent_sub_counts   integer,
           ent_prod_counts  integer,
           ent_prod_income  decimal(12,2),
           member_income    decimal(12,2)
    )
    in tbs_report index in tbs_index partitioning key (enterprise_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1010
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_enterprise_0060_a_tmp2
    (
           user_id          varchar(20),
           enterprise_id    varchar(20),
           city_id          varchar(7),
           group_level      varchar(4),
           snapshot_mark    smallint,
           active_mark      smallint,
           td_type_id       smallint,
           td_term_mark     smallint
    )
    in tbs_report index in tbs_index partitioning key (user_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1020
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_enterprise_0060_a_tmp3
    (
           city_id      varchar(7),
           time_order   smallint,
           off_counts   integer
    )
    in tbs_report index in tbs_index partitioning key (city_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1030
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_enterprise_0060_a_tmp4
    (
           city_id      varchar(7),
           time_order   smallint,
           member_counts integer
    )
    in tbs_report index in tbs_index partitioning key (city_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1040
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_enterprise_0060_a_tmp5
    (
           city_id     varchar(7),
           s_index_id  smallint,
           result      decimal(12,2)
    )
    in tbs_report index in tbs_index partitioning key (s_index_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1050
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_enterprise_0060_a_tmp6
    (
           time_order      smallint,
           user_id         varchar(20),
           enterprise_id   varchar(20),
           city_id         varchar(8),
           group_level     smallint
    )
    in tbs_report index in tbs_index partitioning key (user_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1050
        puts "errmsg:$errmsg"
        return -1
    }


    #Step2.生成临时表数据
  for {set loopmonth $day_l2m_optime;set time_order "3"} {$loopmonth <= $p_optime} {
          set loopmonth [clock format [clock scan "${loopmonth} + 1 months" ] -format "%Y-%m-%d" ];set time_order "$time_order - 1"} {
  #循环开始
    puts $loopmonth
    scan $loopmonth "%04s-%02s-%02s" lpyear lpmonth lpday

    set date_lpmonth    [ai_to_date $loopmonth]
    set date_lpmonthend [ai_to_date [clock format [ clock scan "${lpyear}${lpmonth}${lpday} + 1month - 1day" ] -format "%Y-%m-%d"]]
    set day_lpl1mmonth  [clock format [ clock scan "${lpyear}${lpmonth}${lpday} - 1month" ] -format "%Y-%m-%d"]
    scan $day_lpl1mmonth "%04s-%02s-%02s" lpl1myear lpl1mmonth lpl1mday

    set dw_product_lpyyyymm               "bass2.dw_product_$lpyear$lpmonth"
    set dw_enterprise_msg_lpyyyymm        "bass2.dw_enterprise_msg_$lpyear$lpmonth"
    set dw_enterprise_msg_lpl1myyyymm      "bass2.dw_enterprise_msg_$lpl1myear$lpl1mmonth"
    set dw_enterprise_member_mid_lpyyyymm "bass2.dw_enterprise_member_mid_$lpyear$lpmonth"
    set dw_enterprise_new_unipay_lpyyyymm "bass2.dw_enterprise_new_unipay_$lpyear$lpmonth"
    set dw_enterprise_sub_lpyyyymm        "bass2.dw_enterprise_sub_$lpyear$lpmonth"

    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp1
    (
           enterprise_id,
           city_id,
           time_order,
           group_level,
           month_new_mark,
           member_counts,
           ent_sub_counts,
           ent_prod_counts,
           ent_prod_income,
           member_income
    )
    select a.enterprise_id,
           case when a.level_def_mode = 1 then '888'
                else a.ent_city_id
           end,
           $time_order,
           case when group_level in (0,1) then 'A'
                when group_level in (2,3) then 'B'
                else 'C'
           end,
           case when e.enterprise_id is null
                then 1
                else 0
           end,
           value(b.member_counts,0),
           value(d.ent_sub_counts,0),
           value(c.ent_prod_counts,0),
           value(c.ent_prod_income,0),
           value(b.member_income,0)
      from $dw_enterprise_msg_lpyyyymm a
      left join
           (
            select b.enterprise_id,
                   count(distinct a.user_id) member_counts,
                   sum(a.fact_fee) member_income
              from $dw_product_lpyyyymm a,
                   $dw_enterprise_member_mid_lpyyyymm b
             where a.user_id = b.user_id
               and b.dummy_mark = 0
               and b.test_mark = 0
             group by
                   b.enterprise_id
           ) b on a.enterprise_id = b.enterprise_id
      left join
           (
            select enterprise_id,
                   count(distinct service_id) ent_prod_counts,
                   sum(unipay_fee) ent_prod_income
              from $dw_enterprise_new_unipay_lpyyyymm
             where unipay_fee > 0
             group by
                   enterprise_id
           ) c on a.enterprise_id = c.enterprise_id
      left join
           (
            select enterprise_id,
                   count(distinct service_id) ent_sub_counts
              from $dw_enterprise_sub_lpyyyymm
             where valid_date <= $date_lpmonthend
               and expire_date >= $date_lpmonth
             group by
                   enterprise_id
           ) d on a.enterprise_id = d.enterprise_id
      left join
           (
            select enterprise_id
              from $dw_enterprise_msg_lpl1myyyymm
             where ent_status_id = 0
               and enterprise_id not in (select enterprise_id from $dw_enterprise_msg_lpl1myyyymm 
                                         where is_test = 1 )
           ) e on a.enterprise_id = e.enterprise_id
     where a.ent_status_id = 0
       and a.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_lpyyyymm
                                   where is_test = 1 );"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2010
        puts "errmsg:$errmsg"
        return -1
    }
    
   #循环结束
  }
  


    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp2
    (
           user_id,
           enterprise_id,
           city_id,
           group_level,
           snapshot_mark,
           active_mark,
           td_type_id,
           td_term_mark
    )
    select a.user_id,
           c.enterprise_id,
           case when c.level_def_mode = 1 then '888'
                else c.ent_city_id
           end,
           case when c.group_level in (0,1) then 'A'
                when c.group_level in (2,3) then 'B'
                when c.group_level in (4) then 'C'
           end,
           a.snapshot_mark,
           a.active_mark,
           case when d.td_type_id is not null
                then case when d.td_type_id in (3,5)
                           and f.user_id is null
                          then 9
                          else d.td_type_id
                     end
                else 9
           end,
           value(e.td_term_mark,0)
      from $dw_product_yyyymm a,
           $dw_enterprise_member_mid_yyyymm b
      left join
           (
            select product_no,
                   int(td_type_id) td_type_id
              from $dw_td_check_user_yyyymm
             where td_type_id <> '1'
           ) d on b.product_no = d.product_no
      left join
           (
            select user_id,
                   1 td_term_mark
              from $dw_product_td_yyyymm
             where td_user_mark = 1
               and td_2gcard_mark = 0
               and td_3gcard_mark = 0
           ) e on b.user_id = e.user_id
      left join
           (
            select user_id
              from $dw_enterprise_extsub_rela_yyyymm
             where service_id = '934'
               and rec_status = 1
           ) f on b.user_id = f.user_id,
           $dw_enterprise_msg_yyyymm c
     where a.user_id = b.user_id
       and b.enterprise_id = c.enterprise_id
       and b.test_mark = 0
       and b.dummy_mark = 0
       and c.ent_status_id = 0
       and c.enterprise_id not in ($test_enterprise_id)
       and a.userstatus_id in (1,2,3,6,8);"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }

  for {set loopmonth $day_start_optime;set time_order $month} {$loopmonth <= $p_optime} {
          set loopmonth [clock format [clock scan "${loopmonth} + 1 months" ] -format "%Y-%m-%d"];set time_order "$time_order - 1"} {
  #循环开始
    puts $loopmonth
    puts $time_order
    scan $loopmonth     "%04s-%02s-%02s" lpyear lpmonth lpday

    set date_lpmonth    [ai_to_date $loopmonth]

    set day_lslpmonth   [clock format [ clock scan "${lpyear}${lpmonth}${lpday} - 1month" ] -format "%Y-%m-%d"]
    scan $day_lslpmonth "%04s-%02s-%02s" lslpyear lslpmonth lslpday

    set dw_product_lpyyyymm               "bass2.dw_product_$lpyear$lpmonth"
    set dw_enterprise_msg_lpyyyymm        "bass2.dw_enterprise_msg_$lslpyear$lslpmonth"
    set dw_enterprise_member_mid_lpyyyymm "bass2.dw_enterprise_member_mid_$lslpyear$lslpmonth"

    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp3
    (
           city_id,
           time_order,
           off_counts
    )
    select case when a.level_def_mode = 1 then '888'
                else value(a.ent_city_id,b.ent_city_id)
           end,
           $time_order,
           count(distinct b.user_id)
      from $dw_enterprise_msg_lpyyyymm a,
           $dw_enterprise_member_mid_lpyyyymm b,
           $dw_product_lpyyyymm c
     where a.enterprise_id = b.enterprise_id
       and c.user_id = b.user_id
       and b.test_mark = 0
       and b.dummy_mark = 0
       and c.month_off_mark = 1
       and c.month_new_mark = 0
       and a.ent_status_id = 0
       and a.enterprise_id not in ($test_enterprise_id)
     group by
           case when a.level_def_mode = 1 then '888'
                else value(a.ent_city_id,b.ent_city_id)
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2030
        puts "errmsg:$errmsg"
        return -1
    }

  #循环结束
  }

  for {set loopmonth $day_lsyear_endmonth;set time_order "$month + 1"} {$loopmonth <= $p_optime} {
          set loopmonth [clock format [clock scan "${loopmonth} + 1 months" ] -format "%Y-%m-%d"];set time_order "$time_order - 1"} {
  #循环开始
    puts $loopmonth
    puts $time_order
    scan $loopmonth     "%04s-%02s-%02s" lpyear lpmonth lpday

    set date_lpmonth    [ai_to_date $loopmonth]

    set dw_enterprise_msg_lpyyyymm        "bass2.dw_enterprise_msg_$lpyear$lpmonth"
    set dw_enterprise_member_mid_lpyyyymm "bass2.dw_enterprise_member_mid_$lpyear$lpmonth"

    set sql_buf "
    insert into stat_enterprise_0060_a_tmp4
    (
           city_id,
           time_order,
           member_counts
    )
    select case when b.level_def_mode = 1 then '888'
                else value(b.ent_city_id,a.ent_city_id)
           end,
           $time_order,
           count(distinct a.user_id)
      from $dw_enterprise_member_mid_lpyyyymm a,
           $dw_enterprise_msg_lpyyyymm b
     where a.enterprise_id = b.enterprise_id
       and a.dummy_mark = 0
       and a.test_mark = 0
       and b.ent_status_id = 0
       and b.enterprise_id not in ($test_enterprise_id)
     group by
           case when b.level_def_mode = 1 then '888'
                else value(b.ent_city_id,a.ent_city_id)
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2040
        puts "errmsg:$errmsg"
        return -1
    }

  #循环结束
  }

    #Step3.提取各指标
    ##集团客户到达数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when group_level = 'A' then 2
                when group_level = 'B' then 3
                when group_level = 'C' then 4
           end,
           sum(1)
      from $stat_enterprise_0060_a_tmp1 a,
           (
             select enterprise_id,sum(ent_prod_income + member_income) as income
             from $stat_enterprise_0060_a_tmp1
             group by enterprise_id
             
           ) b
     where time_order=1
     and a.enterprise_id = b.enterprise_id
     and b.income > 0
     group by
           city_id,
           case when group_level = 'A' then 2
                when group_level = 'B' then 3
                when group_level = 'C' then 4
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3010
        puts "errmsg:$errmsg"
        return -1
    }
    
    
    #其中：法人单位集团客户数
     set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           5,
           count(distinct a.enterprise_id)
      from $stat_enterprise_0060_a_tmp1 a,
           (
             select enterprise_id,sum(ent_prod_income + member_income) as income
             from $stat_enterprise_0060_a_tmp1
             group by enterprise_id
             
           ) b,
           (select distinct zw_group_id 
            from Dw_ent_group_relation_$year$month ) c
     where time_order=1
     and a.enterprise_id = b.enterprise_id
     and a.enterprise_id = c.zw_group_id 
     and b.income > 0
     group by
             city_id"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3010
        puts "errmsg:$errmsg"
        return -1
    }
    


    ##集团客户净增数(需要修正口径)
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when group_level = 'A' then 8
                when group_level = 'B' then 9
                when group_level = 'C' then 10
           end,
           sum(case when time_order = 1 then 1 else 0 end)
            - sum(case when time_order = 2 then 1 else 0 end)
      from $stat_enterprise_0060_a_tmp1 a,
           (
             select enterprise_id,sum(ent_prod_income + member_income) as income
             from $stat_enterprise_0060_a_tmp1
             group by enterprise_id
             
           ) b
     where a.time_order in (1,2)
           and a.enterprise_id = b.enterprise_id
           and b.income > 0
     group by
           city_id,
           case when group_level = 'A' then 8
                when group_level = 'B' then 9
                when group_level = 'C' then 10
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3020
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团客户新增数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when group_level = 'A' then 12
                when group_level = 'B' then 13
                when group_level = 'C' then 14
           end,
           sum(1)
      from $stat_enterprise_0060_a_tmp1
     where month_new_mark = 1
       and time_order = 1
     group by
           city_id,
           case when group_level = 'A' then 12
                when group_level = 'B' then 13
                when group_level = 'C' then 14
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3030
        puts "errmsg:$errmsg"
        return -1
    }

    ##统一付费的集团客户到达数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           15,
           sum(1)
      from (
            select enterprise_id,
                   city_id
              from $stat_enterprise_0060_a_tmp1
             where ent_prod_income > 0
               and time_order in (1,2,3)
             group by
                   enterprise_id,
                   city_id
            having sum(1) = 3
           ) a
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3040
        puts "errmsg:$errmsg"
        return -1
    }

    ##使用1项产品的集团客户数
    ##使用2项产品的集团客户数
    ##使用3项以上产品的集团客户数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when ent_prod_counts = 1 then 18
                when ent_prod_counts = 2 then 19
                when ent_prod_counts >= 3 then 20
           end,
           sum(1)
      from $stat_enterprise_0060_a_tmp1
     where time_order = 1
       and ent_prod_counts > 0
     group by
           city_id,
           case when ent_prod_counts = 1 then 18
                when ent_prod_counts = 2 then 19
                when ent_prod_counts >= 3 then 20
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3050
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团产品收入为0的集团客户数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           21,
           count(distinct enterprise_id)
      from $stat_enterprise_0060_a_tmp1
     where member_income > 0
       and ent_prod_income = 0
       and ent_sub_counts > 0
       and time_order in (1,2,3)
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3060
        puts "errmsg:$errmsg"
        return -1
    }

    ##服务协议集团客户数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           22,
           count(distinct enterprise_id)
      from $stat_enterprise_0060_a_tmp1
     where member_income > 0
       and ent_sub_counts = 0
       and time_order in (1,2,3)
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3070
        puts "errmsg:$errmsg"
        return -1
    }

    ##订购产品但整体收入为0的集团客户数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           23,
           count(distinct enterprise_id)
      from $stat_enterprise_0060_a_tmp1
     where member_income = 0
       and ent_prod_income = 0
       and ent_prod_counts = 0
       and ent_sub_counts > 0
       and time_order = 1
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3080
        puts "errmsg:$errmsg"
        return -1
    }

    ##其中：零成员集团客户数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           24,
           count(distinct enterprise_id)
      from $stat_enterprise_0060_a_tmp1
     where member_income = 0
       and member_counts = 0
       and ent_prod_income = 0
       and ent_prod_counts = 0
       and ent_sub_counts > 0
       and time_order = 1
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3090
        puts "errmsg:$errmsg"
        return -1
    }

    ##未订购产品但整体收入为0的集团客户数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           25,
           count(distinct enterprise_id)
      from $stat_enterprise_0060_a_tmp1
     where member_income = 0
       and ent_prod_income = 0
       and ent_prod_counts = 0
       and ent_sub_counts = 0
       and time_order = 1
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3100
        puts "errmsg:$errmsg"
        return -1
    }

    ##其中：零成员集团客户数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           26,
           count(distinct enterprise_id)
      from $stat_enterprise_0060_a_tmp1
     where member_income = 0
       and member_counts = 0
       and ent_prod_income = 0
       and ent_prod_counts = 0
       and ent_sub_counts = 0
       and time_order = 1
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3110
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团成员到达数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when group_level = 'A' then 29
                when group_level = 'B' then 30
                when group_level = 'C' then 31
           end,
           sum(member_counts)
      from $stat_enterprise_0060_a_tmp1
     where time_order = 1
     group by
           city_id,
           case when group_level = 'A' then 29
                when group_level = 'B' then 30
                when group_level = 'C' then 31
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3120
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团成员净增数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when group_level = 'A' then 35
                when group_level = 'B' then 36
                when group_level = 'C' then 37
           end,
           sum(case when time_order = 1 then member_counts else 0 end)
            - sum(case when time_order = 2 then member_counts else 0 end)
      from $stat_enterprise_0060_a_tmp1
     where time_order in (1,2)
     group by
           city_id,
           case when group_level = 'A' then 35
                when group_level = 'B' then 36
                when group_level = 'C' then 37
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3130
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团成员新增数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp6
    (
           time_order,
           user_id,
           enterprise_id,
           city_id,
           group_level
    )
    select 1,
           a.user_id,
           a.enterprise_id,
           case when b.level_def_mode = 1 then '888'
                else value(b.ent_city_id,a.ent_city_id)
           end,
           b.group_level
      from $dw_enterprise_member_mid_yyyymm a,
           $dw_enterprise_msg_yyyymm b
     where a.enterprise_id = b.enterprise_id
       and a.dummy_mark = 0
       and b.ent_status_id = 0
       and b.enterprise_id not in ($test_enterprise_id)
     union all
    select 2,
           a.user_id,
           a.enterprise_id,
           case when b.level_def_mode = 1 then '888'
                else value(b.ent_city_id,a.ent_city_id)
           end,
           b.group_level
      from $dw_enterprise_member_mid_lmyyyymm a,
           $dw_enterprise_msg_lmyyyymm b
     where a.enterprise_id = b.enterprise_id
       and a.dummy_mark = 0
       and b.ent_status_id = 0
       and b.enterprise_id not in ($test_enterprise_id);"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3140
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           case when a.group_level in (0,1) then 39
                when a.group_level in (2,3) then 40
                when a.group_level in (4) then 41
           end,
           count(distinct a.user_id)
      from $stat_enterprise_0060_a_tmp6 a
     where not exists (
            select 1
              from $stat_enterprise_0060_a_tmp6 b
             where a.user_id = b.user_id
               and b.time_order = 2)
       and a.time_order = 1
     group by
           a.city_id,
           case when a.group_level in (0,1) then 39
                when a.group_level in (2,3) then 40
                when a.group_level in (4) then 41
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3140
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团成员离网数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select case when a.level_def_mode = 1 then '888'
                else value(a.ent_city_id,b.ent_city_id)
           end,
           case when a.group_level in (0,1) then 43
                when a.group_level in (2,3) then 44
                when a.group_level in (4) then 45
           end,
           count(distinct b.user_id)
      from $dw_enterprise_msg_lmyyyymm a,
           $dw_enterprise_member_mid_lmyyyymm b,
           $dw_product_yyyymm c
     where a.enterprise_id = b.enterprise_id
       and c.user_id = b.user_id
       and b.test_mark = 0
       and b.dummy_mark = 0
       and c.month_off_mark = 1
       and c.month_new_mark = 0
       and a.ent_status_id = 0
       and a.enterprise_id not in ($test_enterprise_id)
     group by
           case when a.level_def_mode = 1 then '888'
                else value(a.ent_city_id,b.ent_city_id)
           end,
           case when a.group_level in (0,1) then 43
                when a.group_level in (2,3) then 44
                when a.group_level in (4) then 45
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3150
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团成员占移动通信客户比重
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select b.city_id,
           46,
           b.member_counts * 100.00 / a.active_counts
      from (
            select city_id,
                   count(distinct user_id) member_counts
              from $stat_enterprise_0060_a_tmp2
             group by
                   city_id
           ) b
      left join
           (
            select case when b.user_id is not null then b.city_id
                        else a.city_id
                   end city_id,
                   count(distinct a.user_id) active_counts
              from $dw_product_yyyymm a
              left join
                   (
                    select user_id,
                           city_id
                      from $stat_enterprise_0060_a_tmp2
                   ) b on a.user_id = b.user_id
             where a.active_mark = 1
             group by
                   case when b.user_id is not null then b.city_id
                        else a.city_id
                   end
           ) a on a.city_id = b.city_id
    union all
    select '890',
           46,
           b.member_counts * 100.00 / a.active_counts
      from (
            select count(distinct user_id) member_counts
              from $stat_enterprise_0060_a_tmp2
           ) b,
           (
            select count(distinct a.user_id) active_counts
              from $dw_product_yyyymm a
              left join
                   (
                    select user_id,
                           city_id
                      from $stat_enterprise_0060_a_tmp2
                   ) b on a.user_id = b.user_id
             where a.active_mark = 1
           ) a;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3160
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团中高端成员到达数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when group_level = 'A' then 48
                when group_level = 'B' then 49
                when group_level = 'C' then 50
           end,
           count(distinct user_id)
      from $stat_enterprise_0060_a_tmp2
     where snapshot_mark = 1
     group by
           city_id,
           case when group_level = 'A' then 48
                when group_level = 'B' then 49
                when group_level = 'C' then 50
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3170
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团成员通信客户数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           51,
           count(distinct user_id)
      from $stat_enterprise_0060_a_tmp2
     where active_mark = 1
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3180
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团成员离网率
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select b.city_id,
           52,
           value(a.off_counts,0) * 100.00 / float($meday * 12.00 / dayofyear($date_ye_optime)) / b.avg_member_counts
      from (
            select city_id,
                   avg(member_counts) avg_member_counts
              from $stat_enterprise_0060_a_tmp4
             where time_order in (1,2)
             group by
                   city_id
           ) b
      left join
           $stat_enterprise_0060_a_tmp3 a on a.city_id = b.city_id and a.time_order = 1
    union all
    select '890',
           52,
           value(a.off_counts,0) * 100.00 / float($meday * 12.00 / dayofyear($date_ye_optime)) / b.avg_member_counts
      from (
            select avg(member_counts) avg_member_counts
              from (
                    select time_order,
                           sum(member_counts) member_counts
                      from $stat_enterprise_0060_a_tmp4
                     where time_order in (1,2)
                     group by
                           time_order
                   ) b
           ) b,
           (
            select sum(off_counts) off_counts
              from $stat_enterprise_0060_a_tmp3
             where time_order = 1
           ) a;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3190
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团成员累计平均月离网率
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select b.city_id,
           53,
           100.00 * value(a.avg_off_counts,0) / float(dayofyear($date_me_optime)*12.00/dayofyear($date_ye_optime)) / b.avg_member_counts
      from (
            select city_id,
                   avg(member_counts) avg_member_counts
              from $stat_enterprise_0060_a_tmp4
             group by
                   city_id
           ) b
      left join
           (
            select city_id,
                   avg(off_counts) avg_off_counts
              from $stat_enterprise_0060_a_tmp3
             group by
                   city_id
           ) a on a.city_id = b.city_id
    union all
    select '890',
           53,
           100.00 * value(a.avg_off_counts,0) / float(dayofyear($date_me_optime)*12.00/dayofyear($date_ye_optime)) / b.avg_member_counts
      from (
            select avg(member_counts) avg_member_counts
              from (
                    select time_order,
                           sum(member_counts) member_counts
                      from $stat_enterprise_0060_a_tmp4
                     group by
                           time_order
                   ) b
           ) b,
           (
            select avg(off_counts) avg_off_counts
              from (
                    select time_order,
                           sum(off_counts) off_counts
                      from $stat_enterprise_0060_a_tmp3
                     group by
                           time_order
                   ) a
           ) a;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3200
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团TD客户到达数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           54,
           count(distinct user_id)
      from $stat_enterprise_0060_a_tmp2
     where td_type_id in (2,3,4,5)
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3210
        puts "errmsg:$errmsg"
        return -1
    }

    ##其中：集团TD手机客户数
    ##      集团TD数据卡客户数
    ##      集团TD上网本客户数
    ##      集团TD无线座机客户数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when td_type_id = 2 then 55
                when td_type_id = 3 then 56
                when td_type_id = 5 then 57
                when td_type_id = 4 then 58
           end,
           count(distinct a.user_id)
      from $stat_enterprise_0060_a_tmp2 a
     where a.td_type_id in (2,3,4,5)
     group by
           city_id,
           case when td_type_id = 2 then 55
                when td_type_id = 3 then 56
                when td_type_id = 5 then 57
                when td_type_id = 4 then 58
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3220
        puts "errmsg:$errmsg"
        return -1
    }

    ##其中：集团TD终端客户数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           59,
           count(distinct user_id)
      from $stat_enterprise_0060_a_tmp2
     where td_term_mark = 1
       and td_type_id in (2,3,4,5)
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3220
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团产品订购用户数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select value(a.city_id,b.ent_city_id),
           60,
           count(distinct b.user_id)
      from $dw_enterprise_extsub_rela_yyyymm b
      left join $stat_enterprise_0060_a_tmp2 a on a.user_id = b.user_id
     where b.rec_status = 1
     group by
           value(a.city_id,b.ent_city_id);"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3230
        puts "errmsg:$errmsg"
        return -1
    }

    ##其中：集团成员数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           61,
           count(distinct b.user_id)
      from $stat_enterprise_0060_a_tmp2 a,
           $dw_enterprise_extsub_rela_yyyymm b
     where a.user_id = b.user_id
       and b.rec_status = 1
     group by
           a.city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3240
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团产品付费用户数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select value(a.city_id,b.ent_city_id),
           62,
           count(distinct c.user_id)
      from $dw_enterprise_extsub_rela_yyyymm b
      left join $stat_enterprise_0060_a_tmp2 a on a.user_id = b.user_id
      left join
           (
            select a.user_id
              from $dw_acct_shoulditem_yyyymm a,
                   $dim_ent_unipay_item b
             where a.item_id = b.item_id
             group by
                   a.user_id
            having sum(a.fact_fee) > 0
           ) c on b.user_id = c.user_id
     where b.rec_status = 1
     group by
           value(a.city_id,b.ent_city_id);"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3250
        puts "errmsg:$errmsg"
        return -1
    }

    ##其中：集团成员数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           63,
           count(distinct c.user_id)
      from $stat_enterprise_0060_a_tmp2 a,
           $dw_enterprise_extsub_rela_yyyymm b
      left join
           (
            select a.user_id
              from $dw_acct_shoulditem_yyyymm a,
                   $dim_ent_unipay_item b
             where a.item_id = b.item_id
             group by
                   a.user_id
            having sum(a.fact_fee) > 0
           ) c on b.user_id = c.user_id
     where a.user_id = b.user_id
       and b.rec_status = 1
     group by
           a.city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3260
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团产品收入为0的集团成员数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           64,
           sum(a.member_counts)
      from $stat_enterprise_0060_a_tmp1 a
     where a.member_income > 0
       and a.ent_prod_income = 0
       and a.ent_sub_counts > 0
       and a.time_order = 1
     group by
           a.city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3270
        puts "errmsg:$errmsg"
        return -1
    }

    ##服务协议集团成员数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           65,
           sum(member_counts)
      from $stat_enterprise_0060_a_tmp1
     where member_income > 0
       and ent_sub_counts = 0
       and time_order = 1
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3280
        puts "errmsg:$errmsg"
        return -1
    }

    ##订购产品但整体收入为0的集团客户成员数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           66,
           sum(member_counts)
      from $stat_enterprise_0060_a_tmp1
     where member_income = 0
       and ent_prod_income = 0
       and ent_prod_counts = 0
       and ent_sub_counts > 0
       and time_order = 1
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3290
        puts "errmsg:$errmsg"
        return -1
    }

    ##未订购产品且整体收入=0的集团客户成员数
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           67,
           sum(member_counts)
      from $stat_enterprise_0060_a_tmp1
     where member_income = 0
       and ent_prod_income = 0
       and ent_prod_counts = 0
       and ent_sub_counts = 0
       and time_order = 1
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3300
        puts "errmsg:$errmsg"
        return -1
    }

    ##汇总部分指标
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when s_index_id in (2,3,4) then 1
                when s_index_id in (8,9,10) then 7
                when s_index_id in (12,13,14) then 11
                when s_index_id in (29,30,31) then 28
                when s_index_id in (35,36,37) then 34
                when s_index_id in (39,40,41) then 38
                when s_index_id in (43,44,45) then 42
                when s_index_id in (48,49,50) then 47
           end,
           sum(result)
      from $stat_enterprise_0060_a_tmp5
     where s_index_id in (2,3,4,8,9,10,12,13,14,29,30,31,35,36,37,39,40,41,43,44,45,48,49,50)
     group by
           city_id,
           case when s_index_id in (2,3,4) then 1
                when s_index_id in (8,9,10) then 7
                when s_index_id in (12,13,14) then 11
                when s_index_id in (29,30,31) then 28
                when s_index_id in (35,36,37) then 34
                when s_index_id in (39,40,41) then 38
                when s_index_id in (43,44,45) then 42
                when s_index_id in (48,49,50) then 47
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3310
        puts "errmsg:$errmsg"
        return -1
    }

    ##汇总全区数据
    set sql_buf "
    insert into $stat_enterprise_0060_a_tmp5
    (
           city_id,
           s_index_id,
           result
    )
    select '890',
           s_index_id,
           sum(result)
      from $stat_enterprise_0060_a_tmp5
     where s_index_id not in (17,46,52,53)
     group by
           s_index_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3320
        puts "errmsg:$errmsg"
        return -1
    }

    #Step4.清除结果表历史数据
    set sql_buf "delete from $stat_enterprise_0060_a where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 4010
        puts "errmsg:$errmsg"
        return -1
    }
    
    #Step5.生成结果表数据
    set sql_buf "
    insert into $stat_enterprise_0060_a
    (
           op_time,
           city_id,
           s_index_id,
           result
    )
    select $year$month,
           a.city_id,
           a.s_index_id,
           value(b.result,0)
      from (
            select city_id,
                   s_index_id
              from $dim_pub_city a,
                   $ent_index_s_1 b
           ) a
      left join
           $stat_enterprise_0060_a_tmp5 b on a.city_id = b.city_id and a.s_index_id = b.s_index_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5010
        puts "errmsg:$errmsg"
        return -1
    }
    
    exec db2 "connect to bassdb user bass2 using bass2"
    exec db2 "runstats on table $stat_enterprise_0060_a with distribution and indexes all"
    exec db2 "terminate"
    
    #Step6.清除临时表
    set sql_buf "drop table $stat_enterprise_0060_a_tmp1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_a_tmp2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_a_tmp3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_a_tmp4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_a_tmp5;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    return 0
}