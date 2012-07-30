#======================================================================================
#版权归属：Copyright (c) 2010,AsiaInfo.Report.System
#程序名称: stat_enterprise_0060_b.tcl
#程序功能: 2012年集团客户综合统计报表-2、集团收入和业务量
#分析目标: 略
#分析指标: 见维表
#分析维度: 全区 地市
#运行粒度: 月
#运行示例: crt_basetab.sh stat_enterprise_0060_b.tcl 2011-01-01
#创建时间: 2011-4-20
#创 建 人: Asiainfo-Linkage 
#存在问题:
#修改历史: 1、修改信息化收入统计，全部由新集团统付表提取数据，行业应用表不再使用
#          2、2011-7-12 修改集团订购口径，取消原rec_status = 1，改用生效、失效日期
#          3、2011-7-25 根据张阳工单（COM_OTHER_XZ_LS_20110708_49888 ），具体修改内容见工单附件 无线商话增加此统计口径.doc
#          4、2011-11-16 根据曾祥庆工单(COM_OTHER_XZ_LS_20111110_65259)修改增加无线商话数据
#=======================================================================================
proc deal {p_optime p_timestamp} {
    global conn
    global handle

    if [catch {set handle [aidb_open $conn]} errmsg] {
        #trace_sql $errmsg 1000
        return -1
    }

    if {[stat_enterprise_0060_b $p_optime] != 0} {
        aidb_roll $conn
        aidb_close $handle
        return -1
    }

    aidb_commit $conn
    aidb_close $handle

    return 0
}

proc stat_enterprise_0060_b {p_optime} {
    global conn
    global handle

    source stat_insert_index.tcl
    source report.cfg

    #日期处理
    set    date_optime      [ai_to_date $p_optime]
    scan   $p_optime        "%04s-%02s-%02s" year month day
    set    day_l2m_optime   [clock format [ clock scan "${year}${month}${day} - 2month" ] -format "%Y-%m-%d"]
    set    day_start_optime "$year-01-01"
    set    day_lsyear_optime    [clock format [ clock scan "${year}${month}${day} - 1year" ] -format "%Y-%m-%d"]
    scan   $day_lsyear_optime   "%04s-%02s-%02s" lyyear lymonth lyday
    set    day_lsyear_endmonth  "${lyyear}-12-01"
    puts   $day_lsyear_endmonth
    set    date_ye_optime   [ai_to_date "${year}-12-31"]
    set    day_me_optime    [clock format [ clock scan "${year}${month}${day} + 1month - 1day" ] -format "%Y-%m-%d"]
    set    date_me_optime   [ai_to_date $day_me_optime]
    scan   $day_me_optime   "%04s-%02s-%02s" meyear memonth meday

    #源表
    set dw_enterprise_member_mid_yyyymm "bass2.dw_enterprise_member_mid_$year$month"
    set dw_enterprise_msg_yyyymm        "bass2.dw_enterprise_msg_$year$month"
    set dw_enterprise_new_unipay_yyyymm "bass2.dw_enterprise_new_unipay_$year$month"
    set dw_product_yyyymm               "bass2.dw_product_$year$month"
    set dw_product_td_yyyymm            "bass2.dw_product_td_$year$month"
    set dw_td_check_user_yyyymm         "bass2.dw_td_check_user_three_$year$month"
    set dw_call_yyyymm                  "bass2.dw_call_$year$month"
    set dw_acct_owe_yyyymm              "bass2.dw_acct_owe_$year$month"
    set dw_enterprise_industry_apply    "bass2.dw_enterprise_industry_apply"
    set dw_newbusi_ismg_yyyymm          "bass2.dw_newbusi_ismg_$year$month"
    set dw_acct_shoulditem_yyyymm       "bass2.dw_acct_shoulditem_$year$month"

    set dw_product_lyeyyyymm            "bass2.dw_product_${lyyear}12"
    set dw_enterprise_msg_lyeyyyymm     "bass2.dw_enterprise_msg_${lyyear}12"
    set dw_enterprise_member_mid_lyeyyyymm "bass2.dw_enterprise_member_mid_${lyyear}12"

    #目标表
    set stat_enterprise_0060_b          "bass2.stat_enterprise_0060_b"

    #维表
    set dim_pub_city                    "bassweb.dim_pub_city"
    set ent_index_s_2                   "bass2.ent_index_s_2"

    #临时表
    set stat_enterprise_0060_b_tmp1     "bass2.stat_enterprise_0060_b_tmp1"
    set stat_enterprise_0060_b_tmp2     "bass2.stat_enterprise_0060_b_tmp2"
    set stat_enterprise_0060_b_tmp3     "bass2.stat_enterprise_0060_b_tmp3"
    set stat_enterprise_0060_b_tmp4     "bass2.stat_enterprise_0060_b_tmp4"
    set stat_enterprise_0060_b_tmp6     "bass2.stat_enterprise_0060_b_tmp6"
    set stat_enterprise_0060_b_tmp5     "bass2.stat_enterprise_0060_b_tmp5"
    set stat_enterprise_0060_b_tmp7     "bass2.stat_enterprise_0060_b_tmp7"

    #字符串替换
    ##只统计统付收入的信息化产品
    set info_prod_unipay    "'142','906','910','917','924','925','931','933','934','943','951','953','966','912001','912002','10105002','SPEC_400','SPEC|40010'"
    ##统计总收入的信息化产品
    set info_prod_total     "'717','903','904','911','926','936','942','944','945','946','947','949','939001','939002','SPEC|BLACKBERRY'"
    ##测试集团
    set test_enterprise_id  "'89100000000682','89100000000659','89100000000656','89100000000651'"

    #Step1.创建结果临时表
    set sql_buf "drop table $stat_enterprise_0060_b_tmp1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_b_tmp2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_b_tmp3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_b_tmp4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_b_tmp5;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_b_tmp6;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_b_tmp7;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    set sql_buf "
    create table $stat_enterprise_0060_b_tmp1
    (
           enterprise_id    varchar(20),
           city_id          varchar(7),
           group_level      varchar(4),
           total_income     decimal(12,2),
           member_income    decimal(12,2),
           member_iv_income decimal(12,2),
           unipay_income    decimal(12,2),
           ent_prod_income  decimal(12,2),
           sms_income       decimal(12,2),
           mms_income       decimal(12,2),
           ent_prod_unipay_income   decimal(12,2),
           info_income      decimal(12,2),
           info_unipay_income       decimal(12,2),
           owe_fee          decimal(12,2)
    )
    in tbs_report index in tbs_index partitioning key (enterprise_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       #trace_sql $errmsg 1010
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_enterprise_0060_b_tmp2
    (
           user_id          varchar(20),
           enterprise_id    varchar(20),
           city_id          varchar(7),
           fact_fee         decimal(12,2),
           iv_fact_fee      decimal(12,2),
           call_duration_m  integer,
           o_call_dur_m     integer,
           t_call_dur_m     integer,
           local_call_dur_m integer,
           toll_call_dur_m  integer,
           roam_call_dur_m  integer,
           industry_sms_counts integer
    )
    in tbs_report index in tbs_index partitioning key (user_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       #trace_sql $errmsg 1020
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_enterprise_0060_b_tmp3
    (
           city_id          varchar(7),
           time_order       smallint,
           member_income    decimal(12,2),
           call_duration_m  integer,
           member_counts    integer
    )
    in tbs_report index in tbs_index partitioning key (city_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       #trace_sql $errmsg 1030
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_enterprise_0060_b_tmp4
    (
           city_id     varchar(7),
           s_index_id  smallint,
           result      decimal(12,2)
    )
    in tbs_report index in tbs_index partitioning key (city_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       #trace_sql $errmsg 1040
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_enterprise_0060_b_tmp6
    (
           city_id     varchar(7),
           s_index_id  smallint,
           result      decimal(12,2)
    )
    in tbs_report index in tbs_index partitioning key (city_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       #trace_sql $errmsg 1040
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_enterprise_0060_b_tmp5
    (
           enterprise_id    varchar(20),
           time_order       smallint
    )
    in tbs_report index in tbs_index partitioning key (enterprise_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       #trace_sql $errmsg 1050
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_enterprise_0060_b_tmp7
    (
           enterprise_id            varchar(20),
           city_id                  varchar(8),
           group_level              smallint,
           user_id                  varchar(20),
           fact_fee                 decimal(12,2),
           call_duration_m          integer
    )
    in tbs_ods_other index in tbs_index partitioning key (user_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        #trace_sql $errmsg 1060
        puts "errmsg:$errmsg"
        return -1
    }

    #Step2.生成临时表数据
    ##无线商话临时表
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp7
    (
           enterprise_id,
           city_id,
           group_level,
           user_id,
           fact_fee,
           call_duration_m
    )
    select c.enterprise_id,
           c.ent_city_id,
           c.group_level,
           b.user_id,
           b.fact_fee,
           b.call_duration_m
      from $dw_td_check_user_yyyymm a,
           $dw_product_yyyymm b,
           (
            select a.user_id,
                   b.enterprise_id,
                   b.ent_city_id,
                   b.group_level
              from $dw_enterprise_member_mid_yyyymm a,
                   $dw_enterprise_msg_yyyymm b
             where a.enterprise_id = b.enterprise_id
               and a.test_mark = 0
               and a.dummy_mark = 0
               and b.enterprise_name in ('西藏昌都地区金泰大酒店','昌都地区金川宾馆')
           ) c
     where a.product_no = b.product_no
       and b.user_id = c.user_id
       and b.userstatus_id in (1,2,3,6,8)
       and b.test_mark <> 1
       and b.usertype_id in (1,2,9)
       and a.td_type_id = '4';"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2040
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp2
    (
           user_id,
           enterprise_id,
           city_id,
           fact_fee,
           iv_fact_fee,
           call_duration_m,
           o_call_dur_m,
           t_call_dur_m,
           local_call_dur_m,
           toll_call_dur_m,
           roam_call_dur_m,
           industry_sms_counts
    )
    select b.user_id,
           b.enterprise_id,
           case when c.level_def_mode = 1 then '888'
                else c.ent_city_id
           end,
           a.fact_fee,
           a.fact_fee - a.local_fee - a.toll_fee - a.roam_fee,
           a.call_duration_m,
           value(d.o_call_dur_m,0),
           value(d.t_call_dur_m,0),
           value(d.local_call_dur_m,0),
           value(d.toll_call_dur_m,0),
           value(d.roam_call_dur_m,0),
           value(e.industry_sms_counts,0)
      from $dw_product_yyyymm a,
           $dw_enterprise_member_mid_yyyymm b
      left join
           (
            select user_id,
                   sum(case when calltype_id in (0,2,3) then call_duration_m else 0 end) o_call_dur_m,
                   sum(case when calltype_id = 1 then call_duration_m else 0 end) t_call_dur_m,
                   sum(case when tolltype_id = 0 and roamtype_id = 0 then call_duration_m else 0 end) local_call_dur_m,
                   sum(case when tolltype_id <> 0 and roamtype_id = 0 then call_duration_m else 0 end) toll_call_dur_m,
                   sum(case when roamtype_id <> 0 then call_duration_m else 0 end) roam_call_dur_m
              from $dw_call_yyyymm
             group by
                   user_id
           ) d on b.user_id = d.user_id
      left join
           (
            select user_id,
                   sum(counts) industry_sms_counts
              from $dw_newbusi_ismg_yyyymm
             where calltype_id = 1
               and ser_code like '10657%'
             group by
                   user_id
           ) e on b.user_id = e.user_id,
           $dw_enterprise_msg_yyyymm c
     where a.user_id = b.user_id
       and b.enterprise_id = c.enterprise_id
       and b.dummy_mark = 0
       and b.test_mark = 0
       and c.ent_status_id = 0
       and c.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test = 1);"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 2010
        puts "errmsg:$errmsg"
        return -1
    }

    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp1
    (
           enterprise_id,
           city_id,
           group_level,
           total_income,
           member_income,
           member_iv_income,
           unipay_income,
           ent_prod_income,
           sms_income,
           mms_income,
           ent_prod_unipay_income,
           info_income,
           info_unipay_income,
           owe_fee
    )
    select a.enterprise_id,
           case when a.level_def_mode = 1 then '888'
                else a.ent_city_id
           end,
           case when a.group_level in (0,1) then 'A'
                when a.group_level in (2,3) then 'B'
                else 'C'
           end,
           value(c.unipay_fee,0) + value(b.member_income,0),
           value(b.member_income,0),
           value(b.member_iv_income,0),
           value(c.unipay_fee,0),
           value(c.unipay_fee,0) + value(c.non_unipay_fee,0),
           value(d.industry_income,0),
           0,
           value(c.unipay_fee,0),
           value(c.info_fee,0),
           value(c.info_unipay_fee,0),
           value(c.owe_fee,0)
      from $dw_enterprise_msg_yyyymm a
      left join
           (
            select enterprise_id,
                   sum(fact_fee) member_income,
                   sum(iv_fact_fee) member_iv_income
              from $stat_enterprise_0060_b_tmp2
             group by
                   enterprise_id
           ) b on a.enterprise_id = b.enterprise_id
      left join
           (
            select enterprise_id,
                   sum(unipay_fee) unipay_fee,
                   sum(non_unipay_fee) non_unipay_fee,
                   sum(case when a.unipay_fee > 0 then b.owe_fee else 0 end) owe_fee,
                   sum(case when char(a.service_id) in ($info_prod_unipay)
                            then a.unipay_fee
                            when char(a.service_id) in ($info_prod_total)
                            then a.unipay_fee + a.non_unipay_fee
                            else 0
                       end) info_fee,
                   sum(case when char(a.service_id) in ($info_prod_unipay,$info_prod_total)
                            then a.unipay_fee
                            else 0
                       end) info_unipay_fee
              from $dw_enterprise_new_unipay_yyyymm a
              left join
                   (
                    select acct_id,
                           sum(unpay_fee) owe_fee
                      from $dw_acct_owe_yyyymm
                     group by
                           acct_id
                   ) b on a.acct_id = b.acct_id
             group by
                   enterprise_id
           ) c on a.enterprise_id = c.enterprise_id
      left join
           (
            select enterprise_id,
                   sum(fee) industry_income
              from $dw_enterprise_industry_apply
             where op_time = $date_optime
             group by
                   enterprise_id
           ) d on a.enterprise_id = d.enterprise_id
     where a.ent_status_id = 0
       and a.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test = 1);"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }

  for {set loopmonth $day_start_optime;set time_order $month} {$loopmonth <= $p_optime} {
          set loopmonth [clock format [clock scan "${loopmonth} + 1 months" ] -format "%Y-%m-%d" ];set time_order "$time_order - 1"} {
  #循环开始
    puts $loopmonth
    puts $time_order
    scan $loopmonth "%04s-%02s-%02s" lpyear lpmonth lpday

    set date_lpmonth    [ai_to_date $loopmonth]

    set dw_product_lpyyyymm               "bass2.dw_product_$lpyear$lpmonth"
    set dw_enterprise_msg_lpyyyymm        "bass2.dw_enterprise_msg_$lpyear$lpmonth"
    set dw_enterprise_member_mid_lpyyyymm "bass2.dw_enterprise_member_mid_$lpyear$lpmonth"

    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp3
    (
           city_id,
           time_order,
           member_income,
           call_duration_m,
           member_counts
    )
    select case when c.level_def_mode = 1 then '888'
                else c.ent_city_id
           end,
           $time_order,
           sum(a.fact_fee),
           sum(a.call_duration_m),
           count(distinct a.user_id)
      from $dw_product_lpyyyymm a,
           $dw_enterprise_member_mid_lpyyyymm b,
           $dw_enterprise_msg_lpyyyymm c
     where a.user_id = b.user_id
       and b.enterprise_id = c.enterprise_id
       and b.dummy_mark = 0
       and b.test_mark = 0
       and c.ent_status_id = 0
       and c.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_lpyyyymm where is_test=1)
     group by
           case when c.level_def_mode = 1 then '888'
                else c.ent_city_id
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 2030
        puts "errmsg:$errmsg"
        return -1
    }

  #循环结束
  }

    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp3
    (
           city_id,
           time_order,
           member_income,
           call_duration_m,
           member_counts
    )
    select case when c.level_def_mode = 1 then '888'
                else c.ent_city_id
           end,
           -1,
           sum(a.fact_fee),
           sum(a.call_duration_m),
           count(distinct a.user_id)
      from $dw_product_lyeyyyymm a,
           $dw_enterprise_member_mid_lyeyyyymm b,
           $dw_enterprise_msg_lyeyyyymm c
     where a.user_id = b.user_id
       and b.enterprise_id = c.enterprise_id
       and b.dummy_mark = 0
       and b.test_mark = 0
       and c.ent_status_id = 0
       and c.enterprise_id not in ($test_enterprise_id)
     group by
           case when c.level_def_mode = 1 then '888'
                else c.ent_city_id
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 2030
        puts "errmsg:$errmsg"
        return -1
    }

  for {set loopmonth $day_l2m_optime;set time_order "3"} {$loopmonth <= $p_optime} {
          set loopmonth [clock format [clock scan "${loopmonth} + 1 months" ] -format "%Y-%m-%d"];set time_order "$time_order - 1"} {
  #循环开始
    puts $loopmonth
    puts $time_order
    scan $loopmonth     "%04s-%02s-%02s" lpyear lpmonth lpday

    set date_lpmonth    [ai_to_date $loopmonth]

    set date_lpemonth   [ai_to_date [clock format [ clock scan "${lpyear}${lpmonth}${lpday} + 1month - 1day" ] -format "%Y-%m-%d"]]

    set dw_product_lpyyyymm               "bass2.dw_product_$lpyear$lpmonth"
    set dw_enterprise_sub_lpyyyymm        "bass2.dw_enterprise_sub_$lpyear$lpmonth"
    set dw_enterprise_msg_lpyyyymm        "bass2.dw_enterprise_msg_$lpyear$lpmonth"
    set dw_enterprise_member_mid_lpyyyymm "bass2.dw_enterprise_member_mid_$lpyear$lpmonth"

    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp5
    (
           enterprise_id,
           time_order
    )
    select a.enterprise_id,
           $time_order
      from $dw_enterprise_msg_lpyyyymm a
      left join
           (
            select enterprise_id,
                   count(distinct service_id) ent_sub_counts
              from $dw_enterprise_sub_lpyyyymm
             where valid_date <= $date_lpemonth
               and expire_date >= $date_lpmonth
             group by
                   enterprise_id
            having count(distinct service_id) > 0
           ) d on a.enterprise_id = d.enterprise_id,
           (
            select b.enterprise_id,
                   sum(a.fact_fee) member_income
              from $dw_product_lpyyyymm a,
                   $dw_enterprise_member_mid_lpyyyymm b
             where a.user_id = b.user_id
               and b.dummy_mark = 0
               and b.test_mark = 0
             group by
                   b.enterprise_id
            having sum(a.fact_fee) > 0
           ) b
     where a.enterprise_id = b.enterprise_id
       and d.enterprise_id is null
       and a.ent_status_id = 0
       and a.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_lpyyyymm where is_test=1 );"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 2040
        puts "errmsg:$errmsg"
        return -1
    }

  #循环结束
  }

    #Step3.提取各指标
    ##集团客户整体收入
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
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
           sum(total_income)
      from $stat_enterprise_0060_b_tmp1
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

    ##集团成员收入
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           5,
           sum(member_income)
      from $stat_enterprise_0060_b_tmp1
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3020
        puts "errmsg:$errmsg"
        return -1
    }

#    ##其中：增值业务收入
#    set sql_buf "
#    insert into $stat_enterprise_0060_b_tmp4
#    (
#           city_id,
#           s_index_id,
#           result
#    )
#    select city_id,
#           6,
#           sum(member_iv_income)
#      from $stat_enterprise_0060_b_tmp1
#     group by
#           city_id;"
#    puts $sql_buf
#    if [catch {aidb_sql $handle $sql_buf} errmsg] {
#       #trace_sql $errmsg 3030
#        puts "errmsg:$errmsg"
#        return -1
#    }

    ##集团客户统一付费收入
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           6,
           sum(unipay_income)
      from $stat_enterprise_0060_b_tmp1
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3040
        puts "errmsg:$errmsg"
        return -1
    }

    ##服务协议集团客户整体收入
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           7,
           sum(total_income)
      from $stat_enterprise_0060_b_tmp1 a,
           (
            select enterprise_id
              from $stat_enterprise_0060_b_tmp5
             group by
                   enterprise_id
           ) b
     where a.enterprise_id = b.enterprise_id
     group by
           a.city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3050
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团产品收入
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when group_level = 'A' then 10
                when group_level = 'B' then 11
                when group_level = 'C' then 12
           end,
           sum(ent_prod_income)
      from $stat_enterprise_0060_b_tmp1
     group by
           city_id,
           case when group_level = 'A' then 10
                when group_level = 'B' then 11
                when group_level = 'C' then 12
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3060
        puts "errmsg:$errmsg"
        return -1
    }

    ##其中：行业网关下行短信收入
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           14,
           sum(sms_income)
      from $stat_enterprise_0060_b_tmp1
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3070
        puts "errmsg:$errmsg"
        return -1
    }

    ##行业网关下行彩信收入
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           15,
           sum(mms_income)
      from $stat_enterprise_0060_b_tmp1
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3080
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团产品统一付费收入
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           16,
           sum(ent_prod_unipay_income)
      from $stat_enterprise_0060_b_tmp1
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3090
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团信息化收入
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when group_level = 'A' then 18
                when group_level = 'B' then 19
                when group_level = 'C' then 20
           end,
           sum(info_income)
      from $stat_enterprise_0060_b_tmp1
     group by
           city_id,
           case when group_level = 'A' then 18
                when group_level = 'B' then 19
                when group_level = 'C' then 20
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3100
        puts "errmsg:$errmsg"
        return -1
    }
    
    
    
    
        ##20120331 luodk add  与才德吉沟通，将阿里医保收入统计到该项中。
    ##其他集团信息化收入(修改)
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select c.city_id,
           37,
           sum(value(b.fact_fee,0))
      from $dw_acct_shoulditem_yyyymm b,
           $dw_product_yyyymm c
      where b.user_id=c.user_id 
            and b.item_id in (80000802,80000803)
     group by c.city_id
      ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3100
        puts "errmsg:$errmsg"
        return -1
    }
    
    
    
    

    ##集团统一付费信息化收入
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           22,
           sum(info_unipay_income)
      from $stat_enterprise_0060_b_tmp1
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3110
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团成员计费时长
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           23,
           sum(call_duration_m)
      from $stat_enterprise_0060_b_tmp2
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3120
        puts "errmsg:$errmsg"
        return -1
    }

    ##其中主叫计费时长
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           24,
           sum(o_call_dur_m)
      from $stat_enterprise_0060_b_tmp2
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3130
        puts "errmsg:$errmsg"
        return -1
    }

    ##其中被叫计费时长
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           25,
           sum(t_call_dur_m)
      from $stat_enterprise_0060_b_tmp2
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3140
        puts "errmsg:$errmsg"
        return -1
    }

    ##其中本地计费时长
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           26,
           sum(local_call_dur_m)
      from $stat_enterprise_0060_b_tmp2
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3150
        puts "errmsg:$errmsg"
        return -1
    }

    ##其中长途计费时长
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           27,
           sum(toll_call_dur_m)
      from $stat_enterprise_0060_b_tmp2
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3160
        puts "errmsg:$errmsg"
        return -1
    }

    ##其中漫游计费时长
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           28,
           sum(roam_call_dur_m)
      from $stat_enterprise_0060_b_tmp2
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3170
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团短信业务量
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           29,
           sum(industry_sms_counts)
      from $stat_enterprise_0060_b_tmp2
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3170
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团成员本月ARPU
        set sql_buf "
        insert into $stat_enterprise_0060_b_tmp4
        (
               city_id,
               s_index_id,
               result
        )
        select city_id,
               31,
               sum(case when time_order = 1 then member_income else 0 end)
                / float($meday * 12.00 / dayofyear($date_ye_optime)) / sum(member_counts)
          from $stat_enterprise_0060_b_tmp3
         where time_order in (1)
         group by
               city_id
         union all
        select '890',
               31,
               sum(case when time_order = 1 then member_income else 0 end)
                / float($meday * 12.00 / dayofyear($date_ye_optime)) / sum(member_counts)
          from (
                select time_order,
                       sum(member_income) member_income,
                       sum(member_counts) member_counts
                  from $stat_enterprise_0060_b_tmp3
                 where time_order in (1)
                 group by
                       time_order
               ) a;"
        puts $sql_buf
        if [catch {aidb_sql $handle $sql_buf} errmsg] {
           trace_sql $errmsg 3180
            puts "errmsg:$errmsg"
            return -1
        }
    

    ##集团成员本年累计ARPU
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           32,
           sum(case when time_order <> -1 then member_income else 0 end)
            / float(dayofyear($date_me_optime) * 12.00 / dayofyear($date_ye_optime)) / avg(member_counts)
      from $stat_enterprise_0060_b_tmp3
     group by
           city_id
     union all
    select '890',
           32,
           sum(case when time_order <> -1 then member_income else 0 end)
            / float(dayofyear($date_me_optime) * 12.00 / dayofyear($date_ye_optime)) / avg(member_counts)
      from (
            select time_order,
                   sum(member_income) member_income,
                   sum(member_counts) member_counts
              from $stat_enterprise_0060_b_tmp3
             group by
                   time_order
           ) a;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3190
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团成员本月MOU
    if { $month == "01" } {
        set sql_buf "
        insert into $stat_enterprise_0060_b_tmp4
        (
               city_id,
               s_index_id,
               result
        )
        select city_id,
               33,
               sum(case when time_order = 1 then call_duration_m else 0 end)
                / float($meday * 12.00 / dayofyear($date_ye_optime)) / avg(member_counts)
          from $stat_enterprise_0060_b_tmp3
         where time_order in (-1,1)
         group by
               city_id
         union all
        select '890',
               33,
               sum(case when time_order = 1 then call_duration_m else 0 end)
                / float($meday * 12.00 / dayofyear($date_ye_optime)) / avg(member_counts)
          from (
                select time_order,
                       sum(call_duration_m) call_duration_m,
                       sum(member_counts) member_counts
                  from $stat_enterprise_0060_b_tmp3
                 where time_order in (-1,1)
                 group by
                       time_order
               ) a;"
        puts $sql_buf
        if [catch {aidb_sql $handle $sql_buf} errmsg] {
           trace_sql $errmsg 3200
            puts "errmsg:$errmsg"
            return -1
        }
    } else {
        set sql_buf "
        insert into $stat_enterprise_0060_b_tmp4
        (
               city_id,
               s_index_id,
               result
        )
        select city_id,
               33,
               sum(case when time_order = 1 then call_duration_m else 0 end)
                / float($meday * 12.00 / dayofyear($date_ye_optime)) / avg(member_counts)
          from $stat_enterprise_0060_b_tmp3
         where time_order in (1,2)
         group by
               city_id
         union all
        select '890',
               33,
               sum(case when time_order = 1 then call_duration_m else 0 end)
                / float($meday * 12.00 / dayofyear($date_ye_optime)) / avg(member_counts)
          from (
                select time_order,
                       sum(call_duration_m) call_duration_m,
                       sum(member_counts) member_counts
                  from $stat_enterprise_0060_b_tmp3
                 where time_order in (1,2)
                 group by
                       time_order
               ) a;"
        puts $sql_buf
        if [catch {aidb_sql $handle $sql_buf} errmsg] {
           trace_sql $errmsg 3200
            puts "errmsg:$errmsg"
            return -1
        }
    }

    ##集团成员本年累计MOU
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           34,
           sum(case when time_order <> -1 then bigint(call_duration_m) else 0 end)
            / float(dayofyear($date_me_optime) * 12.00 / dayofyear($date_ye_optime)) / avg(member_counts)
      from $stat_enterprise_0060_b_tmp3
     group by
           city_id
     union all
    select '890',
           34,
           sum(case when time_order <> -1 then bigint(call_duration_m) else 0 end)
            / float(dayofyear($date_me_optime) * 12.00 / dayofyear($date_ye_optime)) / avg(member_counts)
      from (
            select time_order,
                   sum(call_duration_m) call_duration_m,
                   sum(member_counts) member_counts
              from $stat_enterprise_0060_b_tmp3
             group by
                   time_order
           ) a;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3210
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团成员综合单价
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           35,
           sum(fact_fee) / sum(bigint(call_duration_m))
      from $stat_enterprise_0060_b_tmp2
     group by
           city_id
     union all
    select '890',
           35,
           sum(fact_fee) / sum(bigint(call_duration_m))
      from $stat_enterprise_0060_b_tmp2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3220
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团成员语音单价
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           36,
           sum(fact_fee - iv_fact_fee) / sum(bigint(call_duration_m))
      from $stat_enterprise_0060_b_tmp2
     group by
           city_id
     union all
    select '890',
           36,
           sum(fact_fee - iv_fact_fee) / sum(bigint(call_duration_m))
      from $stat_enterprise_0060_b_tmp2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3230
        puts "errmsg:$errmsg"
        return -1
    }

#    ##集团客户欠费
#    set sql_buf "
#    insert into $stat_enterprise_0060_b_tmp4
#    (
#           city_id,
#           s_index_id,
#           result
#    )
#    select city_id,
#           38,
#           sum(owe_fee)
#      from $stat_enterprise_0060_b_tmp1
#     group by
#           city_id;"
#    puts $sql_buf
#    if [catch {aidb_sql $handle $sql_buf} errmsg] {
#       #trace_sql $errmsg 3240
#        puts "errmsg:$errmsg"
#        return -1
#    }

    ####根据张阳工单（COM_OTHER_XZ_LS_20110708_49888 ），具体修改内容见工单附件 无线商话增加此统计口径.doc
    ##根据曾祥庆工单(COM_OTHER_XZ_LS_20111110_65259)修改增加无线商话数据
    ##16 17 19 22
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           16,
           sum(fact_fee)
      from $stat_enterprise_0060_b_tmp7
     group by
           city_id
     union all
    select city_id,
           case when group_level in (0,1) then 18
                when group_level in (2,3) then 19
                else 20
           end,
           sum(fact_fee)
      from $stat_enterprise_0060_b_tmp7
     group by
           city_id,
           case when group_level in (0,1) then 18
                when group_level in (2,3) then 19
                else 20
           end
     union all
    select city_id,
           22,
           sum(fact_fee)
      from $stat_enterprise_0060_b_tmp7
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3100
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团产品统一付费收入(修改)
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select '892',
           16,
          sum(value(c.fact_fee,0))
      from
      (SELECT distinct product_instance_id
            FROM dw_product_ins_off_ins_prod_ds
            where cust_party_role_id='89200000007999'
            and is_main_offer=0
            and expire_date>='$day_me_optime 23:59:59.000000'
            and valid_date<= '$day_me_optime 23:59:59.000000'
           ) b,
           $dw_product_yyyymm c
      where b.product_instance_id=c.user_id
    ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3100
        puts "errmsg:$errmsg"
        return -1
    }

    ##B类集团信息化收入(修改)
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select '892',
           19,
           sum(value(c.fact_fee,0))
      from
      (SELECT distinct product_instance_id
            FROM dw_product_ins_off_ins_prod_ds
            where cust_party_role_id='89200000007999'
            and is_main_offer=0
            and expire_date>='$day_me_optime 23:59:59.000000'
            and valid_date<= '$day_me_optime 23:59:59.000000'
           ) b,
           $dw_product_yyyymm c
      where b.product_instance_id=c.user_id
      ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3100
        puts "errmsg:$errmsg"
        return -1
    }
    

    

    ##集团统一付费信息化收入(修改)
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select '892',
           22,
           sum(value(c.fact_fee,0))
      from
            (SELECT distinct product_instance_id
            FROM dw_product_ins_off_ins_prod_ds
            where cust_party_role_id='89200000007999'
            and is_main_offer=0
            and expire_date>='$day_me_optime 23:59:59.000000'
            and valid_date<= '$day_me_optime 23:59:59.000000'
           ) b,
           $dw_product_yyyymm c
      where   b.product_instance_id=c.user_id
     ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3110
        puts "errmsg:$errmsg"
        return -1
    }

    ##汇总部分指标
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when s_index_id in (2,3,4) then 1
                when s_index_id in (10,11,12,13) then 9
                when s_index_id in (18,19,20,21,37) then 17
           end,
           sum(result)
      from $stat_enterprise_0060_b_tmp4
     where s_index_id in (2,3,4,10,11,12,13,18,19,20,21,37)
     group by
           city_id,
           case when s_index_id in (2,3,4) then 1
                when s_index_id in (10,11,12,13) then 9
                when s_index_id in (18,19,20,21,37) then 17
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3250
        puts "errmsg:$errmsg"
        return -1
    }

    ##汇总全区数据
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select '890',
           s_index_id,
           sum(result)
      from $stat_enterprise_0060_b_tmp4
     where s_index_id not in (31,32,33,34,35,36)
     group by
           s_index_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3260
        puts "errmsg:$errmsg"
        return -1
    }

    #汇总数据
    set sql_buf "
    insert into $stat_enterprise_0060_b_tmp6
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           s_index_id,
           sum(result)
      from $stat_enterprise_0060_b_tmp4
     group by city_id,
           s_index_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3260
        puts "errmsg:$errmsg"
        return -1
    }

    #Step4.清除结果表历史数据
    set sql_buf "delete from $stat_enterprise_0060_b where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 4010
        puts "errmsg:$errmsg"
        return -1
    }

    #Step5.生成结果表数据
    set sql_buf "
    insert into $stat_enterprise_0060_b
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
                   $ent_index_s_2 b
           ) a
      left join
           $stat_enterprise_0060_b_tmp6 b on a.city_id = b.city_id and a.s_index_id = b.s_index_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 5010
        puts "errmsg:$errmsg"
        return -1
    }

    exec db2 "connect to bassdb user bass2 using bass2"
    exec db2 "runstats on table $stat_enterprise_0060_b with distribution and indexes all"
    exec db2 "terminate"

    #Step6.清除临时表
    set sql_buf "drop table $stat_enterprise_0060_b_tmp1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_b_tmp2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_b_tmp3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_b_tmp4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_b_tmp5;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_b_tmp6;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_b_tmp7;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    return 0
}