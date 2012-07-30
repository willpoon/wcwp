#======================================================================================
#版权归属：Copyright (c) 2011,AsiaInfo.Report.System
#程序名称: stat_enterprise_0060_e.tcl
#程序功能: 2012年集团客户综合统计报表-5、打包产品
#分析目标: 略
#分析指标: 见维表
#分析维度: 全区 地市
#运行粒度: 月
#运行示例: crt_basetab.sh stat_enterprise_0060_e.tcl 2011-11-01
#创建时间: 2011-12-12
#创 建 人: Asiainfo-Linkage 
#存在问题:
#修改历史: 1、2011-7-12 修改集团订购口径，取消原rec_status = 1，改用生效、失效日期
#=======================================================================================
proc deal {p_optime p_timestamp} {
    global conn
    global handle

    if [catch {set handle [aidb_open $conn]} errmsg] {
        trace_sql $errmsg 1000
        return -1
    }

    if {[stat_enterprise_0060_e $p_optime] != 0} {
        aidb_roll $conn
        aidb_close $handle
        return -1
    }

    aidb_commit $conn
    aidb_close $handle

    return 0
}

proc stat_enterprise_0060_e {p_optime} {
    global conn
    global handle

    source stat_insert_index.tcl
    source report.cfg

    #日期处理
    set    date_optime      [ai_to_date $p_optime]
    scan   $p_optime        "%04s-%02s-%02s" year month day
    set    day_me_optime    [clock format [ clock scan "${year}${month}${day} + 1month - 1day" ] -format "%Y-%m-%d"]
    set    date_me_optime   [ai_to_date $day_me_optime]
    scan   $day_me_optime   "%04s-%02s-%02s" meyear memonth meday
    set    date_nm_optime   [ai_to_date [clock format [ clock scan "${year}${month}${day} + 1month" ] -format "%Y-%m-%d"]]

    #源表
    set dw_enterprise_account_yyyymm    "bass2.dw_enterprise_account_$year$month"
    set dw_enterprise_member_mid_yyyymm "bass2.dw_enterprise_member_mid_$year$month"
    set dw_enterprise_msg_yyyymm        "bass2.dw_enterprise_msg_$year$month"
    set dw_enterprise_new_unipay_yyyymm "bass2.dw_enterprise_new_unipay_$year$month"
    set dw_enterprise_membersub_yyyymm  "bass2.dw_enterprise_membersub_$year$month"
    set dw_enterprise_sub_yyyymm        "bass2.dw_enterprise_sub_$year$month"
    set dw_vpmn_enterprise_yyyymm       "bass2.dw_vpmn_enterprise_$year$month"
    set dw_vpmn_member_yyyymm           "bass2.dw_vpmn_member_$year$month"
    set dw_enterprise_vpmn_rela_yyyymm  "bass2.dw_enterprise_vpmn_rela_$year$month"
    set dw_product_yyyymm               "bass2.dw_product_$year$month"
    set dw_call_yyyymm                  "bass2.dw_call_$year$month"
    set dw_newbusi_ismg_yyyymm          "bass2.dw_newbusi_ismg_$year$month"
    set dw_acct_shoulditem_yyyymm       "bass2.dw_acct_shoulditem_$year$month"
    set dwd_group_order_featur_yyyymmdd "bass2.dwd_group_order_featur_$meyear$memonth$meday"

    #目标表
    set stat_enterprise_0060_e          "bass2.stat_enterprise_0060_e"

    #维表
    set dim_pub_city                    "bassweb.dim_pub_city"
    set dim_ent_unipay_item             "bass2.dim_ent_unipay_item"
    set ent_index_s_4                   "bass2.ent_index_s_4"

    #临时表
    set stat_enterprise_0060_e_tmp1     "bass2.stat_enterprise_0060_e_tmp1"
    set stat_enterprise_0060_e_tmp2     "bass2.stat_enterprise_0060_e_tmp2"
    set stat_enterprise_0060_e_tmp3     "bass2.stat_enterprise_0060_e_tmp3"
    set stat_enterprise_0060_e_tmp4     "bass2.stat_enterprise_0060_e_tmp4"
    set stat_enterprise_0060_e_tmp5     "bass2.stat_enterprise_0060_e_tmp5"


    set test_enterprise_id  "'89100000000682','89100000000659','89100000000656','89100000000651'"
    
    #Step1.创建结果临时表
    set sql_buf "drop table $stat_enterprise_0060_e_tmp1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_e_tmp2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_e_tmp3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_e_tmp4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_e_tmp5;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    ##订购动力100业务包付费使用情况临时表
    set sql_buf "
    create table $stat_enterprise_0060_e_tmp1
    (
           enterprise_id    varchar(20),
           enterprise_name  varchar(200),
           city_id          varchar(8),
           service_id       varchar(20),
           order_mark       smallint,
           total_income     decimal(9,2),
           unipay_income    decimal(9,2)
    )
    in tbs_report index in tbs_index partitioning key (enterprise_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1010
        puts "errmsg:$errmsg"
        return -1
    }
    ##订购动力100业务包中产品的用户数
    set sql_buf "
    create table $stat_enterprise_0060_e_tmp2
    (
           enterprise_id    varchar(20),
           user_id          varchar(20),
           service_id       varchar(20),
           ent_mark         smallint
    )
    in tbs_report index in tbs_index partitioning key (user_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1020
        puts "errmsg:$errmsg"
        return -1
    }
    ##正常在网集团成员
    set sql_buf "
    create table $stat_enterprise_0060_e_tmp3
    (
           enterprise_id    varchar(20),
           user_id          varchar(20)
    )
    in tbs_report index in tbs_index partitioning key (user_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1030
        puts "errmsg:$errmsg"
        return -1
    }
    #订购动力100集团客户临时表
        set sql_buf "
    create table $stat_enterprise_0060_e_tmp5
    (
           enterprise_id    varchar(20)
    )
    in tbs_report index in tbs_index partitioning key (enterprise_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1040
        puts "errmsg:$errmsg"
        return -1
    }
    
    ##结果临时表
    set sql_buf "
    create table $stat_enterprise_0060_e_tmp4
    (
           city_id     varchar(7),
           s_index_id  smallint,
           result      decimal(12,2)
    )
    in tbs_report index in tbs_index partitioning key (city_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1040
        puts "errmsg:$errmsg"
        return -1
    }
   
    #Step2.生成临时表数据
    ##正常在网集团成员
    set sql_buf "
    insert into $stat_enterprise_0060_e_tmp3
    (
           enterprise_id,
           user_id
    )
    select a.enterprise_id,
           a.user_id
      from $dw_enterprise_member_mid_yyyymm a,
           $dw_product_yyyymm c,
           $dw_enterprise_msg_yyyymm b
     where a.user_id = c.user_id
       and a.enterprise_id = b.enterprise_id
       and a.dummy_mark = 0
       and a.test_mark = 0
       and b.ent_status_id = 0
       and b.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test = 1 )
       and c.userstatus_id in (1,2,3,6,8)
       and c.usertype_id in (1,2,9);"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2010
        puts "errmsg:$errmsg"
        return -1
    }

    ##订购集团产品临时表
    ####对订购集团专线系列产品的集团，service_id分拆为912001（互联网专线）、912002（数据专线），方便统计
    set sql_buf "
    insert into $stat_enterprise_0060_e_tmp2
    (
           enterprise_id,
           user_id,
           service_id,
           ent_mark
    )
    select distinct
           a.enterprise_id,
           b.user_id,
           case when a.service_id = '912'
                then case when a.prod_id = '91201001' then '912001'
                          when a.prod_id = '91201002' then '912002'
                          else '912'
                     end
                else a.service_id
           end,
           value(b.ent_mark,0)
      from $dw_enterprise_sub_yyyymm a
      left join
           (
            select b.enterprise_id,
                   b.order_id,
                   b.user_id,
                   case when c.user_id is not null then 1 else 0 end as ent_mark
              from $dw_enterprise_membersub_yyyymm b
              left join
                   $stat_enterprise_0060_e_tmp3 c
                on b.enterprise_id = c.enterprise_id
               and b.user_id = c.user_id
             where b.valid_date <= $date_me_optime
               and b.expire_date >= $date_optime
           ) b
        on a.enterprise_id = b.enterprise_id
       and a.order_id = b.order_id
     where a.valid_date <= $date_me_optime
       and a.expire_date >= $date_optime;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }

    ##VPMN订购
    set sql_buf "
    insert into $stat_enterprise_0060_e_tmp2
    (
           enterprise_id,
           user_id,
           service_id,
           ent_mark
    )
    select a.enterprise_id,
           a.user_id,
           'VPMN',
           case when b.user_id is not null then 1 else 0 end
      from (
            select b.enterprise_id,
                   c.user_id
              from $dw_vpmn_enterprise_yyyymm a
              left join
                   $dw_vpmn_member_yyyymm c
                on a.vpmn_id = c.vpmn_id
               and c.valid_date < $date_nm_optime
               and c.expire_date >= $date_nm_optime,
                   $dw_enterprise_vpmn_rela_yyyymm b
             where a.vpmn_id = b.vpmn_id
               and a.scpid = 1
               and a.sts = 0
               and a.valid_date < $date_nm_optime
               and a.expire_date >= $date_nm_optime
           ) a
      left join
           $stat_enterprise_0060_e_tmp3 b
        on a.enterprise_id = b.enterprise_id
       and a.user_id = b.user_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团订购产品付费使用情况临时表
    set sql_buf "
    insert into $stat_enterprise_0060_e_tmp1
    (
           enterprise_id,
           enterprise_name,
           city_id,
           service_id,
           order_mark,
           total_income,
           unipay_income
    )
    select a.enterprise_id,
           a.enterprise_name,
           a.city_id,
           a.service_id,
           1,
           case when a.service_id <> 'VPMN' then value(b.total_fee,0)
                else value(d.total_fee,0)
           end,
           case when a.service_id <> 'VPMN' then value(b.unipay_fee,0)
                else value(d.unipay_fee,0)
           end
      from (
            select a.enterprise_id,
                   b.enterprise_name,
                   case when b.level_def_mode = 1 then '888'
                        else b.ent_city_id
                   end as city_id,
                   a.service_id
              from $stat_enterprise_0060_e_tmp2 a,
                   $dw_enterprise_msg_yyyymm b
             where a.enterprise_id = b.enterprise_id
                   and b.ent_status_id = 0
                   and b.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test = 1 )
             group by
                   a.enterprise_id,
                   b.enterprise_name,
                   case when b.level_def_mode = 1 then '888'
                        else b.ent_city_id
                   end,
                   a.service_id
           ) a
      left join
           (
            select a.enterprise_id,
                   char(a.service_id) as service_id,
                   sum(a.unipay_fee + a.non_unipay_fee) as total_fee,
                   sum(a.unipay_fee) as unipay_fee
              from $dw_enterprise_new_unipay_yyyymm a
             where a.test_mark = 0
             group by
                   a.enterprise_id,
                   a.service_id
           ) b
        on a.enterprise_id = b.enterprise_id
       and a.service_id = b.service_id
      left join
           (
            select b.enterprise_id,
                   b.service_id,
                   sum(a.fact_fee) as total_fee,
                   sum(case when c.acct_id is not null then a.fact_fee else 0 end) as unipay_fee
              from $dw_acct_shoulditem_yyyymm a
              left join $dw_enterprise_account_yyyymm c
                on a.acct_id = c.acct_id
               and c.rec_status = 1,
                   $stat_enterprise_0060_e_tmp2 b
             where a.user_id = b.user_id
               and b.service_id = 'VPMN'
               and a.item_id in (80000704,80000053)
             group by
                   b.enterprise_id,
                   b.service_id
           ) d
        on a.enterprise_id = d.enterprise_id
       and a.service_id = d.service_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2030
        puts "errmsg:$errmsg"
        return -1
    }
    
#订购动力100业务包集团客户
        set sql_buf "
    insert into $stat_enterprise_0060_e_tmp5
    (
           enterprise_id
    )
    select distinct  c.enterprise_id
    from bass2.dw_product_ins_off_ins_prod_$year$month a,
          bass2.dw_product_$year$month b,
          bass2.dw_enterprise_msg_$year$month c
    where a.offer_id in (select product_item_id from bass2.dim_prod_up_product_item
                         where name like '%动力100%动力%'
                         and item_type = 'OFFER_PLAN')
          and a.valid_date <= $date_me_optime 
          and a.expire_date >= $date_optime
          and a.product_instance_id = b.user_id
          and b.cust_id = c.cust_id
          and c.ent_status_id = 0
          and c.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test = 1 )
   ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }
    
 #动力业务包订购集团数
        set sql_buf "
    insert into $stat_enterprise_0060_e_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select case when b.level_def_mode = 1 then '888'
                        else b.ent_city_id
                   end as city_id,
           1,
           count(distinct a.enterprise_id)
    from $stat_enterprise_0060_e_tmp5 a,
         bass2.dw_enterprise_msg_$year$month b
    where a.enterprise_id = b.enterprise_id
         and b.ent_status_id = 0
         and b.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test = 1 )
    group by case when b.level_def_mode = 1 then '888'
                        else b.ent_city_id
                   end
   ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }  
    
 #动力业务包订购用户数 
          set sql_buf "
    insert into $stat_enterprise_0060_e_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select case when b.level_def_mode = 1 then '888'
                        else b.ent_city_id
                   end as city_id,
           2,
           count(distinct a.user_id)
    from $stat_enterprise_0060_e_tmp2 a,
         bass2.dw_enterprise_msg_$year$month b,
         $stat_enterprise_0060_e_tmp5 c
    where a.enterprise_id = b.enterprise_id
         and a.enterprise_id = c.enterprise_id
         and b.ent_status_id = 0
         and b.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test = 1 )
    and a.service_id in ('VPMN','912001','924','933')
    group by case when b.level_def_mode = 1 then '888'
                        else b.ent_city_id
                   end
   ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }  
    
#动力业务包总收入
      
         set sql_buf "
    insert into $stat_enterprise_0060_e_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           case when grouping(a.service_id)=1 then 3
                  else  case when a.service_id = 'VPMN' then 5
                                       when a.service_id = '933'  then 6
                                       when a.service_id in('926','939002')  then 7
                                       when a.service_id in ('925')  then 8
                                       when a.service_id = '924'  then 9
                                       when a.service_id in ('903','939001')  then 10
                                       when a.service_id = '931'  then 11
                                       end 
                   end as s_index_id,
           sum( a.total_income)
    from $stat_enterprise_0060_e_tmp1 a,
          $stat_enterprise_0060_e_tmp5 c
    where  a.enterprise_id = c.enterprise_id
         and a.service_id in ('VPMN','933','926','939002','925','924','903','939001','931')
    group by  a.city_id,
                   cube(a.service_id)
   ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }  
    
#动力业务包统一付费收入  
 
           set sql_buf "
    insert into $stat_enterprise_0060_e_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           4,
           sum(a.unipay_income)
    from $stat_enterprise_0060_e_tmp1 a,
          $stat_enterprise_0060_e_tmp5 c
    where  a.enterprise_id = c.enterprise_id
         and a.service_id in ('VPMN','933','926','939002','925','924','903','939001','931')
    group by  a.city_id
   ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }   
    
#######################################################



    ####汇总全区数据
    set sql_buf "
    insert into $stat_enterprise_0060_e_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select '890',
           s_index_id,
           sum(result)
      from $stat_enterprise_0060_e_tmp4
     group by
           s_index_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3190
        puts "errmsg:$errmsg"
        return -1
    }

    #Step4.清除结果表历史数据
    set sql_buf "delete from $stat_enterprise_0060_e where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 4010
        puts "errmsg:$errmsg"
        return -1
    }

    #Step5.生成结果表数据
    set sql_buf "
    insert into $stat_enterprise_0060_e
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
                   $ent_index_s_4 b
           ) a
      left join
           $stat_enterprise_0060_e_tmp4 b on a.city_id = b.city_id and a.s_index_id = b.s_index_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5010
        puts "errmsg:$errmsg"
        return -1
    }

    exec db2 "connect to bassdb user bass2 using bass2"
    exec db2 "runstats on table $stat_enterprise_0060_e with distribution and indexes all"
    exec db2 "terminate"

    #Step6.清除临时表
    set sql_buf "drop table $stat_enterprise_0060_e_tmp1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_e_tmp2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_e_tmp3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_e_tmp4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_e_tmp5;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    return 0
}