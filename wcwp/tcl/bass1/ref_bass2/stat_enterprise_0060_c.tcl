#======================================================================================
#版权归属：Copyright (c) 2011,AsiaInfo.Report.System
#程序名称: stat_enterprise_0060_c.tcl
#程序功能: 2012年集团客户综合统计报表-3、集团产品整体情况
#分析目标: 略
#分析指标: 见维表
#分析维度: 全区 地市
#运行粒度: 月
#运行示例: crt_basetab.sh stat_enterprise_0060_c.tcl 2011-11-01
#创建时间: 2011-12-12
#创 建 人: Asiainfo-Linkage 
#存在问题:
#修改历史: 1、2011-7-12 修改集团订购口径，取消原rec_status = 1，改用生效、失效日期
#          2、2012-7-5 根据熊成丞工单DEM_BASS_XZ_20120626_114507609语言类产品中新增无线商话数据
#=======================================================================================
proc deal {p_optime p_timestamp} {
    global conn
    global handle

    if [catch {set handle [aidb_open $conn]} errmsg] {
        trace_sql $errmsg 1000
        return -1
    }

    if {[stat_enterprise_0060_c $p_optime] != 0} {
        aidb_roll $conn
        aidb_close $handle
        return -1
    }

    aidb_commit $conn
    aidb_close $handle

    return 0
}

proc stat_enterprise_0060_c {p_optime} {
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
    set dw_product_ins_off_ins_prod_ds  "bass2.dw_product_ins_off_ins_prod_ds"

    #目标表
    set stat_enterprise_0060_c          "bass2.stat_enterprise_0060_c"

    #维表
    set dim_pub_city                    "bassweb.dim_pub_city"
    set dim_ent_unipay_item             "bass2.dim_ent_unipay_item"
    set ent_index_s_5                   "bass2.ent_index_s_5"

    #临时表
    set stat_enterprise_0060_c_tmp1     "bass2.stat_enterprise_0060_c_tmp1"
    set stat_enterprise_0060_c_tmp2     "bass2.stat_enterprise_0060_c_tmp2"
    set stat_enterprise_0060_c_tmp3     "bass2.stat_enterprise_0060_c_tmp3"
    set stat_enterprise_0060_c_tmp4     "bass2.stat_enterprise_0060_c_tmp4"
    set stat_enterprise_0060_c_tmp5     "bass2.stat_enterprise_0060_c_tmp5"

    #字符串替换
    set voice_prod      "'VPMN','917','931','933','WBT'"
    set sms_mms_prod    "'910','911','142','906','904','943','951','936','953'"
    set data_conn_prod  "'912001','912002','934'"
    set data_app_prod   "'926','939002','SPEC|BLACKBERRY','717','924','925','903','939001','10105002','945'"
    set iot_prod        "'942','944','946','949','947'"
    set other_prod      "'142','717','903','904','906','910','911','912','917','924',
                         '925','926','931','933','934','936','942','942','943','944',
                         '945','946','947','949','951','953','912001','912002',
                         '939001','939002','10105002','SPEC|BLACKBERRY'"
    set mas_prod        "'142','939001','939002'"
    set adc_prod        "'910','906','904','951','953','926','924','925','903'"
    set test_enterprise_id  "'89100000000682','89100000000659','89100000000656','89100000000651'"
    
    #Step1.创建结果临时表
    set sql_buf "drop table $stat_enterprise_0060_c_tmp1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_c_tmp2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_c_tmp3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_c_tmp4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_c_tmp5;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    ##集团订购产品付费使用情况临时表
    set sql_buf "
    create table $stat_enterprise_0060_c_tmp1
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
    ##订购集团产品临时表
    set sql_buf "
    create table $stat_enterprise_0060_c_tmp2
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
    create table $stat_enterprise_0060_c_tmp3
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
    ##结果临时表
    set sql_buf "
    create table $stat_enterprise_0060_c_tmp4
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
    ##集团产品使用情况临时表
    set sql_buf "
    create table $stat_enterprise_0060_c_tmp5
    (
           enterprise_id    varchar(20),
           city_id          varchar(8),
           service_id       varchar(20),
           call_duration_m  integer,
           sms_count        integer,
           mms_count        integer,
           data_flow        integer,
           mail_count       integer
    )
    in tbs_ods_other index in tbs_index partitioning key (enterprise_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1050
        puts "errmsg:$errmsg"
        return -1
    }

    #Step2.生成临时表数据
    ##正常在网集团成员
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp3
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
       and b.enterprise_id not in (select enterprise_id from bass2.dw_enterprise_msg_$year$month 
                                   where is_test = 1 )
       and c.userstatus_id in (1,2,3,6,8)
       and c.test_mark <> 1 and c.free_mark<>1
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
    insert into $stat_enterprise_0060_c_tmp2
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
                   $stat_enterprise_0060_c_tmp3 c
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
    insert into $stat_enterprise_0060_c_tmp2
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
           $stat_enterprise_0060_c_tmp3 b
        on a.enterprise_id = b.enterprise_id
       and a.user_id = b.user_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }
    
    #根据熊成丞工单：增加无线商话
     set sql_buf "
    insert into $stat_enterprise_0060_c_tmp2
    (
           enterprise_id,
           user_id,
           service_id,
           ent_mark
    )
    select  a.enterprise_id
           , b.user_id
           ,'WBT'
           ,case when b.user_id is not null then 1 else 0 end
    from (
         select b.enterprise_id
               ,a.product_instance_id
         from $dw_enterprise_msg_yyyymm b,
              bass2.dw_product_ins_off_ins_prod_$year$month a
         where a.cust_party_role_id=b.enterprise_id
           and a.offer_id in (112091501003) 
           and a.is_main_offer=0
           and date(a.expire_date)>=$date_nm_optime
           and date(a.valid_date)<$date_nm_optime) a 
    left join
           $stat_enterprise_0060_c_tmp3 b
        on a.enterprise_id = b.enterprise_id
       and a.product_instance_id = b.user_id;"
    puts $sql_buf
    
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }
    
    
    ##集团订购产品付费使用情况临时表
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp1
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
           case when a.service_id = 'VPMN' then value(d.total_fee,0)
                when a.service_id = 'WBT' then value(e.total_fee,0)
                else value(b.total_fee,0)
                
           end,
           case when a.service_id = 'VPMN' then value(d.unipay_fee,0)
                when a.service_id = 'WBT' then value(e.total_fee,0)
                else value(b.unipay_fee,0)
           end
      from (
            select a.enterprise_id,
                   b.enterprise_name,
                   case when b.level_def_mode = 1 then '888'
                        else b.ent_city_id
                   end as city_id,
                   a.service_id
              from $stat_enterprise_0060_c_tmp2 a,
                   $dw_enterprise_msg_yyyymm b
             where a.enterprise_id = b.enterprise_id
             and b.enterprise_id not in (select enterprise_id from bass2.dw_enterprise_msg_$year$month 
                                   where is_test = 1 )
             and b.ent_status_id = 0
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
                   $stat_enterprise_0060_c_tmp2 b
             where a.user_id = b.user_id
               and b.service_id = 'VPMN'
               and a.item_id in (80000704,80000053)
             group by
                   b.enterprise_id,
                   b.service_id
           ) d
        on a.enterprise_id = d.enterprise_id
        and a.service_id = d.service_id
        left join (
                select a.enterprise_id,
                       a.service_id,
                       sum(b.fact_fee) as total_fee,
                       sum(b.fact_fee) as unipay_fee 
                from 
                    $stat_enterprise_0060_c_tmp2 a ,
                    $dw_product_yyyymm b
                where a.service_id = 'WBT'  
                 and  a.user_id=b.user_id
               group by a.enterprise_id,
                        a.service_id
         ) e
         on a.enterprise_id = e.enterprise_id
         and a.service_id = e.service_id
       ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2030
        puts "errmsg:$errmsg"
        return -1
    }

    ##集团产品使用情况临时表
    ####移动400业务量
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp5
    (
           enterprise_id,
           city_id,
           service_id,
           call_duration_m
    )
    select a.enterprise_id,
           a.city_id,
           a.service_id,
           sum(b.call_duration_m)
      from (
            select a.enterprise_id,
                   case when c.level_def_mode = 1 then '888'
                        else c.ent_city_id
                   end as city_id,
                   a.service_id,
                   b.user_id,
                   b.product_no
              from $stat_enterprise_0060_c_tmp2 a,
                   $dw_product_yyyymm b,
                   $dw_enterprise_msg_yyyymm c
             where a.enterprise_id = c.enterprise_id
               and a.enterprise_id = b.cust_id
               and b.product_no like '400%'
               and a.service_id = '931'
               and c.enterprise_id not in (select enterprise_id from bass2.dw_enterprise_msg_$year$month 
                                   where is_test = 1 )
               and c.ent_status_id = 0                    
           ) a
      left join
           $dw_call_yyyymm b
        on a.user_id = b.user_id
     group by
           a.enterprise_id,
           a.city_id,
           a.service_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2040
        puts "errmsg:$errmsg"
        return -1
    }

    ####集团V网业务量
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp5
    (
           enterprise_id,
           city_id,
           service_id,
           call_duration_m
    )
    select a.enterprise_id,
           case when c.level_def_mode = 1 then '888'
                else c.ent_city_id
           end,
           a.service_id,
           sum(b.call_duration_m)
      from $stat_enterprise_0060_c_tmp2 a,
           $dw_call_yyyymm b,
           $dw_enterprise_msg_yyyymm c
     where a.enterprise_id = c.enterprise_id
       and a.user_id = b.user_id
       and a.service_id = 'VPMN'
       and b.opposite_id in ($opposite_inner_id)
       and c.enterprise_id not in (select enterprise_id from bass2.dw_enterprise_msg_$year$month 
                                   where is_test = 1 )
       and c.ent_status_id = 0  
     group by
           a.enterprise_id,
           case when c.level_def_mode = 1 then '888'
                else c.ent_city_id
           end,
           a.service_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2050
        puts "errmsg:$errmsg"
        return -1
    }
    
    
    ##无线商话计费时长
     set sql_buf "
    insert into $stat_enterprise_0060_c_tmp5
    (
           enterprise_id,
           city_id,
           service_id,
           call_duration_m
    )
       select  a.enterprise_id,
               b.ent_city_id,
               a.service_id,
               sum(c.call_duration_m)
        from   $stat_enterprise_0060_c_tmp2 a,
               $dw_enterprise_msg_yyyymm b ,
               $dw_product_yyyymm c
        where  a.enterprise_id = b.enterprise_id
               and a.enterprise_id not in (select enterprise_id from bass2.dw_enterprise_msg_201206 
                     where is_test = 1 )
               and a.user_id = c.user_id
               and a.service_id = 'WBT'
         group by   a.enterprise_id,
                    b.ent_city_id,
                    a.service_id;"
               
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2050
        puts "errmsg:$errmsg"
        return -1
    }                 
              

    #Step3.分产品统计各指标
    ##按承载方式分类
    #select city_id,
    #       case when service_id in ($voice_prod) then 1
    #            when service_id in ($sms_mms_prod) then 2
    #            when service_id in ($data_conn_prod) then 3
    #            when service_id in ($data_app_prod) then 4
    #            when service_id in ($iot_prod) then 5
    #            when service_id not in ($other_prod) then 6
    #       end
    #  from $stat_enterprise_0060_c_tmp1
    ####订购集团客户数
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when service_id in ($voice_prod) then 1
                when service_id in ($sms_mms_prod) then 6
                when service_id in ($data_conn_prod) then 12
                when service_id in ($data_app_prod) then 17
                when service_id in ($iot_prod) then 22
                when service_id not in ($other_prod) then 27
           end,
           count(distinct enterprise_id)
      from $stat_enterprise_0060_c_tmp1
     group by
           city_id,
           case when service_id in ($voice_prod) then 1
                when service_id in ($sms_mms_prod) then 6
                when service_id in ($data_conn_prod) then 12
                when service_id in ($data_app_prod) then 17
                when service_id in ($iot_prod) then 22
                when service_id not in ($other_prod) then 27
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3010
        puts "errmsg:$errmsg"
        return -1
    }

    ####付费集团客户数
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when service_id in ($voice_prod) then 2
                when service_id in ($sms_mms_prod) then 7
                when service_id in ($data_conn_prod) then 13
                when service_id in ($data_app_prod) then 18
                when service_id in ($iot_prod) then 23
                when service_id not in ($other_prod) then 28
           end,
           count(distinct enterprise_id)
      from $stat_enterprise_0060_c_tmp1
     where unipay_income > 0
     group by
           city_id,
           case when service_id in ($voice_prod) then 2
                when service_id in ($sms_mms_prod) then 7
                when service_id in ($data_conn_prod) then 13
                when service_id in ($data_app_prod) then 18
                when service_id in ($iot_prod) then 23
                when service_id not in ($other_prod) then 28
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3020
        puts "errmsg:$errmsg"
        return -1
    }

    ####总收入
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when service_id in ($voice_prod) then 4
                when service_id in ($sms_mms_prod) then 10
                when service_id in ($data_conn_prod) then 15
                when service_id in ($data_app_prod) then 20
                when service_id in ($iot_prod) then 25
                when service_id not in ($other_prod) then 30
           end,
           sum(total_income)
      from $stat_enterprise_0060_c_tmp1
     group by
           city_id,
           case when service_id in ($voice_prod) then 4
                when service_id in ($sms_mms_prod) then 10
                when service_id in ($data_conn_prod) then 15
                when service_id in ($data_app_prod) then 20
                when service_id in ($iot_prod) then 25
                when service_id not in ($other_prod) then 30
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3030
        puts "errmsg:$errmsg"
        return -1
    }

    ####统一付费收入
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when service_id in ($voice_prod) then 5
                when service_id in ($sms_mms_prod) then 11
                when service_id in ($data_conn_prod) then 16
                when service_id in ($data_app_prod) then 21
                when service_id in ($iot_prod) then 26
                when service_id not in ($other_prod) then 31
           end,
           sum(unipay_income)
      from $stat_enterprise_0060_c_tmp1
     group by
           city_id,
           case when service_id in ($voice_prod) then 5
                when service_id in ($sms_mms_prod) then 11
                when service_id in ($data_conn_prod) then 16
                when service_id in ($data_app_prod) then 21
                when service_id in ($iot_prod) then 26
                when service_id not in ($other_prod) then 31
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3040
        puts "errmsg:$errmsg"
        return -1
    }

    ####计费时长
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           3,
           sum(call_duration_m)
      from $stat_enterprise_0060_c_tmp5
     where service_id in ($voice_prod)
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3050
        puts "errmsg:$errmsg"
        return -1
    }

    ####短信业务量
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           8,
           sum(sms_count)
      from $stat_enterprise_0060_c_tmp5
     where service_id in ($sms_mms_prod)
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3060
        puts "errmsg:$errmsg"
        return -1
    }

    ####彩信业务量
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           9,
           sum(mms_count)
      from $stat_enterprise_0060_c_tmp5
     where service_id in ($sms_mms_prod)
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3070
        puts "errmsg:$errmsg"
        return -1
    }

    ####移动数据流量
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when service_id in ($data_conn_prod) then 14
                when service_id in ($data_app_prod) then 19
                when service_id in ($iot_prod) then 24
                when service_id not in ($other_prod) then 29
           end,
           sum(data_flow)
      from $stat_enterprise_0060_c_tmp5
     where service_id not in ($sms_mms_prod)
     group by
           city_id,
           case when service_id in ($data_conn_prod) then 14
                when service_id in ($data_app_prod) then 19
                when service_id in ($iot_prod) then 24
                when service_id not in ($other_prod) then 29
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3080
        puts "errmsg:$errmsg"
        return -1
    }

    ##按模式分类
    #select city_id,
    #       case when service_id in ($mas_prod) then 1
    #            when service_id in ($adc_prod) then 2
    #       end
    #  from $stat_enterprise_0060_c_tmp1
    # where service_id in ($mas_prod,$adc_prod)

    ####订购集团客户数
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when service_id in ($mas_prod) then 32
                when service_id in ($adc_prod) then 42
           end,
           count(distinct enterprise_id)
      from $stat_enterprise_0060_c_tmp1
     where service_id in ($mas_prod,$adc_prod)
     group by
           city_id,
           case when service_id in ($mas_prod) then 32
                when service_id in ($adc_prod) then 42
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3090
        puts "errmsg:$errmsg"
        return -1
    }

    ####使用集团客户数
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when service_id in ($mas_prod) then 33
                when service_id in ($adc_prod) then 43
           end,
           count(distinct enterprise_id)
      from $stat_enterprise_0060_c_tmp5
     where sms_count + mms_count + data_flow + mail_count > 0
       and service_id in ($mas_prod,$adc_prod)
     group by
           city_id,
           case when service_id in ($mas_prod) then 33
                when service_id in ($adc_prod) then 43
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3100
        puts "errmsg:$errmsg"
        return -1
    }

    ####付费集团客户数
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when service_id in ($mas_prod) then 34
                when service_id in ($adc_prod) then 44
           end,
           count(distinct enterprise_id)
      from $stat_enterprise_0060_c_tmp1
     where unipay_income > 0
       and service_id in ($mas_prod,$adc_prod)
     group by
           city_id,
           case when service_id in ($mas_prod) then 34
                when service_id in ($adc_prod) then 44
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3110
        puts "errmsg:$errmsg"
        return -1
    }

    ####使用且付费的集团客户数
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           case when a.service_id in ($mas_prod) then 35
                when a.service_id in ($adc_prod) then 45
           end,
           count(distinct a.enterprise_id)
      from $stat_enterprise_0060_c_tmp1 a,
           $stat_enterprise_0060_c_tmp5 b
     where a.enterprise_id = b.enterprise_id
       and a.service_id = b.service_id
       and b.sms_count + b.mms_count + b.data_flow + b.mail_count > 0
       and b.service_id in ($mas_prod,$adc_prod)
       and a.unipay_income > 0
       and a.service_id in ($mas_prod,$adc_prod)
     group by
           a.city_id,
           case when a.service_id in ($mas_prod) then 35
                when a.service_id in ($adc_prod) then 45
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3120
        puts "errmsg:$errmsg"
        return -1
    }

    ####短信业务量
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when service_id in ($mas_prod) then 36
                when service_id in ($adc_prod) then 46
           end,
           sum(sms_count)
      from $stat_enterprise_0060_c_tmp5
     where service_id in ($mas_prod,$adc_prod)
     group by
           city_id,
           case when service_id in ($mas_prod) then 36
                when service_id in ($adc_prod) then 46
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3130
        puts "errmsg:$errmsg"
        return -1
    }

    ####彩信业务量
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when service_id in ($mas_prod) then 37
                when service_id in ($adc_prod) then 47
           end,
           sum(mms_count)
      from $stat_enterprise_0060_c_tmp5
     where service_id in ($mas_prod,$adc_prod)
     group by
           city_id,
           case when service_id in ($mas_prod) then 37
                when service_id in ($adc_prod) then 47
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3140
        puts "errmsg:$errmsg"
        return -1
    }

    ####邮件业务量
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when service_id in ($mas_prod) then 38
                when service_id in ($adc_prod) then 48
           end,
           sum(mail_count)
      from $stat_enterprise_0060_c_tmp5
     where service_id in ($mas_prod,$adc_prod)
     group by
           city_id,
           case when service_id in ($mas_prod) then 38
                when service_id in ($adc_prod) then 48
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3150
        puts "errmsg:$errmsg"
        return -1
    }

    ####移动数据流量
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when service_id in ($mas_prod) then 39
                when service_id in ($adc_prod) then 49
           end,
           sum(data_flow)
      from $stat_enterprise_0060_c_tmp5
     where service_id in ($mas_prod,$adc_prod)
     group by
           city_id,
           case when service_id in ($mas_prod) then 39
                when service_id in ($adc_prod) then 49
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3160
        puts "errmsg:$errmsg"
        return -1
    }

    ####总收入
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when service_id in ($mas_prod) then 40
                when service_id in ($adc_prod) then 50
           end,
           sum(total_income)
      from $stat_enterprise_0060_c_tmp1
     where service_id in ($mas_prod,$adc_prod)
     group by
           city_id,
           case when service_id in ($mas_prod) then 40
                when service_id in ($adc_prod) then 50
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3170
        puts "errmsg:$errmsg"
        return -1
    }

    ####统一付费收入
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           case when service_id in ($mas_prod) then 41
                when service_id in ($adc_prod) then 51
           end,
           sum(unipay_income)
      from $stat_enterprise_0060_c_tmp1
     where service_id in ($mas_prod,$adc_prod)
     group by
           city_id,
           case when service_id in ($mas_prod) then 41
                when service_id in ($adc_prod) then 51
           end;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3180
        puts "errmsg:$errmsg"
        return -1
    }

    ####汇总全区数据
    set sql_buf "
    insert into $stat_enterprise_0060_c_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select '890',
           s_index_id,
           sum(result)
      from $stat_enterprise_0060_c_tmp4
     group by
           s_index_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3190
        puts "errmsg:$errmsg"
        return -1
    }

    #Step4.清除结果表历史数据
    set sql_buf "delete from $stat_enterprise_0060_c where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 4010
        puts "errmsg:$errmsg"
        return -1
    }

    #Step5.生成结果表数据
    set sql_buf "
    insert into $stat_enterprise_0060_c
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
                   $ent_index_s_5 b
           ) a
      left join
           $stat_enterprise_0060_c_tmp4 b on a.city_id = b.city_id and a.s_index_id = b.s_index_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5010
        puts "errmsg:$errmsg"
        return -1
    }

    exec db2 "connect to bassdb user bass2 using bass2"
    exec db2 "runstats on table $stat_enterprise_0060_c with distribution and indexes all"
    exec db2 "terminate"

    #Step6.清除临时表
   set sql_buf "drop table $stat_enterprise_0060_c_tmp1;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
   }
   set sql_buf "drop table $stat_enterprise_0060_c_tmp2;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
   }
   set sql_buf "drop table $stat_enterprise_0060_c_tmp3;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
   }
   set sql_buf "drop table $stat_enterprise_0060_c_tmp4;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
   }
   set sql_buf "drop table $stat_enterprise_0060_c_tmp5;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
   }

    return 0
}