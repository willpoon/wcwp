#======================================================================================
#版权归属：Copyright (c) 2011,AsiaInfo.Report.System
#程序名称: stat_enterprise_0060_f.tcl
#程序功能: 2012年集团客户综合统计报表-6、重点行业报表
#分析目标: 略
#分析指标: 见维表
#分析维度: 全区 地市
#运行粒度: 月
#运行示例: crt_basetab.sh stat_enterprise_0060_f.tcl 2011-11-01
#创建时间: 2011-12-16
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

    if {[stat_enterprise_0060_f $p_optime] != 0} {
        aidb_roll $conn
        aidb_close $handle
        return -1
    }

    aidb_commit $conn
    aidb_close $handle

    return 0
}

proc stat_enterprise_0060_f {p_optime} {
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
    set dwd_enterprise_member_info_yyyymm  "bass2.Dwd_enterprise_cust_msg_$year$month"

    #目标表
    set stat_enterprise_0060_f          "bass2.stat_enterprise_0060_f"

    #维表
    set dim_pub_city                    "bassweb.dim_pub_city"
    set dim_ent_unipay_item             "bass2.dim_ent_unipay_item"
    set ent_index_s_6                   "bass2.ent_index_s_6"

    #临时表
    set stat_enterprise_0060_f_tmp1     "bass2.stat_enterprise_0060_f_tmp1"
    set stat_enterprise_0060_f_tmp2     "bass2.stat_enterprise_0060_f_tmp2"
    set stat_enterprise_0060_f_tmp3     "bass2.stat_enterprise_0060_f_tmp3"
    set stat_enterprise_0060_f_tmp4     "bass2.stat_enterprise_0060_f_tmp4"
    set stat_enterprise_0060_f_tmp5     "bass2.stat_enterprise_0060_f_tmp5"

    #字符串替换
    ##只统计统付收入的信息化产品
    set info_prod_unipay    "'142','906','910','917','924','925','931','933','934','943','951','953','966','912001','912002','10105002','SPEC_400','SPEC|40010'"
    ##统计总收入的信息化产品
    set info_prod_total     "'717','903','904','911','926','936','942','944','945','946','947','949','939001','939002','SPEC|BLACKBERRY'"
    set test_enterprise_id  "'89100000000682','89100000000659','89100000000656','89100000000651'"
    
    #Step1.创建结果临时表
    set sql_buf "drop table $stat_enterprise_0060_f_tmp1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_f_tmp2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_f_tmp3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_f_tmp4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_f_tmp5;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    #集团收入情况与集团成员
    set sql_buf "
    create table $stat_enterprise_0060_f_tmp1
    (
           enterprise_id    varchar(20),
           city_id          varchar(7),
           member_counts    integer,
           ent_sub_counts   integer,
           ent_prod_income  decimal(12,2),
           prod_total_income decimal(12,2),
           member_income    decimal(12,2),
           info_fee          decimal(12,2)
    )
    in tbs_report index in tbs_index partitioning key (enterprise_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1010
        puts "errmsg:$errmsg"
        return -1
    }
    
     set sql_buf "
    insert into $stat_enterprise_0060_f_tmp1
    (
           enterprise_id,
           city_id,
           member_counts,
           ent_sub_counts,
           ent_prod_income,
           prod_total_income,
           member_income,
           info_fee
    )
    select a.enterprise_id,
           case when a.level_def_mode = 1 then '888'
                else a.ent_city_id
           end,
           value(b.member_counts,0),
           value(d.ent_sub_counts,0),
           value(c.ent_prod_income,0),
           value(c.prod_total_income,0),
           value(b.member_income,0),
           value(c.info_fee,0)
      from $dw_enterprise_msg_yyyymm a
      left join
           (
            select b.enterprise_id,
                   count(distinct a.user_id) member_counts,
                   sum(a.fact_fee) member_income
              from $dw_product_yyyymm a,
                   $dw_enterprise_member_mid_yyyymm b
             where a.user_id = b.user_id
               and b.dummy_mark = 0
               and b.test_mark = 0
             group by
                   b.enterprise_id
           ) b on a.enterprise_id = b.enterprise_id
      left join
           (
            select enterprise_id,
                   sum(unipay_fee) ent_prod_income,
                   sum(unipay_fee+non_unipay_fee) prod_total_income,
                   sum(case when char(service_id) in ($info_prod_unipay)
                            then unipay_fee
                            when char(service_id) in ($info_prod_total)
                            then unipay_fee + non_unipay_fee
                            else 0
                       end) info_fee
              from $dw_enterprise_new_unipay_yyyymm
             group by
                   enterprise_id
           ) c on a.enterprise_id = c.enterprise_id
      left join
           (
            select enterprise_id,
                   count(distinct service_id) ent_sub_counts
              from $dw_enterprise_sub_yyyymm
             where valid_date <= $date_me_optime
               and expire_date >= $date_optime
             group by
                   enterprise_id
           ) d on a.enterprise_id = d.enterprise_id
     where a.ent_status_id = 0
       and a.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test = 1);"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2010
        puts "errmsg:$errmsg"
        return -1
    }
    
    #集团所属行业
    set sql_buf "
    create table $stat_enterprise_0060_f_tmp2
    (
           enterprise_id    varchar(20),
           esp_industry     smallint,
           esp_industry2    smallint
    )
    in tbs_report index in tbs_index partitioning key (enterprise_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1020
        puts "errmsg:$errmsg"
        return -1
    }
    
        set sql_buf " insert into bass2.stat_enterprise_0060_f_tmp2
                 select group_cust_id,esp_industry ,esp_industry2
                 from $dwd_enterprise_member_info_yyyymm
            
    "
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 1020
        puts "errmsg:$errmsg"
        return -1
    }
      
    ##结果临时表
    set sql_buf "
    create table $stat_enterprise_0060_f_tmp4
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
   
#step2:生成数据入临时结果表
#集团客户到达数
      
         set sql_buf "
    insert into $stat_enterprise_0060_f_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           case when  esp_industry = 5 and esp_industry2 = 1 then 1
                when  esp_industry = 5 and esp_industry2 = 2 then 12
                when  esp_industry = 5 and esp_industry2 = 3 then 23
                when  esp_industry = 5 and esp_industry2 = 4 then 34
                when  esp_industry = 6 and esp_industry2 = 2 then 78
                when  esp_industry = 6 and esp_industry2 = 1 then 89
                when  esp_industry = 7 and esp_industry2 = 1 then 100
                when  esp_industry = 7 and esp_industry2 = 2 then 111
                when  esp_industry = 7 and esp_industry2 = 3 then 122
                when  esp_industry = 8 and esp_industry2 = 1 then 133
                when  esp_industry = 8 and esp_industry2 = 2 then 144
                when  esp_industry = 8 and esp_industry2 = 3 then 155
                when  esp_industry = 9 and esp_industry2 = 1 then 166
                when  esp_industry = 9 and esp_industry2 = 2 then 177
                when  esp_industry = 9 and esp_industry2 = 3 then 188
                when  esp_industry = 10 and esp_industry2 = 1 then 199
                when  esp_industry = 10 and esp_industry2 = 2 then 210
                when  esp_industry = 10 and esp_industry2 = 3 then 221
                when  esp_industry = 10 and esp_industry2 = 4 then 232
                when  esp_industry = 10 and esp_industry2 = 5 then 243
                when  esp_industry = 11 and esp_industry2 = 1 then 254
                when  esp_industry = 11 and esp_industry2 = 2 then 265
                when  esp_industry = 11 and esp_industry2 = 3 then 276
                when  esp_industry = 11 and esp_industry2 = 4 then 287
                when  esp_industry = 11 and esp_industry2 = 5 then 298
                when  esp_industry = 11 and esp_industry2 = 6 then 309
                when  esp_industry = 11 and esp_industry2 = 7 then 320
                when  esp_industry = 11 and esp_industry2 = 8 then 331
                when  esp_industry = 12 and esp_industry2 = 1 then 342
                when  esp_industry = 12 and esp_industry2 = 2 then 353
                when  esp_industry = 12 and esp_industry2 = 3 then 364
                when  esp_industry = 12 and esp_industry2 = 4 then 375
                when  esp_industry = 12 and esp_industry2 = 5 then 386
                when  esp_industry = 12 and esp_industry2 = 6 then 397
                when  esp_industry = 12 and esp_industry2 = 7 then 408
                when  esp_industry = 12 and esp_industry2 = 8 then 419
                when  esp_industry = 12 and esp_industry2 = 9 then 430
                when  esp_industry = 12 and esp_industry2 = 10 then 441
                else 999  end as s_index_id,
           count(distinct a.enterprise_id)
    from bass2.stat_enterprise_0060_f_tmp1 a,
         bass2.stat_enterprise_0060_f_tmp2 b
     where a.enterprise_id = b.enterprise_id
     and b.esp_industry in (5,6,7,8,9,10,11,12)
     group by a.city_id,
            case when  esp_industry = 5 and esp_industry2 = 1 then 1
                when  esp_industry = 5 and esp_industry2 = 2 then 12
                when  esp_industry = 5 and esp_industry2 = 3 then 23
                when  esp_industry = 5 and esp_industry2 = 4 then 34
                when  esp_industry = 6 and esp_industry2 = 2 then 78
                when  esp_industry = 6 and esp_industry2 = 1 then 89
                when  esp_industry = 7 and esp_industry2 = 1 then 100
                when  esp_industry = 7 and esp_industry2 = 2 then 111
                when  esp_industry = 7 and esp_industry2 = 3 then 122
                when  esp_industry = 8 and esp_industry2 = 1 then 133
                when  esp_industry = 8 and esp_industry2 = 2 then 144
                when  esp_industry = 8 and esp_industry2 = 3 then 155
                when  esp_industry = 9 and esp_industry2 = 1 then 166
                when  esp_industry = 9 and esp_industry2 = 2 then 177
                when  esp_industry = 9 and esp_industry2 = 3 then 188
                when  esp_industry = 10 and esp_industry2 = 1 then 199
                when  esp_industry = 10 and esp_industry2 = 2 then 210
                when  esp_industry = 10 and esp_industry2 = 3 then 221
                when  esp_industry = 10 and esp_industry2 = 4 then 232
                when  esp_industry = 10 and esp_industry2 = 5 then 243
                when  esp_industry = 11 and esp_industry2 = 1 then 254
                when  esp_industry = 11 and esp_industry2 = 2 then 265
                when  esp_industry = 11 and esp_industry2 = 3 then 276
                when  esp_industry = 11 and esp_industry2 = 4 then 287
                when  esp_industry = 11 and esp_industry2 = 5 then 298
                when  esp_industry = 11 and esp_industry2 = 6 then 309
                when  esp_industry = 11 and esp_industry2 = 7 then 320
                when  esp_industry = 11 and esp_industry2 = 8 then 331
                when  esp_industry = 12 and esp_industry2 = 1 then 342
                when  esp_industry = 12 and esp_industry2 = 2 then 353
                when  esp_industry = 12 and esp_industry2 = 3 then 364
                when  esp_industry = 12 and esp_industry2 = 4 then 375
                when  esp_industry = 12 and esp_industry2 = 5 then 386
                when  esp_industry = 12 and esp_industry2 = 6 then 397
                when  esp_industry = 12 and esp_industry2 = 7 then 408
                when  esp_industry = 12 and esp_industry2 = 8 then 419
                when  esp_industry = 12 and esp_industry2 = 9 then 430
                when  esp_industry = 12 and esp_industry2 = 10 then 441
                else 999  end 
   ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }  
    
#其中：订购一项信息化产品的集团客户数

         set sql_buf "
    insert into $stat_enterprise_0060_f_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           case when  esp_industry = 5 and esp_industry2 = 1   and ent_sub_counts=1 then 2
                when  esp_industry = 5 and esp_industry2 = 2   and ent_sub_counts=1  then 13
                when  esp_industry = 5 and esp_industry2 = 3   and ent_sub_counts=1  then 24
                when  esp_industry = 5 and esp_industry2 = 4   and ent_sub_counts=1  then 35
                when  esp_industry = 6 and esp_industry2 = 2   and ent_sub_counts=1  then 79
                when  esp_industry = 6 and esp_industry2 = 1   and ent_sub_counts=1  then 90
                when  esp_industry = 7 and esp_industry2 = 1   and ent_sub_counts=1  then 101
                when  esp_industry = 7 and esp_industry2 = 2   and ent_sub_counts=1  then 112
                when  esp_industry = 7 and esp_industry2 = 3   and ent_sub_counts=1  then 123
                when  esp_industry = 8 and esp_industry2 = 1   and ent_sub_counts=1  then 134
                when  esp_industry = 8 and esp_industry2 = 2   and ent_sub_counts=1  then 145
                when  esp_industry = 8 and esp_industry2 = 3   and ent_sub_counts=1  then 156
                when  esp_industry = 9 and esp_industry2 = 1   and ent_sub_counts=1  then 167
                when  esp_industry = 9 and esp_industry2 = 2   and ent_sub_counts=1  then 178
                when  esp_industry = 9 and esp_industry2 = 3   and ent_sub_counts=1  then 189
                when  esp_industry = 10 and esp_industry2 = 1  and ent_sub_counts=1  then 200
                when  esp_industry = 10 and esp_industry2 = 2  and ent_sub_counts=1  then 211
                when  esp_industry = 10 and esp_industry2 = 3  and ent_sub_counts=1  then 222
                when  esp_industry = 10 and esp_industry2 = 4  and ent_sub_counts=1  then 233
                when  esp_industry = 10 and esp_industry2 = 5  and ent_sub_counts=1  then 244
                when  esp_industry = 11 and esp_industry2 = 1  and ent_sub_counts=1  then 255
                when  esp_industry = 11 and esp_industry2 = 2  and ent_sub_counts=1  then 266
                when  esp_industry = 11 and esp_industry2 = 3  and ent_sub_counts=1  then 277
                when  esp_industry = 11 and esp_industry2 = 4  and ent_sub_counts=1  then 288
                when  esp_industry = 11 and esp_industry2 = 5  and ent_sub_counts=1  then 299
                when  esp_industry = 11 and esp_industry2 = 6  and ent_sub_counts=1  then 310
                when  esp_industry = 11 and esp_industry2 = 7  and ent_sub_counts=1  then 321
                when  esp_industry = 11 and esp_industry2 = 8  and ent_sub_counts=1  then 332
                when  esp_industry = 12 and esp_industry2 = 1  and ent_sub_counts=1  then 343
                when  esp_industry = 12 and esp_industry2 = 2  and ent_sub_counts=1  then 354
                when  esp_industry = 12 and esp_industry2 = 3  and ent_sub_counts=1  then 365
                when  esp_industry = 12 and esp_industry2 = 4  and ent_sub_counts=1  then 376
                when  esp_industry = 12 and esp_industry2 = 5  and ent_sub_counts=1  then 387
                when  esp_industry = 12 and esp_industry2 = 6  and ent_sub_counts=1  then 398
                when  esp_industry = 12 and esp_industry2 = 7  and ent_sub_counts=1  then 409
                when  esp_industry = 12 and esp_industry2 = 8  and ent_sub_counts=1  then 420
                when  esp_industry = 12 and esp_industry2 = 9  and ent_sub_counts=1  then 431
                when  esp_industry = 12 and esp_industry2 = 10 and ent_sub_counts=1  then 442
                else 999  end as s_index_id,
           count(distinct a.enterprise_id)
    from bass2.stat_enterprise_0060_f_tmp1 a,
         bass2.stat_enterprise_0060_f_tmp2 b
     where a.enterprise_id = b.enterprise_id
           and b.esp_industry in (5,6,7,8,9,10,11,12)
     group by a.city_id,
             case when  esp_industry = 5 and esp_industry2 = 1   and ent_sub_counts=1 then 2
                when  esp_industry = 5 and esp_industry2 = 2   and ent_sub_counts=1  then 13
                when  esp_industry = 5 and esp_industry2 = 3   and ent_sub_counts=1  then 24
                when  esp_industry = 5 and esp_industry2 = 4   and ent_sub_counts=1  then 35
                when  esp_industry = 6 and esp_industry2 = 2   and ent_sub_counts=1  then 79
                when  esp_industry = 6 and esp_industry2 = 1   and ent_sub_counts=1  then 90
                when  esp_industry = 7 and esp_industry2 = 1   and ent_sub_counts=1  then 101
                when  esp_industry = 7 and esp_industry2 = 2   and ent_sub_counts=1  then 112
                when  esp_industry = 7 and esp_industry2 = 3   and ent_sub_counts=1  then 123
                when  esp_industry = 8 and esp_industry2 = 1   and ent_sub_counts=1  then 134
                when  esp_industry = 8 and esp_industry2 = 2   and ent_sub_counts=1  then 145
                when  esp_industry = 8 and esp_industry2 = 3   and ent_sub_counts=1  then 156
                when  esp_industry = 9 and esp_industry2 = 1   and ent_sub_counts=1  then 167
                when  esp_industry = 9 and esp_industry2 = 2   and ent_sub_counts=1  then 178
                when  esp_industry = 9 and esp_industry2 = 3   and ent_sub_counts=1  then 189
                when  esp_industry = 10 and esp_industry2 = 1  and ent_sub_counts=1  then 200
                when  esp_industry = 10 and esp_industry2 = 2  and ent_sub_counts=1  then 211
                when  esp_industry = 10 and esp_industry2 = 3  and ent_sub_counts=1  then 222
                when  esp_industry = 10 and esp_industry2 = 4  and ent_sub_counts=1  then 233
                when  esp_industry = 10 and esp_industry2 = 5  and ent_sub_counts=1  then 244
                when  esp_industry = 11 and esp_industry2 = 1  and ent_sub_counts=1  then 255
                when  esp_industry = 11 and esp_industry2 = 2  and ent_sub_counts=1  then 266
                when  esp_industry = 11 and esp_industry2 = 3  and ent_sub_counts=1  then 277
                when  esp_industry = 11 and esp_industry2 = 4  and ent_sub_counts=1  then 288
                when  esp_industry = 11 and esp_industry2 = 5  and ent_sub_counts=1  then 299
                when  esp_industry = 11 and esp_industry2 = 6  and ent_sub_counts=1  then 310
                when  esp_industry = 11 and esp_industry2 = 7  and ent_sub_counts=1  then 321
                when  esp_industry = 11 and esp_industry2 = 8  and ent_sub_counts=1  then 332
                when  esp_industry = 12 and esp_industry2 = 1  and ent_sub_counts=1  then 343
                when  esp_industry = 12 and esp_industry2 = 2  and ent_sub_counts=1  then 354
                when  esp_industry = 12 and esp_industry2 = 3  and ent_sub_counts=1  then 365
                when  esp_industry = 12 and esp_industry2 = 4  and ent_sub_counts=1  then 376
                when  esp_industry = 12 and esp_industry2 = 5  and ent_sub_counts=1  then 387
                when  esp_industry = 12 and esp_industry2 = 6  and ent_sub_counts=1  then 398
                when  esp_industry = 12 and esp_industry2 = 7  and ent_sub_counts=1  then 409
                when  esp_industry = 12 and esp_industry2 = 8  and ent_sub_counts=1  then 420
                when  esp_industry = 12 and esp_industry2 = 9  and ent_sub_counts=1  then 431
                when  esp_industry = 12 and esp_industry2 = 10 and ent_sub_counts=1  then 442
                else 999  end
   ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }

     #订购两项信息化产品的集团客户数  
           set sql_buf "
    insert into $stat_enterprise_0060_f_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           case when  esp_industry = 5 and esp_industry2 = 1   and ent_sub_counts=2  then 3 
                when  esp_industry = 5 and esp_industry2 = 2   and ent_sub_counts=2  then 14
                when  esp_industry = 5 and esp_industry2 = 3   and ent_sub_counts=2  then 25
                when  esp_industry = 5 and esp_industry2 = 4   and ent_sub_counts=2  then 36
                when  esp_industry = 6 and esp_industry2 = 2   and ent_sub_counts=2  then 80 
                when  esp_industry = 6 and esp_industry2 = 1   and ent_sub_counts=2  then 91 
                when  esp_industry = 7 and esp_industry2 = 1   and ent_sub_counts=2  then 102
                when  esp_industry = 7 and esp_industry2 = 2   and ent_sub_counts=2  then 113
                when  esp_industry = 7 and esp_industry2 = 3   and ent_sub_counts=2  then 124
                when  esp_industry = 8 and esp_industry2 = 1   and ent_sub_counts=2  then 135
                when  esp_industry = 8 and esp_industry2 = 2   and ent_sub_counts=2  then 146
                when  esp_industry = 8 and esp_industry2 = 3   and ent_sub_counts=2  then 157
                when  esp_industry = 9 and esp_industry2 = 1   and ent_sub_counts=2  then 168
                when  esp_industry = 9 and esp_industry2 = 2   and ent_sub_counts=2  then 179
                when  esp_industry = 9 and esp_industry2 = 3   and ent_sub_counts=2  then 190
                when  esp_industry = 10 and esp_industry2 = 1  and ent_sub_counts=2  then 201
                when  esp_industry = 10 and esp_industry2 = 2  and ent_sub_counts=2  then 212
                when  esp_industry = 10 and esp_industry2 = 3  and ent_sub_counts=2  then 223
                when  esp_industry = 10 and esp_industry2 = 4  and ent_sub_counts=2  then 234
                when  esp_industry = 10 and esp_industry2 = 5  and ent_sub_counts=2  then 245
                when  esp_industry = 11 and esp_industry2 = 1  and ent_sub_counts=2  then 256
                when  esp_industry = 11 and esp_industry2 = 2  and ent_sub_counts=2  then 267
                when  esp_industry = 11 and esp_industry2 = 3  and ent_sub_counts=2  then 278
                when  esp_industry = 11 and esp_industry2 = 4  and ent_sub_counts=2  then 289
                when  esp_industry = 11 and esp_industry2 = 5  and ent_sub_counts=2  then 300
                when  esp_industry = 11 and esp_industry2 = 6  and ent_sub_counts=2  then 311
                when  esp_industry = 11 and esp_industry2 = 7  and ent_sub_counts=2  then 322
                when  esp_industry = 11 and esp_industry2 = 8  and ent_sub_counts=2  then 333
                when  esp_industry = 12 and esp_industry2 = 1  and ent_sub_counts=2  then 344
                when  esp_industry = 12 and esp_industry2 = 2  and ent_sub_counts=2  then 355
                when  esp_industry = 12 and esp_industry2 = 3  and ent_sub_counts=2  then 366
                when  esp_industry = 12 and esp_industry2 = 4  and ent_sub_counts=2  then 377
                when  esp_industry = 12 and esp_industry2 = 5  and ent_sub_counts=2  then 388
                when  esp_industry = 12 and esp_industry2 = 6  and ent_sub_counts=2  then 399
                when  esp_industry = 12 and esp_industry2 = 7  and ent_sub_counts=2  then 410
                when  esp_industry = 12 and esp_industry2 = 8  and ent_sub_counts=2  then 421
                when  esp_industry = 12 and esp_industry2 = 9  and ent_sub_counts=2  then 432
                when  esp_industry = 12 and esp_industry2 = 10 and ent_sub_counts=2  then 443
                else 999  end as s_index_id,
           count(distinct a.enterprise_id)
    from bass2.stat_enterprise_0060_f_tmp1 a,
         bass2.stat_enterprise_0060_f_tmp2 b
     where a.enterprise_id = b.enterprise_id
           and b.esp_industry in (5,6,7,8,9,10,11,12)
     group by a.city_id,
            case when  esp_industry = 5 and esp_industry2 = 1   and ent_sub_counts=2  then 3 
                when  esp_industry = 5 and esp_industry2 = 2   and ent_sub_counts=2  then 14
                when  esp_industry = 5 and esp_industry2 = 3   and ent_sub_counts=2  then 25
                when  esp_industry = 5 and esp_industry2 = 4   and ent_sub_counts=2  then 36
                when  esp_industry = 6 and esp_industry2 = 2   and ent_sub_counts=2  then 80 
                when  esp_industry = 6 and esp_industry2 = 1   and ent_sub_counts=2  then 91 
                when  esp_industry = 7 and esp_industry2 = 1   and ent_sub_counts=2  then 102
                when  esp_industry = 7 and esp_industry2 = 2   and ent_sub_counts=2  then 113
                when  esp_industry = 7 and esp_industry2 = 3   and ent_sub_counts=2  then 124
                when  esp_industry = 8 and esp_industry2 = 1   and ent_sub_counts=2  then 135
                when  esp_industry = 8 and esp_industry2 = 2   and ent_sub_counts=2  then 146
                when  esp_industry = 8 and esp_industry2 = 3   and ent_sub_counts=2  then 157
                when  esp_industry = 9 and esp_industry2 = 1   and ent_sub_counts=2  then 168
                when  esp_industry = 9 and esp_industry2 = 2   and ent_sub_counts=2  then 179
                when  esp_industry = 9 and esp_industry2 = 3   and ent_sub_counts=2  then 190
                when  esp_industry = 10 and esp_industry2 = 1  and ent_sub_counts=2  then 201
                when  esp_industry = 10 and esp_industry2 = 2  and ent_sub_counts=2  then 212
                when  esp_industry = 10 and esp_industry2 = 3  and ent_sub_counts=2  then 223
                when  esp_industry = 10 and esp_industry2 = 4  and ent_sub_counts=2  then 234
                when  esp_industry = 10 and esp_industry2 = 5  and ent_sub_counts=2  then 245
                when  esp_industry = 11 and esp_industry2 = 1  and ent_sub_counts=2  then 256
                when  esp_industry = 11 and esp_industry2 = 2  and ent_sub_counts=2  then 267
                when  esp_industry = 11 and esp_industry2 = 3  and ent_sub_counts=2  then 278
                when  esp_industry = 11 and esp_industry2 = 4  and ent_sub_counts=2  then 289
                when  esp_industry = 11 and esp_industry2 = 5  and ent_sub_counts=2  then 300
                when  esp_industry = 11 and esp_industry2 = 6  and ent_sub_counts=2  then 311
                when  esp_industry = 11 and esp_industry2 = 7  and ent_sub_counts=2  then 322
                when  esp_industry = 11 and esp_industry2 = 8  and ent_sub_counts=2  then 333
                when  esp_industry = 12 and esp_industry2 = 1  and ent_sub_counts=2  then 344
                when  esp_industry = 12 and esp_industry2 = 2  and ent_sub_counts=2  then 355
                when  esp_industry = 12 and esp_industry2 = 3  and ent_sub_counts=2  then 366
                when  esp_industry = 12 and esp_industry2 = 4  and ent_sub_counts=2  then 377
                when  esp_industry = 12 and esp_industry2 = 5  and ent_sub_counts=2  then 388
                when  esp_industry = 12 and esp_industry2 = 6  and ent_sub_counts=2  then 399
                when  esp_industry = 12 and esp_industry2 = 7  and ent_sub_counts=2  then 410
                when  esp_industry = 12 and esp_industry2 = 8  and ent_sub_counts=2  then 421
                when  esp_industry = 12 and esp_industry2 = 9  and ent_sub_counts=2  then 432
                when  esp_industry = 12 and esp_industry2 = 10 and ent_sub_counts=2  then 443
                else 999  end
   ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }
 
   #  订购两项以上信息化产品的集团客户数
           set sql_buf "
    insert into $stat_enterprise_0060_f_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           case when  esp_industry = 5 and esp_industry2 = 1   and ent_sub_counts>2  then 4 
                when  esp_industry = 5 and esp_industry2 = 2   and ent_sub_counts>2  then 15
                when  esp_industry = 5 and esp_industry2 = 3   and ent_sub_counts>2  then 26
                when  esp_industry = 5 and esp_industry2 = 4   and ent_sub_counts>2  then 37
                when  esp_industry = 6 and esp_industry2 = 2   and ent_sub_counts>2  then 81 
                when  esp_industry = 6 and esp_industry2 = 1   and ent_sub_counts>2  then 92 
                when  esp_industry = 7 and esp_industry2 = 1   and ent_sub_counts>2  then 103
                when  esp_industry = 7 and esp_industry2 = 2   and ent_sub_counts>2  then 114
                when  esp_industry = 7 and esp_industry2 = 3   and ent_sub_counts>2  then 125
                when  esp_industry = 8 and esp_industry2 = 1   and ent_sub_counts>2  then 136
                when  esp_industry = 8 and esp_industry2 = 2   and ent_sub_counts>2  then 147
                when  esp_industry = 8 and esp_industry2 = 3   and ent_sub_counts>2  then 158
                when  esp_industry = 9 and esp_industry2 = 1   and ent_sub_counts>2  then 169
                when  esp_industry = 9 and esp_industry2 = 2   and ent_sub_counts>2  then 180
                when  esp_industry = 9 and esp_industry2 = 3   and ent_sub_counts>2  then 191
                when  esp_industry = 10 and esp_industry2 = 1  and ent_sub_counts>2  then 202
                when  esp_industry = 10 and esp_industry2 = 2  and ent_sub_counts>2  then 213
                when  esp_industry = 10 and esp_industry2 = 3  and ent_sub_counts>2  then 224
                when  esp_industry = 10 and esp_industry2 = 4  and ent_sub_counts>2  then 235
                when  esp_industry = 10 and esp_industry2 = 5  and ent_sub_counts>2  then 246
                when  esp_industry = 11 and esp_industry2 = 1  and ent_sub_counts>2  then 257
                when  esp_industry = 11 and esp_industry2 = 2  and ent_sub_counts>2  then 268
                when  esp_industry = 11 and esp_industry2 = 3  and ent_sub_counts>2  then 279
                when  esp_industry = 11 and esp_industry2 = 4  and ent_sub_counts>2  then 290
                when  esp_industry = 11 and esp_industry2 = 5  and ent_sub_counts>2  then 301
                when  esp_industry = 11 and esp_industry2 = 6  and ent_sub_counts>2  then 312
                when  esp_industry = 11 and esp_industry2 = 7  and ent_sub_counts>2  then 323
                when  esp_industry = 11 and esp_industry2 = 8  and ent_sub_counts>2  then 334
                when  esp_industry = 12 and esp_industry2 = 1  and ent_sub_counts>2  then 345
                when  esp_industry = 12 and esp_industry2 = 2  and ent_sub_counts>2  then 356
                when  esp_industry = 12 and esp_industry2 = 3  and ent_sub_counts>2  then 367
                when  esp_industry = 12 and esp_industry2 = 4  and ent_sub_counts>2  then 378
                when  esp_industry = 12 and esp_industry2 = 5  and ent_sub_counts>2  then 389
                when  esp_industry = 12 and esp_industry2 = 6  and ent_sub_counts>2  then 400
                when  esp_industry = 12 and esp_industry2 = 7  and ent_sub_counts>2  then 411
                when  esp_industry = 12 and esp_industry2 = 8  and ent_sub_counts>2  then 422
                when  esp_industry = 12 and esp_industry2 = 9  and ent_sub_counts>2  then 433
                when  esp_industry = 12 and esp_industry2 = 10 and ent_sub_counts>2  then 444
                else 999  end as s_index_id,
           count(distinct a.enterprise_id)
    from bass2.stat_enterprise_0060_f_tmp1 a,
         bass2.stat_enterprise_0060_f_tmp2 b
     where a.enterprise_id = b.enterprise_id
           and b.esp_industry in (5,6,7,8,9,10,11,12)
     group by a.city_id,
            case when  esp_industry = 5 and esp_industry2 = 1   and ent_sub_counts>2  then 4 
                when  esp_industry = 5 and esp_industry2 = 2   and ent_sub_counts>2  then 15
                when  esp_industry = 5 and esp_industry2 = 3   and ent_sub_counts>2  then 26
                when  esp_industry = 5 and esp_industry2 = 4   and ent_sub_counts>2  then 37
                when  esp_industry = 6 and esp_industry2 = 2   and ent_sub_counts>2  then 81 
                when  esp_industry = 6 and esp_industry2 = 1   and ent_sub_counts>2  then 92 
                when  esp_industry = 7 and esp_industry2 = 1   and ent_sub_counts>2  then 103
                when  esp_industry = 7 and esp_industry2 = 2   and ent_sub_counts>2  then 114
                when  esp_industry = 7 and esp_industry2 = 3   and ent_sub_counts>2  then 125
                when  esp_industry = 8 and esp_industry2 = 1   and ent_sub_counts>2  then 136
                when  esp_industry = 8 and esp_industry2 = 2   and ent_sub_counts>2  then 147
                when  esp_industry = 8 and esp_industry2 = 3   and ent_sub_counts>2  then 158
                when  esp_industry = 9 and esp_industry2 = 1   and ent_sub_counts>2  then 169
                when  esp_industry = 9 and esp_industry2 = 2   and ent_sub_counts>2  then 180
                when  esp_industry = 9 and esp_industry2 = 3   and ent_sub_counts>2  then 191
                when  esp_industry = 10 and esp_industry2 = 1  and ent_sub_counts>2  then 202
                when  esp_industry = 10 and esp_industry2 = 2  and ent_sub_counts>2  then 213
                when  esp_industry = 10 and esp_industry2 = 3  and ent_sub_counts>2  then 224
                when  esp_industry = 10 and esp_industry2 = 4  and ent_sub_counts>2  then 235
                when  esp_industry = 10 and esp_industry2 = 5  and ent_sub_counts>2  then 246
                when  esp_industry = 11 and esp_industry2 = 1  and ent_sub_counts>2  then 257
                when  esp_industry = 11 and esp_industry2 = 2  and ent_sub_counts>2  then 268
                when  esp_industry = 11 and esp_industry2 = 3  and ent_sub_counts>2  then 279
                when  esp_industry = 11 and esp_industry2 = 4  and ent_sub_counts>2  then 290
                when  esp_industry = 11 and esp_industry2 = 5  and ent_sub_counts>2  then 301
                when  esp_industry = 11 and esp_industry2 = 6  and ent_sub_counts>2  then 312
                when  esp_industry = 11 and esp_industry2 = 7  and ent_sub_counts>2  then 323
                when  esp_industry = 11 and esp_industry2 = 8  and ent_sub_counts>2  then 334
                when  esp_industry = 12 and esp_industry2 = 1  and ent_sub_counts>2  then 345
                when  esp_industry = 12 and esp_industry2 = 2  and ent_sub_counts>2  then 356
                when  esp_industry = 12 and esp_industry2 = 3  and ent_sub_counts>2  then 367
                when  esp_industry = 12 and esp_industry2 = 4  and ent_sub_counts>2  then 378
                when  esp_industry = 12 and esp_industry2 = 5  and ent_sub_counts>2  then 389
                when  esp_industry = 12 and esp_industry2 = 6  and ent_sub_counts>2  then 400
                when  esp_industry = 12 and esp_industry2 = 7  and ent_sub_counts>2  then 411
                when  esp_industry = 12 and esp_industry2 = 8  and ent_sub_counts>2  then 422
                when  esp_industry = 12 and esp_industry2 = 9  and ent_sub_counts>2  then 433
                when  esp_industry = 12 and esp_industry2 = 10 and ent_sub_counts>2  then 444
                else 999  end
   ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }
 
 #集团成员到达数 
           set sql_buf "
    insert into $stat_enterprise_0060_f_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           case when  esp_industry = 5 and esp_industry2 = 1     then 5 
                when  esp_industry = 5 and esp_industry2 = 2     then 16
                when  esp_industry = 5 and esp_industry2 = 3     then 27
                when  esp_industry = 5 and esp_industry2 = 4     then 38
                when  esp_industry = 6 and esp_industry2 = 2     then 82 
                when  esp_industry = 6 and esp_industry2 = 1     then 93 
                when  esp_industry = 7 and esp_industry2 = 1     then 104
                when  esp_industry = 7 and esp_industry2 = 2     then 115
                when  esp_industry = 7 and esp_industry2 = 3     then 126
                when  esp_industry = 8 and esp_industry2 = 1     then 137
                when  esp_industry = 8 and esp_industry2 = 2     then 148
                when  esp_industry = 8 and esp_industry2 = 3     then 159
                when  esp_industry = 9 and esp_industry2 = 1     then 170
                when  esp_industry = 9 and esp_industry2 = 2     then 181
                when  esp_industry = 9 and esp_industry2 = 3     then 192
                when  esp_industry = 10 and esp_industry2 = 1    then 203
                when  esp_industry = 10 and esp_industry2 = 2    then 214
                when  esp_industry = 10 and esp_industry2 = 3    then 225
                when  esp_industry = 10 and esp_industry2 = 4    then 236
                when  esp_industry = 10 and esp_industry2 = 5    then 247
                when  esp_industry = 11 and esp_industry2 = 1    then 258
                when  esp_industry = 11 and esp_industry2 = 2    then 269
                when  esp_industry = 11 and esp_industry2 = 3    then 280
                when  esp_industry = 11 and esp_industry2 = 4    then 291
                when  esp_industry = 11 and esp_industry2 = 5    then 302
                when  esp_industry = 11 and esp_industry2 = 6    then 313
                when  esp_industry = 11 and esp_industry2 = 7    then 324
                when  esp_industry = 11 and esp_industry2 = 8    then 335
                when  esp_industry = 12 and esp_industry2 = 1    then 346
                when  esp_industry = 12 and esp_industry2 = 2    then 357
                when  esp_industry = 12 and esp_industry2 = 3    then 368
                when  esp_industry = 12 and esp_industry2 = 4    then 379
                when  esp_industry = 12 and esp_industry2 = 5    then 390
                when  esp_industry = 12 and esp_industry2 = 6    then 401
                when  esp_industry = 12 and esp_industry2 = 7    then 412
                when  esp_industry = 12 and esp_industry2 = 8    then 423
                when  esp_industry = 12 and esp_industry2 = 9    then 434
                when  esp_industry = 12 and esp_industry2 = 10   then 445
                else 999  end as s_index_id,
           sum(a.member_counts)
    from bass2.stat_enterprise_0060_f_tmp1 a,
         bass2.stat_enterprise_0060_f_tmp2 b
     where a.enterprise_id = b.enterprise_id
           and b.esp_industry in (5,6,7,8,9,10,11,12)
     group by a.city_id,
             case when  esp_industry = 5 and esp_industry2 = 1     then 5 
                when  esp_industry = 5 and esp_industry2 = 2     then 16
                when  esp_industry = 5 and esp_industry2 = 3     then 27
                when  esp_industry = 5 and esp_industry2 = 4     then 38
                when  esp_industry = 6 and esp_industry2 = 2     then 82 
                when  esp_industry = 6 and esp_industry2 = 1     then 93 
                when  esp_industry = 7 and esp_industry2 = 1     then 104
                when  esp_industry = 7 and esp_industry2 = 2     then 115
                when  esp_industry = 7 and esp_industry2 = 3     then 126
                when  esp_industry = 8 and esp_industry2 = 1     then 137
                when  esp_industry = 8 and esp_industry2 = 2     then 148
                when  esp_industry = 8 and esp_industry2 = 3     then 159
                when  esp_industry = 9 and esp_industry2 = 1     then 170
                when  esp_industry = 9 and esp_industry2 = 2     then 181
                when  esp_industry = 9 and esp_industry2 = 3     then 192
                when  esp_industry = 10 and esp_industry2 = 1    then 203
                when  esp_industry = 10 and esp_industry2 = 2    then 214
                when  esp_industry = 10 and esp_industry2 = 3    then 225
                when  esp_industry = 10 and esp_industry2 = 4    then 236
                when  esp_industry = 10 and esp_industry2 = 5    then 247
                when  esp_industry = 11 and esp_industry2 = 1    then 258
                when  esp_industry = 11 and esp_industry2 = 2    then 269
                when  esp_industry = 11 and esp_industry2 = 3    then 280
                when  esp_industry = 11 and esp_industry2 = 4    then 291
                when  esp_industry = 11 and esp_industry2 = 5    then 302
                when  esp_industry = 11 and esp_industry2 = 6    then 313
                when  esp_industry = 11 and esp_industry2 = 7    then 324
                when  esp_industry = 11 and esp_industry2 = 8    then 335
                when  esp_industry = 12 and esp_industry2 = 1    then 346
                when  esp_industry = 12 and esp_industry2 = 2    then 357
                when  esp_industry = 12 and esp_industry2 = 3    then 368
                when  esp_industry = 12 and esp_industry2 = 4    then 379
                when  esp_industry = 12 and esp_industry2 = 5    then 390
                when  esp_industry = 12 and esp_industry2 = 6    then 401
                when  esp_industry = 12 and esp_industry2 = 7    then 412
                when  esp_industry = 12 and esp_industry2 = 8    then 423
                when  esp_industry = 12 and esp_industry2 = 9    then 434
                when  esp_industry = 12 and esp_industry2 = 10   then 445
                else 999  end
   ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    } 
    
 #集团客户整体收入    
           set sql_buf "
    insert into $stat_enterprise_0060_f_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           case when  esp_industry = 5 and esp_industry2 = 1     then 6 
                when  esp_industry = 5 and esp_industry2 = 2     then 17
                when  esp_industry = 5 and esp_industry2 = 3     then 28
                when  esp_industry = 5 and esp_industry2 = 4     then 39
                when  esp_industry = 6 and esp_industry2 = 2     then 83 
                when  esp_industry = 6 and esp_industry2 = 1     then 94 
                when  esp_industry = 7 and esp_industry2 = 1     then 105
                when  esp_industry = 7 and esp_industry2 = 2     then 116
                when  esp_industry = 7 and esp_industry2 = 3     then 127
                when  esp_industry = 8 and esp_industry2 = 1     then 138
                when  esp_industry = 8 and esp_industry2 = 2     then 149
                when  esp_industry = 8 and esp_industry2 = 3     then 160
                when  esp_industry = 9 and esp_industry2 = 1     then 171
                when  esp_industry = 9 and esp_industry2 = 2     then 182
                when  esp_industry = 9 and esp_industry2 = 3     then 193
                when  esp_industry = 10 and esp_industry2 = 1    then 204
                when  esp_industry = 10 and esp_industry2 = 2    then 215
                when  esp_industry = 10 and esp_industry2 = 3    then 226
                when  esp_industry = 10 and esp_industry2 = 4    then 237
                when  esp_industry = 10 and esp_industry2 = 5    then 248
                when  esp_industry = 11 and esp_industry2 = 1    then 259
                when  esp_industry = 11 and esp_industry2 = 2    then 270
                when  esp_industry = 11 and esp_industry2 = 3    then 281
                when  esp_industry = 11 and esp_industry2 = 4    then 292
                when  esp_industry = 11 and esp_industry2 = 5    then 303
                when  esp_industry = 11 and esp_industry2 = 6    then 314
                when  esp_industry = 11 and esp_industry2 = 7    then 325
                when  esp_industry = 11 and esp_industry2 = 8    then 336
                when  esp_industry = 12 and esp_industry2 = 1    then 347
                when  esp_industry = 12 and esp_industry2 = 2    then 358
                when  esp_industry = 12 and esp_industry2 = 3    then 369
                when  esp_industry = 12 and esp_industry2 = 4    then 380
                when  esp_industry = 12 and esp_industry2 = 5    then 391
                when  esp_industry = 12 and esp_industry2 = 6    then 402
                when  esp_industry = 12 and esp_industry2 = 7    then 413
                when  esp_industry = 12 and esp_industry2 = 8    then 424
                when  esp_industry = 12 and esp_industry2 = 9    then 435
                when  esp_industry = 12 and esp_industry2 = 10   then 446
                else 999  end as s_index_id,
           sum(a.member_income+a.ent_prod_income)
    from bass2.stat_enterprise_0060_f_tmp1 a,
         bass2.stat_enterprise_0060_f_tmp2 b
     where a.enterprise_id = b.enterprise_id
           and b.esp_industry in (5,6,7,8,9,10,11,12)
     group by a.city_id,
             case when  esp_industry = 5 and esp_industry2 = 1     then 6 
                when  esp_industry = 5 and esp_industry2 = 2     then 17
                when  esp_industry = 5 and esp_industry2 = 3     then 28
                when  esp_industry = 5 and esp_industry2 = 4     then 39
                when  esp_industry = 6 and esp_industry2 = 2     then 83 
                when  esp_industry = 6 and esp_industry2 = 1     then 94 
                when  esp_industry = 7 and esp_industry2 = 1     then 105
                when  esp_industry = 7 and esp_industry2 = 2     then 116
                when  esp_industry = 7 and esp_industry2 = 3     then 127
                when  esp_industry = 8 and esp_industry2 = 1     then 138
                when  esp_industry = 8 and esp_industry2 = 2     then 149
                when  esp_industry = 8 and esp_industry2 = 3     then 160
                when  esp_industry = 9 and esp_industry2 = 1     then 171
                when  esp_industry = 9 and esp_industry2 = 2     then 182
                when  esp_industry = 9 and esp_industry2 = 3     then 193
                when  esp_industry = 10 and esp_industry2 = 1    then 204
                when  esp_industry = 10 and esp_industry2 = 2    then 215
                when  esp_industry = 10 and esp_industry2 = 3    then 226
                when  esp_industry = 10 and esp_industry2 = 4    then 237
                when  esp_industry = 10 and esp_industry2 = 5    then 248
                when  esp_industry = 11 and esp_industry2 = 1    then 259
                when  esp_industry = 11 and esp_industry2 = 2    then 270
                when  esp_industry = 11 and esp_industry2 = 3    then 281
                when  esp_industry = 11 and esp_industry2 = 4    then 292
                when  esp_industry = 11 and esp_industry2 = 5    then 303
                when  esp_industry = 11 and esp_industry2 = 6    then 314
                when  esp_industry = 11 and esp_industry2 = 7    then 325
                when  esp_industry = 11 and esp_industry2 = 8    then 336
                when  esp_industry = 12 and esp_industry2 = 1    then 347
                when  esp_industry = 12 and esp_industry2 = 2    then 358
                when  esp_industry = 12 and esp_industry2 = 3    then 369
                when  esp_industry = 12 and esp_industry2 = 4    then 380
                when  esp_industry = 12 and esp_industry2 = 5    then 391
                when  esp_industry = 12 and esp_industry2 = 6    then 402
                when  esp_industry = 12 and esp_industry2 = 7    then 413
                when  esp_industry = 12 and esp_industry2 = 8    then 424
                when  esp_industry = 12 and esp_industry2 = 9    then 435
                when  esp_industry = 12 and esp_industry2 = 10   then 446
                else 999  end
   ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    } 

#集团客户统一付费收入    
           set sql_buf "
    insert into $stat_enterprise_0060_f_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           case when  esp_industry = 5 and esp_industry2 = 1     then 7 
                when  esp_industry = 5 and esp_industry2 = 2     then 18
                when  esp_industry = 5 and esp_industry2 = 3     then 29
                when  esp_industry = 5 and esp_industry2 = 4     then 40
                when  esp_industry = 6 and esp_industry2 = 2     then 84 
                when  esp_industry = 6 and esp_industry2 = 1     then 95 
                when  esp_industry = 7 and esp_industry2 = 1     then 106
                when  esp_industry = 7 and esp_industry2 = 2     then 117
                when  esp_industry = 7 and esp_industry2 = 3     then 128
                when  esp_industry = 8 and esp_industry2 = 1     then 139
                when  esp_industry = 8 and esp_industry2 = 2     then 150
                when  esp_industry = 8 and esp_industry2 = 3     then 161
                when  esp_industry = 9 and esp_industry2 = 1     then 172
                when  esp_industry = 9 and esp_industry2 = 2     then 183
                when  esp_industry = 9 and esp_industry2 = 3     then 194
                when  esp_industry = 10 and esp_industry2 = 1    then 205
                when  esp_industry = 10 and esp_industry2 = 2    then 216
                when  esp_industry = 10 and esp_industry2 = 3    then 227
                when  esp_industry = 10 and esp_industry2 = 4    then 238
                when  esp_industry = 10 and esp_industry2 = 5    then 249
                when  esp_industry = 11 and esp_industry2 = 1    then 260
                when  esp_industry = 11 and esp_industry2 = 2    then 271
                when  esp_industry = 11 and esp_industry2 = 3    then 282
                when  esp_industry = 11 and esp_industry2 = 4    then 293
                when  esp_industry = 11 and esp_industry2 = 5    then 304
                when  esp_industry = 11 and esp_industry2 = 6    then 315
                when  esp_industry = 11 and esp_industry2 = 7    then 326
                when  esp_industry = 11 and esp_industry2 = 8    then 337
                when  esp_industry = 12 and esp_industry2 = 1    then 348
                when  esp_industry = 12 and esp_industry2 = 2    then 359
                when  esp_industry = 12 and esp_industry2 = 3    then 370
                when  esp_industry = 12 and esp_industry2 = 4    then 381
                when  esp_industry = 12 and esp_industry2 = 5    then 392
                when  esp_industry = 12 and esp_industry2 = 6    then 403
                when  esp_industry = 12 and esp_industry2 = 7    then 414
                when  esp_industry = 12 and esp_industry2 = 8    then 425
                when  esp_industry = 12 and esp_industry2 = 9    then 436
                when  esp_industry = 12 and esp_industry2 = 10   then 447
                else 999  end as s_index_id,
           sum(a.ent_prod_income)
    from bass2.stat_enterprise_0060_f_tmp1 a,
         bass2.stat_enterprise_0060_f_tmp2 b
     where a.enterprise_id = b.enterprise_id
           and b.esp_industry in (5,6,7,8,9,10,11,12)
     group by a.city_id,
             case when  esp_industry = 5 and esp_industry2 = 1     then 7 
                when  esp_industry = 5 and esp_industry2 = 2     then 18
                when  esp_industry = 5 and esp_industry2 = 3     then 29
                when  esp_industry = 5 and esp_industry2 = 4     then 40
                when  esp_industry = 6 and esp_industry2 = 2     then 84 
                when  esp_industry = 6 and esp_industry2 = 1     then 95 
                when  esp_industry = 7 and esp_industry2 = 1     then 106
                when  esp_industry = 7 and esp_industry2 = 2     then 117
                when  esp_industry = 7 and esp_industry2 = 3     then 128
                when  esp_industry = 8 and esp_industry2 = 1     then 139
                when  esp_industry = 8 and esp_industry2 = 2     then 150
                when  esp_industry = 8 and esp_industry2 = 3     then 161
                when  esp_industry = 9 and esp_industry2 = 1     then 172
                when  esp_industry = 9 and esp_industry2 = 2     then 183
                when  esp_industry = 9 and esp_industry2 = 3     then 194
                when  esp_industry = 10 and esp_industry2 = 1    then 205
                when  esp_industry = 10 and esp_industry2 = 2    then 216
                when  esp_industry = 10 and esp_industry2 = 3    then 227
                when  esp_industry = 10 and esp_industry2 = 4    then 238
                when  esp_industry = 10 and esp_industry2 = 5    then 249
                when  esp_industry = 11 and esp_industry2 = 1    then 260
                when  esp_industry = 11 and esp_industry2 = 2    then 271
                when  esp_industry = 11 and esp_industry2 = 3    then 282
                when  esp_industry = 11 and esp_industry2 = 4    then 293
                when  esp_industry = 11 and esp_industry2 = 5    then 304
                when  esp_industry = 11 and esp_industry2 = 6    then 315
                when  esp_industry = 11 and esp_industry2 = 7    then 326
                when  esp_industry = 11 and esp_industry2 = 8    then 337
                when  esp_industry = 12 and esp_industry2 = 1    then 348
                when  esp_industry = 12 and esp_industry2 = 2    then 359
                when  esp_industry = 12 and esp_industry2 = 3    then 370
                when  esp_industry = 12 and esp_industry2 = 4    then 381
                when  esp_industry = 12 and esp_industry2 = 5    then 392
                when  esp_industry = 12 and esp_industry2 = 6    then 403
                when  esp_industry = 12 and esp_industry2 = 7    then 414
                when  esp_industry = 12 and esp_industry2 = 8    then 425
                when  esp_industry = 12 and esp_industry2 = 9    then 436
                when  esp_industry = 12 and esp_industry2 = 10   then 447
                else 999  end
   ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }     
    
 #集团成员收入   
           set sql_buf "
    insert into $stat_enterprise_0060_f_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           case when  esp_industry = 5 and esp_industry2 = 1     then 8 
                when  esp_industry = 5 and esp_industry2 = 2     then 19
                when  esp_industry = 5 and esp_industry2 = 3     then 30
                when  esp_industry = 5 and esp_industry2 = 4     then 41
                when  esp_industry = 6 and esp_industry2 = 2     then 85 
                when  esp_industry = 6 and esp_industry2 = 1     then 96 
                when  esp_industry = 7 and esp_industry2 = 1     then 107
                when  esp_industry = 7 and esp_industry2 = 2     then 118
                when  esp_industry = 7 and esp_industry2 = 3     then 129
                when  esp_industry = 8 and esp_industry2 = 1     then 140
                when  esp_industry = 8 and esp_industry2 = 2     then 151
                when  esp_industry = 8 and esp_industry2 = 3     then 162
                when  esp_industry = 9 and esp_industry2 = 1     then 173
                when  esp_industry = 9 and esp_industry2 = 2     then 184
                when  esp_industry = 9 and esp_industry2 = 3     then 195
                when  esp_industry = 10 and esp_industry2 = 1    then 206
                when  esp_industry = 10 and esp_industry2 = 2    then 217
                when  esp_industry = 10 and esp_industry2 = 3    then 228
                when  esp_industry = 10 and esp_industry2 = 4    then 239
                when  esp_industry = 10 and esp_industry2 = 5    then 250
                when  esp_industry = 11 and esp_industry2 = 1    then 261
                when  esp_industry = 11 and esp_industry2 = 2    then 272
                when  esp_industry = 11 and esp_industry2 = 3    then 283
                when  esp_industry = 11 and esp_industry2 = 4    then 294
                when  esp_industry = 11 and esp_industry2 = 5    then 305
                when  esp_industry = 11 and esp_industry2 = 6    then 316
                when  esp_industry = 11 and esp_industry2 = 7    then 327
                when  esp_industry = 11 and esp_industry2 = 8    then 338
                when  esp_industry = 12 and esp_industry2 = 1    then 349
                when  esp_industry = 12 and esp_industry2 = 2    then 360
                when  esp_industry = 12 and esp_industry2 = 3    then 371
                when  esp_industry = 12 and esp_industry2 = 4    then 382
                when  esp_industry = 12 and esp_industry2 = 5    then 393
                when  esp_industry = 12 and esp_industry2 = 6    then 404
                when  esp_industry = 12 and esp_industry2 = 7    then 415
                when  esp_industry = 12 and esp_industry2 = 8    then 426
                when  esp_industry = 12 and esp_industry2 = 9    then 437
                when  esp_industry = 12 and esp_industry2 = 10   then 448
                else 999  end as s_index_id,
           sum(a.member_income)
    from bass2.stat_enterprise_0060_f_tmp1 a,
         bass2.stat_enterprise_0060_f_tmp2 b
     where a.enterprise_id = b.enterprise_id
           and b.esp_industry in (5,6,7,8,9,10,11,12)
     group by a.city_id,
             case when  esp_industry = 5 and esp_industry2 = 1     then 8 
                when  esp_industry = 5 and esp_industry2 = 2     then 19
                when  esp_industry = 5 and esp_industry2 = 3     then 30
                when  esp_industry = 5 and esp_industry2 = 4     then 41
                when  esp_industry = 6 and esp_industry2 = 2     then 85 
                when  esp_industry = 6 and esp_industry2 = 1     then 96 
                when  esp_industry = 7 and esp_industry2 = 1     then 107
                when  esp_industry = 7 and esp_industry2 = 2     then 118
                when  esp_industry = 7 and esp_industry2 = 3     then 129
                when  esp_industry = 8 and esp_industry2 = 1     then 140
                when  esp_industry = 8 and esp_industry2 = 2     then 151
                when  esp_industry = 8 and esp_industry2 = 3     then 162
                when  esp_industry = 9 and esp_industry2 = 1     then 173
                when  esp_industry = 9 and esp_industry2 = 2     then 184
                when  esp_industry = 9 and esp_industry2 = 3     then 195
                when  esp_industry = 10 and esp_industry2 = 1    then 206
                when  esp_industry = 10 and esp_industry2 = 2    then 217
                when  esp_industry = 10 and esp_industry2 = 3    then 228
                when  esp_industry = 10 and esp_industry2 = 4    then 239
                when  esp_industry = 10 and esp_industry2 = 5    then 250
                when  esp_industry = 11 and esp_industry2 = 1    then 261
                when  esp_industry = 11 and esp_industry2 = 2    then 272
                when  esp_industry = 11 and esp_industry2 = 3    then 283
                when  esp_industry = 11 and esp_industry2 = 4    then 294
                when  esp_industry = 11 and esp_industry2 = 5    then 305
                when  esp_industry = 11 and esp_industry2 = 6    then 316
                when  esp_industry = 11 and esp_industry2 = 7    then 327
                when  esp_industry = 11 and esp_industry2 = 8    then 338
                when  esp_industry = 12 and esp_industry2 = 1    then 349
                when  esp_industry = 12 and esp_industry2 = 2    then 360
                when  esp_industry = 12 and esp_industry2 = 3    then 371
                when  esp_industry = 12 and esp_industry2 = 4    then 382
                when  esp_industry = 12 and esp_industry2 = 5    then 393
                when  esp_industry = 12 and esp_industry2 = 6    then 404
                when  esp_industry = 12 and esp_industry2 = 7    then 415
                when  esp_industry = 12 and esp_industry2 = 8    then 426
                when  esp_industry = 12 and esp_industry2 = 9    then 437
                when  esp_industry = 12 and esp_industry2 = 10   then 448
                else 999  end
   ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }   
    
    #集团产品总收入   
           set sql_buf "
    insert into $stat_enterprise_0060_f_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           case when  esp_industry = 5 and esp_industry2 = 1     then 9 
                when  esp_industry = 5 and esp_industry2 = 2     then 20
                when  esp_industry = 5 and esp_industry2 = 3     then 31
                when  esp_industry = 5 and esp_industry2 = 4     then 42
                when  esp_industry = 6 and esp_industry2 = 2     then 86 
                when  esp_industry = 6 and esp_industry2 = 1     then 97 
                when  esp_industry = 7 and esp_industry2 = 1     then 108
                when  esp_industry = 7 and esp_industry2 = 2     then 119
                when  esp_industry = 7 and esp_industry2 = 3     then 130
                when  esp_industry = 8 and esp_industry2 = 1     then 141
                when  esp_industry = 8 and esp_industry2 = 2     then 152
                when  esp_industry = 8 and esp_industry2 = 3     then 163
                when  esp_industry = 9 and esp_industry2 = 1     then 174
                when  esp_industry = 9 and esp_industry2 = 2     then 185
                when  esp_industry = 9 and esp_industry2 = 3     then 196
                when  esp_industry = 10 and esp_industry2 = 1    then 207
                when  esp_industry = 10 and esp_industry2 = 2    then 218
                when  esp_industry = 10 and esp_industry2 = 3    then 229
                when  esp_industry = 10 and esp_industry2 = 4    then 240
                when  esp_industry = 10 and esp_industry2 = 5    then 251
                when  esp_industry = 11 and esp_industry2 = 1    then 262
                when  esp_industry = 11 and esp_industry2 = 2    then 273
                when  esp_industry = 11 and esp_industry2 = 3    then 284
                when  esp_industry = 11 and esp_industry2 = 4    then 295
                when  esp_industry = 11 and esp_industry2 = 5    then 306
                when  esp_industry = 11 and esp_industry2 = 6    then 317
                when  esp_industry = 11 and esp_industry2 = 7    then 328
                when  esp_industry = 11 and esp_industry2 = 8    then 339
                when  esp_industry = 12 and esp_industry2 = 1    then 350
                when  esp_industry = 12 and esp_industry2 = 2    then 361
                when  esp_industry = 12 and esp_industry2 = 3    then 372
                when  esp_industry = 12 and esp_industry2 = 4    then 383
                when  esp_industry = 12 and esp_industry2 = 5    then 394
                when  esp_industry = 12 and esp_industry2 = 6    then 405
                when  esp_industry = 12 and esp_industry2 = 7    then 416
                when  esp_industry = 12 and esp_industry2 = 8    then 427
                when  esp_industry = 12 and esp_industry2 = 9    then 438
                when  esp_industry = 12 and esp_industry2 = 10   then 449
                else 999  end as s_index_id,
           sum(a.prod_total_income)
    from bass2.stat_enterprise_0060_f_tmp1 a,
         bass2.stat_enterprise_0060_f_tmp2 b
     where a.enterprise_id = b.enterprise_id
           and b.esp_industry in (5,6,7,8,9,10,11,12)
     group by a.city_id,
             case when  esp_industry = 5 and esp_industry2 = 1     then 9 
                when  esp_industry = 5 and esp_industry2 = 2     then 20
                when  esp_industry = 5 and esp_industry2 = 3     then 31
                when  esp_industry = 5 and esp_industry2 = 4     then 42
                when  esp_industry = 6 and esp_industry2 = 2     then 86 
                when  esp_industry = 6 and esp_industry2 = 1     then 97 
                when  esp_industry = 7 and esp_industry2 = 1     then 108
                when  esp_industry = 7 and esp_industry2 = 2     then 119
                when  esp_industry = 7 and esp_industry2 = 3     then 130
                when  esp_industry = 8 and esp_industry2 = 1     then 141
                when  esp_industry = 8 and esp_industry2 = 2     then 152
                when  esp_industry = 8 and esp_industry2 = 3     then 163
                when  esp_industry = 9 and esp_industry2 = 1     then 174
                when  esp_industry = 9 and esp_industry2 = 2     then 185
                when  esp_industry = 9 and esp_industry2 = 3     then 196
                when  esp_industry = 10 and esp_industry2 = 1    then 207
                when  esp_industry = 10 and esp_industry2 = 2    then 218
                when  esp_industry = 10 and esp_industry2 = 3    then 229
                when  esp_industry = 10 and esp_industry2 = 4    then 240
                when  esp_industry = 10 and esp_industry2 = 5    then 251
                when  esp_industry = 11 and esp_industry2 = 1    then 262
                when  esp_industry = 11 and esp_industry2 = 2    then 273
                when  esp_industry = 11 and esp_industry2 = 3    then 284
                when  esp_industry = 11 and esp_industry2 = 4    then 295
                when  esp_industry = 11 and esp_industry2 = 5    then 306
                when  esp_industry = 11 and esp_industry2 = 6    then 317
                when  esp_industry = 11 and esp_industry2 = 7    then 328
                when  esp_industry = 11 and esp_industry2 = 8    then 339
                when  esp_industry = 12 and esp_industry2 = 1    then 350
                when  esp_industry = 12 and esp_industry2 = 2    then 361
                when  esp_industry = 12 and esp_industry2 = 3    then 372
                when  esp_industry = 12 and esp_industry2 = 4    then 383
                when  esp_industry = 12 and esp_industry2 = 5    then 394
                when  esp_industry = 12 and esp_industry2 = 6    then 405
                when  esp_industry = 12 and esp_industry2 = 7    then 416
                when  esp_industry = 12 and esp_industry2 = 8    then 427
                when  esp_industry = 12 and esp_industry2 = 9    then 438
                when  esp_industry = 12 and esp_industry2 = 10   then 449
                else 999  end
   ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }     
    
   #集团产品统一付费收入 
           set sql_buf "
    insert into $stat_enterprise_0060_f_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           case when  esp_industry = 5 and esp_industry2 = 1     then 10
                when  esp_industry = 5 and esp_industry2 = 2     then 21
                when  esp_industry = 5 and esp_industry2 = 3     then 32
                when  esp_industry = 5 and esp_industry2 = 4     then 43
                when  esp_industry = 6 and esp_industry2 = 2     then 87 
                when  esp_industry = 6 and esp_industry2 = 1     then 98 
                when  esp_industry = 7 and esp_industry2 = 1     then 109
                when  esp_industry = 7 and esp_industry2 = 2     then 120
                when  esp_industry = 7 and esp_industry2 = 3     then 131
                when  esp_industry = 8 and esp_industry2 = 1     then 142
                when  esp_industry = 8 and esp_industry2 = 2     then 153
                when  esp_industry = 8 and esp_industry2 = 3     then 164
                when  esp_industry = 9 and esp_industry2 = 1     then 175
                when  esp_industry = 9 and esp_industry2 = 2     then 186
                when  esp_industry = 9 and esp_industry2 = 3     then 197
                when  esp_industry = 10 and esp_industry2 = 1    then 208
                when  esp_industry = 10 and esp_industry2 = 2    then 219
                when  esp_industry = 10 and esp_industry2 = 3    then 230
                when  esp_industry = 10 and esp_industry2 = 4    then 241
                when  esp_industry = 10 and esp_industry2 = 5    then 252
                when  esp_industry = 11 and esp_industry2 = 1    then 263
                when  esp_industry = 11 and esp_industry2 = 2    then 274
                when  esp_industry = 11 and esp_industry2 = 3    then 285
                when  esp_industry = 11 and esp_industry2 = 4    then 296
                when  esp_industry = 11 and esp_industry2 = 5    then 307
                when  esp_industry = 11 and esp_industry2 = 6    then 318
                when  esp_industry = 11 and esp_industry2 = 7    then 329
                when  esp_industry = 11 and esp_industry2 = 8    then 340
                when  esp_industry = 12 and esp_industry2 = 1    then 351
                when  esp_industry = 12 and esp_industry2 = 2    then 362
                when  esp_industry = 12 and esp_industry2 = 3    then 373
                when  esp_industry = 12 and esp_industry2 = 4    then 384
                when  esp_industry = 12 and esp_industry2 = 5    then 395
                when  esp_industry = 12 and esp_industry2 = 6    then 406
                when  esp_industry = 12 and esp_industry2 = 7    then 417
                when  esp_industry = 12 and esp_industry2 = 8    then 428
                when  esp_industry = 12 and esp_industry2 = 9    then 439
                when  esp_industry = 12 and esp_industry2 = 10   then 450
                else 999  end as s_index_id,
           sum(a.ent_prod_income)
    from bass2.stat_enterprise_0060_f_tmp1 a,
         bass2.stat_enterprise_0060_f_tmp2 b
     where a.enterprise_id = b.enterprise_id
           and b.esp_industry in (5,6,7,8,9,10,11,12)
     group by a.city_id,
             case when  esp_industry = 5 and esp_industry2 = 1     then 10
                when  esp_industry = 5 and esp_industry2 = 2     then 21
                when  esp_industry = 5 and esp_industry2 = 3     then 32
                when  esp_industry = 5 and esp_industry2 = 4     then 43
                when  esp_industry = 6 and esp_industry2 = 2     then 87 
                when  esp_industry = 6 and esp_industry2 = 1     then 98 
                when  esp_industry = 7 and esp_industry2 = 1     then 109
                when  esp_industry = 7 and esp_industry2 = 2     then 120
                when  esp_industry = 7 and esp_industry2 = 3     then 131
                when  esp_industry = 8 and esp_industry2 = 1     then 142
                when  esp_industry = 8 and esp_industry2 = 2     then 153
                when  esp_industry = 8 and esp_industry2 = 3     then 164
                when  esp_industry = 9 and esp_industry2 = 1     then 175
                when  esp_industry = 9 and esp_industry2 = 2     then 186
                when  esp_industry = 9 and esp_industry2 = 3     then 197
                when  esp_industry = 10 and esp_industry2 = 1    then 208
                when  esp_industry = 10 and esp_industry2 = 2    then 219
                when  esp_industry = 10 and esp_industry2 = 3    then 230
                when  esp_industry = 10 and esp_industry2 = 4    then 241
                when  esp_industry = 10 and esp_industry2 = 5    then 252
                when  esp_industry = 11 and esp_industry2 = 1    then 263
                when  esp_industry = 11 and esp_industry2 = 2    then 274
                when  esp_industry = 11 and esp_industry2 = 3    then 285
                when  esp_industry = 11 and esp_industry2 = 4    then 296
                when  esp_industry = 11 and esp_industry2 = 5    then 307
                when  esp_industry = 11 and esp_industry2 = 6    then 318
                when  esp_industry = 11 and esp_industry2 = 7    then 329
                when  esp_industry = 11 and esp_industry2 = 8    then 340
                when  esp_industry = 12 and esp_industry2 = 1    then 351
                when  esp_industry = 12 and esp_industry2 = 2    then 362
                when  esp_industry = 12 and esp_industry2 = 3    then 373
                when  esp_industry = 12 and esp_industry2 = 4    then 384
                when  esp_industry = 12 and esp_industry2 = 5    then 395
                when  esp_industry = 12 and esp_industry2 = 6    then 406
                when  esp_industry = 12 and esp_industry2 = 7    then 417
                when  esp_industry = 12 and esp_industry2 = 8    then 428
                when  esp_industry = 12 and esp_industry2 = 9    then 439
                when  esp_industry = 12 and esp_industry2 = 10   then 450
                else 999  end
   ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }   
    
    #集团信息化收入
             set sql_buf "
    insert into $stat_enterprise_0060_f_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select a.city_id,
           case when  esp_industry = 5 and esp_industry2 = 1     then 11
                when  esp_industry = 5 and esp_industry2 = 2     then 22
                when  esp_industry = 5 and esp_industry2 = 3     then 33
                when  esp_industry = 5 and esp_industry2 = 4     then 44
                when  esp_industry = 6 and esp_industry2 = 2     then 88 
                when  esp_industry = 6 and esp_industry2 = 1     then 99 
                when  esp_industry = 7 and esp_industry2 = 1     then 110
                when  esp_industry = 7 and esp_industry2 = 2     then 121
                when  esp_industry = 7 and esp_industry2 = 3     then 132
                when  esp_industry = 8 and esp_industry2 = 1     then 143
                when  esp_industry = 8 and esp_industry2 = 2     then 154
                when  esp_industry = 8 and esp_industry2 = 3     then 165
                when  esp_industry = 9 and esp_industry2 = 1     then 176
                when  esp_industry = 9 and esp_industry2 = 2     then 187
                when  esp_industry = 9 and esp_industry2 = 3     then 198
                when  esp_industry = 10 and esp_industry2 = 1    then 209
                when  esp_industry = 10 and esp_industry2 = 2    then 220
                when  esp_industry = 10 and esp_industry2 = 3    then 231
                when  esp_industry = 10 and esp_industry2 = 4    then 242
                when  esp_industry = 10 and esp_industry2 = 5    then 253
                when  esp_industry = 11 and esp_industry2 = 1    then 264
                when  esp_industry = 11 and esp_industry2 = 2    then 275
                when  esp_industry = 11 and esp_industry2 = 3    then 286
                when  esp_industry = 11 and esp_industry2 = 4    then 297
                when  esp_industry = 11 and esp_industry2 = 5    then 308
                when  esp_industry = 11 and esp_industry2 = 6    then 319
                when  esp_industry = 11 and esp_industry2 = 7    then 330
                when  esp_industry = 11 and esp_industry2 = 8    then 341
                when  esp_industry = 12 and esp_industry2 = 1    then 352
                when  esp_industry = 12 and esp_industry2 = 2    then 363
                when  esp_industry = 12 and esp_industry2 = 3    then 374
                when  esp_industry = 12 and esp_industry2 = 4    then 385
                when  esp_industry = 12 and esp_industry2 = 5    then 396
                when  esp_industry = 12 and esp_industry2 = 6    then 407
                when  esp_industry = 12 and esp_industry2 = 7    then 418
                when  esp_industry = 12 and esp_industry2 = 8    then 429
                when  esp_industry = 12 and esp_industry2 = 9    then 440
                when  esp_industry = 12 and esp_industry2 = 10   then 451
                else 999  end as s_index_id,
           sum(a.info_fee)
    from bass2.stat_enterprise_0060_f_tmp1 a,
         bass2.stat_enterprise_0060_f_tmp2 b
     where a.enterprise_id = b.enterprise_id
           and b.esp_industry in (5,6,7,8,9,10,11,12)
     group by a.city_id,
             case when  esp_industry = 5 and esp_industry2 = 1     then 11
                when  esp_industry = 5 and esp_industry2 = 2     then 22
                when  esp_industry = 5 and esp_industry2 = 3     then 33
                when  esp_industry = 5 and esp_industry2 = 4     then 44
                when  esp_industry = 6 and esp_industry2 = 2     then 88 
                when  esp_industry = 6 and esp_industry2 = 1     then 99 
                when  esp_industry = 7 and esp_industry2 = 1     then 110
                when  esp_industry = 7 and esp_industry2 = 2     then 121
                when  esp_industry = 7 and esp_industry2 = 3     then 132
                when  esp_industry = 8 and esp_industry2 = 1     then 143
                when  esp_industry = 8 and esp_industry2 = 2     then 154
                when  esp_industry = 8 and esp_industry2 = 3     then 165
                when  esp_industry = 9 and esp_industry2 = 1     then 176
                when  esp_industry = 9 and esp_industry2 = 2     then 187
                when  esp_industry = 9 and esp_industry2 = 3     then 198
                when  esp_industry = 10 and esp_industry2 = 1    then 209
                when  esp_industry = 10 and esp_industry2 = 2    then 220
                when  esp_industry = 10 and esp_industry2 = 3    then 231
                when  esp_industry = 10 and esp_industry2 = 4    then 242
                when  esp_industry = 10 and esp_industry2 = 5    then 253
                when  esp_industry = 11 and esp_industry2 = 1    then 264
                when  esp_industry = 11 and esp_industry2 = 2    then 275
                when  esp_industry = 11 and esp_industry2 = 3    then 286
                when  esp_industry = 11 and esp_industry2 = 4    then 297
                when  esp_industry = 11 and esp_industry2 = 5    then 308
                when  esp_industry = 11 and esp_industry2 = 6    then 319
                when  esp_industry = 11 and esp_industry2 = 7    then 330
                when  esp_industry = 11 and esp_industry2 = 8    then 341
                when  esp_industry = 12 and esp_industry2 = 1    then 352
                when  esp_industry = 12 and esp_industry2 = 2    then 363
                when  esp_industry = 12 and esp_industry2 = 3    then 374
                when  esp_industry = 12 and esp_industry2 = 4    then 385
                when  esp_industry = 12 and esp_industry2 = 5    then 396
                when  esp_industry = 12 and esp_industry2 = 6    then 407
                when  esp_industry = 12 and esp_industry2 = 7    then 418
                when  esp_industry = 12 and esp_industry2 = 8    then 429
                when  esp_industry = 12 and esp_industry2 = 9    then 440
                when  esp_industry = 12 and esp_industry2 = 10   then 451
                else 999  end
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
    insert into $stat_enterprise_0060_f_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select '890',
           s_index_id,
           sum(result)
      from $stat_enterprise_0060_f_tmp4
      where s_index_id not in (999)
     group by
           s_index_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 3190
        puts "errmsg:$errmsg"
        return -1
    }

    #Step4.清除结果表历史数据
    set sql_buf "delete from $stat_enterprise_0060_f where op_time = $year$month;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 4010
        puts "errmsg:$errmsg"
        return -1
    }

    #Step5.生成结果表数据
    set sql_buf "
    insert into $stat_enterprise_0060_f
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
                   $ent_index_s_6 b
           ) a
      left join
           $stat_enterprise_0060_f_tmp4 b on a.city_id = b.city_id and a.s_index_id = b.s_index_id
           and b.s_index_id not in (999)
           ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 5010
        puts "errmsg:$errmsg"
        return -1
    }

    exec db2 "connect to bassdb user bass2 using bass2"
    exec db2 "runstats on table $stat_enterprise_0060_f with distribution and indexes all"
    exec db2 "terminate"

    #Step6.清除临时表
    set sql_buf "drop table $stat_enterprise_0060_f_tmp1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_f_tmp2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_f_tmp3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_f_tmp4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_f_tmp5;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    return 0
}