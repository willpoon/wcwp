#======================================================================================
#��Ȩ������Copyright (c) 2011,AsiaInfo.Report.System
#��������: stat_enterprise_0060_d.tcl
#������: 2012�꼯�ſͻ��ۺ�ͳ�Ʊ���-4����Ԫ��Ʒ
#����Ŀ��: ��
#����ָ��: ��ά��
#����ά��: ȫ�� ����
#��������: ��
#����ʾ��: crt_basetab.sh stat_enterprise_0060_d.tcl 2011-11-01
#����ʱ��: 2011-12-12
#�� �� ��: Asiainfo-Linkage 
#��������:
#�޸���ʷ: 1��2011-7-12 �޸ļ��Ŷ����ھ���ȡ��ԭrec_status = 1��������Ч��ʧЧ����
#          2��2011-7-22 ��������������COM_OTHER_XZ_LS_20110708_49888 ���������޸����ݼ��������� �����̻����Ӵ�ͳ�ƿھ�.doc
#          3��2011-11-16 ���������칤��(COM_OTHER_XZ_LS_20111110_65259)�޸����������̻�����
#          4��2012-7-5 �����ܳ�ة����DEM_BASS_XZ_20120626_114507609�޸������̻��ھ���ԭ�ھ���ע��
#=======================================================================================
proc deal {p_optime p_timestamp} {
    global conn
    global handle

    if [catch {set handle [aidb_open $conn]} errmsg] {
        #trace_sql $errmsg 1000
        return -1
    }

    if {[stat_enterprise_0060_d $p_optime] != 0} {
        aidb_roll $conn
        aidb_close $handle
        return -1
    }

    aidb_commit $conn
    aidb_close $handle

    return 0
}

proc stat_enterprise_0060_d {p_optime} {
    global conn
    global handle

    source stat_insert_index.tcl
    source report.cfg

    #���ڴ���
    set    date_optime      [ai_to_date $p_optime]
    scan   $p_optime        "%04s-%02s-%02s" year month day
    set    day_me_optime    [clock format [ clock scan "${year}${month}${day} + 1month - 1day" ] -format "%Y-%m-%d"]
    set    date_me_optime   [ai_to_date $day_me_optime]
    scan   $day_me_optime   "%04s-%02s-%02s" meyear memonth meday
    set    date_nm_optime   [ai_to_date [clock format [ clock scan "${year}${month}${day} + 1month" ] -format "%Y-%m-%d"]]

    #�����ַ���
    set test_enterprise_id  "'89100000000682','89100000000659','89100000000656','89100000000651'"

    #Դ��
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
    #set dwd_group_order_featur_yyyymmdd "bass2.dwd_group_order_featur_20110731"
    set dw_td_check_user_yyyymm         "bass2.dw_td_check_user_three_$year$month"
    set dw_product_ins_off_ins_prod_yyyymm "bass2.dw_product_ins_off_ins_prod_$year$month"
    set dw_product_ins_off_ins_prod_ds     "bass2.dw_product_ins_off_ins_prod_ds"

    #Ŀ���
    set stat_enterprise_0060_d          "bass2.stat_enterprise_0060_d"

    #ά��
    set dim_pub_city                    "bassweb.dim_pub_city"
    set dim_ent_unipay_item             "bass2.dim_ent_unipay_item"
    set ent_index_s_3                   "bass2.ent_index_s_3"

    #��ʱ��
    set stat_enterprise_0060_d_tmp1     "bass2.stat_enterprise_0060_d_tmp1"
    set stat_enterprise_0060_d_tmp2     "bass2.stat_enterprise_0060_d_tmp2"
    set stat_enterprise_0060_d_tmp3     "bass2.stat_enterprise_0060_d_tmp3"
    set stat_enterprise_0060_d_tmp4     "bass2.stat_enterprise_0060_d_tmp4"
    set stat_enterprise_0060_d_tmp5     "bass2.stat_enterprise_0060_d_tmp5"
    set stat_enterprise_0060_d_tmp6     "bass2.stat_enterprise_0060_d_tmp6"
    set stat_enterprise_0060_d_tmp7     "bass2.stat_enterprise_0060_d_tmp7"

    #Step1.���������ʱ��
    set sql_buf "drop table $stat_enterprise_0060_d_tmp1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_d_tmp2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_d_tmp3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_d_tmp4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_d_tmp5;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_d_tmp6;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_d_tmp7;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    set sql_buf "
    create table $stat_enterprise_0060_d_tmp1
    (
           enterprise_id    varchar(20),
           enterprise_name  varchar(200),
           city_id          varchar(8),
           service_id       varchar(20),
           order_mark       smallint,
           order_user_count integer,
           order_mem_count  integer,
           pay_user_count   integer,
           pay_mem_count    integer,
           total_income     decimal(9,2),
           unipay_income    decimal(9,2)
    )
    in tbs_report index in tbs_index partitioning key (enterprise_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        #trace_sql $errmsg 1010
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_enterprise_0060_d_tmp2
    (
           enterprise_id    varchar(20),
           user_id          varchar(20),
           service_id       varchar(20),
           ent_mark         smallint
    )
    in tbs_report index in tbs_index partitioning key (user_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        #trace_sql $errmsg 1020
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_enterprise_0060_d_tmp3
    (
           enterprise_id    varchar(20),
           user_id          varchar(20)
    )
    in tbs_report index in tbs_index partitioning key (user_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        #trace_sql $errmsg 1030
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_enterprise_0060_d_tmp4
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
    create table $stat_enterprise_0060_d_tmp5
    (
           user_id          varchar(20),
           acct_id          varchar(20),
           enterprise_id    varchar(20),
           service_id       varchar(20),
           ent_mark         smallint,
           fact_fee         decimal(12,2)
    )
    in tbs_report index in tbs_index partitioning key (user_id) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        #trace_sql $errmsg 1050
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_enterprise_0060_d_tmp6
    (
           enterprise_id            varchar(20),
           city_id                  varchar(8),
           product_no               varchar(15),
           call_counts              integer,
           local_call_counts        integer,
           toll_call_counts         integer,
           call_duration_m          integer,
           local_call_duration_m    integer,
           toll_call_duration_m     integer,
           call_fee                 decimal(12,2),
           sms_fee                  decimal(12,2),
           total_fee                decimal(12,2)
    )
    in tbs_ods_other index in tbs_index partitioning key (product_no) using hashing not logged initially;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        #trace_sql $errmsg 1060
        puts "errmsg:$errmsg"
        return -1
    }
    set sql_buf "
    create table $stat_enterprise_0060_d_tmp7
    (
           enterprise_id            varchar(20),
           city_id                  varchar(8),
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

    #Step2.������ʱ������
    ##�����������ų�Ա
    set sql_buf "
    insert into $stat_enterprise_0060_d_tmp3
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
       and b.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test = 1)
       and c.userstatus_id in (1,2,3,6,8)
       and c.usertype_id in (1,2,9);"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2010
        puts "errmsg:$errmsg"
        return -1
    }

    ##�������Ų�Ʒ��ʱ��
    ####�Զ�������ר��ϵ�в�Ʒ�ļ��ţ�service_id�ֲ�Ϊ912001��������ר�ߣ���912002������ר�ߣ�������ͳ��
    set sql_buf "
    insert into $stat_enterprise_0060_d_tmp2
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
                   $stat_enterprise_0060_d_tmp3 c
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

    ##VPMN����
    set sql_buf "
    insert into $stat_enterprise_0060_d_tmp2
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
           $stat_enterprise_0060_d_tmp3 b
        on a.enterprise_id = b.enterprise_id
       and a.user_id = b.user_id,
       $dw_enterprise_msg_yyyymm c
       where a.enterprise_id = c.enterprise_id
           and c.ent_status_id = 0
           and c.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test = 1) 
       ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }
    
    ##JTDH�̺Ŷ���
     set sql_buf "
    insert into $stat_enterprise_0060_d_tmp2
    (
           enterprise_id,
           user_id,
           service_id,
           ent_mark
    )
    select a.enterprise_id,
           a.user_id,
           'JTDH',
           case when b.user_id is not null then 1 else 0 end
      from (
            select b.enterprise_id,
                   c.user_id
              from $dw_vpmn_enterprise_yyyymm a,
                   $dw_vpmn_member_yyyymm c,
                   $dw_enterprise_vpmn_rela_yyyymm b
             where a.vpmn_id = b.vpmn_id
               and a.scpid = 1
               and a.sts = 0
               and a.valid_date < $date_nm_optime
               and a.expire_date >= $date_nm_optime
               and c.isdn is not null
               and a.vpmn_id = c.vpmn_id
               and c.valid_date < $date_nm_optime
               and c.expire_date >= $date_nm_optime
           ) a
      left join
           $stat_enterprise_0060_d_tmp3 b
        on a.enterprise_id = b.enterprise_id
       and a.user_id = b.user_id,
       $dw_enterprise_msg_yyyymm c
       where a.enterprise_id = c.enterprise_id
           and c.ent_status_id = 0
           and c.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test = 1) 
       ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }

    ##�м��Ų�Ʒ��Ŀ���û�
    set sql_buf "
    insert into $stat_enterprise_0060_d_tmp5
    (
           user_id,
           acct_id,
           enterprise_id,
           service_id,
           ent_mark,
           fact_fee
    )
    select a.user_id,
           a.acct_id,
           a.enterprise_id,
           a.service_id,
           a.ent_mark,
           value(b.fact_fee,0)
      from (
            select distinct
                   a.enterprise_id,
                   b.user_id,
                   b.acct_id,
                   case when a.service_id = '912'
                        then case when a.prod_id = '91201001' then '912001'
                                  when a.prod_id = '91201002' then '912002'
                                  else '912'
                             end
                        else a.service_id
                   end as service_id,
                   value(b.ent_mark,0) as ent_mark
              from $dw_enterprise_sub_yyyymm a
              left join
                   (
                    select b.enterprise_id,
                           b.order_id,
                           b.user_id,
                           b.acct_id,
                           case when c.user_id is not null then 1 else 0 end as ent_mark
                      from $dw_enterprise_membersub_yyyymm b
                      left join
                           $stat_enterprise_0060_d_tmp3 c
                        on b.enterprise_id = c.enterprise_id
                       and b.user_id = c.user_id
                     where b.valid_date <= $date_me_optime
                       and b.expire_date >= $date_optime
                   ) b
                on a.enterprise_id = b.enterprise_id
               and a.order_id = b.order_id
             where a.valid_date <= $date_me_optime
               and a.expire_date >= $date_optime
           ) a
      left join
           (
            select a.user_id,
                   a.acct_id,
                   char(b.service_id) as service_id,
                   sum(a.fact_fee) as fact_fee
              from $dw_acct_shoulditem_yyyymm a,
                   $dim_ent_unipay_item b
             where a.item_id = b.item_id
             group by
                   a.user_id,
                   a.acct_id,
                   b.service_id
           ) b
        on a.user_id = b.user_id
       and a.acct_id = b.acct_id
       and a.service_id = b.service_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2020
        puts "errmsg:$errmsg"
        return -1
    }

    ##���Ŷ�����Ʒ����ʹ�������ʱ��
    ####order_user_count �����û����������û��ͼ��ų�Ա��
    ####order_mem_count ���ų�Ա������
    ####pay_user_count �����û����������û��ͼ��ų�Ա��
    ####pay_mem_count ���ѳ�Ա��
    set sql_buf "
    insert into $stat_enterprise_0060_d_tmp1
    (
           enterprise_id,
           enterprise_name,
           city_id,
           service_id,
           order_mark,
           order_user_count,
           order_mem_count,
           pay_user_count,
           pay_mem_count,
           total_income,
           unipay_income
    )
    select a.enterprise_id,
           a.enterprise_name,
           a.city_id,
           a.service_id,
           1,
           a.order_user_count,
           a.order_mem_count,
           case when a.service_id not in ('VPMN','JTDH') then value(c.pay_user_count,0)
                when a.service_id = 'JTDH' then value(e.pay_user_count,0)
                else value(d.pay_user_count,0)
           end,
           case when a.service_id not in ('VPMN','JTDH') then value(c.pay_mem_count,0)
                when a.service_id = 'JTDH' then value(e.pay_mem_count,0)
                else value(d.pay_mem_count,0)
           end,
           case when a.service_id not in ('VPMN','JTDH') then value(b.total_fee,0)
                when a.service_id = 'JTDH' then value(e.total_fee,0)
                else value(d.total_fee,0)
           end,
           case when a.service_id not in ('VPMN','JTDH') then value(b.unipay_fee,0)
                when a.service_id = 'JTDH' then value(e.unipay_fee,0)
                else value(d.unipay_fee,0)
           end
      from (
            select a.enterprise_id,
                   b.enterprise_name,
                   case when b.level_def_mode = 1 then '888'
                        else b.ent_city_id
                   end as city_id,
                   a.service_id,
                   1,
                   count(distinct a.user_id) as order_user_count,
                   count(distinct case when a.ent_mark = 1 then a.user_id end) as order_mem_count
              from $stat_enterprise_0060_d_tmp2 a,
                   $dw_enterprise_msg_yyyymm b
             where a.enterprise_id = b.enterprise_id
                and b.ent_status_id = 0
                and b.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test = 1)
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
            select enterprise_id,
                   service_id,
                   count(distinct user_id) as pay_user_count,
                   count(distinct case when ent_mark = 1 then user_id end) as pay_mem_count
              from $stat_enterprise_0060_d_tmp5
             where fact_fee > 0
             group by
                   enterprise_id,
                   service_id
           ) c
        on a.enterprise_id = c.enterprise_id
       and a.service_id = c.service_id
      left join
           (
            select b.enterprise_id,
                   b.service_id,
                   count(distinct case when a.fact_fee > 0 then b.user_id end) as pay_user_count,
                   count(distinct case when a.fact_fee > 0 and b.ent_mark = 1 then b.user_id end) as pay_mem_count,
                   sum(a.fact_fee) as total_fee,
                   sum(case when c.acct_id is not null then a.fact_fee else 0 end) as unipay_fee
              from $dw_acct_shoulditem_yyyymm a
              left join $dw_enterprise_account_yyyymm c
                on a.acct_id = c.acct_id
               and c.rec_status = 1,
                   $stat_enterprise_0060_d_tmp2 b
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
                    select b.enterprise_id,
                   b.service_id,
                   count(distinct case when a.fact_fee > 0 then b.user_id end) as pay_user_count,
                   count(distinct case when a.fact_fee > 0 and b.ent_mark = 1 then b.user_id end) as pay_mem_count,
                   sum(a.fact_fee) as total_fee,
                   sum(case when c.acct_id is not null then a.fact_fee else 0 end) as unipay_fee
              from $dw_acct_shoulditem_yyyymm a
              left join $dw_enterprise_account_yyyymm c
                on a.acct_id = c.acct_id
               and c.rec_status = 1,
                   $stat_enterprise_0060_d_tmp2 b
             where a.user_id = b.user_id
               and b.service_id = 'JTDH'
               and a.item_id in (80000704)
             group by
                   b.enterprise_id,
                   b.service_id
         ) e on a.enterprise_id = e.enterprise_id
       and a.service_id = e.service_id  
         ;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2030
        puts "errmsg:$errmsg"
        return -1
    }

    ##�����̻���ʱ��
    #�����ܳ�ة����DEM_BASS_XZ_20120626_114507609�޸Ŀھ���ԭ�ھ���ע��
#    set sql_buf "
#    insert into $stat_enterprise_0060_d_tmp7
#    (
#           enterprise_id,
#           city_id,
#           user_id,
#           fact_fee,
#           call_duration_m
#    )
#    select c.enterprise_id,
#           c.ent_city_id,
#           b.user_id,
#           b.fact_fee,
#           b.call_duration_m
#      from $dw_td_check_user_yyyymm a,
#           $dw_product_yyyymm  b,
#           (
#            select a.user_id,
#                   b.enterprise_id,
#                   b.ent_city_id
#              from $dw_enterprise_member_mid_yyyymm a,
#                   $dw_enterprise_msg_yyyymm b
#             where a.enterprise_id = b.enterprise_id
#               and a.test_mark = 0
#               and a.dummy_mark = 0
#               and b.enterprise_name in ('���ز���������̩��Ƶ�','���������𴨱���')
#           ) c
#     where a.product_no = b.product_no
#       and b.user_id = c.user_id
#       and b.userstatus_id in (1,2,3,6,8)
#       and b.test_mark <> 1
#       and b.usertype_id in (1,2,9)
#       and a.td_type_id = '4';"
   
   
    set sql_buf "
    insert into $stat_enterprise_0060_d_tmp7
    (
           enterprise_id,
           city_id,
           user_id,
           fact_fee,
           call_duration_m
    )    
    select b.enterprise_id
          ,b.ent_city_id
          ,c.user_id
          ,value(c.fact_fee,0)
          ,value(c.call_duration_m,0)
    from $dw_enterprise_msg_yyyymm b,
         $dw_product_ins_off_ins_prod_yyyymm a 
   left join 
        (select user_id
               ,fact_fee
               ,call_duration_m
        from  $dw_product_yyyymm 
        where userstatus_id in (1,2,3,6,8)
            and test_mark <> 1 and free_mark<>1
            and usertype_id in (1,2,9)
        ) c  on a.product_instance_id=c.user_id 
    where a.cust_party_role_id=b.enterprise_id
      and a.offer_id in (112091501003)
      and a.is_main_offer=0
      and value(b.is_test,0)<>1
      and date(a.expire_date)>$date_nm_optime
      and date(a.valid_date)<$date_nm_optime;"
  
       
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        trace_sql $errmsg 2040
        puts "errmsg:$errmsg"
        return -1
    }
     
    #Step3.�ֲ�Ʒͳ�Ƹ�ָ��
    ##��ͨ�����Ŷ�����ϵͳ�ƵĲ�Ʒ
    ####�������ſͻ���
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   ( 
          city_id,
          s_index_id,
          result
   ) 
   select a.city_id,
          case when a.service_id = 'VPMN' then 1
               when a.service_id = 'JTDH' then 2
               when a.service_id = '917' then 49
               when a.service_id = '931' then 57
               when a.service_id = '933' then 100
               when a.service_id = '910' then 107
               when a.service_id = '911' then 124
               when a.service_id in ('142','906') then 136
               when a.service_id = '904' then 146
               when a.service_id = '943' then 151
               when a.service_id = '936' then 165
               when a.service_id = '953' then 171
               when a.service_id = '912001' then 180
               when a.service_id = '912002' then 195
               when a.service_id = '934' then 230
               when a.service_id in ('926','939002') then 247
               when a.service_id in ('SPEC|BLACKBERRY','717') then 257
               when a.service_id in ('924') then 288
               when a.service_id in ('925') then 307
               when a.service_id in ('903','939001') then 327
               when a.service_id in ('10105002') then 335
               when a.service_id in ('945') then 341
               when a.service_id in ('942') then 456
               when a.service_id in ('949') then 479
               when a.service_id in ('944') then 480
               when a.service_id in ('946') then 481
               when a.service_id in ('947') then 496
               when a.service_id not in ('142','717','903','904','906','910','911','912','917','924',
                                         '925','926','931','933','934','936','942','942','943','944',
                                         '945','946','947','949','951','953','912001','912002',
                                         '939001','939002','10105002','SPEC|BLACKBERRY')
               then 502
          end,
          count(distinct a.enterprise_id)
     from $stat_enterprise_0060_d_tmp1 a
    group by
          a.city_id,
          case when a.service_id = 'VPMN' then 1
               when a.service_id = 'JTDH' then 2
               when a.service_id = '917' then 49
               when a.service_id = '931' then 57
               when a.service_id = '933' then 100
               when a.service_id = '910' then 107
               when a.service_id = '911' then 124
               when a.service_id in ('142','906') then 136
               when a.service_id = '904' then 146
               when a.service_id = '943' then 151
               when a.service_id = '936' then 165
               when a.service_id = '953' then 171
               when a.service_id = '912001' then 180
               when a.service_id = '912002' then 195
               when a.service_id = '934' then 230
               when a.service_id in ('926','939002') then 247
               when a.service_id in ('SPEC|BLACKBERRY','717') then 257
               when a.service_id in ('924') then 288
               when a.service_id in ('925') then 307
               when a.service_id in ('903','939001') then 327
               when a.service_id in ('10105002') then 335
               when a.service_id in ('945') then 341
               when a.service_id in ('942') then 456
               when a.service_id in ('949') then 479
               when a.service_id in ('944') then 480
               when a.service_id in ('946') then 481
               when a.service_id in ('947') then 496
               when a.service_id not in ('142','717','903','904','906','910','911','912','917','924',
                                         '925','926','931','933','934','936','942','942','943','944',
                                         '945','946','947','949','951','953','912001','912002',
                                         '939001','939002','10105002','SPEC|BLACKBERRY')
               then 502
          end;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3010
       puts "errmsg:$errmsg"
       return -1
   }

   ####��Ҫ�ϲ�/�ֲ�ҵ��Ķ������ſͻ���
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          case when a.service_id = '911' then
                    case when a.enterprise_name like '%�׶�԰' then 125
                         when a.enterprise_name like '%С' or a.enterprise_name like '%Сѧ' then 126
                         else 127
                    end
               when a.service_id in ('924','925') then 275
               when a.service_id in ('949') then 401
               when a.service_id in ('946','942') then 402
               when a.service_id in ('944') then 403
               when a.service_id in ('947') then 407
          end,
          count(distinct a.enterprise_id)
     from $stat_enterprise_0060_d_tmp1 a
    where a.service_id in ('911','924','925','942','944','946','947','949')
    group by
          a.city_id,
          case when a.service_id = '911' then
                    case when a.enterprise_name like '%�׶�԰' then 125
                         when a.enterprise_name like '%С' or a.enterprise_name like '%Сѧ' then 126
                         else 127
                    end
               when a.service_id in ('924','925') then 275
               when a.service_id in ('949') then 401
               when a.service_id in ('946','942') then 402
               when a.service_id in ('944') then 403
               when a.service_id in ('947') then 407
          end;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3020
       puts "errmsg:$errmsg"
       return -1
   }

   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          478,
          count(distinct a.enterprise_id)
     from $stat_enterprise_0060_d_tmp1 a
    where a.service_id in ('944','946','949')
    group by
          a.city_id,
          434;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3020
       puts "errmsg:$errmsg"
       return -1
   }

   ######�������Ķ������ſͻ���
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          400,
          count(distinct a.enterprise_id)
     from $stat_enterprise_0060_d_tmp1 a
    where a.service_id in ('942','944','946','947','949')
    group by
          a.city_id;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3030
       puts "errmsg:$errmsg"
       return -1
   }

   ####���Ѽ��ſͻ���
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          case when a.service_id = 'VPMN' then 5
               when a.service_id = 'JTDH' then 6
               when a.service_id = '917' then 51
               when a.service_id = '931' then 59
               when a.service_id = '933' then 101
               when a.service_id = '910' then 109
               when a.service_id in ('142','906') then 137
               when a.service_id = '904' then 147
               when a.service_id = '943' then 153
               when a.service_id = '936' then 166
               when a.service_id = '953' then 172
               when a.service_id = '912001' then 181
               when a.service_id = '912002' then 196
               when a.service_id = '934' then 232
               when a.service_id in ('926','939002') then 249
               when a.service_id in ('SPEC|BLACKBERRY','717') then 261
               when a.service_id in ('924') then 294
               when a.service_id in ('925') then 312
               when a.service_id in ('903','939001') then 328
               when a.service_id in ('10105002') then 337
               when a.service_id in ('945') then 343
               when a.service_id in ('942') then 458
               when a.service_id in ('944','946','949') then 483
               when a.service_id in ('947') then 497
               when a.service_id not in ('142','717','903','904','906','910','911','912','917','924',
                                         '925','926','931','933','934','936','942','942','943','944',
                                         '945','946','947','949','951','953','912001','912002',
                                         '939001','939002','10105002','SPEC|BLACKBERRY')
               then 503
          end,
          count(distinct a.enterprise_id)
     from $stat_enterprise_0060_d_tmp1 a
    where a.unipay_income > 0
    group by
          a.city_id,
          case when a.service_id = 'VPMN' then 5
               when a.service_id = 'JTDH' then 6
               when a.service_id = '917' then 51
               when a.service_id = '931' then 59
               when a.service_id = '933' then 101
               when a.service_id = '910' then 109
               when a.service_id in ('142','906') then 137
               when a.service_id = '904' then 147
               when a.service_id = '943' then 153
               when a.service_id = '936' then 166
               when a.service_id = '953' then 172
               when a.service_id = '912001' then 181
               when a.service_id = '912002' then 196
               when a.service_id = '934' then 232
               when a.service_id in ('926','939002') then 249
               when a.service_id in ('SPEC|BLACKBERRY','717') then 261
               when a.service_id in ('924') then 294
               when a.service_id in ('925') then 312
               when a.service_id in ('903','939001') then 328
               when a.service_id in ('10105002') then 337
               when a.service_id in ('945') then 343
               when a.service_id in ('942') then 458
               when a.service_id in ('944','946','949') then 483
               when a.service_id in ('947') then 497
               when a.service_id not in ('142','717','903','904','906','910','911','912','917','924',
                                         '925','926','931','933','934','936','942','942','943','944',
                                         '945','946','947','949','951','953','912001','912002',
                                         '939001','939002','10105002','SPEC|BLACKBERRY')
               then 503
          end;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3040
       puts "errmsg:$errmsg"
       return -1
   }

   ####��Ҫ�ϲ�/�ֲ�ҵ��ĸ��Ѽ��ſͻ���
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          case when a.service_id in ('924','925') then 276
               when a.service_id in ('942','944','946','947','949') then 408
          end,
          count(distinct a.enterprise_id)
     from $stat_enterprise_0060_d_tmp1 a
    where a.unipay_income > 0
      and a.service_id in ('924','925','942','944','946','947','949')
    group by
          a.city_id,
          case when a.service_id in ('924','925') then 276
               when a.service_id in ('942','944','946','947','949') then 408
          end;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3050
       puts "errmsg:$errmsg"
       return -1
   }

   ####�����û���
   ######����ͨΪ��Ա��������ҵ��Ϊ�û��������޶����ų�Ա��
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          case when a.service_id = 'VPMN' then 7
               when a.service_id = 'JTDH' then 8
               when a.service_id = '917' then 52
               when a.service_id = '933' then 103
               when a.service_id = '911' then 117
               when a.service_id = '943' then 154
               when a.service_id = '951' then 161
               when a.service_id = '936' then 167
               when a.service_id = '934' then 233
               when a.service_id in ('926','939002') then 250
               when a.service_id in ('SPEC|BLACKBERRY','717') then 263
               when a.service_id in ('903','939001') then 329
               when a.service_id in ('10105002') then 338
               when a.service_id in ('945') then 344
               when a.service_id in ('942') then 459
               when a.service_id in ('949') then 485
               when a.service_id in ('944') then 486
               when a.service_id in ('946') then 487
               when a.service_id in ('947') then 498
               when a.service_id not in ('142','717','903','904','906','910','911','912','917','924',
                                         '925','926','931','933','934','936','942','942','943','944',
                                         '945','946','947','949','951','953','912001','912002',
                                         '939001','939002','10105002','SPEC|BLACKBERRY')
               then 504
          end,
          sum(case when a.service_id = '945' then a.order_mem_count else a.order_user_count end)
     from $stat_enterprise_0060_d_tmp1 a
    group by
          a.city_id,
          case when a.service_id = 'VPMN' then 7
               when a.service_id = 'JTDH' then 8
               when a.service_id = '917' then 52
               when a.service_id = '933' then 103
               when a.service_id = '911' then 117
               when a.service_id = '943' then 154
               when a.service_id = '951' then 161
               when a.service_id = '936' then 167
               when a.service_id = '934' then 233
               when a.service_id in ('926','939002') then 250
               when a.service_id in ('SPEC|BLACKBERRY','717') then 263
               when a.service_id in ('903','939001') then 329
               when a.service_id in ('10105002') then 338
               when a.service_id in ('945') then 344
               when a.service_id in ('942') then 459
               when a.service_id in ('949') then 485
               when a.service_id in ('944') then 486
               when a.service_id in ('946') then 487
               when a.service_id in ('947') then 498
               when a.service_id not in ('142','717','903','904','906','910','911','912','917','924',
                                         '925','926','931','933','934','936','942','942','943','944',
                                         '945','946','947','949','951','953','912001','912002',
                                         '939001','939002','10105002','SPEC|BLACKBERRY')
               then 504
          end;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3060
       puts "errmsg:$errmsg"
       return -1
   }

   ####��Ҫ�ϲ�/�ֲ�ҵ��Ķ����û���
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          case when a.service_id = '911' then
                    case when a.enterprise_name like '%�׶�԰' then 118
                         when a.enterprise_name like '%С' or a.enterprise_name like '%Сѧ' then 119
                         else 120
                    end
               when a.service_id in ('944','946','949') then 484
          end,
          sum(case when a.service_id = '945' then a.order_mem_count else a.order_user_count end)
     from $stat_enterprise_0060_d_tmp1 a
    where a.service_id in ('911','944','946','949')
    group by
          a.city_id,
          case when a.service_id = '911' then
                    case when a.enterprise_name like '%�׶�԰' then 118
                         when a.enterprise_name like '%С' or a.enterprise_name like '%Сѧ' then 119
                         else 120
                    end
               when a.service_id in ('944','946','949') then 484
          end;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3070
       puts "errmsg:$errmsg"
       return -1
   }

   ####�����������û���
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          case when a.service_id in ('949') then 410
               when a.service_id in ('942','946') then 411
               when a.service_id in ('944') then 412
               when a.service_id in ('947') then 416
          end,
          sum(case when a.service_id = '945' then a.order_mem_count else a.order_user_count end)
     from $stat_enterprise_0060_d_tmp1 a
    where a.service_id in ('942','944','946','947','949')
    group by
          a.city_id,
          case when a.service_id in ('949') then 410
               when a.service_id in ('942','946') then 411
               when a.service_id in ('944') then 412
               when a.service_id in ('947') then 416
          end;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3080
       puts "errmsg:$errmsg"
       return -1
   }

   ####�������ϲ������û���
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          409,
          sum(case when a.service_id = '945' then a.order_mem_count else a.order_user_count end)
     from $stat_enterprise_0060_d_tmp1 a
    where a.service_id in ('942','944','946','947','949')
    group by
          a.city_id;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3090
       puts "errmsg:$errmsg"
       return -1
   }

   ####�����û���
   ######����ͨΪ��Ա��������ҵ��Ϊ�û��������޶����ų�Ա��
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          case when a.service_id = 'VPMN' then 11
               when a.service_id = 'JTDH' then 12
               when a.service_id = '917' then 54
               when a.service_id = '933' then 104
               when a.service_id = '911' then 123
               when a.service_id = '951' then 162
               when a.service_id = '936' then 168
               when a.service_id = '934' then 235
               when a.service_id in ('926','939002') then 252
               when a.service_id in ('SPEC|BLACKBERRY','717') then 267
               when a.service_id in ('903','939001') then 330
               when a.service_id in ('10105002') then 338
               when a.service_id in ('945') then 346
               when a.service_id in ('942') then 461
               when a.service_id in ('944','946','949') then 489
               when a.service_id not in ('142','717','903','904','906','910','911','912','917','924',
                                         '925','926','931','933','934','936','942','942','943','944',
                                         '945','946','947','949','951','953','912001','912002',
                                         '939001','939002','10105002','SPEC|BLACKBERRY')
               then 505
          end,
          sum(case when a.service_id = '945' then a.pay_mem_count else a.pay_user_count end)
     from $stat_enterprise_0060_d_tmp1 a
    group by
          a.city_id,
          case when a.service_id = 'VPMN' then 11
               when a.service_id = 'JTDH' then 12
               when a.service_id = '917' then 54
               when a.service_id = '933' then 104
               when a.service_id = '911' then 123
               when a.service_id = '951' then 162
               when a.service_id = '936' then 168
               when a.service_id = '934' then 235
               when a.service_id in ('926','939002') then 252
               when a.service_id in ('SPEC|BLACKBERRY','717') then 267
               when a.service_id in ('903','939001') then 330
               when a.service_id in ('10105002') then 338
               when a.service_id in ('945') then 346
               when a.service_id in ('942') then 461
               when a.service_id in ('944','946','949') then 489
               when a.service_id not in ('142','717','903','904','906','910','911','912','917','924',
                                         '925','926','931','933','934','936','942','942','943','944',
                                         '945','946','947','949','951','953','912001','912002',
                                         '939001','939002','10105002','SPEC|BLACKBERRY')
               then 505
          end;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3100
       puts "errmsg:$errmsg"
       return -1
   }

   ####�����������û���
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          417,
          sum(case when a.service_id = '945' then a.order_mem_count else a.order_user_count end)
     from $stat_enterprise_0060_d_tmp1 a
    where a.service_id in ('942','944','946','947','949')
    group by
          a.city_id;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3110
       puts "errmsg:$errmsg"
       return -1
   }

   ####������
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          case when a.service_id = 'VPMN' then 17
               when a.service_id = 'JTDH' then 18
               when a.service_id = '933' then 105
               when a.service_id = '911' then 131
               when a.service_id = '904' then 149
               when a.service_id = '951' then 163
               when a.service_id = '936' then 169
               when a.service_id in ('926','939002') then 255
               when a.service_id in ('SPEC|BLACKBERRY','717') then 271
               when a.service_id in ('903','939001') then 333
               when a.service_id in ('945') then 348
               when a.service_id in ('942') then 463
               when a.service_id not in ('142','717','903','904','906','910','911','912','917','924',
                                         '925','926','931','933','934','936','942','942','943','944',
                                         '945','946','947','949','951','953','912001','912002',
                                         '939001','939002','10105002','SPEC|BLACKBERRY')
               then 508
          end,
          sum(a.total_income)
     from $stat_enterprise_0060_d_tmp1 a
    group by
          a.city_id,
          case when a.service_id = 'VPMN' then 17
               when a.service_id = 'JTDH' then 18
               when a.service_id = '933' then 105
               when a.service_id = '911' then 131
               when a.service_id = '904' then 149
               when a.service_id = '951' then 163
               when a.service_id = '936' then 169
               when a.service_id in ('926','939002') then 255
               when a.service_id in ('SPEC|BLACKBERRY','717') then 271
               when a.service_id in ('903','939001') then 333
               when a.service_id in ('945') then 348
               when a.service_id in ('942') then 463
               when a.service_id not in ('142','717','903','904','906','910','911','912','917','924',
                                         '925','926','931','933','934','936','942','942','943','944',
                                         '945','946','947','949','951','953','912001','912002',
                                         '939001','939002','10105002','SPEC|BLACKBERRY')
               then 508
          end;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3120
       puts "errmsg:$errmsg"
       return -1
   }

   ####������������
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
         434,
          sum(total_income)
     from $stat_enterprise_0060_d_tmp1 a
    where a.service_id in ('942','944','946','947','949')
    group by
          a.city_id;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3130
       puts "errmsg:$errmsg"
       return -1
   }

   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          case when a.service_id = '949' then 435
               when a.service_id in ('942','946') then 436
               when a.service_id in ('944') then 437
               when a.service_id in ('947') then 441
          end,
          sum(total_income)
     from $stat_enterprise_0060_d_tmp1 a
    where a.service_id in ('942','944','946','947','949')
    group by
          a.city_id,
          case when a.service_id = '949' then 435
               when a.service_id in ('942','946') then 436
               when a.service_id in ('944') then 437
               when a.service_id in ('947') then 441
          end;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3130
       puts "errmsg:$errmsg"
       return -1
   }

   ####ͳһ��������
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          case when a.service_id = 'VPMN' then 19
               when a.service_id = 'JTDH' then 20
               when a.service_id = '917' then 56
               when a.service_id = '931' then 72
               when a.service_id = '933' then 106
               when a.service_id = '910' then 111
               when a.service_id = '911' then 132
               when a.service_id in ('142','906') then 140
               when a.service_id = '904' then 150
               when a.service_id = '943' then 160
               when a.service_id = '951' then 164
               when a.service_id = '936' then 170
               when a.service_id = '953' then 173
               when a.service_id = '912001' then 194
               when a.service_id = '912002' then 209
               when a.service_id = '934' then 237
               when a.service_id in ('926','939002') then 256
               when a.service_id in ('SPEC|BLACKBERRY','717') then 273
               when a.service_id = '924' then 306
               when a.service_id = '925' then 317
               when a.service_id in ('903','939001') then 334
               when a.service_id = '10105002' then 340
               when a.service_id in ('945') then 349
               when a.service_id in ('942') then 464
               when a.service_id in ('949') then 493
               when a.service_id in ('944') then 494
               when a.service_id in ('946') then 495
               when a.service_id in ('947') then 501
               when a.service_id not in ('142','717','903','904','906','910','911','912','917','924',
                                         '925','926','931','933','934','936','942','942','943','944',
                                         '945','946','947','949','951','953','912001','912002',
                                         '939001','939002','10105002','SPEC|BLACKBERRY')
               then 509
          end,
          sum(a.unipay_income)
     from $stat_enterprise_0060_d_tmp1 a
    group by
          a.city_id,
          case when a.service_id = 'VPMN' then 19
               when a.service_id = 'JTDH' then 20
               when a.service_id = '917' then 56
               when a.service_id = '931' then 72
               when a.service_id = '933' then 106
               when a.service_id = '910' then 111
               when a.service_id = '911' then 132
               when a.service_id in ('142','906') then 140
               when a.service_id = '904' then 150
               when a.service_id = '943' then 160
               when a.service_id = '951' then 164
               when a.service_id = '936' then 170
               when a.service_id = '953' then 173
               when a.service_id = '912001' then 194
               when a.service_id = '912002' then 209
               when a.service_id = '934' then 237
               when a.service_id in ('926','939002') then 256
               when a.service_id in ('SPEC|BLACKBERRY','717') then 273
               when a.service_id = '924' then 306
               when a.service_id = '925' then 317
               when a.service_id in ('903','939001') then 334
               when a.service_id = '10105002' then 340
               when a.service_id in ('945') then 349
               when a.service_id in ('942') then 464
               when a.service_id in ('949') then 493
               when a.service_id in ('944') then 494
               when a.service_id in ('946') then 495
               when a.service_id in ('947') then 501
               when a.service_id not in ('142','717','903','904','906','910','911','912','917','924',
                                         '925','926','931','933','934','936','942','942','943','944',
                                         '945','946','947','949','951','953','912001','912002',
                                         '939001','939002','10105002','SPEC|BLACKBERRY')
               then 509
          end
    union all
   select city_id,
          102,
          count(distinct enterprise_id)
     from $stat_enterprise_0060_d_tmp1 a
    where unipay_income > 0
    group by
          city_id;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3140
       puts "errmsg:$errmsg"
       return -1
   }

   ####��Ҫ�ϲ�/�ֲ�ҵ���ͳһ��������
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          case when a.service_id in ('924','925') then 277
               when a.service_id in ('942','944','946','947','949') then 442
          end,
          sum(a.unipay_income)
     from $stat_enterprise_0060_d_tmp1 a
    where a.service_id in ('924','925','942','944','946','947','949')
    group by
          a.city_id,
          case when a.service_id in ('924','925') then 277
               when a.service_id in ('942','944','946','947','949') then 442
          end;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3150
       puts "errmsg:$errmsg"
       return -1
   }

   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          case when a.service_id in ('949') then 443
               when a.service_id in ('942','946') then 444
               when a.service_id in ('944') then 445
               when a.service_id in ('947') then 449
          end,
          sum(a.unipay_income)
     from $stat_enterprise_0060_d_tmp1 a
    where a.service_id in ('942','944','946','947','949')
    group by
          a.city_id,
          case when a.service_id in ('949') then 443
               when a.service_id in ('942','946') then 444
               when a.service_id in ('944') then 445
               when a.service_id in ('947') then 449
          end;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3150
       puts "errmsg:$errmsg"
       return -1
   }

   ####������
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          492,
          sum(a.unipay_income)
     from $stat_enterprise_0060_d_tmp1 a
    where a.service_id in ('944','946','949')
    group by
          a.city_id;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3160
       puts "errmsg:$errmsg"
       return -1
   }

   ##û�м��Ŷ����Ĳ�Ʒ/�����Ʒ/����ָ��
   ####����V���û�������
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select city_id,
          15,
          case when s_index_id = 17 then result else 0 end
           - case when s_index_id = 19 then result else 0 end
     from $stat_enterprise_0060_d_tmp4
    where s_index_id in (17,19);"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3170
       puts "errmsg:$errmsg"
       return -1
   }


   ####���У��̺�V���û�������
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select city_id,
          16,
          case when s_index_id = 18 then result else 0 end
           - case when s_index_id = 20 then result else 0 end
     from $stat_enterprise_0060_d_tmp4
    where s_index_id in (18,20);"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3170
       puts "errmsg:$errmsg"
       return -1
   }
   
   ####����V��ʹ�ü��ſͻ���
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select case when c.level_def_mode = 1 then '888'
               else c.ent_city_id
          end,
          3,
          count(distinct a.enterprise_id)
     from $stat_enterprise_0060_d_tmp2 a,
          $dw_call_yyyymm b,
          $dw_enterprise_msg_yyyymm c
    where a.enterprise_id = c.enterprise_id
      and a.user_id = b.user_id
      and a.service_id = 'VPMN'
      and b.opposite_id in ($opposite_inner_id)
      and c.ent_status_id = 0
      and c.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test= 1)
    group by
          case when c.level_def_mode = 1 then '888'
               else c.ent_city_id
          end;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3180
       puts "errmsg:$errmsg"
       return -1
   }
   
#    ####����V��ʹ�ü��ſͻ���--���У��̺�V��ʹ�ü��ſͻ���
#   set sql_buf "
#   insert into $stat_enterprise_0060_d_tmp4
#   (
#          city_id,
#          s_index_id,
#          result
#   )
#   select case when c.level_def_mode = 1 then '888'
#               else c.ent_city_id
#          end,
#          4,
#          count(distinct a.enterprise_id)
#     from $stat_enterprise_0060_d_tmp2 a,
#          $dw_call_yyyymm b,
#          $dw_enterprise_msg_yyyymm c
#    where a.enterprise_id = c.enterprise_id
#      and a.user_id = b.user_id
#      and a.service_id = 'JTDH'
#      and b.opposite_id in ($opposite_inner_id)
#    group by
#          case when c.level_def_mode = 1 then '888'
#               else c.ent_city_id
#          end;"
#   puts $sql_buf
#   if [catch {aidb_sql $handle $sql_buf} errmsg] {
#       trace_sql $errmsg 3180
#       puts "errmsg:$errmsg"
#       return -1
#   }

   ####����V��ʹ���û���
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select case when c.level_def_mode = 1 then '888'
               else c.ent_city_id
          end,
          9,
          count(distinct a.user_id)
     from $stat_enterprise_0060_d_tmp2 a,
          $dw_call_yyyymm b,
          $dw_enterprise_msg_yyyymm c
    where a.enterprise_id = c.enterprise_id
      and a.user_id = b.user_id
      and a.service_id = 'VPMN'
      and b.opposite_id in ($opposite_inner_id)
      and c.ent_status_id = 0
      and c.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test= 1)
    group by
          case when c.level_def_mode = 1 then '888'
               else c.ent_city_id
          end;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3190
       puts "errmsg:$errmsg"
       return -1
   }
   
#    ####����V��ʹ���û���--���У��̺�V��ʹ���û���
#   set sql_buf "
#   insert into $stat_enterprise_0060_d_tmp4
#   (
#          city_id,
#          s_index_id,
#          result
#   )
#   select case when c.level_def_mode = 1 then '888'
#               else c.ent_city_id
#          end,
#          10,
#          count(distinct a.user_id)
#     from $stat_enterprise_0060_d_tmp2 a,
#          $dw_call_yyyymm b,
#          $dw_enterprise_msg_yyyymm c
#    where a.enterprise_id = c.enterprise_id
#      and a.user_id = b.user_id
#      and a.service_id = 'JTDH'
#      and b.opposite_id in ($opposite_inner_id)
#    group by
#          case when c.level_def_mode = 1 then '888'
#               else c.ent_city_id
#          end;"
#   puts $sql_buf
#   if [catch {aidb_sql $handle $sql_buf} errmsg] {
#       trace_sql $errmsg 3190
#       puts "errmsg:$errmsg"
#       return -1
#   }

   ####����V���Ʒ�ʱ��
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select case when c.level_def_mode = 1 then '888'
               else c.ent_city_id
          end,
          13,
          sum(b.call_duration_m)
     from $stat_enterprise_0060_d_tmp2 a,
          $dw_call_yyyymm b,
          $dw_enterprise_msg_yyyymm c
    where a.enterprise_id = c.enterprise_id
      and a.user_id = b.user_id
      and a.service_id = 'VPMN'
      and b.opposite_id in ($opposite_inner_id)
      and c.ent_status_id = 0
      and c.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test= 1)
    group by
          case when c.level_def_mode = 1 then '888'
               else c.ent_city_id
          end;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3200
       puts "errmsg:$errmsg"
       return -1
   }

   ####�ƶ�400ҵ����
   ######fee_type 1-ͨ���� 2-���ŷ� 3-��������
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp6
   (
          enterprise_id,
          city_id,
          product_no,
          call_counts,
          local_call_counts,
          toll_call_counts,
          call_duration_m,
          local_call_duration_m,
          toll_call_duration_m,
          call_fee,
          sms_fee,
          total_fee
   )
   select a.enterprise_id,
          a.city_id,
          a.product_no,
          sum(b.call_counts),
          sum(case when b.tolltype_id = 0 then b.call_counts else 0 end),
          sum(case when b.tolltype_id <> 0 then b.call_counts else 0 end),
          sum(b.call_duration_m),
          sum(case when b.tolltype_id = 0 then b.call_duration_m else 0 end),
          sum(case when b.tolltype_id <> 0 then b.call_duration_m else 0 end),
          sum(case when c.fee_type = 1 then c.fact_fee else 0 end),
          sum(case when c.fee_type = 2 then c.fact_fee else 0 end),
          sum(c.fact_fee)
     from (
           select a.enterprise_id,
                  case when c.level_def_mode = 1 then '888'
                       else c.ent_city_id
                  end as city_id,
                  b.user_id,
                  b.product_no
             from $stat_enterprise_0060_d_tmp2 a,
                  $dw_product_yyyymm b,
                  $dw_enterprise_msg_yyyymm c
            where a.enterprise_id = c.enterprise_id
              and a.enterprise_id = b.cust_id
              and b.product_no like '400%'
              and a.service_id = '931'
              and c.ent_status_id = 0
              and c.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test= 1)
          ) a
     left join
          $dw_call_yyyymm b
       on a.user_id = b.user_id
     left join
          (
           select a.user_id,
                  case when b.item_id = 80000517 then 2
                       when b.item_id in (80000549,80000550) then 1
                       else 3
                  end as fee_type,
                  sum(a.fact_fee) fact_fee
             from $dw_acct_shoulditem_yyyymm a,
                  $dim_ent_unipay_item b
            where a.item_id = b.item_id
              and b.service_id = 931
            group by
                  a.user_id,
                  case when b.item_id = 80000517 then 2
                       when b.item_id in (80000549,80000550) then 1
                       else 3
                  end
          ) c
       on a.user_id = c.user_id
    group by
          a.enterprise_id,
          a.city_id,
          a.product_no;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3210
       puts "errmsg:$errmsg"
       return -1
   }

   ####�ƶ�400ʹ�ü��ſͻ���
   ####�ƶ�400��ͨ������
   ####���У��ƶ�400��ͨ4001������
   ####�ƶ�400ʹ�ú�����
   ####�ƶ�400���Ѻ�����
   ####�ƶ�400�������д��������У�
   ####���У��ƶ�400�����������д��������У�
   ####�ƶ�400��;�������д��������У�
   ####�ƶ�400�Ʒ�ʱ�������У�
   ####���У��ƶ�400���ؼƷ�ʱ�������У�
   ####�ƶ�400��;�Ʒ�ʱ�������У�
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select city_id,
          58,
          count(distinct enterprise_id) as result
     from $stat_enterprise_0060_d_tmp6
    where call_counts > 0
    group by
          city_id
    union all
   select city_id,
          60,
          count(distinct product_no)
     from $stat_enterprise_0060_d_tmp6
    group by
          city_id
    union all
   select city_id,
          61,
          count(distinct product_no)
     from $stat_enterprise_0060_d_tmp6
    where product_no like '4001%'
    group by
          city_id
    union all
   select city_id,
          62,
          count(distinct product_no)
     from $stat_enterprise_0060_d_tmp6
    where call_counts > 0
    group by
          city_id
    union all
   select city_id,
          63,
          count(distinct product_no)
     from $stat_enterprise_0060_d_tmp6
    where total_fee > 0
    group by
          city_id
    union all
   select city_id,
          65,
          sum(call_counts)
     from $stat_enterprise_0060_d_tmp6
    group by
          city_id
    union all
   select city_id,
          66,
          sum(local_call_counts)
     from $stat_enterprise_0060_d_tmp6
    group by
          city_id
    union all
   select city_id,
          67,
          sum(toll_call_counts)
     from $stat_enterprise_0060_d_tmp6
    group by
          city_id
    union all
   select city_id,
          68,
          sum(call_duration_m)
     from $stat_enterprise_0060_d_tmp6
    group by
          city_id
    union all
   select city_id,
          69,
          sum(local_call_duration_m)
     from $stat_enterprise_0060_d_tmp6
    group by
          city_id
    union all
   select city_id,
          70,
          sum(toll_call_duration_m)
     from $stat_enterprise_0060_d_tmp6
    group by
          city_id
    union all
   select city_id,
          73,
          sum(call_fee)
     from $stat_enterprise_0060_d_tmp6
    group by
          city_id
    union all
   select city_id,
          74,
          sum(sms_fee)
     from $stat_enterprise_0060_d_tmp6
    group by
          city_id;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3220
       puts "errmsg:$errmsg"
       return -1
   }

   ####������ר�߶�������
   ####����ר�߶�������
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          s_index_id,
          count(distinct a.order_id)
     from (
           select distinct
                  a.enterprise_id,
                  a.prod_id,
                  case when c.level_def_mode = 1 then '888'
                       else c.ent_city_id
                  end as city_id,
                  a.acct_id,
                  a.order_id,
                  b.feature_value,
                  case when a.prod_id = '91201001'
                       then 182
                       when a.prod_id = '91201002'
                       then 197
                  end as s_index_id
             from $dw_enterprise_sub_yyyymm a,
                  $dwd_group_order_featur_yyyymmdd b,
                  $dw_enterprise_msg_yyyymm c
            where a.order_id = b.order_id
              and b.feature_id in ('805102001','805102002')
              and date(b.valid_date) <= $date_me_optime
              and date(b.expire_date) >= $date_optime
              and a.enterprise_id = c.enterprise_id
              and c.ent_status_id = 0
              and c.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test = 1)
          ) a
    group by
          a.city_id,
          a.s_index_id;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3230
       puts "errmsg:$errmsg"
       return -1
   }

   ####������ר�߸�������
   ####����ר�߸�������
   ######case when e.feature_value='1' then 1
   ######when e.feature_value='2' then 2
   ######when e.feature_value='3' then 4
   ######when e.feature_value='4' then 8
   ######when e.feature_value='5' then 34
   ######when e.feature_value='6' then 10
   ######when e.feature_value='7' then 20
   ######when e.feature_value='8' then 6
   ######when e.feature_value='9' then 100
   ######when e.feature_value='10' then 28
   ######end
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select a.city_id,
          s_index_id,
          count(distinct a.order_id)
     from (
           select distinct
                  a.enterprise_id,
                  a.prod_id,
                  case when c.level_def_mode = 1 then '888'
                       else c.ent_city_id
                  end as city_id,
                  a.acct_id,
                  a.order_id,
                  b.feature_value,
                  case when a.prod_id = '91201001'
                       then
                            case when b.feature_value = '2' then 184
                                 when b.feature_value = '3' then 185
                                 when b.feature_value = '4' then 186
                                 when b.feature_value = '6' then 187
                                 when b.feature_value = '5' then 188
                                 when b.feature_value = '9' then 189
                                 else 193
                            end
                       when a.prod_id = '91201002'
                       then
                            case when b.feature_value = '2' then 199
                                 when b.feature_value = '3' then 200
                                 when b.feature_value = '4' then 201
                                 when b.feature_value = '6' then 202
                                 when b.feature_value = '5' then 203
                                 when b.feature_value = '9' then 204
                                 else 208
                            end
                  end as s_index_id
             from $dw_enterprise_sub_yyyymm a,
                  $dwd_group_order_featur_yyyymmdd b,
                  $dw_enterprise_msg_yyyymm c,
                  $dw_enterprise_new_unipay_yyyymm d
            where a.order_id = b.order_id
              and b.feature_id in ('805102001','805102002')
              and date(b.valid_date) <= $date_me_optime
              and date(b.expire_date) >= $date_optime
              and a.enterprise_id = c.enterprise_id
              and c.ent_status_id = 0
              and c.enterprise_id not in (select enterprise_id from $dw_enterprise_msg_yyyymm where is_test = 1)
              and a.enterprise_id = d.enterprise_id
              and a.acct_id = d.acct_id
              and d.unipay_fee > 0
              and d.service_id in (912001,912002)
          ) a
    group by
          a.city_id,
          a.s_index_id;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3230
       puts "errmsg:$errmsg"
       return -1
   }

   ####������ר�߸��������ϼ�
   ####����ר�߸��������ϼ�
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select city_id,
          case when s_index_id in (184,185,186,187,188,189,190,191,192,193) then 183
               when s_index_id in (199,200,201,202,203,204,205,206,207,208) then 198
          end,
          sum(result)
     from $stat_enterprise_0060_d_tmp4
    where s_index_id in (184,185,186,187,188,189,190,191,192,193,
                         199,200,201,202,203,204,205,206,207,208)
    group by
          city_id,
          case when s_index_id in (184,185,186,187,188,189,190,191,192,193) then 183
               when s_index_id in (199,200,201,202,203,204,205,206,207,208) then 198
          end;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3240
       puts "errmsg:$errmsg"
       return -1
   }

#   ####����ר�߶�������
#   ####����ר�߸�������
#   ####����ר��ͳ������
#   set sql_buf "
#   insert into $stat_enterprise_0060_d_tmp4
#   (
#          city_id,
#          s_index_id,
#          result
#   )
#   select city_id,
#          case when s_index_id in (174,187) then 466
#               when s_index_id in (175,188) then 467
#               when s_index_id in (184,199) then 468
#          end,
#          sum(result)
#     from $stat_enterprise_0060_d_tmp4
#    where s_index_id in (174,175,184,187,188,199)
#    group by
#          city_id,
#          case when s_index_id in (174,187) then 466
#               when s_index_id in (175,188) then 467
#               when s_index_id in (184,199) then 468
#          end;"
#   puts $sql_buf
#   if [catch {aidb_sql $handle $sql_buf} errmsg] {
#       #trace_sql $errmsg 3250
#       puts "errmsg:$errmsg"
#       return -1
#   }

    #Step6.��������������COM_OTHER_XZ_LS_20110708_49888 ���޸Ľ����
    ##���������칤��(COM_OTHER_XZ_LS_20111110_65259)�޸����������̻�����
    set sql_buf "
    insert into $stat_enterprise_0060_d_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           41,
           count(distinct enterprise_id)
      from $stat_enterprise_0060_d_tmp7
     group by
           city_id
     union all
    select city_id,
           42,
           count(distinct enterprise_id)
      from $stat_enterprise_0060_d_tmp7
     where call_duration_m > 0
     group by
           city_id
     union all
    select city_id,
           43,
           count(distinct enterprise_id)
      from $stat_enterprise_0060_d_tmp7
     where fact_fee > 0
     group by
           city_id
     union all
    select city_id,
           44,
           count(distinct user_id)
      from $stat_enterprise_0060_d_tmp7
     group by
           city_id
     union all
    select city_id,
           45,
           count(distinct user_id)
      from $stat_enterprise_0060_d_tmp7
     where call_duration_m > 0
     group by
           city_id
     union all
    select city_id,
           46,
           count(distinct user_id)
      from $stat_enterprise_0060_d_tmp7
     where fact_fee > 0
     group by
           city_id
     union all
    select city_id,
           47,
           sum(call_duration_m)
      from $stat_enterprise_0060_d_tmp7
     group by
           city_id
     union all
    select city_id,
           48,
           sum(fact_fee)
      from $stat_enterprise_0060_d_tmp7
     group by
           city_id;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 5010
       puts "errmsg:$errmsg"
       return -1
    }
           
#    set sql_buf "
#    insert into $stat_enterprise_0060_d_tmp4
#    (
#           city_id,
#           s_index_id,
#           result
#    )
#    select '892',
#           a.s_index_id,
#           1
#      from $ent_index_s_3 a
#     where a.s_index_id in (41,42,43);"
#    puts $sql_buf
#    if [catch {aidb_sql $handle $sql_buf} errmsg] {
#       trace_sql $errmsg 5010
#       puts "errmsg:$errmsg"
#       return -1
#    }
#
#    set sql_buf "
#    insert into $stat_enterprise_0060_d_tmp4
#    (
#           city_id,
#           s_index_id,
#           result
#    )
#  select '892',
#         a.s_index_id,
#         b.user_num
#    from  $ent_index_s_3 a,
#          (SELECT count(distinct product_instance_id) user_num
#           FROM dw_product_ins_off_ins_prod_ds
#           where cust_party_role_id='89200000007999'
#           and is_main_offer=0
#           and expire_date>='$day_me_optime 23:59:59.000000'
#           and valid_date<= '$day_me_optime 23:59:59.000000'
#          ) b
#    where a.s_index_id in (44,45,46)
#;"
#  puts $sql_buf
#  if [catch {aidb_sql $handle $sql_buf} errmsg] {
#      trace_sql $errmsg 5010
#      puts "errmsg:$errmsg"
#      return -1
#  }
#
#   set sql_buf "
#  insert into $stat_enterprise_0060_d_tmp4
#   (
#          city_id,
#          s_index_id,
#          result
#   )
#  select '892',
#         48,
#         sum(value(c.fact_fee,0))
#    from
#          (SELECT distinct product_instance_id
#           FROM dw_product_ins_off_ins_prod_ds
#           where cust_party_role_id='89200000007999'
#           and is_main_offer=0
#           and expire_date>='$day_me_optime 23:59:59.000000'
#           and valid_date<= '$day_me_optime 23:59:59.000000'
#          ) b,
#          $dw_product_yyyymm c
#    where  b.product_instance_id=c.user_id
#;"
#  puts $sql_buf
#  if [catch {aidb_sql $handle $sql_buf} errmsg] {
#      trace_sql $errmsg 5010
#      puts "errmsg:$errmsg"
#      return -1
#  }
#
#   set sql_buf "
#  insert into $stat_enterprise_0060_d_tmp4
#   (
#          city_id,
#          s_index_id,
#          result
#   )
#  select '892',
#         47,
#         sum(value(c.call_duration_m,0))
#    from
#          (SELECT distinct product_instance_id
#           FROM dw_product_ins_off_ins_prod_ds
#           where cust_party_role_id='89200000007999'
#           and is_main_offer=0
#           and expire_date>='$day_me_optime 23:59:59.000000'
#           and valid_date<= '$day_me_optime 23:59:59.000000'
#          ) b,
#          $dw_product_yyyymm c
#    where  b.product_instance_id=c.user_id
#;"
#  puts $sql_buf
#  if [catch {aidb_sql $handle $sql_buf} errmsg] {
#      trace_sql $errmsg 5010
#      puts "errmsg:$errmsg"
#      return -1
#  }
#  
  
  
  
##20120331 luodk add  ��ŵ¼���ͨ������ҽ��ͨ�����û�����
    set sql_buf "
    insert into $stat_enterprise_0060_d_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select b.city_id,
           514,
           count(b.user_Id)
      from $dw_product_ins_off_ins_prod_yyyymm a,
           $dw_product_yyyymm b
      where a.product_instance_id = b.user_id
            and a.offer_id in (111090001774,113500001080,113500001079)
            and date(a.valid_date) < $date_nm_optime
            and date(a.expire_date) >= $date_nm_optime
            and b.usertype_id in (1,2,9) and b.userstatus_id in (1,2,3,6,8) and b.test_mark <> 1
     group by b.city_id
      ;"
     puts $sql_buf
  if [catch {aidb_sql $handle $sql_buf} errmsg] {
      trace_sql $errmsg 50514
      puts "errmsg:$errmsg"
      return -1
  }
  

##20120331 luodk add  ��ŵ¼���ͨ��������ҽ��ͨ����ͳ�Ƶ������С�
    set sql_buf "
    insert into $stat_enterprise_0060_d_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select c.city_id,
           515,
           sum(value(b.fact_fee,0))
      from $dw_acct_shoulditem_yyyymm b,
           $dw_product_yyyymm c
      where b.user_id=c.user_id 
            and b.item_id in (80000802,80000803)
     group by c.city_id
      ;"
     puts $sql_buf
  if [catch {aidb_sql $handle $sql_buf} errmsg] {
      trace_sql $errmsg 50515
      puts "errmsg:$errmsg"
      return -1
  }
  
##����ͨ����ҵ����##############################################

    set sql_buf "
    insert into $stat_enterprise_0060_d_tmp4
    (
           city_id,
           s_index_id,
           result
    )
    select city_id,
           110,
            sum(counts) 
              from $dw_newbusi_ismg_yyyymm
             where calltype_id = 1
               and ser_code like '10657%'
               and oper_code = 'QXZ0011201'
             group by city_id
      ;"
     puts $sql_buf
  if [catch {aidb_sql $handle $sql_buf} errmsg] {
      trace_sql $errmsg 50515
      puts "errmsg:$errmsg"
      return -1
  }
    



   ####����ȫ������
   set sql_buf "
   insert into $stat_enterprise_0060_d_tmp4
   (
          city_id,
          s_index_id,
          result
   )
   select '890',
          s_index_id,
          sum(result)
     from $stat_enterprise_0060_d_tmp4
    group by
          s_index_id;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 3260
       puts "errmsg:$errmsg"
       return -1
   }

   #Step4.����������ʷ����
   set sql_buf "delete from $stat_enterprise_0060_d where op_time = $year$month;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 4010
       puts "errmsg:$errmsg"
       return -1
   }

   #Step5.���ɽ��������
   set sql_buf "
   insert into $stat_enterprise_0060_d
   (
          op_time,
          city_id,
          s_index_id,
          result
   )
   select $year$month,
          a.city_id,
          a.s_index_id,
          sum(value(b.result,0))
     from (
           select city_id,
                  s_index_id
             from $dim_pub_city a,
                  $ent_index_s_3 b
          ) a
     left join
          $stat_enterprise_0060_d_tmp4 b on a.city_id = b.city_id and a.s_index_id = b.s_index_id
    group by
          a.city_id,
          a.s_index_id;"
   puts $sql_buf
   if [catch {aidb_sql $handle $sql_buf} errmsg] {
       trace_sql $errmsg 5010
       puts "errmsg:$errmsg"
       return -1
   }



    exec db2 "connect to bassdb user bass2 using bass2"
    exec db2 "runstats on table $stat_enterprise_0060_d with distribution and indexes all"
    exec db2 "terminate"

    #Step6.�����ʱ��
    set sql_buf "drop table $stat_enterprise_0060_d_tmp1;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_d_tmp2;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_d_tmp3;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_d_tmp4;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_d_tmp5;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_d_tmp6;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }
    set sql_buf "drop table $stat_enterprise_0060_d_tmp7;"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
    }

    return 0
}