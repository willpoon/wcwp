#======================================================================================
#版权归属：Copyright (c) 2011,AsiaInfo.Report.System
#程序名称: stat_market_0135_d.tcl
#程序功能: 违规预警模型-短信发送
#分析目标: 略
#分析指标: 见维表
#分析维度: 全区 地市 区县 渠道
#运行粒度: 月
#运行示例: crt_basetab.sh stat_market_0135_d.tcl 2011-04-01
#创建时间: 2011-6-14
#创 建 人: Asiainfo-Linkage HuangBo
#存在问题:
#修改历史:
#=======================================================================================
proc deal {p_optime p_timestamp} {
    global conn
    global handle

    if [catch {set handle [aidb_open $conn]} errmsg] {
        #trace_sql $errmsg 1000
        return -1
    }

    if {[stat_market_0135_d $p_optime] != 0} {
        aidb_roll $conn
        aidb_close $handle
        return -1
    }

    aidb_commit $conn
    aidb_close $handle

    return 0
}

proc stat_market_0135_d {p_optime} {
    global conn
    global handle

    source stat_insert_index.tcl
    source report.cfg

    #日期处理
    set    date_optime      [ai_to_date $p_optime]
    scan   $p_optime        "%04s-%02s-%02s" year month day

    #源表
    set stat_market_0135_a_final    "bass2.stat_market_0135_a_final"
    set stat_market_0135_b_final    "bass2.stat_market_0135_b_final"
    set stat_market_0135_c_final    "bass2.stat_market_0135_c_final"

    #目标表
    set stat_market_0135_d          "bass2.stat_market_0135_d"

    #维表
    set stat_market_0135_d_s_lkp    "bass2.stat_market_0135_d_s_lkp"

    #临时表

    #Step1.生成短信内容
    ##全区短信
    set sql_buf "
    insert into app.sms_send_info
    (
           message_content,
           mobile_num
    )
    select '您好，'||
            '$year'||'年'||
            rtrim(char(int($month))) ||'月全区有'||
            rtrim(char(value(b.chnl_count,0))) ||'个渠道疑似窜卡，'||
            rtrim(char(value(c.chnl_count,0))) ||'个渠道疑似养卡，'||
            rtrim(char(value(d.chnl_count,0))) ||'个渠道酬金异常，请登录经分系统核查处理。',
           a.mobile
      from $stat_market_0135_d_s_lkp a
      left join 
           (
            select count(distinct channel_id) chnl_count
              from $stat_market_0135_a_final
             where warn_level in (1,2,3)
               and op_time = $year$month
           ) b on 1=1
      left join 
           (
            select count(distinct channel_id) chnl_count
              from $stat_market_0135_b_final
             where warn_level in (1,2,3)
               and op_time = $year$month
           ) c on 1=1
      left join 
           (
            select count(distinct channel_id) chnl_count
              from $stat_market_0135_c_final
             where warn_level in (1,2,3)
               and op_time = $year$month
           ) d on 1=1
     where a.city_id = '890';"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        #trace_sql $errmsg 1010
        puts "errmsg:$errmsg"
        return -1
    }

    ##分地市短信
    set sql_buf "
    insert into app.sms_send_info
    (
           message_content,
           mobile_num
    )
    select '您好，'||
            '$year'||'年'||
            rtrim(char(int($month))) ||'月你分公司有'|| 
            rtrim(char(value(b.chnl_count,0))) ||'个渠道疑似窜卡，'||
            rtrim(char(value(c.chnl_count,0))) ||'个渠道疑似养卡，'||
            rtrim(char(value(d.chnl_count,0))) ||'个渠道酬金异常，请登录经分系统核查处理。',
           a.mobile
      from $stat_market_0135_d_s_lkp a
      left join 
           (
            select city_id,
                   count(distinct channel_id) chnl_count
              from $stat_market_0135_a_final
             where warn_level in (1,2,3)
               and op_time = $year$month
             group by 
                   city_id
           ) b on a.city_id = b.city_id
      left join 
           (
            select city_id,
                   count(distinct channel_id) chnl_count
              from $stat_market_0135_b_final
             where warn_level in (1,2,3)
               and op_time = $year$month
             group by 
                   city_id
           ) c on a.city_id = c.city_id
      left join 
           (
            select city_id,
                   count(distinct channel_id) chnl_count
              from $stat_market_0135_c_final
             where warn_level in (1,2,3)
               and op_time = $year$month
             group by 
                   city_id
           ) d on a.city_id = d.city_id
     where a.city_id <> '890';"
    puts $sql_buf
    if [catch {aidb_sql $handle $sql_buf} errmsg] {
        #trace_sql $errmsg 1020
        puts "errmsg:$errmsg"
        return -1
    }

    return 0
}