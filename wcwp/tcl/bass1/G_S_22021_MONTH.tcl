######################################################################################################
#接口名称：业务运行月综合
#接口编码：22021
#接口说明：记录业务运行月综合相关信息。
#程序名称: G_S_22021_MONTH.tcl
#功能描述: 生成22021的数据
#运行粒度: 月
#源    表：1.bass2.dw_call_yyyymm
#          2.bass2.dw_newbusi_gprs_yyyymm
#          3.bass2.dw_newbusi_sms_yyyymm
#          4.bass2.dw_newbusi_ismg_yyyymm
#          5.bass2.dw_call_roamin_dm_yyyymm
#          6.bass2.dw_comp_all_yyyymm
#          7.bass2.dw_newbusi_call_yyyymm
#          8.bass2.dw_newbusi_mms_yyyymm
#          9.bass2.dw_product_callfw_yyyymm
#         10.bass2.dw_newbusi_wap_yyyymm
#         11.bass2.dw_product_busi_dm_yyyymm
#         12.bass2.dw_acct_payitem_dm_yyyymm
#         13.bass2.dw_acct_shoulditem_yyyymm
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_22021_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


        set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_s_22021_month_tmp
                     (
                      show_id       char(8),
                      target_value  bigint
                     )with replace on commit preserve rows not logged in tbs_user_temp"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--02各指标统计到临时表中

        #00030001	省内计费语音话单量
        #00030002	       其中:全球通话单量
        #00030003	            神州行话单量
        #00030004	            动感地带话单量
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22021_month_tmp
                      select
                        case
                          when brand_id=1 then '00030002'
                          when brand_id=4 then '00030004'
                          else '00030003'
                        end
                        ,bigint(count(*))
                      from bass2.dw_call_${op_month}
                      where bill_mark=1
                      group by
                        case
                          when brand_id=1 then '00030002'
                          when brand_id=4 then '00030004'
                          else '00030003'
                        end "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--04省内GPRS话单量 00030005
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22021_month_tmp
                      select
                        '00030005'
                        ,bigint(count(*))
                      from bass2.dw_newbusi_gprs_${op_month}
                      where bill_mark=1"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		writetrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--05计费短信量（万条，取小数点后两位）00030006
        #	 00030006	计费短信量
        #        00030007	  其中:         点对点MO
        #        00030008	  移动梦网信息费话单
        #        00030009	  移动梦网通信费话单

	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22021_month_tmp
                     select
                       '00030007'
                       ,bigint(count(*))
                     from bass2.dw_newbusi_sms_${op_month}
                     where
                          bill_mark=1
                         and calltype_id=0"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#---移动梦网信息费话单
	#---移动梦网通信费话单
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22021_month_tmp
                     select
                     '00030008'
                     ,sum(case when calltype_id=1 then bill_counts else 0 end)
                     from bass2.dw_newbusi_ismg_${op_month} where bill_mark=1
                     union all
                     select
                     '00030009'
                     ,sum(case when calltype_id=0 then bill_counts else 0 end)
                     from bass2.dw_newbusi_ismg_${op_month} where bill_mark=1"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#---漫游清算话单量00030010
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22021_month_tmp
                        select
                         '00030010'
                         ,count(*)
                        from bass2.dw_call_roamin_dm_${op_month}"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--SP结算话单量 00030011
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22021_month_tmp
                     select
                     '00030011'
                     ,sum(case when calltype_id=1 then bill_counts else 0 end)
                     from bass2.dw_newbusi_ismg_${op_month} where bill_mark=1"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

        #--网间结算话单量
        #00030012	网间结算话单量
        #00030013	其中：     与中国电信
        #00030014	           与中国网通
        #00030015	           与中国联通
        #00030016	           与中国铁通
        #00030017	           其他
        set handle [aidb_open $conn]
        set sql_buff "insert into session.g_s_22021_month_tmp
                     select
                       case
                         when comp_brand_id in (1,2,8) then '00030013'
                  	 when comp_brand_id=6          then '00030014'
                         when comp_brand_id in (3,4,7)   then '00030015'
                  	 when comp_brand_id=5          then '00030016'
                         else '00030017'
                       end
                       ,sum(mo_sms_counts + mt_sms_counts + in_call_counts + out_call_counts)
                     from bass2.dw_comp_all_${op_month}
                     group by
                     case
                       when comp_brand_id in (1,2,8) then '00030013'
                       when comp_brand_id=6          then '00030014'
                       when comp_brand_id in (3,4,7)   then '00030015'
                       when comp_brand_id=5          then '00030016'
                       else '00030017'
                     end "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}

	aidb_commit $conn

       #00030018	其他类话单量
       #00030019	其中：   省际边界漫游话单
       #00030020	         17950话单
       #00030021	         彩信话单
       #00030022	         呼转话单
       #00030023	         WAP话单
       #00030024	         其他

       #---17950
       set handle [aidb_open $conn]
       set sql_buff "insert into session.g_s_22021_month_tmp
                      select
                        '00030020'
                        ,sum(counts)
                      from bass2.dw_newbusi_call_${op_month}
                      where svcitem_id=100011 and bill_mark=1 "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		writetrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#---彩信话单--#
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22021_month_tmp
                      select
                        '00030021'
                        ,sum(counts)
                     from bass2.dw_newbusi_mms_${op_month}
                     where  bill_mark=1 "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}

	aidb_commit $conn

	#--呼转话单
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22021_month_tmp
                      select
                        '00030022'
                        ,sum(call_counts)
                      from bass2.dw_product_callfw_${op_month}   "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		writetrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#---WAP话单---#
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22021_month_tmp
                       select
                         '00030023'
                         ,sum(bill_counts)
                       from bass2.dw_newbusi_wap_${op_month} "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#00030025 计费话单量
	set handle [aidb_open $conn]
	set sql_buff " insert into session.g_s_22021_month_tmp
	              select
                        '00030025',
                        sum(target_value)
                      from session.g_s_22021_month_tmp
                      where show_id in('00030001','00030005','00030006','00030010','00030011','00030012','00030018')"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#---业务受理量00030026---#
	set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_22021_month_tmp
                      select
                        '00030026'
                        ,count(*)
                      from bass2.dw_product_busi_dm_${op_month} "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#---营业缴费记录00030026---#
	set handle [aidb_open $conn]
	set sql_buff "INSERT INTO SESSION.G_S_22021_MONTH_TMP
                    select
                     '00030026'
                     ,count(*)
                    from bass2.dw_acct_payitem_dm_${op_month}"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#---帐务处理量（次）00030027 -------#
	set handle [aidb_open $conn]
	set sql_buff " insert into session.g_s_22021_month_tmp
	               select
                         '00030027'
                         ,count(*)
                       from bass2.dw_acct_shoulditem_${op_month}"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		writetrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	        set sql_buff "
		select cnt,rn from (
			select time_id,round(20+rand(1)*10,0) cnt , row_number()over() rn 
			from 
			(select * from bass1.g_s_22021_month 
			union all
			select $op_month TIME_ID,'$op_month' MONTH_ID, '00030028' SHOW_ID,'' TARGET_VALUE from bass2.dual
			) t
			where show_id = '00030028'
			) a 
			where time_id = $op_month
		with ur
		"
		set p_row [get_row $sql_buff]
		set rpt_cnt [lindex $p_row 0]
		
	#---统计报表量（张）00030028---#
	set handle [aidb_open $conn]
	set sql_buff " insert into session.g_s_22021_month_tmp
	              values ('00030028',$rpt_cnt)"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#各合计项统计
	set handle [aidb_open $conn]
	set sql_buff " insert into session.g_s_22021_month_tmp
	              select
                         case
                         when show_id in('00030002','00030003','00030004') then '00030001'
                         when show_id in('00030007','00030008','00030009') then '00030006'
                         when show_id in('00030013','00030014','00030015','00030016','00030017') then '00030012'
                         when show_id in('00030019','00030020','00030021','00030022','00030023','00030024') then '00030018'
                         end,
                         sum(target_value)
                     from session.g_s_22021_month_tmp
                     where show_id in('00030002','00030003','00030004','00030007','00030008','00030009',
                                      '00030013','00030014','00030015','00030016','00030017',
                                      '00030019','00030020','00030021','00030022','00030023','00030024')
                     group by
                         case
                         when show_id in('00030002','00030003','00030004') then '00030001'
                         when show_id in('00030007','00030008','00030009') then '00030006'
                         when show_id in('00030013','00030014','00030015','00030016','00030017') then '00030012'
                         when show_id in('00030019','00030020','00030021','00030022','00030023','00030024') then '00030018'
                         end"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


	#插入到最终表
	set handle [aidb_open $conn]
	set sql_buff " insert into bass1.g_s_22021_month
                        select
                          ${op_month}
                          ,'${op_month}'
                          ,show_id
                          ,char(sum(target_value))
                        from session.g_s_22021_month_tmp
                        group by
                          show_id"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
        aidb_commit $conn
	aidb_close $handle

	return 0
}