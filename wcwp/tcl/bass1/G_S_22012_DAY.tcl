######################################################################################################
#接口名称：日KPI
#接口编码：22012
#接口说明：记录日KPI信息
#程序名称: G_S_22012_DAY.tcl
#功能描述: 生成22012的数据
#运行粒度: 日
#源    表：1.bass2.dw_acct_shoulditem_yyyymmdd--
#          2.bass2.dw_comp_all_dt--
#          3.bass2.Dw_comp_cust_dt--
#          4.bass2.dw_product_yyyymmdd --
#          5.bass2.dw_comp_all_yyyymmdd--
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.因为目前竞争对手这快的资料不能从短信出，所以数据不准确，该问题目前无法解决。
#          2.该接口程序因为用到了dt，所以不能重跑以前的数据。
#修改历史: 20090422 修改了当月日军语音用户数统计算法 夏华学  
#修改历史：20090708 修改当日收费用户数统计口径 去掉 usertype_id=8 的集团虚拟用户
#          20090902 1.6.2规范新增几个字段(新增客户数、客户到达数等)
#          20090928 当月收费用户数上报暂时为0
#          20091022 1.6.3规范修改(去掉'当月收费用户数'指标等)
#          20091123 修改上网本口径 a.apn_ni not in ('CMTDS') 为drtype_id not in (8307)
#          20100120 修改数据卡也算新增客户数，剔除条件crm_brand_id2<>70 修改客户到达数据、在网客户数的口径 userstatus_id in (1,2,3,6,8)
#          20111231 1.短信计费量（不含行业网关短信） 2.行业网关短信计费量
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	      #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #当天 yyyy-mm-dd
        set optime $op_time
        #今天的日期，格式dd(例：输入20070411 返回11)
        set today_dd [string range $op_time 8 9]
        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]
        #上月 YYYYMM
        set last_month [GetLastMonth [string range $op_month 0 5]]
        #上月最后一天
        set last_month_last_day [GetLastDay [string range $op_month 0 5]01]
        puts $last_month_last_day        

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_22012_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

###
###	#02:当月收费用户数
###    set handle [aidb_open $conn]
###    set sql_buff "select
###            			count(distinct a.user_id)
###			            from  bass2.dw_acct_shoulditem_$timestamp a
###			              inner join 
###			              (
###		                 select user_id from bass2.dw_product_$timestamp where userstatus_id<>0 and usertype_id in (1,2,9)
###		                 except
###		                 select user_id from bass2.dw_product_${last_month_last_day} where userstatus_id in (0,4,5,7)
###		                 ) b
###			               on a.user_id=b.user_id
###			            with ur  
###             		"	               
###    puts $sql_buff
###    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace $errmsg 1001
###		return -1
###	}
###	if [catch {set M_BILL_USERS [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1002
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	puts "当月收费用户数:$M_BILL_USERS"
###

	#03:新增客户数
    set handle [aidb_open $conn]
    set sql_buff "select count(user_id) 
                    from bass2.dw_product_$timestamp
                   where usertype_id in (1,2,9) 
                     and day_new_mark = 1 and test_mark<>1
             		"	               
    puts $sql_buff
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set M_NEW_USERS [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "新增客户数为:$M_NEW_USERS"


	#04:客户到达数
    set handle [aidb_open $conn]
    set sql_buff "select count(user_id) 
                    from bass2.dw_product_$timestamp
                   where usertype_id in (1,2,9) 
                     and userstatus_id in (1,2,3,6,8)
                     and test_mark<>1
             		"	               
    puts $sql_buff
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set M_DAO_USERS [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "客户到达数为:$M_DAO_USERS"


	#05:计费时长
##    set handle [aidb_open $conn]
##    set sql_buff "select sum(bigint(a.call_duration_m)) 
##                   from  bass2.dw_call_$timestamp a,bass2.dw_product_$timestamp b 
##                  where a.user_id =  b.user_id
##                    and b.usertype_id in (1,2,9)
##                    and b.test_mark <>1 and b.crm_brand_id2<>70
##										and b.userstatus_id not in (2,4,5,7,8)
##             		"	     

    set handle [aidb_open $conn]
    set sql_buff "select sum(bigint(a.call_duration_m)) 
                   from  bass2.dw_call_$timestamp a,bass2.dw_product_$timestamp b 
                  where a.user_id =  b.user_id
                    and b.usertype_id in (1,2,9)
                    and b.test_mark <>1 and b.userstatus_id not in (4,5,7,9)
             		"	 
             		          
    puts $sql_buff
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set M_BILL_DURATION [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "计费时长为:$M_BILL_DURATION"


##	#06:移动数据流量
##    set handle [aidb_open $conn]
##    set sql_buff "select sum(bigint(a.upflow1+a.upflow2+a.downflow1+a.downflow2))/1024/1024 
##                    from bass2.dw_newbusi_gprs_$timestamp a,bass2.dw_product_$timestamp b 
##                   where a.user_id = b.user_id
##                     and b.usertype_id in (1,2,9) and b.userstatus_id in (1,2,3,6) 
##             		"	               
##    puts $sql_buff
##    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace $errmsg 1001
##		return -1
##	}
##	if [catch {set M_DATA_FLOWS [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1002
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	puts "移动数据流量为:$M_DATA_FLOWS"


	#06:移动数据流量(去除上网本流量)
##    set handle [aidb_open $conn]
##    set sql_buff "select sum(bigint(a.upflow1+a.upflow2+a.downflow1+a.downflow2))/1024/1024 
##                    from bass2.dw_newbusi_gprs_$timestamp a,bass2.dw_product_$timestamp b 
##                   where a.user_id = b.user_id
##                     and b.usertype_id in (1,2,9)
##					 and b.test_mark <>1 
##					 and b.crm_brand_id2<>70
##					 and b.userstatus_id not in (2,4,5,7,8)
##                     and a.drtype_id not in (8307)
##             		"
##
    set handle [aidb_open $conn]
    set sql_buff "select sum(bigint(a.upflow1+a.upflow2+a.downflow1+a.downflow2))/1024/1024 
                    from bass2.dw_newbusi_gprs_$timestamp a,bass2.dw_product_$timestamp b 
                   where a.user_id = b.user_id
                     and b.usertype_id in (1,2,9)
					 and b.test_mark <>1 
					 and b.userstatus_id not in (4,5,7,9)
                     and a.drtype_id not in (8307)
             		"

    puts $sql_buff
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set M_DATA_FLOWS [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "移动数据流量为:$M_DATA_FLOWS"


	#07:短信计费量
	#--点对点短信
	#--移动秘书(语音)(12580)、全球呼自动秘书、呼转短信、音信互动(语音杂志本地)(12590)、语音短信(语音清单)
	#--集团客户短信(梦网下行话单)#--梦网短信计费量（包含‘音信互动短信计费量’）
	
##    set handle [aidb_open $conn]
##    set sql_buff "
##		    select sum(cnts) from 
##		    (
##					 select value(sum(counts),0) cnts from bass2.dw_newbusi_sms_$timestamp  a,bass2.dw_product_$timestamp b 
##						where a.user_id=b.user_id
##						  and  b.usertype_id in (1,2,9) and b.userstatus_id in (1,2,3,6) 
##						  and  a.calltype_id=0
##						  and  a.final_state=1
##						union all
##						select value(sum(counts),0) cnts from  bass2.dw_newbusi_call_$timestamp a,bass2.dw_product_$timestamp b 
##						where  a.user_id=b.user_id
##						  and b.usertype_id in (1,2,9) and b.userstatus_id  in (1,2,3,6) 
##						  and a.calltype_id=0
##						  and a.svcitem_id in  (100003,100021,100025,100026,100027)
##						union  all
##						select value(sum(counts),0) cnts from bass2.dw_newbusi_ismg_$timestamp  a,bass2.dw_product_$timestamp b 
##						where a.user_id=b.user_id
##						  and  b.usertype_id in (1,2,9) and b.userstatus_id in (1,2,3,6) 
##						  and  a.calltype_id=0
##						  and  a.send_state='0'
##						  and  a.svcitem_id in (300009) 
##						union all
##						select value(sum(counts),0) cnts from  bass2.dw_newbusi_ismg_$timestamp a,bass2.dw_product_$timestamp b 
##							where a.user_id  = b.user_id
##							  and b.usertype_id in (1,2,9) and b.userstatus_id in  (1,2,3,6) 
##							  and a.record_type in (0,1,10,11)
##							  and a.send_state='0'
##					) as aa
##           "	               
##    puts $sql_buff
##    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace $errmsg 1001
##		return -1
##	}
##	if [catch {set M_BILL_SMS [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1002
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	puts "短信计费量为:$M_BILL_SMS"
##


# 保持和21007/04005/04014接口一致
#1. meng wang  substr(a.sp_code,1,12) is not null
#2. 语音杂志短信话单   ('12590')       
#3.集团短信
#4.移动秘书、全球呼呼转短信、语音短信
    set handle [aidb_open $conn]
    set sql_buff "
		    select sum(cnts) from 
		    (
						select value(sum(counts),0) cnts from bass2.dw_newbusi_ismg_$timestamp  a,bass2.dw_product_$timestamp b 
						where a.user_id=b.user_id
						  and b.usertype_id in (1,2,9)
						  and b.test_mark <>1 
						  and b.userstatus_id not in (4,5,7,9)
						  and a.calltype_id in (0,1,10,11)
						  and a.send_state='0'
						  and a.drtype_id<>61102 
						  and substr(a.sp_code,1,12) is not null
						  and a.svcitem_id in (300001,300002,300003,300004)
					union all		
					 select value(sum(counts),0) cnts from bass2.dw_newbusi_sms_$timestamp  a,bass2.dw_product_$timestamp b 
						where a.user_id=b.user_id
						  and b.usertype_id in (1,2,9)
						  and b.test_mark <>1
						  and b.userstatus_id not in (4,5,7,9)
						  and  a.calltype_id in (0)
						  and  a.final_state=1
					) as aa
           "	               
    puts $sql_buff
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set M_BILL_SMS [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "短信计费量为:$M_BILL_SMS"

#2011.12.31 行业网关短信计费量
   set handle [aidb_open $conn]
    set sql_buff "
					select value(count(0),0) cnts from    G_S_04016_DAY 
					where time_id = $timestamp 
					and RECORD_TYPE in ('00','01','10','11')
					and SEND_STATUS = '0'
           "	               
    puts $sql_buff
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set M_BILL_HANGYE_SMS [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "行业网关短信计费量为:$M_BILL_HANGYE_SMS"


	#08:离网客户数
    set handle [aidb_open $conn]
    set sql_buff "select count(user_id) 
                    from bass2.dw_product_$timestamp  
                   where usertype_id in (1,2,9) 
                     and day_off_mark = 1 
                     and userstatus_id not in (1,2,3,6,8)
                     and test_mark<>1
             		 "	               
    puts $sql_buff
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set M_OFF_USERS [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "离网客户数为:$M_OFF_USERS"


	#09:彩信计费量
    set handle [aidb_open $conn]
    set sql_buff "
        select sum(cnts) from 
            (
             select sum(counts) cnts from bass2.dw_newbusi_mms_$timestamp a,bass2.dw_product_$timestamp b 
							where  a.user_id  = b.user_id
							  and b.usertype_id in (1,2,9)
							  and b.test_mark <>1 
							  and b.userstatus_id not in (4,5,7,9)
							  and a.mm_type=0
							  and a.app_type in (0)
							  and a.send_status in (0,1,2,3)
						union all
						select sum(counts) cnts from bass2.dw_newbusi_mms_$timestamp a,bass2.dw_product_$timestamp b 
							where  a.user_id  = b.user_id
							  and b.usertype_id in (1,2,9)
							  and b.test_mark <>1 
							  and b.userstatus_id not in (4,5,7,9)
							  and a.app_type in (1,2,3,4)
						) as aa
            "	               
    puts $sql_buff
    if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set M_BILL_MMS [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "彩信计费量为:$M_BILL_MMS"


  #指标全部入库
  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_22012_day values
	             (
	              $timestamp
	              ,'$timestamp'
                ,'$M_NEW_USERS'
                ,'$M_DAO_USERS'
                ,'$M_BILL_DURATION'
                ,'$M_DATA_FLOWS'
                ,'$M_BILL_SMS'
                ,'$M_BILL_HANGYE_SMS'
                ,'$M_OFF_USERS'
                ,'$M_BILL_MMS'
	             ) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}             
##############################################
	return 0      
}                     
