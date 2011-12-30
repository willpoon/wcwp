######################################################################################################
#接口单元名称：电子渠道登录信息
#接口单元编码：22067
#接口单元说明：记录各电子渠道登录的相关信息
#程序名称: G_S_22067_MONTH.tcl
#功能描述: 生成22067的数据
#运行粒度: 月
#源    表：
#1.  bass2.dw_acct_payment_dm
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzhiwei
#编写时间：2011-04-26
#问题记录：1.
#修改历史: 1. 1.7.2 规范
#20110820，根据集团调查，为保证和二经一致，采用二经口径。
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      #set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01] 
      set last_month [GetLastMonth [string range $op_month 0 5]]
      #set curr_month_first_day [string range $timestamp 0 5]01
      #puts $curr_month_first_day
      #yyyy--mm-dd
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      

        global app_name
        set app_name "G_S_22067_MONTH.tcl"
          
    #删除本期数据
	set sql_buf "delete from bass1.G_S_22067_MONTH where time_id=$op_month"
	exec_sql $sql_buf
#
	set sql_buf "ALTER TABLE BASS1.G_S_22067_MONTH_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf

#此字段仅取以下分类：
#01:门户网站
#02:10086电话营业厅
#03: 短信营业厅
#04:WAP网站
#05:自助终端（包括所有的自助终端，即包括实体渠道和24小时营业厅内布放的自助终端，还包括商场等场所独立摆放的自助终端。）
#

    #电子渠道登陆客户清单-------------
    #网站
#这里用二经的代码，代码有误：dw_product_ 不能用号码直接关联 光限制  a.userstatus_id>0 是不够的 , 这里改成 a.userstatus_id in (1,2,3,6,8)

    set sql_buff "
	    insert into G_S_22067_MONTH_1
					  (
							e_product_no
							,type
						)
				select distinct 
				  b.login_product_no,
				  '01'
             from  bass2.dw_product_$op_month a,bass2.dw_wcc_user_action_$op_month b
             where b.login_product_no=a.product_no and b.login_type=1 and b.is_success=1
                   and a.userstatus_id in (1,2,3,6,8)
		   and replace(char(date(b.login_time)),'-','') like '$op_month%'
	    "

    exec_sql $sql_buff    

    #短信
    set sql_buff "
	    insert into G_S_22067_MONTH_1
					  (
							e_product_no
							,type
						)
					select distinct 
					  a.phone_id,
					  '03'
			from  bass2.dw_kf_sms_cmd_receive_dm_$op_month a,
                            bass2.dw_product_$op_month b
                        where  a.phone_id = b.product_no 
				and b.userstatus_id in (1,2,3,6,8)
	    "

    exec_sql $sql_buff       

    
    #热线人工
    set sql_buff "
	    insert into G_S_22067_MONTH_1
					  (
							e_product_no
							,type
						)
					select distinct 
					  b.product_no,
					  '02'
	  	       from bass2.dw_custsvc_cps_log_call_dm_$op_month a,
	  	            bass2.dw_product_$op_month b
	  	        where a.tel_calling = b.product_no 
	  	                and a.tel_called = '10086'
				             and b.userstatus_id in (1,2,3,6)
	    "

    exec_sql $sql_buff   

#dw_wcc_user_action_yyyymm

    #自助终端
    set sql_buff "
	    insert into G_S_22067_MONTH_1
					  (
							e_product_no
							,type
						)
					select distinct 
					  b.user_id,
					  '05'
					from (select distinct product_instance_id
               from bass2.dw_product_ord_cust_dm_$op_month
                           where channel_type='9'
               union 
               select distinct user_id as product_instance_id
               from bass2.dw_acct_payitem_dm_$op_month
               where paytype_id in ('4162','4864') ) a,
              bass2.dw_product_$op_month b
         where a.product_instance_id = b.user_id
	    "

    exec_sql $sql_buff 


  aidb_runstats bass1.G_S_22067_MONTH_1 3

#wap 营业厅尚未开放 04
# 04 wap  参考二经
    set sql_buff "
	    insert into G_S_22067_MONTH_1
					  (
							e_product_no
							,type
						)
		select distinct a.product_instance_id
		,'04'
		from bass2.dw_product_ord_cust_dm_$op_month a,
		bass2.dw_product_ord_busi_dm_$op_month b,
		bass2.DW_PRODUCT_ORD_BUSI_OTHER_$op_month  c,
		   bass2.dw_product_$op_month d
		where a.customer_order_id = b.customer_order_id
		and b.busioper_order_id =c.busioper_order_id 
		and c.ext_6 like '%SRC=206%'   
		   and a.product_instance_id = d.user_id  
with ur
	    "

    exec_sql $sql_buff 
	  	 set sql_buf "insert into session.stat_market_channel_key_0133_tmp
	  	   select distinct a.product_instance_id
			,'03'
         from bass2.dw_product_ord_cust_dm_$op_month a,
                      bass2.dw_product_ord_busi_dm_$op_month b,
                      bass2.DW_PRODUCT_ORD_BUSI_OTHER_$op_month  c,
					           bass2.dw_product_$op_month d
          where a.customer_order_id = b.customer_order_id
                      and b.busioper_order_id =c.busioper_order_id 
                      and c.ext_6 like '%SRC=206%'   
					           and a.product_instance_id = d.user_id     
        "

#result
    set sql_buff "
	    insert into G_S_22067_MONTH
					  (
         TIME_ID
        ,OP_MONTH
        ,E_CHANNEL_TYPE
        ,LOGIN_CUST_CNT
						)
					select 
        $op_month TIME_ID
        ,'$op_month' OP_MONTH
        ,type E_CHANNEL_TYPE
        ,char(count(0)) LOGIN_CUST_CNT					
					from G_S_22067_MONTH_1
					group by type
	    "

    exec_sql $sql_buff 



  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_S_22067_MONTH"
  set pk   "OP_MONTH||E_CHANNEL_TYPE"
        chkpkunique ${tabname} ${pk} ${op_month}
        #
        
  aidb_runstats bass1.G_S_22067_MONTH 3


	return 0
}
