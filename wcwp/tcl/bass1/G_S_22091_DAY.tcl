######################################################################################################
#接口名称：实体渠道业务办理日汇总
#接口编码：22091
#接口说明：记录实体渠道业务办理日汇总信息，涉及自营厅、委托经营厅、24小时自助营业厅或社会代理网点
#程序名称: G_S_22091_DAY.tcl
#功能描述: 生成22091的数据
#运行粒度: 月
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2011-06-08
#问题记录：
#修改历史: ref:22062
# 2011.12.01 将用组织机构编码记录的办理转成用channel_id记录的
# 2011.12.01 增加3个字段:3、修改接口22091（实体渠道业务办理日汇总）,增加属性“合约计划终端销售量”、“裸机终端销售量”、“集采终端销售量”；
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#set dt_list { 2011-07-31	2011-07-30	2011-07-29	2011-07-28	2011-07-27	2011-07-26	2011-07-25	2011-07-24	2011-07-23	2011-07-22	2011-07-21	2011-07-20	2011-07-19	2011-07-18	2011-07-17	2011-07-16	2011-07-15	2011-07-14	2011-07-13	2011-07-12	2011-07-11	2011-07-10	2011-07-09	2011-07-08	2011-07-07	2011-07-06	2011-07-05	2011-07-04	2011-07-03	2011-07-02	2011-07-01  }
#set dt_list { 2011-06-30	2011-06-29	2011-06-28	2011-06-27	2011-06-26	2011-06-25	2011-06-24	2011-06-23	2011-06-22	2011-06-21	2011-06-20	2011-06-19	2011-06-18	2011-06-17	2011-06-16	2011-06-15	2011-06-14	2011-06-13	2011-06-12	2011-06-11	2011-06-10	2011-06-09	2011-06-08	2011-06-07	2011-06-06	2011-06-05	2011-06-04	2011-06-03	2011-06-02	2011-06-01  }

#set dt_list { 2011-08-13	2011-08-14	2011-08-15	2011-08-16	2011-08-17	2011-08-18 }
#set dt_list { 2011-07-31	2011-07-30	2011-07-29	2011-07-28 2011-07-20	2011-08-01	2011-08-02	2011-08-03	2011-08-04	2011-08-05	2011-08-06	2011-08-07	2011-08-08	2011-08-09	2011-08-10	2011-08-11	2011-08-12 2011-08-13 }
# 通过 foreach 循环
#
#	foreach dt ${dt_list} {
#		set op_time $dt
#		puts $op_time
#		p22091 $op_time $optime_month
#	}
#

# 通过 while 循环
# set i 0 设置重跑日期上限 0 为 昨日
#	set i 1
# 设置重跑日期下限 , $i<= n   ,  n 越大，越久远
#	while { $i<=100 } {
#	        set sql_buff "select char(current date - ( 1+$i ) days) from bass2.dual"
#	        set op_time [get_single $sql_buff]
#	
#	if { $op_time >= "2011-06-01" } {
#	puts $op_time
#	p22091 $op_time $optime_month
#	
#	}
#	incr i
#	}
#	
#set op_time 2011-11-13
	p22091 $op_time $optime_month

return 0

}

proc p22091 { op_time optime_month } {
#set op_time 2011-06-07
   #当天 yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
		
    #当天 yyyy-mm-dd
    set optime $op_time

    #本月 yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month

    #本月第一天 yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day
		#自然月
		set curr_month [string range $op_time 0 3][string range $op_time 5 6]
    #本月最后一天 yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day
		global app_name
		set app_name "G_S_22091_DAY.tcl"        


    #删除正式表本月数据
    set sql_buff "delete from bass1.G_S_22091_DAY where time_id=$timestamp"
    exec_sql $sql_buff

	set sql_buf "ALTER TABLE BASS1.G_S_22091_DAY_TMP_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"

    exec_sql $sql_buf


    # 1 插入临时表   新增客户数
    set sql_buff "
			insert into  G_S_22091_DAY_TMP_1
          (  	CHANNEL_ID
						, ACCEPT_TYPE
						, NEW_USERS
						, HAND_CNT
						, HAND_FEE
						, CARD_SALE_CNT
						, CARD_SALE_FEE
						, ACCEPT_CNT
						, VAL_OPEN_CNT
						, IMP_ACCEPT_CNT
						, TERM_SALE_CNT
						, OTHER_SALE_CNT
						, ACCEPT_BAS_CNT
						, QUERY_BAS_CNT
						, OFF_ACCEPT_CNT    )
			select channel_id
						,'1'
						,count(1)
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
			from bass2.dw_product_$timestamp
			where DAY_NEW_MARK = 1
			group by Channel_ID
			"
    exec_sql $sql_buff

    # 2 插入临时表  缴费笔数 缴费金额 
    set sql_buff "
			insert into  G_S_22091_DAY_TMP_1
          (  	CHANNEL_ID
						, ACCEPT_TYPE
						, NEW_USERS
						, HAND_CNT
						, HAND_FEE
						, CARD_SALE_CNT
						, CARD_SALE_FEE
						, ACCEPT_CNT
						, VAL_OPEN_CNT
						, IMP_ACCEPT_CNT
						, TERM_SALE_CNT
						, OTHER_SALE_CNT
						, ACCEPT_BAS_CNT
						, QUERY_BAS_CNT
						, OFF_ACCEPT_CNT   )
      select staff_org_id
						,case when opt_code in ('4464','4465','4864','4865') then '2' else '1' end
						,0
						,count(distinct payment_id)
						,sum(amount)
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
			from BASS2.dw_acct_payment_dm_$curr_month a 
			where a.OP_TIME = '$op_time'
			and a.opt_code not in (select paytype_id from bass2.dim_acct_paytype where paytype_name like '%空中充值%')
			and lower(key_num) not like 'd%'
			and length(key_num)  = 11 
			group by staff_org_id
							,case when opt_code in ('4464','4465','4864','4865') then '2' else '1' end
			"
    exec_sql $sql_buff

#2011-05-04 19:15:49

    # 2 插入临时表  充值卡销售张数 充值卡销售金额
    set sql_buff "
			insert into  G_S_22091_DAY_TMP_1
          (  	CHANNEL_ID
						, ACCEPT_TYPE
						, NEW_USERS
						, HAND_CNT
						, HAND_FEE
						, CARD_SALE_CNT
						, CARD_SALE_FEE
						, ACCEPT_CNT
						, VAL_OPEN_CNT
						, IMP_ACCEPT_CNT
						, TERM_SALE_CNT
						, OTHER_SALE_CNT
						, ACCEPT_BAS_CNT
						, QUERY_BAS_CNT
						, OFF_ACCEPT_CNT   )
      select 
            CHANNEL_ID
						,'1'
						,0
						,0
						,0
						,count(0)
						,sum(value(card_value,0)/100)
						,0
						,0
						,0
						,0
						,0
						,0
						,0
						,0
			from  BASS2.dw_channel_local_busi_$timestamp
			where entity_type in(72,73 ) and rec_status=1
			and date(done_date) = '$op_time'
			group by CHANNEL_ID
			"
    exec_sql $sql_buff

#
    # 3 插入临时表 ACCEPT_CNT   增值业务办理笔数

    set sql_buff "
			insert into  G_S_22091_DAY_TMP_1
          (  	CHANNEL_ID
						, ACCEPT_TYPE
						, NEW_USERS
						, HAND_CNT
						, HAND_FEE
						, CARD_SALE_CNT
						, CARD_SALE_FEE
						, ACCEPT_CNT
						, VAL_OPEN_CNT
						, IMP_ACCEPT_CNT
						, TERM_SALE_CNT
						, OTHER_SALE_CNT
						, ACCEPT_BAS_CNT
						, QUERY_BAS_CNT
						, OFF_ACCEPT_CNT)
				select
				 a.org_id
				 			 ,case when a.channel_type='9' then '2' else '1' end accept_type
				       ,0
				       ,0
				       ,0
				       ,0
				       ,0			 
				 ,count(*)
				       ,0
				       ,0
				       ,0
				       ,0
				       ,0
				       ,0
				       ,0			
				  from bass2.dw_product_ord_cust_dm_$curr_month a,
				       bass2.dw_product_ord_offer_dm_$curr_month b,
				       bass2.dim_prod_up_product_item d,
				       bass2.dim_pub_channel e,
				       bass2.dim_sys_org_role_type f,
				       bass2.dim_cfg_static_data g,
				       bass2.Dim_channel_info h
				 where a.customer_order_id = b.customer_order_id
					and date(b.CREATE_DATE) = '$op_time'
				   and b.offer_id = d.product_item_id
				   and a.org_id = e.channel_id
				   and a.channel_type = g.code_value
				   and g.code_type = '911000'
				   and b.offer_type=2
				   and e.channeltype_id = f.org_role_type_id
				   and a.org_id = h.channel_id
				   and h.channel_type_class in (90105,90102)
				   and a.channel_type in ('b','9','5')
				 group by  a.org_id,
				         case when a.channel_type='9' then '2' else '1' end
			"
    exec_sql $sql_buff

## 20110821 补充 自助终端增值业务办理笔数


    set sql_buff "
			insert into  G_S_22091_DAY_TMP_1
          (  	CHANNEL_ID
						, ACCEPT_TYPE
						, NEW_USERS
						, HAND_CNT
						, HAND_FEE
						, CARD_SALE_CNT
						, CARD_SALE_FEE
						, ACCEPT_CNT
						, VAL_OPEN_CNT
						, IMP_ACCEPT_CNT
						, TERM_SALE_CNT
						, OTHER_SALE_CNT
						, ACCEPT_BAS_CNT
						, QUERY_BAS_CNT
						, OFF_ACCEPT_CNT)
				select
				 b.CHANNEL_ID
					,'2' accept_type
				       ,0
				       ,0
				       ,0
				       ,0
				       ,0			 
					,count(0)
				       ,0
				       ,0
				       ,0
				       ,0
				       ,0
				       ,0
				       ,0			
				 
            from  bass2.dw_product_$timestamp a,
             (select * from bass2.dw_product_busi_dm_$curr_month where op_time = '$op_time') b
            where a.user_id=b.user_id and b.so_mode='5' 
            and b.op_id in (select op_id from  bass2.dim_boss_staff where op_name like '%自助%')
            and b.process_id not in (2,3,22,14,24,12,13)
            group by b.CHANNEL_ID
			"
    exec_sql $sql_buff


#VAL_OPEN_CNT 增值业务开通量

source /bassapp/bass1/tcl/inc_G_S_22091_DAY.tcl
Deal_imp $op_time $optime_month

    set sql_buff "
			insert into  G_S_22091_DAY_TMP_1
          (  	CHANNEL_ID
						, ACCEPT_TYPE
						, NEW_USERS
						, HAND_CNT
						, HAND_FEE
						, CARD_SALE_CNT
						, CARD_SALE_FEE
						, ACCEPT_CNT
						, VAL_OPEN_CNT
						, IMP_ACCEPT_CNT
						, TERM_SALE_CNT
						, OTHER_SALE_CNT
						, ACCEPT_BAS_CNT
						, QUERY_BAS_CNT
						, OFF_ACCEPT_CNT)
				select
				 a.org_id
				 			 ,case when a.channel_type='9' then '2' else '1' end accept_type
				       ,0
				       ,0
				       ,0
				       ,0
				       ,0
				       ,0
				 ,count(distinct value(a.PRODUCT_INSTANCE_ID,'0') || value(char(b.OFFER_ID),'0') )
				       ,0
				       ,0
				       ,0
				       ,0
				       ,0
				       ,0
				  from bass2.dw_product_ord_cust_dm_$curr_month a,
				       bass2.dw_product_ord_offer_dm_$curr_month b,
				       bass2.dim_prod_up_product_item d,
				       bass2.dim_pub_channel e,
				       bass2.dim_sys_org_role_type f,
				       bass2.dim_cfg_static_data g,
				       bass2.Dim_channel_info h
				 where a.customer_order_id = b.customer_order_id
					and date(b.CREATE_DATE) = '$op_time'
				   and b.offer_id = d.product_item_id
				   and a.org_id = e.channel_id
				   and a.channel_type = g.code_value
				   and g.code_type = '911000'
				   and b.offer_type=2
				   and e.channeltype_id = f.org_role_type_id
				   and a.org_id = h.channel_id
				   and h.channel_type_class in (90105,90102)
				   and a.channel_type in ('b','9','5')
				 group by  a.org_id,
				         case when a.channel_type='9' then '2' else '1' end
			"
    exec_sql $sql_buff


#IMP_ACCEPT_CNT  重点增值业务开通量


    set sql_buff "
			insert into  G_S_22091_DAY_TMP_1
          (  	CHANNEL_ID
						, ACCEPT_TYPE
						, NEW_USERS
						, HAND_CNT
						, HAND_FEE
						, CARD_SALE_CNT
						, CARD_SALE_FEE
						, ACCEPT_CNT
						, VAL_OPEN_CNT
						, IMP_ACCEPT_CNT
						, TERM_SALE_CNT
						, OTHER_SALE_CNT
						, ACCEPT_BAS_CNT
						, QUERY_BAS_CNT
						, OFF_ACCEPT_CNT)
      select bigint(channel_id)
			       ,accept_type
			       ,0
			       ,0
			       ,0
			       ,0
			       ,0
			       ,0 
			       ,0
			       ,bigint(cnt) 
			       ,0
			       ,0
			       ,0
			       ,0
			       ,0
			from (select time_id, channel_id ,ACCEPT_TYPE, sum(cnt) cnt from BASS1.G_S_22091_DAY_P2 group by  time_id, channel_id ,ACCEPT_TYPE)  t
		 where TIME_ID=$timestamp
			"
    exec_sql $sql_buff



    # 4 插入临时表   定制终端销售笔数 其中定制手机销售笔数 办理类基础服务笔数 查询类基础服务笔数 套餐办理笔数
    set sql_buff "
			insert into  G_S_22091_DAY_TMP_1
          (   CHANNEL_ID
						, ACCEPT_TYPE
						, NEW_USERS
						, HAND_CNT
						, HAND_FEE
						, CARD_SALE_CNT
						, CARD_SALE_FEE
						, ACCEPT_CNT
						, VAL_OPEN_CNT
						, IMP_ACCEPT_CNT
						, TERM_SALE_CNT
						, OTHER_SALE_CNT
						, ACCEPT_BAS_CNT
						, QUERY_BAS_CNT
						, OFF_ACCEPT_CNT      )
			select  org_id
						,'1'
			      ,0
			      ,0
			      ,0
			      ,0
			      ,0
			      ,0
			      ,0
			      ,0
						,0
						--,sum( case when offer_name like '%购机%' then 1 else 0 end ) OTHER_SALE_CNT
						,0
						,sum( case when log_type=1 
						and busi_id in (191000000012,191000000013,191000000014,191000000015,191000000016,191000000060,191000000065,191000000072,191000001021) 
						then 1 else 0 end ) ACCEPT_BAS_CNT
						,sum( case when log_type<>1 then 1 else 0 end ) QUERY_BAS_CNT
						,sum( case when log_type=1 and busi_id in (193000000001,193000000002,191000000007,191000000008) then 1 else 0 end ) OFF_ACCEPT_CNT
			from BASS2.dw_product_ord_so_log_dm_$curr_month
			where op_time = '$op_time'
			group by org_id
		"
		
    exec_sql $sql_buff
    
    set sql_buff "
			insert into  G_S_22091_DAY_TMP_1
          (   CHANNEL_ID
						, ACCEPT_TYPE
						, NEW_USERS
						, HAND_CNT
						, HAND_FEE
						, CARD_SALE_CNT
						, CARD_SALE_FEE
						, ACCEPT_CNT
						, VAL_OPEN_CNT
						, IMP_ACCEPT_CNT
						, TERM_SALE_CNT
						, OTHER_SALE_CNT
						, ACCEPT_BAS_CNT
						, QUERY_BAS_CNT
						, OFF_ACCEPT_CNT      )
			select   bigint(a.org_id)
						,'1'
			      ,0
			      ,0
			      ,0
			      ,0
			      ,0
			      ,0
			      ,0
			      ,0
						,count(0) TERM_SALE_CNT
						,sum(case when TERM_TYPE in ('0','1') then 1 else 0 end ) OTHER_SALE_CNT
						,0
						,0
						,0
				 from    
				 bass2.dw_res_ctms_exchg_$timestamp a ,   BASS2.DIM_TERM_TAC b 
				where  a.sale_type like '%销售%'
				and substr(a.imei,1,8)  = b.TAC_NUM 
				and date(a.CREATE_DATE) = '$op_time'
							group by bigint(a.org_id)
		"

    exec_sql $sql_buff

## add : 20110823 增加 实体渠道 自助终端 办理 ：
## 1.  办理类基础服务笔数


    set sql_buff "
			insert into  G_S_22091_DAY_TMP_1
          (   CHANNEL_ID
						, ACCEPT_TYPE
						, NEW_USERS
						, HAND_CNT
						, HAND_FEE
						, CARD_SALE_CNT
						, CARD_SALE_FEE
						, ACCEPT_CNT
						, VAL_OPEN_CNT
						, IMP_ACCEPT_CNT
						, TERM_SALE_CNT
						, OTHER_SALE_CNT
						, ACCEPT_BAS_CNT
						, QUERY_BAS_CNT
						, OFF_ACCEPT_CNT
						)
			select   a.org_id
			      ,'2'
			      ,0
			      ,0
			      ,0
			      ,0
			      ,0
			      ,0
			      ,0
			      ,0
				,0
				,0
				,count(0)
				,0
				,0
			FROM bass2.DW_PRODUCT_ORD_CUST_DM_$curr_month  a
			where OP_ID in ( select OP_ID from  bass2.DIM_BOSS_STAFF  where op_name like '%自助终端%' )
			and date(a.op_time) = '$op_time'
			group by a.org_id
			 with ur
" 
  

    exec_sql $sql_buff




##2. 查询类基础服务笔数

    set sql_buff "
			insert into  G_S_22091_DAY_TMP_1
          (   CHANNEL_ID
						, ACCEPT_TYPE
						, NEW_USERS
						, HAND_CNT
						, HAND_FEE
						, CARD_SALE_CNT
						, CARD_SALE_FEE
						, ACCEPT_CNT
						, VAL_OPEN_CNT
						, IMP_ACCEPT_CNT
						, TERM_SALE_CNT
						, OTHER_SALE_CNT
						, ACCEPT_BAS_CNT
						, QUERY_BAS_CNT
						, OFF_ACCEPT_CNT
						)
			select   a.org_id
			      ,'2'
			      ,0
			      ,0
			      ,0
			      ,0
			      ,0
			      ,0
			      ,0
			      ,0
				,0
				,0
				,0
				,count(0)
				,0
			from    
	bass2.dw_product_ord_so_log_dm_$curr_month a
	, (
	  select product_item_id ,name from bass2.dim_prod_up_product_item where item_type='BUSINESS'
	and name like '%查询%'
	) b
	where BUSI_ID  = product_item_id
	and  OP_ID in ( select OP_ID from  bass2.DIM_BOSS_STAFF  where op_name like '%自助终端%' )
	and date(a.op_time) = '$op_time'
	group by a.org_id
	with ur
" 
  

    exec_sql $sql_buff

	set sql_buf "ALTER TABLE BASS1.G_S_22091_DAY_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"

    exec_sql $sql_buf


# 20110823 -- 加入 and a.channel_type <> 90886 条件 剔除 电子缴费
#     select * from    
#   bass2.Dim_channel_info  where channel_type = 90886

    ##
    #往正式表插入本月数据
    set sql_buff "
    insert into BASS1.G_S_22091_DAY_1
		(	 	
         TIME_ID
        ,DEAL_DATE
        ,CHANNEL_ID
        ,DEAL_TYPE
        ,NEW_USER_CNT
        ,PAYMENT_REC_CNT
        ,PAYMENT_REC_FEE
        ,CARD_SALE_CNT
        ,VAL_BUSI_REC_CNT
        ,VAL_BUSI_OPEN_CNT
        ,IMP_VAL_OPEN_CNT
        ,TERM_SALE_CNT
        ,MOBILE_SALE_CNT
        ,BASE_REC_CNT
        ,QRY_REC_CNT				
			)
	SELECT
	   $timestamp
	 	,'$timestamp'
	 	,trim(char(a.CHANNEL_ID))
		,b.ACCEPT_TYPE
		,char( sum( b.NEW_USERS				))
		,char( sum( b.HAND_CNT        ))
		,char( sum( b.HAND_FEE        ))
		,char( sum( b.CARD_SALE_CNT   ))
		,char( sum( b.ACCEPT_CNT      ))
		,char( sum( b.VAL_OPEN_CNT    ))
		,char( sum( b.IMP_ACCEPT_CNT  ))
		,char( sum( b.TERM_SALE_CNT   ))
		,char( sum( b.OTHER_SALE_CNT  ))
		,char( sum( b.ACCEPT_BAS_CNT  ))
		,char( sum( b.QUERY_BAS_CNT   ))
	FROM bass2.Dim_channel_info A
	inner join G_S_22091_DAY_TMP_1 b on a.channel_id=b.channel_id
	WHERE A.CHANNEL_TYPE_CLASS IN (90105,90102)
	and a.channel_type <> 90886
	group by trim(char(a.CHANNEL_ID))
		,b.ACCEPT_TYPE
	"
    exec_sql $sql_buff

### 进一步处理  organize_id & channel_id  转换的问题 ！


    ##
    #往正式表插入本月数据
    #2011.12.01 将用组织机构编码记录的办理转成用channel_id记录的
    set sql_buff "
    insert into BASS1.G_S_22091_DAY_1
		(	 	
         TIME_ID
        ,DEAL_DATE
        ,CHANNEL_ID
        ,DEAL_TYPE
        ,NEW_USER_CNT
        ,PAYMENT_REC_CNT
        ,PAYMENT_REC_FEE
        ,CARD_SALE_CNT
        ,VAL_BUSI_REC_CNT
        ,VAL_BUSI_OPEN_CNT
        ,IMP_VAL_OPEN_CNT
        ,TERM_SALE_CNT
        ,MOBILE_SALE_CNT
        ,BASE_REC_CNT
        ,QRY_REC_CNT				
			)
	SELECT
	   $timestamp
	 	,'$timestamp'
	 	,trim(char(a.CHANNEL_ID))
		,b.ACCEPT_TYPE
		,char( sum( b.NEW_USERS	))
		,char( sum( b.HAND_CNT        ))
		,char( sum( b.HAND_FEE        ))
		,char( sum( b.CARD_SALE_CNT   ))
		,char( sum( b.ACCEPT_CNT      ))
		,char( sum( b.VAL_OPEN_CNT    ))
		,char( sum( b.IMP_ACCEPT_CNT  ))
		,char( sum( b.TERM_SALE_CNT   ))
		,char( sum( b.OTHER_SALE_CNT  ))
		,char( sum( b.ACCEPT_BAS_CNT  ))
		,char( sum( b.QUERY_BAS_CNT   ))
	FROM bass2.Dim_channel_info A
	inner join G_S_22091_DAY_TMP_1 b on a.organize_id = b.channel_id 
	--假设G_S_22091_DAY_TMP_1有通过组织机构编码记录办理的，故用organize_id 关联
	WHERE A.CHANNEL_TYPE_CLASS IN (90105,90102)
	and a.channel_type <> 90886
	and char(b.channel_id) not in (select CHANNEL_ID from G_S_22091_DAY_1  where time_id = $timestamp )
	group by trim(char(a.CHANNEL_ID))
		,b.ACCEPT_TYPE
	with ur
	"
    exec_sql $sql_buff




# 进一步汇总
    set sql_buff "
    insert into BASS1.G_S_22091_DAY
		(	 	
         TIME_ID
        ,DEAL_DATE
        ,CHANNEL_ID
        ,DEAL_TYPE
        ,NEW_USER_CNT
        ,PAYMENT_REC_CNT
        ,PAYMENT_REC_FEE
        ,CARD_SALE_CNT
        ,VAL_BUSI_REC_CNT
        ,VAL_BUSI_OPEN_CNT
        ,IMP_VAL_OPEN_CNT
		--,TERM_SALE_CNT
        --,MOBILE_SALE_CNT
        ,BASE_REC_CNT
        ,QRY_REC_CNT	
	--2011.12.01 1.7.7 以下新增三个指标暂无法提取，报0
	--2012.08.01 1.8.2 以下新增8个指标暂无法提取，报0
        ,CONTRACT_DZ_SALE_ALLCNT
        ,CONTRACT_DZ_SALE_MOBCNT
        ,CONTRACT_FEIDZ_SALE_CNT
        ,CONTRACT_FEIDZ_SALE_MOBCNT
        ,NAKE_DZ_SALE_CNT
        ,NAKE_DZ_SALE_MOBCNT
        ,NAKE_FEIDZ_SALE_CNT
        ,NAKE_FEIDZ_SALE_MOBCNT			
	  )
	SELECT
	           TIME_ID
        ,DEAL_DATE
        ,CHANNEL_ID
        ,DEAL_TYPE
	,char(sum(bigint(NEW_USER_CNT))) NEW_USER_CNT
	,char(sum(bigint(PAYMENT_REC_CNT))) PAYMENT_REC_CNT
	,char(sum(bigint(PAYMENT_REC_FEE))) PAYMENT_REC_FEE
	,char(sum(bigint(CARD_SALE_CNT))) CARD_SALE_CNT
	,char(sum(bigint(VAL_BUSI_REC_CNT))) VAL_BUSI_REC_CNT
	,char(sum(bigint(VAL_BUSI_OPEN_CNT))) VAL_BUSI_OPEN_CNT
	,char(sum(bigint(IMP_VAL_OPEN_CNT))) IMP_VAL_OPEN_CNT
	--,char(sum(bigint(TERM_SALE_CNT))) TERM_SALE_CNT
	--,char(sum(bigint(MOBILE_SALE_CNT))) MOBILE_SALE_CNT
	,char(sum(bigint(BASE_REC_CNT))) BASE_REC_CNT
	,char(sum(bigint(QRY_REC_CNT))) QRY_REC_CNT
        ,char(sum(bigint(TERM_SALE_CNT)))  CONTRACT_DZ_SALE_ALLCNT
        ,char(sum(bigint(MOBILE_SALE_CNT)))  CONTRACT_DZ_SALE_MOBCNT
        ,'0'  CONTRACT_FEIDZ_SALE_CNT
        ,'0'  CONTRACT_FEIDZ_SALE_MOBCNT
        ,'0'  NAKE_DZ_SALE_CNT
        ,'0'  NAKE_DZ_SALE_MOBCNT
        ,'0'  NAKE_FEIDZ_SALE_CNT
        ,'0'  NAKE_FEIDZ_SALE_MOBCNT		
	from G_S_22091_DAY_1 a
	where time_id = $timestamp
	group by TIME_ID,DEAL_DATE,CHANNEL_ID,DEAL_TYPE
with ur
"
    exec_sql $sql_buff


  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_S_22091_DAY"
        set pk                  "DEAL_DATE||CHANNEL_ID||DEAL_TYPE"
        chkpkunique ${tabname} ${pk} ${timestamp}
        #
  
  set sql_buff "
  select count(0)
		from bass1.g_s_22091_day where TIME_ID = $timestamp
		and  channel_id not in 
			(
			select char(a.channel_id)
			from bass2.dim_channel_info a
			 where a.channel_type_class in (90105,90102)
			 and a.channel_type <> 90886
			)
  "
chkzero2 $sql_buff "invaid channel_id"

  aidb_runstats bass1.$tabname 3



	return 0
}

