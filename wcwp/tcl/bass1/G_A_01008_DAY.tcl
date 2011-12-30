######################################################################################################
#接口名称：跨省跨国集团客户
#接口编码：01008
#接口说明：为支持一级经分进行全网跨省跨国集团客户业务分析，该接口上报集团客户标识与有限公司
#          BBOSS规定的跨省跨国集团客户编码间的对应关系。
#程序名称: G_A_01008_DAY.tcl
#功能描述: 生成01008的数据
#运行粒度: 日
#源    表：1
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: 

#20110821 using for scrpt testing 20110821
#######################################################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#set dt_list { 2011-07-31	2011-07-30	2011-07-29	2011-07-28	2011-07-27	2011-07-26	2011-07-25	2011-07-24	2011-07-23	2011-07-22	2011-07-21	2011-07-20	2011-07-19	2011-07-18	2011-07-17	2011-07-16	2011-07-15	2011-07-14	2011-07-13	2011-07-12	2011-07-11	2011-07-10	2011-07-09	2011-07-08	2011-07-07	2011-07-06	2011-07-05	2011-07-04	2011-07-03	2011-07-02	2011-07-01  }
set dt_list { 2011-06-30	2011-06-29	2011-06-28	2011-06-27	2011-06-26	2011-06-25	2011-06-24	2011-06-23	2011-06-22	2011-06-21	2011-06-20	2011-06-19	2011-06-18	2011-06-17	2011-06-16	2011-06-15	2011-06-14	2011-06-13	2011-06-12	2011-06-11	2011-06-10	2011-06-09	2011-06-08	2011-06-07	2011-06-06	2011-06-05	2011-06-04	2011-06-03	2011-06-02	2011-06-01  }


#set dt_list { 2011-08-13	2011-08-14	2011-08-15	2011-08-16	2011-08-17	2011-08-18 }
#set dt_list { 2011-07-31	2011-07-30	2011-07-29	2011-07-28 2011-07-20	2011-08-01	2011-08-02	2011-08-03	2011-08-04	2011-08-05	2011-08-06	2011-08-07	2011-08-08	2011-08-09	2011-08-10	2011-08-11	2011-08-12 2011-08-13 }


foreach dt ${dt_list} {
	set op_time $dt
	puts $op_time
	Deal22091 $op_time $optime_month
}

#Deal22091 $op_time $optime_month

return 0
}

proc Deal22091 { op_time optime_month } {
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

	set sql_buf "ALTER TABLE BASS1.G_S_22091_DAY_TMP_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"

    exec_sql $sql_buf


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
						,sum( case when offer_name like '%购机%' then 1 else 0 end )
						,sum( case when log_type=1 and busi_id in (191000000012,191000000013,191000000014,191000000015,191000000016,191000000060,191000000065,191000000072,191000001021) then 1 else 0 end )
						,sum( case when log_type<>1 then 1 else 0 end )
						,sum( case when log_type=1 and busi_id in (193000000001,193000000002,191000000007,191000000008) then 1 else 0 end )
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


    ##
    #往正式表插入本月数据
    set sql_buff "
    insert into BASS1.G_S_22091_DAY_T20110822
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
		,char( sum( b.NEW_USERS))
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
	group by trim(char(a.CHANNEL_ID))
					,b.ACCEPT_TYPE
	"
    exec_sql $sql_buff

	return 0
}