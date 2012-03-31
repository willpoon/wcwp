
######################################################################################################		
#接口名称: 银行缴费记录                                                               
#接口编码：22093                                                                                          
#接口说明：记录银行代缴、代扣及托收费用信息。
#程序名称: G_S_22093_DAY.tcl                                                                            
#功能描述: 生成22093的数据
#运行粒度: DAY
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20120328
#问题记录：
#修改历史: 1. panzw 20120328	中国移动一级经营分析系统省级数据接口规范 (V1.7.9) 
#######################################################################################################   



proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
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
		set app_name "G_S_22093_DAY.tcl"        


    #删除正式表本月数据
    set sql_buff "delete from bass1.G_S_22093_DAY where time_id=$timestamp"
    exec_sql $sql_buff

		set sql_buff "
		insert into G_S_22093_DAY
		select 
			$timestamp time_id
			,'$timestamp'  CHRG_DT
			,replace(substr(char(PEER_DATE),12,8),'.','')  CHRG_TM
			,key_num MSISDN
			,case 
				when opt_code in ('4103','4144') then '01'
				when opt_code in ('4115') then '03'
			end CHRG_TYPE
			,char(bigint(AMOUNT)) CHRG_AMT
		from BASS2.dw_acct_payment_dm_$curr_month a
		where  replace(char(a.OP_TIME),'-','') = '$timestamp' 
		and OPT_CODE in ('4103','4144','4115')
		with ur
		"
exec_sql $sql_buff


  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_S_22093_DAY"
  ##~   set pk   "DEAL_DATE||CHANNEL_ID||DEAL_TYPE||IMP_VAL_TYPE"
        ##~   chkpkunique ${tabname} ${pk} ${timestamp}
        ##~   #
  aidb_runstats bass1.$tabname 3
  
	return 0

}




##~   #===================================================================================================
##~   #四、相关指标   　      　
##~   #客户总缴费金额                     元  CN4000  CN4001  CN4002  CN4003  CN4004
##~   #    其中：通过充值卡缴费总金额     元  CN4100  CN4101  CN4102  CN4103  CN4104
##~   #          通过银行代收缴费总金额         元    CN4200  CN4201  CN4202  CN4203  CN4204
##~   #          通过自办营业厅缴费总金额     元      CN4300  CN4301  CN4302  CN4303  CN4304
##~   #          其他渠道客户缴费总金额         元    CN4400  CN4401  CN4402  CN4403  CN4404

        ##~   set sql_buf01 "select
                             ##~   case when b.locntype_id = 1 then 1
                                  ##~   when b.locntype_id = 2 then 2
                                  ##~   when b.locntype_id = 3 then 3
                                  ##~   else 4 end,
                             ##~   case when a.PAYTYPE_ID in ('4158','4159') then 41
                                  ##~   when a.PAYTYPE_ID in ('4103','4144') then 42
                            ##~   when a.PAYTYPE_ID in ('4101','4801','4104') then 43
                            ##~   else 44 end,
                             ##~   sum(a.recv_cash) as pay_fee
                 ##~   from DW_ACCT_PAYITEM_${year}${month} a left outer join STAT_ZD_VILLAGE_USERS_${year}${month} b
                 ##~   on a.user_id=b.user_id
                 ##~   where a.rec_sts=0
                 ##~   group by
                       ##~   case when b.locntype_id = 1 then 1
                                  ##~   when b.locntype_id = 2 then 2
                                  ##~   when b.locntype_id = 3 then 3
                                  ##~   else 4 end,
                             ##~   case when a.PAYTYPE_ID in ('4158','4159') then 41
                                  ##~   when a.PAYTYPE_ID in ('4103','4144') then 42
                            ##~   when a.PAYTYPE_ID in ('4101','4801','4104') then 43
                            ##~   else 44 end"
							
							
							
##~   4238	银行代扣	1	   
##~   4239	银行代扣冲正	1	   



##~   PAYTYPE_ID	CNT	PAYTYPE_ID	PAYTYPE_NAME	STS	   
##~   4101	809877	4101	营业厅前台缴费	1	   
##~   4162	617675	4162	自助缴费	1	   
##~   4158	410067	4158	二卡合一	1	   
##~   GJFW	290236	GJFW	分摊预存帐务分摊	1	   
##~   4187	143526	4187	代理商资金转移	1	   
##~   GJFE	134889	GJFE	送预存[前台]	1	   
##~   4864	98921	4864	[跨区]自助服务终端现金缴费	1	   
##~   4801	86346	4801	[跨区]前台缴费	1	   
##~   GJFBG	54139	GJFBG	产品变更缴费预存	1	   
##~   4132	47610	4132	CBOSS缴费	1	   
##~   GJFKH	36585	GJFKH	开户缴费预存	1	   
##~   FPDJ	28851	FPDJ	税务局发票兑奖	1	   
##~   GJFY1	19186	GJFY1	邮政帐户缴费	1	   
##~   4118	9421	4118	过户预付费转移	1	   
##~   gJFG	9320	gJFG	跨区空中充值	1	   
##~   4178	9133	4178	过户预付费转移出	1	   
##~   4205	7065	4205	手机支付-用户缴费	1	   
##~   4104	4071	4104	冲正[前台缴费]	1	   
##~   4103	3427	4103	银行代收费	1	   
##~   1199	2663	1199	集团成员生日送话费	1	   
##~   GQT9	2397	GQT9	奖励送预存	1	   
##~   GJFM	1792	GJFM	转账入	1	   
##~   GJFL	1792	GJFL	转账出	1	   
##~   4188	1519	4188	代理商前台充值	1	   
##~   4108	1387	4108	缴费撤单	1	   
##~   4465	1119	4465	自助服务终端现金缴费撤单	1	   
##~   4114	729	4114	预付费退费	1	   
##~   4468	640	4468	网上缴费	1	   
##~   GTFBG	310	GTFBG	产品变更缴费预存回滚	1	   
##~   GTFKH	193	GTFKH	开户缴费预存回滚	1	   
##~   4865	175	4865	[跨区]自助服务终端现金缴费撤单	1	   
##~   4148	124	4148	CBOSS缴费冲正	1	   
##~   4208	96	4208	代理商资金转移冲正	1	   
##~   GTFY1	88	GTFY1	邮政帐户缴费返销	1	   
##~   4115	29	4115	托收缴费	1	   
##~   GJFFZ	18	GJFFZ	复装缴费预存	1	   
##~   GTFF	8	GTFF	内部代理商一级帐户缴费返销	1	   
##~   GJFg	6	GJFg	本机号码空中充值	1	   
##~   gTFG	4	gTFG	跨区空中充值返销	1	   
##~   4429	2	4429	代销商帐户取款	1	   
					
					