
######################################################################################################		
#接口名称: 渠道缴费记录                                                               
#接口编码：22094                                                                                          
#接口说明：记录用户在实体渠道及电子渠道缴费详单信息。
#程序名称: G_S_22094_DAY.tcl                                                                            
#功能描述: 生成22094的数据
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
		set app_name "G_S_22094_DAY.tcl"        


    #删除正式表本月数据
    set sql_buff "delete from bass1.G_S_22094_DAY where time_id=$timestamp"
    exec_sql $sql_buff

		set sql_buff "
		insert into G_S_22094_DAY
		(
         TIME_ID
        ,CHRG_DT
        ,CHRG_TM
        ,MSISDN
        ,CHNL_ID
        ,CHRG_TYPE
        ,CHRG_AMT		
		)
		select 
			$timestamp time_id
			,'$timestamp'  CHRG_DT
			,replace(substr(char(PEER_DATE),12,8),'.','')  CHRG_TM
			,key_num MSISDN
			,case when opt_code = '4464' then 'BASS1_ST' else char(staff_org_id) end channel_id
			,case 
				when CERTIFICATE_TYPE = '0' then '1'
				when CERTIFICATE_TYPE = '1' then '2'
				when opt_code = '4205' then '3' 
				else '4' end CHRG_TYPE
			,char(bigint(amount)) CHRG_AMT
		from BASS2.dw_acct_payment_dm_$curr_month a
		where  replace(char(a.OP_TIME),'-','') = '$timestamp' 
			and opt_code not in (select paytype_id from bass2.dim_acct_paytype where paytype_name like '%空中充值%')
			and lower(key_num) not like 'd%'
			and opt_code not in ('4464','4864','4468','SJJF2','4115')
			and length(key_num)  = 11 
		with ur
		"
	
exec_sql $sql_buff

##~   电子渠道 渠道差异问题_西藏.xls

		set sql_buff "
		insert into G_S_22094_DAY
		(
         TIME_ID
        ,CHRG_DT
        ,CHRG_TM
        ,MSISDN
        ,CHNL_ID
        ,CHRG_TYPE
        ,CHRG_AMT		
		)		
 select 		
			$timestamp TIME_ID
			,'$timestamp'  CHRG_DT
			,replace(substr(char(PEER_DATE),12,8),'.','')  CHRG_TM
			,key_num MSISDN
			,case when b.opt_code in ('4464','4864') then 'BASS1_ST' 
					 when b.opt_code in ('4468') then  'BASS1_WB'
					 else ' ' end channel_id
			,case 
				when CERTIFICATE_TYPE = '0' then '1'
				when CERTIFICATE_TYPE = '1' then '2'
				when opt_code = '4205' then '3' 
				else '4' end CHRG_TYPE
			,char(bigint(amount)) CHRG_AMT
		from  bass2.dw_product_$timestamp a
			, (select * from bass2.dw_acct_payment_dm_$curr_month where op_time = '$op_time') b
		where a.user_id=b.user_id 
		and b.opt_code in ('4464','4864','4468','SJJF2','4115')
		and (case when b.opt_code in ('4464','4864') then 'BASS1_ST' 
					 when b.opt_code in ('4468') then  'BASS1_WB'
					 else ' ' end
			) 
		in ('BASS1_ST','BASS1_WB')               
	with ur
		"
	
exec_sql $sql_buff



		##~   and OPT_CODE in 
					##~   ('4205' --手机支付-用户缴费
					##~   ,'4101'	--营业厅前台缴费
					##~   ,'4464' --自助服务终端现金缴费
					##~   )
					

  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_S_22094_DAY"
  ##~   set pk   "DEAL_DATE||CHANNEL_ID||DEAL_TYPE||IMP_VAL_TYPE"
        ##~   chkpkunique ${tabname} ${pk} ${timestamp}
        ##~   #
  aidb_runstats bass1.$tabname 3
  
	return 0

}




##~   select *
##~   from 
##~   (
##~   select OPT_CODE paytype_id, count(0) cnt 
##~   --,  count(distinct OPT_CODE ) 
##~   from BASS2.dw_acct_payment_dm_201202 
##~   where staff_org_id = 11111144
##~   group by  OPT_CODE 
##~   ) t , bass2.DIM_ACCT_PAYTYPE b 
##~   where t.paytype_id = b.paytype_id 

##~   PAYTYPE_ID	CNT	PAYTYPE_ID	PAYTYPE_NAME	STS	   
##~   4101	968	4101	营业厅前台缴费	1	   
##~   4114	19	4114	预付费退费	1	   
##~   4118	198	4118	过户预付费转移	1	   
##~   4178	198	4178	过户预付费转移出	1	   
##~   4408	19	4408	营业撤单退费	1	   
##~   4464	7474	4464	自助服务终端现金缴费	1	   
##~   4465	6	4465	自助服务终端现金缴费撤单	1	   
##~   4801	69	4801	[跨区]前台缴费	1	   
##~   4864	988	4864	[跨区]自助服务终端现金缴费	1	   
##~   GJFBG	858	GJFBG	产品变更缴费预存	1	   
##~   GJFF	14	GJFF	内部代理商一级帐户缴费	1	   
##~   GJFFZ	1	GJFFZ	复装缴费预存	1	   
##~   GJFKH	523	GJFKH	开户缴费预存	1	   
##~   GJFL	91	GJFL	转账出	1	   
##~   GJFM	91	GJFM	转账入	1	   
##~   GTFBG	11	GTFBG	产品变更缴费预存回滚	1	   
##~   GTFKH	5	GTFKH	开户缴费预存回滚	1	   
					


##~   CERTIFICATE_TYPE 凭证类型
##~   0-现金
##~   1-记录支票编号，
##~   2-记录银行卡号，
##~   3-记录代金券编号，
##~   4-记录记帐凭证编号，
##~   5-托单编号 
##~   6-充值卡号 
##~   7-缴费卡


##~   属性编码	属性名称	属性描述	属性类型	备注
##~   00		记录行号	唯一标识记录在接口数据文件中的行号。	number(8)	
##~   01		缴费日期	格式YYYYMMDD	CHAR(8)	
##~   02		缴费时间	格式HH24MISSS	CHAR(6)	
##~   03		MSISDN	被缴费的手机号码	CHAR(15)	不允许为空
##~   04		渠道标识	若缴费渠道为实体渠道，参见【实体渠道基础信息（日增量）】接口中的“实体渠道标识”属性
##~   若缴费渠道为电子渠道，按如下规则填写：网站、热线、短信、wap、自助终端电子渠道分别对应填写'BASS1_WB', 'BASS1_HL', 'BASS1_SM', 'BASS1_WP', 'BASS1_ST' (字符区分大小写)	CHAR(40)	不允许为空
##~   05		缴费类型	此字段仅取以下分类：
##~   1：现金；
##~   2：银行卡；
##~   3：手机支付；	NUMBER(1)	不允许为空
##~   06		缴费金额	单位：元
##~   注：不包括空中充值缴费金额；	NUMBER(8)	
	##~   0x0D0A	回车换行符		



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
					
					