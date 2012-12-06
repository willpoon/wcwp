
######################################################################################################		
#接口名称: 社会渠道酬金及补贴信息                                                               
#接口编码：22062                                                                                          
#接口说明：记录社会渠道人工前台业务办理部分所产生的酬金及社会渠道的补贴、激励信息，其中社会渠道包括“委托经营厅”、“社会代理网点”；
#程序名称: G_S_22062_MONTH.tcl                                                                            
#功能描述: 生成22062的数据
#运行粒度: MONTH
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20111128
#问题记录：
#修改历史: 1. panzw 20111128	1.7.7 newly added
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
		##~   set optime_month 2012-05
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]   
        #上月 YYYYMM
        set last_month [GetLastMonth [string range $op_month 0 5]]
            
	 set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
	puts $this_month_first_day
	set this_month_last_day  [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
	puts $this_month_last_day
	global app_name
	set app_name "G_S_22062_MONTH.tcl"    

        #删除本期数据
	set sql_buff "delete from bass1.G_S_22062_MONTH where time_id=$op_month"
  exec_sql $sql_buff
  
	set sql_buff "
insert into 	 bass1.G_S_22062_MONTH 
select 
        $op_month TIME_ID
        ,'$op_month' OP_MONTH
        ,char(CHANNEL_ID) CHANNEL_ID
        --,char(bigint(sum(case when BUSI_NOTES like '%放号%'  then RESULT else 0 end))) NUM_ACT_REWARD
        --,char(bigint(sum(case when BUSI_NOTES like '%放号%'  then RESULT else 0 end))) NUM_SHLD_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%放号%' and MONTHS like '%当月%'  then RESULT else 0 end))) NUM_INIT_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%放号%' and MONTHS not like '%当月%'  then RESULT else 0 end))) NUM_DELAY_ACT_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%放号%' and MONTHS not like '%当月%'  then RESULT else 0 end))) NUM_DELAY_SHLD_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%前台缴费酬金%' 
				or BUSI_NOTES  like '%银行代收费酬金%'  
				or BUSI_NOTES  like '%营销活动老用户预存费酬金%'  
				or BUSI_NOTES  like '%邮政公司当月代收费酬金%'  
				then RESULT else 0 end))) DAISHOU_REWARD
        ,'0' TERM_AGREE_REWARD
        ,'0' TERM_ONLY_REWARD
        ,'0' TERM_CUSTOMIZE_REWARD
        ,'0' TERM_MOBILE_REWARD
        ,'0' TERM_JICAI_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%新业务-无线音乐俱乐部%' 
				or BUSI_NOTES  like '%新业务-彩铃%'
				then RESULT else 0 end))) VAL_TYPE1_REWARD
        ,char(bigint(sum(case when BUSI_NOTES like '%新业务-飞信功能%' 
				or BUSI_NOTES  like '%新业务-139手机邮箱%元版%'
				or BUSI_NOTES  like '%号簿管家%'
				then RESULT else 0 end))) VAL_TYPE2_REWARD
       /** 
	   ,char(bigint(sum(case when BUSI_NOTES like '%音乐随身听%' 
				or BUSI_NOTES  like '%歌曲下载%'
				or BUSI_NOTES  like '%12580生活播报%'
				or BUSI_NOTES  like '%无线体育俱乐部%'
				or BUSI_NOTES  like '%信息管家%'
				or BUSI_NOTES  like '%手机商界%'
				or BUSI_NOTES  like '%手机医疗%'
				or BUSI_NOTES  like '%手机地图%'
				or BUSI_NOTES  like '%手机导航%'
				or BUSI_NOTES  like '%快讯%'
				or BUSI_NOTES  like '%手机报%'
				or BUSI_NOTES  like '%手机阅读%'
				or BUSI_NOTES  like '%手机视频%'
				or BUSI_NOTES  like '%手机游戏%'
				or BUSI_NOTES  like '%手机电视%'
				then RESULT else 0 end))) VAL_TYPE3_REWARD
	  **/
	  ,char(bigint(sum(case when BUSI_NOTES like '新业务%' 
				and not (
				BUSI_NOTES like '%新业务-无线音乐俱乐部%' 
				or BUSI_NOTES  like '%新业务-彩铃%'
				or BUSI_NOTES like '%新业务-飞信功能%' 
				or BUSI_NOTES  like '%新业务-139手机邮箱%元版%'
				or BUSI_NOTES  like '%号簿管家%'				
				)
				then RESULT else 0 end))) VAL_TYPE3_REWARD
	,char(bigint(sum(case when BUSI_NOTES like '%移动应用商城%'  then RESULT else 0 end))) VAL_DIANBO
        ,'0' STORE_SUSIDY
        ,'0' SALE_ACTIVE_REWARD
        ,'0' ADD_REWARD
        ,'0' B_CLASS_REWARD
from bass2.stat_channel_reward_0019  a
where op_time = $op_month
and CHANNEL_TYPE in (90105,90102)
group by char(CHANNEL_ID)
with ur
"
  exec_sql $sql_buff

#删除非法CHANNEL_ID
##~   4.1割接前後數據不也一樣，用的表也不同

if { $op_month <= 201203 } {
	set TAB06035 "G_A_06035_DAY_OLD20120331"
} else {
	set TAB06035 "G_A_06035_DAY"
}

##~   set sql_buff "
##~   delete from G_S_22062_MONTH 
##~   where time_id = $op_month
##~   and channel_id  in (
		##~   select distinct channel_id from G_S_22062_MONTH where time_id = $op_month
		##~   except
		##~   select distinct channel_id
		##~   from
		##~   (
		##~   select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
		##~   from ${TAB06035} a
		##~   where time_id / 100 <= $op_month
		##~   ) t where t.rn =1  and CHNL_STATE = '1'
		##~   ) 
##~   "
  ##~   exec_sql $sql_buff
  
##~   来自22063
   set sql_buff "
				delete from (select * from bass1.g_s_22062_month  where time_id =$op_month) t 
				where channel_id not in (select distinct channel_id from bass1.g_i_06021_month where time_id =$op_month and channel_type<>'1')
			"
	
    exec_sql $sql_buff
    
	
  aidb_runstats bass1.G_S_22062_MONTH 3

  #1.检查chkpkunique
        set tabname "G_S_22062_MONTH"
        set pk                  "OP_MONTH||CHANNEL_ID"
        chkpkunique ${tabname} ${pk} ${op_month}
        
	
# 校验：
set sql_buff "
	select count(0) from (
		select distinct channel_id from G_S_22062_MONTH where time_id = $op_month
		except
		select distinct channel_id
		from
		(
		select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
		from ${TAB06035} a
		where time_id / 100 <= $op_month
		) t where t.rn =1  and CHNL_STATE = '1'
	) o
	with ur
"
chkzero2 $sql_buff "22062有非法channel_id! "

##~   先跑22063 少量误差，不调整

##~   set sql_buff "
	##~   select count(0)
		##~   from (
		##~   select TIME_ID ,channel_id
		##~   ,sum(bigint(FH_REWARD)) FH_REWARD
		##~   from G_S_22063_MONTH 
		##~   where time_id = $op_month
		##~   group by  TIME_ID ,channel_id
		##~   ) a ,
		##~   (
		##~   select 			
				 ##~   a.TIME_ID	
				##~   ,a.channel_id         
				##~   ,sum(bigint(NUM_ACT_REWARD))	NUM_ACT_REWARD
				##~   ,sum(bigint(NUM_DELAY_ACT_REWARD))			
				##~   ,sum(bigint(NUM_DELAY_ACT_REWARD)+bigint(NUM_ACT_REWARD))			
		##~   from ( select * from G_S_22062_MONTH where time_id = $op_month ) a, ( select * from G_I_06021_MONTH where time_id = $op_month )  b 		
		##~   where a.time_id = $op_month			
		##~   and a.channel_id = b.channel_id
		##~   and b.CHANNEL_STATUS = '1'
		##~   and CHANNEL_TYPE in ('2','3')
		##~   group by  a.time_id ,a.channel_id		
		##~   ) b 
		##~   where a.CHANNEL_ID = b.CHANNEL_ID
		##~   and  FH_REWARD <> NUM_ACT_REWARD
	##~   with ur
##~   "
##~   chkzero2 $sql_buff "22062 和 22063 放号酬金不一致! "

##~   update (select * from G_S_22063_MONTH where time_id = $op_month ) a
##~   set NUM_ACT_REWARD = ''


			set grade 2
	        set alarmcontent "检查渠道酬金报表0019数据是否为空或正常！"
	        WriteAlarm $app_name $op_month $grade ${alarmcontent}



return 0

}

#	
#	属性编码	属性名称	属性描述	属性类型	备注
#	01		月份	格式：YYYYMM	CHAR(6)	联合主键
#	02		社会渠道标识	参见【实体渠道基础信息（日增量）】接口中的“实体渠道标识”属性。	CHAR(40)	联合主键
#	03		卡号销售实发酬金	单位：元	NUMBER(10)	
#	04		卡号销售应发酬金	单位：元	NUMBER(10)	
#	05		卡号销售首付酬金	单位：元	NUMBER(10)	
#	06		卡号销售递延实发酬金	单位：元	NUMBER(10)	
#	07		卡号销售递延应发酬金	单位：元	NUMBER(10)	
#	08		网点代收话费酬金	单位：元	NUMBER(10)	
#	x09		合约计划终端销售酬金	单位：元	NUMBER(10)	
#	x10		终端裸机销售酬金	单位：元	NUMBER(10)	
#	x11		定制终端销售酬金	单位：元	NUMBER(10)	
#	x12		   其中定制手机销售酬金	单位：元	NUMBER(10)	
#	x13		集采终端销售酬金	单位：元	NUMBER(10)	
#	14		增值业务包月类型一酬金	增值业务包月类型一业务是我公司自行运营的业务，包括无线音乐俱乐部、多媒体彩铃。单位：元	NUMBER(10)	
#	15		增值业务包月类型二酬金	增值业务包月类型二业务是我公司与第三方公司合作运营，我公司按用户规模向运营公司支付服务费的业务，包括飞信会员、139邮箱收费版、号簿管家。单位：元	NUMBER(10)	
#	16		增值业务包月类型三酬金	增值业务包月类型三业务室我公司与内容提供商合作运营，我公司与内容提供商进行业务收入分成的业务，包括音乐随身听、歌曲下载、12580生活播报、无线体育俱乐部、信息管家、手机商界、手机医疗、手机地图、手机导航、快讯、手机报、手机阅读、手机视频、手机游戏、手机电视。单位：元	NUMBER(10)	
#	17		增值业务点播类酬金	增值业务点播类业务是我公司与内容提供商合作运营，我公司与内容提供商进行业务收入分成的业务，包括移动应用商城。单位：元	NUMBER(10)	
#	x18		门店补贴	单位：元	NUMBER(10)	
#	x19		销售激励	单位：元	NUMBER(10)	
#	x20		奖励酬金	单位：元	NUMBER(10)	
#	x21		B类目录酬金	单位：元	NUMBER(10)	
#	


##~   select OP_TIME , count(0) 
##~   ,  count(distinct COUNTS ) 
##~   ,  count(distinct RESULT ) 
##~   from bass2.stat_channel_reward_0019 
##~   group by  OP_TIME 
##~   order by 1 


