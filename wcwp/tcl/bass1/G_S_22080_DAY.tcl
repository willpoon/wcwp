######################################################################################################
#接口单元名称：统一查询退订日汇总
#接口单元编码：22080
#接口单元说明：采集增值业务0000统一查询和退订服务的日报数据，包括“客户查询量、退订量、业务退订失败量、客户投诉量、当天退订业务总数”等;可计算出“业务退订率、退订失败率、客户平均每次退订业务的数量”等。
#程序名称: G_S_22080_DAY.tcl
#功能描述: 生成22080的数据
#运行粒度: 日
#源    表：
#1.bass2.DW_THREE_ITEM_STAT_DM_$op_month 
#2.	BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_$op_month
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzhiwei
#编写时间：2011-04-26
#问题记录：1.
#修改历史: 1. 1.7.2 规范
# 2011-11-08 panzhiwei
#曾祥庆zxq  11:56:49
#根据集团通报的8月份增值业务0000统一查询和退订服务的实施情况我公司退订率高达72%，远远高于其它省公司。经我部核实，现我公司数据业务退订率计算方式为：所有经“0000”查询后退订业务总数÷用户发送“0000”查询总次数；但这一计算方式包括用户发送一次短信同时退订多个业务的情况，从而导致目前我公司退订率过高。
#
#为避免总部对我公司连续通报，敬请贵部将我公司数据业务0000退订率计算方式按照其它省公司计算方式做如下调整：
#
#1、    用户通过 “0000”上行，查询并退订一次的所有数据业务退定数算为一次；
#
#2、    用户发送“0000”查询总次数不做调整
# note: 
##	1. 取 BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM 作为退订量
##	2. 退订量应包含退订失败量
##	3.业务退订量	：指当天退订的订购关系总数 ， 应该小于退订量。
# 	2011-11-11 根据曾详庆反馈，退订量还是偏大，所以改成：  count(distinct PHONE_ID||char(SP_ID)||SP_CODE) ->  count(distinct PHONE_ID) 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
      set op_month [string range $timestamp 0 5]

      set curr_month [string range $op_time 0 3][string range $op_time 5 6]


  #删除本期数据
	set sql_buff "delete from bass1.G_S_22080_DAY where time_id=$timestamp"
	exec_sql $sql_buff
	
##~   set sql_buff "
	##~   select count(distinct PHONE_ID) cnt3 
##~   from     BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_$op_month
	##~   where replace(char(CREATE_DATE),'-','') = '$timestamp'
##~   with ur
##~   "

##~   set RESULT_VAL [get_single $sql_buff]

  ##~   #直接来源于二经用户表数据，新的接口表
  ##~   #更新业务退订量CANCEL_BUSI_CNT（可能不是很准，规范指的是每次查询后，发起所退订的业务量?）
  ##~   #CANCEL_CNT口径也要重新确认。
	##~   set sql_buff "
	##~   insert into bass1.G_S_22080_DAY
		  ##~   (
         ##~   TIME_ID
        ##~   ,OP_TIME
        ##~   ,QRY_CNT
        ##~   ,CANCEL_CNT
        ##~   ,CANCEL_FAIL_CNT
        ##~   ,COMPLAINT_CNT
        ##~   ,CANCEL_BUSI_TYPE_CNT
		  ##~   )
 ##~   select      $timestamp TIME_ID
             ##~   ,replace(char(date(a.create_date)),'-','') op_time
             ##~   ,char(a.TYCX_QUERY)             qry_cnt
             ##~   ,'${RESULT_VAL}'                cancel_cnt
             ##~   ,char(a.TYCX_TUIDING_FAIL)      cancel_fail_cnt
             ##~   ,char(a.TYCX_TOUSU_LIANG)       complaint_cnt
             ##~   ,char( case when (${RESULT_VAL} - a.TYCX_TUIDING_FAIL) < 0 
			##~   then 0 else (${RESULT_VAL} - a.TYCX_TUIDING_FAIL) 
		   ##~   end 
		  ##~   ) CANCEL_BUSI_CNT
        ##~   from  bass2.DW_THREE_ITEM_STAT_DM_$op_month a ,
              ##~   (select  replace(char(date(a.create_date)),'-','') op_time
              					##~   ,count(0) CANCEL_BUSI_CNT
                       ##~   from   
                       	##~   BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_$op_month a
                        ##~   where replace(char(date(a.create_date)),'-','') =  '$timestamp'  
                        ##~   group by replace(char(date(a.create_date)),'-','')
                    ##~   ) b 
        ##~   where replace(char(date(a.create_date)),'-','') = '$timestamp' 
##~   and    replace(char(date(a.create_date)),'-','') = b.op_time
##~   with ur
  ##~   "
	##~   exec_sql $sql_buff


### 市场部通报，退订率偏大，吴春要求修改口径。

##~   --查询量
	set sql_buff "
	select count(0) FROM BASS2.DW_KF_SMS_DYNAMIC_PARA_$timestamp a
	,bass2.dw_kf_sms_cmd_receive_dm_$curr_month b 
	where a.SMS_ID = b.SMS_ID and date( b.STS_DATE) = '$op_time' 
	and substr(dyn_key,1,6)= '405003'
	with ur
	"
set RESULT_VAL1 [get_single $sql_buff]

##~   --退订量
	set sql_buff "
	select count(0) from  BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_$curr_month
	where CREATE_DATE = '$op_time'
	with ur
	"

set RESULT_VAL2 [get_single $sql_buff]

##~   --退订失败量
	set sql_buff "
	select count(0) from  BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_$curr_month 
	where  CREATE_DATE = '$op_time' and STS = 0
	with ur
	"
set RESULT_VAL3 [get_single $sql_buff]


	set sql_buff "
	insert into bass1.G_S_22080_DAY
		  (
         TIME_ID
        ,OP_TIME
        ,QRY_CNT
        ,CANCEL_CNT
        ,CANCEL_FAIL_CNT
        ,COMPLAINT_CNT
        ,CANCEL_BUSI_TYPE_CNT
		  )
 select      $timestamp TIME_ID
			,'$timestamp' OP_TIME
			,'$RESULT_VAL1' QRY_CNT
			,'$RESULT_VAL2' CANCEL_CNT
			,'$RESULT_VAL3' CANCEL_FAIL_CNT
			,'0' COMPLAINT_CNT
             ,char( case when (${RESULT_VAL1} - $RESULT_VAL3) < 0 
					then 0 else (${RESULT_VAL1} - $RESULT_VAL3) 
				end ) CANCEL_BUSI_TYPE_CNT			
from bass2.dual
with ur
  "
	exec_sql $sql_buff



##~   --退订量
##~   select count(*) from KF.TONGYI_TUIDING where  
##~   to_char(create_date ,'yyyymmdd') between '20120701' and '20120731';

##~   select count(0) from  BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_201207 
##~   where CREATE_DATE between '2012-07-01' and  '2012-07-31'

##~   37018


##~   --退订失败量
##~   select count(*) from KF.TONGYI_TUIDING where sts=0 and 
##~   to_char(create_date ,'yyyymmdd') between '20120801' and '20120825'; 

##~   select count(0) from  BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_201207 
##~   where CREATE_DATE between '2012-07-01' and  '2012-07-31' and STS = 0
##~   1070


##~   --退订成功量
##~   select count(*) from KF.TONGYI_TUIDING where sts in(1,2) and 
##~   to_char(create_date ,'yyyymmdd') between '20120801' and '20120825'; 

##~   select count(0) from  BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_201207 
##~   where CREATE_DATE between '2012-07-01' and  '2012-07-31' and sts in(1,2)

##~   35948



##~   退订率=(退订量/查询量)*100%
##~   退订失败率=(退订失败量/退订量)*100%
##~   退订成功率=(退订成功量/退订量)*100%


##~   其他相关附件1口径，后续统计；如统计时间没明确规定，请经分需要时找我，谢谢！




  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_S_22080_DAY"
	set pk 			"OP_TIME"
	chkpkunique ${tabname} ${pk} ${timestamp}
	#
  aidb_runstats bass1.$tabname 3
  
	return 0
}
