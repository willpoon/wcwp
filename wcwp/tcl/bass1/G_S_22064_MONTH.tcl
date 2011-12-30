######################################################################################################
#接口名称：实体渠道重点增值业务办理信息
#接口编码：22064
#接口说明：记录实体渠道业务办理信息，涉及自营厅、委托经营厅或社会代理网点
#程序名称: G_S_22064_MONTH.tcl
#功能描述: 生成22064的数据
#运行粒度: 月
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuqf
#编写时间：2010-12-25
#问题记录：1.7.0 规范新增接口，原则上，只抓了现有主要的增值业务
#修改历史:  liuzhilong 20110413 修改Dw_cm_busi_radius_YYYYMM 为 Dw_cm_busi_radius_dm_yyyymm
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

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

    #本月最后一天 yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day

    #删除正式表本月数据
    set sql_buff "delete from bass1.G_S_22064_MONTH where time_id=$op_month"
    puts $sql_buff
    exec_sql $sql_buff


####01:来电提醒---          手机证券  offer_id in (113110176435,113110178916,113110179337,113110179338,113110179339,113110181288,113110198228,113110198230)       --08           
####02:语音信箱---          号簿管家  offer_id in (113090000003)      --03
####03:号簿管家             115	飞信业务        --06
####04:短信回执             117	手机动画        --12
####05:信息管家             118	手机视频        --25
####06:飞信会员             126	BLACKBERRY业务  --10
####07:139邮箱收费版        132	无线音乐俱乐部  --11
####08:手机证券             137	手机地图        --19
####09:手机商界             139	无线音乐下载业务--16
####10:blackberry           708	游戏平台业务受理--26
####11:无线音乐俱乐部       709	短信回执业务    --04
####12:手机动漫             716	彩信冲印        --27
####13:彩铃                 730	飞信好友业务    --06
####14:多媒体彩铃           733 手机阅读业务    --22
####15:音乐随身听           139邮箱收费版 offer_id in (113090003319,113090003320,113090003321,111090033319)  --07
####16:歌曲下载             726 交警通          --20
####17:手机电视             140 手机钱包业务    --28
####18:手机医疗             来电提醒 offer_id in (111090002601,111090002603,111090009284,111090009286,111090009289,111090009317,111099001631) --01
####19:手机地图             语音信箱 offer_id in (111090002901)  --02
####20:手机导航             信息管家 offer_id in (113110166406)  --05
####21:快讯                 无线体育俱乐部  offer_id in (113110203699)         --24
####22:手机阅读             手机商界 offer_id in (111090003215,113110180134) --09
####23:手机报               彩铃     offer_id in (111090001223,111090009160,111090009188,111090009244,111090009282,111090009299,111093301007,111098000030,112093301001,112093301002)   --13
####24:无线体育俱乐部       多媒体彩铃 offer_id in (111000000705)   --14
####25:手机视频             音乐随身听 offer_id in ()   --15
####26:手机游戏             手机电视   offer_id in (113110175125,113110195329)   --17
####27:彩像                 手机医疗   offer_id in (111098000044,113110180139,113110197954)   --18
####28:移动应用商城         快讯       offer_id in (113110156821,113110166390,113110168281,113110178283,113110208633,113110208635)   --21
####                        手机报     offer_id in (111090009253,111090009287,111099900633,111099900635,113110174469,113110174473,113110174479,113110174481,113110174482,113110174483,113110174488,113110174643,113110211995,113110212290)   --23


    # 建临时表
    set sql_buff "
			DECLARE GLOBAL TEMPORARY TABLE SESSION.G_S_22064_MONTH_TMP
          (
            CHANNEL_ID      		BIGINT,
            ACCEPT_TYPE         INT,
            IMP_ACCEPTTYPE      CHARACTER(2),
            CNT                 BIGINT
           )
      PARTITIONING KEY (CHANNEL_ID,ACCEPT_TYPE) USING HASHING
      WITH REPLACE ON COMMIT PRESERVE ROWS NOT LOGGED IN TBS_USER_TEMP
			"
    puts $sql_buff
    exec_sql $sql_buff


    #1、通过营业日志抓取中心业务SP的各类增值业务
    #往临时表插入本月数据
    set sql_buff "
    insert into SESSION.G_S_22064_MONTH_TMP
				  (
				   channel_id
					,accept_type
					,imp_accepttype
					,cnt)
		select b.org_id
           ,case when b.so_mode='5' then 2 else 1 end accept_type
		       ,case when b.busi_type in ('115','730') then '06'
		             when b.busi_type='117' then '12'
		             when b.busi_type='118' then '25'
		             when b.busi_type='126' then '10'
		             when b.busi_type='132' then '11'
		             when b.busi_type='137' then '19'
		             when b.busi_type='139' then '16'
		             when b.busi_type='708' then '26'
		             when b.busi_type='709' then '04'
		             when b.busi_type='716' then '27'
		             when b.busi_type='733' then '22'
		             when b.busi_type='726' then '20'
		             when b.busi_type='140' then '28'
		        end imp_accepttype
		       ,count(distinct b.so_nbr) 
		from bass2.dw_channel_info_$op_month a,
		(
			select c.org_id,b.busi_type,b.so_nbr,b.so_mode 
			  from bass2.Dw_cm_busi_radius_dm_$op_month b,
			       bass2.dw_product_ord_so_log_dm_$op_month c 
			 where b.ext_holds2=c.so_log_id
		 ) b
		where a.channel_id = b.org_id
		  and a.channel_type_class in (90105,90102)
		  and b.busi_type in ('115','117','118','126','132','137','139','708','709','716','730','733','726','140')
		group by  b.org_id,
		          case when b.so_mode='5' then 2 else 1 end,
	            case when b.busi_type in ('115','730') then '06'
			             when b.busi_type='117' then '12'
			             when b.busi_type='118' then '25'
			             when b.busi_type='126' then '10'
			             when b.busi_type='132' then '11'
			             when b.busi_type='137' then '19'
			             when b.busi_type='139' then '16'
			             when b.busi_type='708' then '26'
			             when b.busi_type='709' then '04'
			             when b.busi_type='716' then '27'
			             when b.busi_type='733' then '22'
			             when b.busi_type='726' then '20'
			             when b.busi_type='140' then '28'
			        end		          
	"
    puts $sql_buff
    exec_sql $sql_buff



    #2、通过订单来抓取个人、集团等增值受理业务
    #往临时表插入本月数据
    set sql_buff "
    insert into SESSION.G_S_22064_MONTH_TMP
				  (
				   channel_id
					,accept_type
					,imp_accepttype
					,cnt)
			select
			 a.org_id,
			 case when a.channel_type='9' then 2 else 1 end accept_type,
			 case when b.offer_id in (113110176435,113110178916,113110179337,113110179338,113110179339,113110181288,113110198228,113110198230) then '08'
			      when b.offer_id in (113090000003) then '03'
			      when b.offer_id in (113090003319,113090003320,113090003321,111090033319) then '07'
			      when b.offer_id in (111090002601,111090002603,111090009284,111090009286,111090009289,111090009317,111099001631) then '01'
			      when b.offer_id in (111090002901) then '02'
			      when b.offer_id in (113110166406) then '05'
			      when b.offer_id in (113110203699) then '24'
			      when b.offer_id in (111090003215,113110180134) then '09'
			      when b.offer_id in (111090001223,111090009160,111090009188,111090009244,111090009282,111090009299,111093301007,111098000030,112093301001,112093301002) then '13'
			      when b.offer_id in (111000000705) then '14'
			      when b.offer_id in (113110175125,113110195329) then '17'
			      when b.offer_id in (111098000044,113110180139,113110197954) then '18'
			      when b.offer_id in (113110156821,113110166390,113110168281,113110178283,113110208633,113110208635) then '21'
			      when b.offer_id in (111090009253,111090009287,111099900633,111099900635,113110174469,113110174473,113110174479,113110174481,113110174482,113110174483,113110174488,113110174643,113110211995,113110212290) then '23'
			 end imp_accepttype,
       count(*)
			  from bass2.dw_product_ord_cust_$op_month a,
			       bass2.dw_product_ord_offer_dm_$op_month b,
			       bass2.dim_prod_up_product_item d,
			       bass2.dim_pub_channel e,
			       bass2.dim_sys_org_role_type f,
			       bass2.dim_cfg_static_data g,
			       bass2.dw_channel_info_$op_month h
			 where a.customer_order_id = b.customer_order_id
			   and b.offer_id = d.product_item_id
			   and a.org_id = e.channel_id
			   and a.channel_type = g.code_value
			   and g.code_type = '911000'
			   and b.offer_type=2
			   and e.channeltype_id = f.org_role_type_id
			   and a.org_id = h.channel_id
			   and h.channel_type_class in (90105,90102)
			   and b.offer_id in 
			   (113110176435,113110178916,113110179337,113110179338,113110179339,113110181288,113110198228,113110198230,
					113090000003,
					113090003319,113090003320,113090003321,111090033319,
					111090002601,111090002603,111090009284,111090009286,111090009289,111090009317,111099001631,
					111090002901,
					113110166406,
					113110203699,
					111090003215,113110180134,
					111090001223,111090009160,111090009188,111090009244,111090009282,111090009299,111093301007,111098000030,112093301001,112093301002,
					111000000705,
					113110175125,113110195329,
					111098000044,113110180139,113110197954,
					113110156821,113110166390,113110168281,113110178283,113110208633,113110208635,
					111090009253,111090009287,111099900633,111099900635,113110174469,113110174473,113110174479,113110174481,113110174482,113110174483,113110174488,113110174643,113110211995,113110212290
					)
			   and a.channel_type in ('b','9','5')
			 group by  a.org_id,
			         case when a.channel_type='9' then 2 else 1 end,
							 case when b.offer_id in (113110176435,113110178916,113110179337,113110179338,113110179339,113110181288,113110198228,113110198230) then '08'
							      when b.offer_id in (113090000003) then '03'
							      when b.offer_id in (113090003319,113090003320,113090003321,111090033319) then '07'
							      when b.offer_id in (111090002601,111090002603,111090009284,111090009286,111090009289,111090009317,111099001631) then '01'
							      when b.offer_id in (111090002901) then '02'
							      when b.offer_id in (113110166406) then '05'
							      when b.offer_id in (113110203699) then '24'
							      when b.offer_id in (111090003215,113110180134) then '09'
							      when b.offer_id in (111090001223,111090009160,111090009188,111090009244,111090009282,111090009299,111093301007,111098000030,112093301001,112093301002) then '13'
							      when b.offer_id in (111000000705) then '14'
							      when b.offer_id in (113110175125,113110195329) then '17'
							      when b.offer_id in (111098000044,113110180139,113110197954) then '18'
							      when b.offer_id in (113110156821,113110166390,113110168281,113110178283,113110208633,113110208635) then '21'
							      when b.offer_id in (111090009253,111090009287,111099900633,111099900635,113110174469,113110174473,113110174479,113110174481,113110174482,113110174483,113110174488,113110174643,113110211995,113110212290) then '23'
							 end 
	"
    puts $sql_buff
    exec_sql $sql_buff


    #插入目标表
    set sql_buff "
    insert into BASS1.G_S_22064_MONTH
				  (
				   time_id
				  ,statmonth
				  ,channel_id
					,accept_type
					,imp_accepttype
					,cnt)
		select $op_month
		      ,'$op_month'
		      ,trim(char(channel_id))
					,trim(char(accept_type))
					,imp_accepttype
					,char(sum(cnt))
			from SESSION.G_S_22064_MONTH_TMP
	group by trim(char(channel_id)),
	         trim(char(accept_type)),
	         imp_accepttype
	"
    puts $sql_buff
    exec_sql $sql_buff






	return 0
}


#内部函数部分
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}
#--------------------------------------------------------------------------------------------------------------

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle


	return $result
}
#--------------------------------------------------------------------------------------------------------------




