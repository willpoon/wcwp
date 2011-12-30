######################################################################################################
#接口名称：用户
#接口编码：02004
#接口说明：这里的用户是指定购了语音主体服务，被分配了唯一MSISDN的主体。
#          这里的用户包括了"智能网用户"、"全球通或地方品牌普通手机用户"，
#          "移动公话用户"、使用"用户业务类型"区分。用户数据中只包括个人手机用户（普通/测试/公免）
#          和数据SIM卡用户，不包括集团产品相关用户、IP直通车用户资料
#程序名称: G_A_02004_DAY.tcl
#功能描述: 生成02004的数据
#运行粒度: 日
#源    表：1.bass2.dw_product_bass1_yyyymmdd
#          3.bass1.INT_02004_02008_YYYYMM
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.增加区域标志字段@20070801 By tym
#         2.将case 
#                         when a.crm_brand_id2 in (30,190) then '02'
#                         else '01' 
#                        end  
#           修改为 '01'       原因：现在不存在智能网用户    夏华学
#修改历史:  20091126 用 dw_product_bass1_ 替换原来的用户表
#           20100823 修正两部分数据，见下头代码 liuqf
# 2011.11.30 v.1.7.7 panzw 2、修改接口02004（用户）	
#修改属性“入网渠道标识”属性描述，增加BASS1_UM(通过实体、电子、直销渠道入网，但无法归并到相应的实体、电子、直销渠道)、BASS1_UA(通过实体、电子、直销以外的渠道入网);
#修改填写说明：对于入网渠道字段暂时上报BASS1_UM/BASS1_UA的记录，省份需尽快确定其具体入网渠道并以增量方式更新；一经在做月统计时以当月最后一天为截止日期；
#暂无法识别此类入网渠道，暂不修改
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
	global env

	set Optime $op_time
	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set op_month [string range $op_time 0 3][string range $op_time 5 6]
	#上月  yyyymm
	set last_month [GetLastMonth [string range $op_month 0 5]]
	#上上月 yyyymm
	set last_last_month [GetLastMonth [string range $last_month 0 5]]
	#今天的日期，格式dd(例：输入20070411 返回11,输入20070708，返回8)
	set today_dd [format "%.0f" [string range $Timestamp 6 7]]
	
        append op_time_month ${optime_month}-01
        set db_user $env(DB_USER)

	set sql_buff "\
		DELETE FROM $db_user.G_A_02004_DAY where time_id=$Timestamp"
 exec_sql $sql_buff




	set sql_buff "insert into $db_user.G_A_02004_DAY
                       (
                       time_id
                       ,user_id
                       ,cust_id
                       ,usertype_id
                       ,create_date
                       ,user_bus_typ_id
                       ,product_no
                       ,imsi
                       ,cmcc_id
                       ,channel_id
                       ,mcv_typ_id
                       ,prompt_type
                       ,subs_style_id
                       ,brand_id
                       ,sim_code
                       )
                     select
                       $Timestamp
                       ,a.user_id
                       ,a.cust_id
                       ,case 
                         when a.test_mark=1 then '3' 
                         when a.free_mark=1 then '2'
                         else '1' 
                        end
                       ,replace(char(create_date),'-','')
                       ,'01'
                       ,a.product_no
                       ,value(a.imsi,'0')
                       ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.city_id)),'13101')
                       ,char(a.channel_id)
                       ,'1'
				       ,case
				         when a.accttype_id=0 then '1'
				         else '2'
				        end
                       ,'04'
                       ,b.brand_id
                       ,case 
                         when a.crm_brand_id2=70 then '1' 
                         else '0' 
                        end
                     from 
                       bass2.dw_product_bass1_$Timestamp a,
                       (
                        select 
                          * 
                        from 
                          $db_user.INT_02004_02008_${op_month} 
                        where
                          op_time=$Timestamp
                          and brand_flag=1
                       )b              
                     where 
                       a.user_id = b.user_id "
 exec_sql $sql_buff
	
	set sql_buff "delete  from BASS1.g_a_02004_day a
                     
                     where a.time_id=$Timestamp and user_id = '1110117651'	"
 exec_sql $sql_buff

#每日调整用户信息,完成02004接口和02008接口用户数据一致情况 --201008023 ------------------------第1段----------------------

#20100823 调整之前，重新处理了6月27日的历史离网的用户数据
#---备份
###insert into   BASS1.G_A_02008_DAY_20100823bak
###select * from BASS1.G_A_02008_DAY;
###commit;
###
###insert into   BASS1.G_A_02004_DAY_20100823bak
###select * from BASS1.G_A_02004_DAY;
###commit;
###
###
###--清除那天数据
###delete from BASS1.G_A_02004_DAY
###where time_id=20100627;
###commit;
###
###delete from BASS1.G_A_02008_DAY
###where time_id=20100627;
###commit;
###
###
###--插入送给集团的数据
###insert into BASS1.G_A_02004_DAY
###select * from G_A_02004_DAY_take
###where time_id=20100627;
###commit;
###
###
###insert into BASS1.G_A_02008_DAY
###select * from G_A_02008_DAY_take
###where time_id=20100627;
###commit;

	#清空临时表3
	set sql_buff "delete from BASS1.temp_check_02004 where time_id=$Timestamp"
 exec_sql $sql_buff


	#调整用户数
	set sql_buff "
			insert into BASS1.temp_check_02004 
			select $Timestamp,user_id from 
				(
				select distinct user_id from G_A_02008_DAY 
				except
				select distinct user_id from G_A_02004_DAY 
				)a
			"
 exec_sql $sql_buff

	#02004接口数据修正
	set sql_buff "
			insert into G_A_02004_DAY
			   (
			    time_id
			   ,user_id
			   ,cust_id
			   ,usertype_id
			   ,create_date
			   ,user_bus_typ_id
			   ,product_no
			   ,imsi
			   ,cmcc_id
			   ,channel_id
			   ,mcv_typ_id
			   ,prompt_type
			   ,subs_style_id
			   ,brand_id
			   ,sim_code
			   )
			 select
			   $Timestamp
			   ,a.user_id
			   ,a.cust_id
			   ,case 
			     when a.test_mark=1 then '3' 
			     when a.free_mark=1 then '2'
			     else '1' 
			    end
			   ,replace(char(create_date),'-','')
			   ,'01'
			   ,a.product_no
			   ,value(a.imsi,'0')
			   ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.city_id)),'13101')
			   ,char(a.channel_id)
			   ,'1'
				,case
				when a.accttype_id=0 then '1'
				else '2'
				end
			   ,'04'
			   ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(a.brand_id)),'2') as brand_id
			   ,case 
			     when a.crm_brand_id2=70 then '1' 
			     else '0' 
			    end
			 from bass2.dw_product_bass1_$Timestamp a,
			      BASS1.temp_check_02004 b             
			 where a.user_id = b.user_id
			   and b.time_id = $Timestamp
	"
 exec_sql $sql_buff


#/**
#458	涉及入网渠道、创建渠道或申请渠道通过电子渠道和直销渠道办理修订：
#1、(02004)用户接口“入网渠道标识”若通过电子渠道或直销渠道办理，
#按如下规则填写特定分类标识：
#网站、热线、短信、wap、自助终端电子渠道和直销渠道分别对应填写
#'BASS1_WB', 'BASS1_HL','BASS1_SM','BASS1_WP','BASS1_ST', 'BASS1_DS' 
#(字符区分大小写)， 省公司不得占用以上6编码另做它用。
#2、 (01002)个人客户、(01004)集团客户和(02013) IP直通车固定用户的
#“创建渠道标识”及“申请渠道标识”参见(02004)用户接口上报。	
#1.7.3	2011-5-17	自数据日期20110601起生效
#**/
#add     

#2011-06-01 21:57:54 trans channel
#add

set sql_buff "alter table bass1.G_A_02004_DAY_CHNL_MID activate not logged initially with empty table"
exec_sql $sql_buff


set sql_buff "
insert into G_A_02004_DAY_CHNL_MID
select 
         a.TIME_ID
        ,a.USER_ID
        ,a.CUST_ID
        ,a.USERTYPE_ID
        ,a.CREATE_DATE
        ,a.USER_BUS_TYP_ID
        ,a.PRODUCT_NO
        ,a.IMSI
        ,a.CMCC_ID
        ,case when b.CHANNEL_ID is null then 'BASS1_DS' 
        		else a.channel_id end CHANNEL_ID
        ,a.MCV_TYP_ID
        ,a.PROMPT_TYPE
        ,a.SUBS_STYLE_ID
        ,a.BRAND_ID
        ,a.SIM_CODE
from G_A_02004_DAY a
left join (select distinct CHANNEL_ID  from G_I_06021_MONTH )  b on a.CHANNEL_ID = b.CHANNEL_ID
where a.time_id = $Timestamp
"
exec_sql $sql_buff

	set sql_buff "\
		DELETE FROM $db_user.G_A_02004_DAY where time_id=$Timestamp
		"
  exec_sql $sql_buff

set sql_buff "
	insert into  $db_user.G_A_02004_DAY 
	select * from G_A_02004_DAY_CHNL_MID
	"
exec_sql $sql_buff



#修正品牌变动导致用户入网时间变动的情况 20100823----------------------------------第2段---------------------------------
#
#
#	#清空临时表5
#	set handle [ aidb_open $conn ]
#	set sqlbuf "delete from BASS1.temp_check_02004_2 where time_id=$Timestamp"
#	puts $sqlbuf
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#
#
#	#调整用户数
#	set handle [ aidb_open $conn ]
#	set sqlbuf "
#			insert into BASS1.temp_check_02004_2 
#			select distinct $Timestamp,user_id,create_date
#			  from bass1.G_A_02004_DAY
#			 where create_date='20100701'
#			   and time_id=20100701
#			"
#	puts $sqlbuf
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#
#
#	#清空临时表6
#	set handle [ aidb_open $conn ]
#	set sqlbuf "delete from BASS1.G_A_02004_DAY_create "
#	puts $sqlbuf
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#
#	#02004接口数据修正,先插入G_A_02004_DAY_create表中
#	set handle [ aidb_open $conn ]
#	set sqlbuf "
#			insert into G_A_02004_DAY_create
#			   (
#			    time_id
#			   ,user_id
#			   ,cust_id
#			   ,usertype_id
#			   ,create_date
#			   ,user_bus_typ_id
#			   ,product_no
#			   ,imsi
#			   ,cmcc_id
#			   ,channel_id
#			   ,mcv_typ_id
#			   ,prompt_type
#			   ,subs_style_id
#			   ,brand_id
#			   ,sim_code
#			   )
#			 select
#			   $Timestamp
#			   ,a.user_id
#			   ,a.cust_id
#			   ,case 
#			     when a.test_mark=1 then '3' 
#			     when a.free_mark=1 then '2'
#			     else '1' 
#			    end
#			   ,replace(char(a.create_date),'-','')
#			   ,'01'
#			   ,a.product_no
#			   ,'0' as imsi
#			   ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(a.city_id)),'13100')
#			   ,char(a.channel_id)
#			   ,'1'
#				,case
#				when a.accttype_id=0 then '1'
#				else '2'
#				end
#			   ,'04'
#			   ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(a.brand_id)),'2') as brand_id
#			   ,case 
#			     when a.crm_brand_id2=70 then '1' 
#			     else '0' 
#			    end
#			 from bass2.dw_product_bass1_$Timestamp a,
#			      BASS1.temp_check_02004_2 b             
#			 where a.user_id = b.user_id
#			   and b.time_id = $Timestamp
#	"
#	puts $sqlbuf
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#
#
#
#	#清空临时表7,提取当天存在的记录用户清单
#	set handle [ aidb_open $conn ]
#	set sqlbuf "delete from BASS1.G_A_02004_DAY_create_2 "
#	puts $sqlbuf
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#
#	#02004接口数据修正,先插入G_A_02004_DAY_create_2表中
#	set handle [ aidb_open $conn ]
#	set sqlbuf "
#			insert into G_A_02004_DAY_create_2
#			   (
#			    time_id
#			   ,user_id
#			   )
#			 select
#			    a.time_id
#			   ,a.user_id
#			 from BASS1.G_A_02004_DAY a,
#			      BASS1.G_A_02004_DAY_create b             
#			 where a.user_id = b.user_id
#			   and b.time_id = $Timestamp
#			   and a.time_id = $Timestamp
#	"
#	puts $sqlbuf
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#
#
#
#	#删除02004接口表中当天存在的记录用户清单
#	set handle [ aidb_open $conn ]
#	set sqlbuf "delete from BASS1.G_A_02004_DAY where user_id in (select user_id from bass1.G_A_02004_DAY_create_2 where time_id = $Timestamp ) and time_id = $Timestamp"
#	puts $sqlbuf
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#
#	#02004接口数据修正最后一部，插入需修复的用户清单数据
#	set handle [ aidb_open $conn ]
#	set sqlbuf "
#			insert into G_A_02004_DAY
#			   (
#			    time_id
#			   ,user_id
#			   ,cust_id
#			   ,usertype_id
#			   ,create_date
#			   ,user_bus_typ_id
#			   ,product_no
#			   ,imsi
#			   ,cmcc_id
#			   ,channel_id
#			   ,mcv_typ_id
#			   ,prompt_type
#			   ,subs_style_id
#			   ,brand_id
#			   ,sim_code
#			   )
#			 select
#			    time_id
#			   ,user_id
#			   ,cust_id
#			   ,usertype_id
#			   ,create_date
#			   ,user_bus_typ_id
#			   ,product_no
#			   ,imsi
#			   ,cmcc_id
#			   ,channel_id
#			   ,mcv_typ_id
#			   ,prompt_type
#			   ,subs_style_id
#			   ,brand_id
#			   ,sim_code
#			 from BASS1.G_A_02004_DAY_create
#			 where time_id = $Timestamp
#	"
#	puts $sqlbuf
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#
#
#


#	
#       #增加区域标志字段@20070801 By tym
#       if {$today_dd > 12} {
#           set RegDatFrmMis "bass2.stat_zd_village_users_$last_month" 
#       } else {
#	   set RegDatFrmMis "bass2.stat_zd_village_users_$last_last_month" 
#       }    
#              
#        set handle [aidb_open $conn]
#	set sql_buff "update BASS1.g_a_02004_day a
#                     set a.region_flag=value(( select 
#                                                 char(LOCNTYPE_ID)  
#                                               from 
#                                                 $RegDatFrmMis b 
#                                               where 
#                                                 a.time_id=$Timestamp 
#                                                 and a.user_id=b.user_id
#                                               ),'2')
#                     where a.time_id=$Timestamp	"
#        puts $sql_buff
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	       
#2011-07-31 16:16:18
source /bassapp/bass1/tcl/INT_02004_02062_SIMCODE.tcl
Deal_imp $op_time $optime_month

#20111021 修复完毕屏蔽
#source /bassapp/bass1/tcl/INT_FIX_TMP.tcl
#Deal_fiximsi $op_time $optime_month

return 0

}
################################参考#################
# when crm_brand_id2 in (30,190) then '02'  --提出标准神州行和新标准神州行--
#####################################################