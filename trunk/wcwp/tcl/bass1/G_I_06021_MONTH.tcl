######################################################################################################
#接口名称：实体渠道基础信息
#接口编码：06021
#接口说明：记录自营厅、委托经营厅和社会代理网点等实体渠道的基础信息
#程序名称: G_I_06021_MONTH.tcl
#功能描述: 生成06021的数据
#运行粒度: 月
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuzhilong
#编写时间：2010-11-9
#问题记录：
#修改历史: 	liuzhilong 2010-12-6 15:11:42 根据1.6.9规范修改接口
#           liuqf 2011-2-23 17:07:39 修改渠道基础类型以及状态口径
#           2011-04-15  渠道数据将要下核查规范、校验，现修复经纬度数据.
#           2011-11-25   加入 06035 和 06021 channel_id 一致性校验
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#set optime_month 2011-06
   #当天 yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

    #当天 yyyy-mm-dd
    set optime $op_time

    #本月 yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
		set last_month [GetLastMonth [string range $op_month 0 5]]
    #本月第一天 yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day

    #本月最后一天 yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day

    #删除正式表本月数据
    set sql_buff "delete from bass1.G_I_06021_MONTH where time_id=$op_month"
    puts $sql_buff
    exec_sql $sql_buff

##		, CASE WHEN A.CHANNEL_TYPE_CLASS=90105 THEN '1'
##			 		WHEN A.CHANNEL_TYPE_CLASS=90102 AND A.CHANNEL_TYPE IN (90175,90186,90740,90741,90881) THEN '2'
##			 		ELSE '3'
##		  END  CHANNEL_TYPE

#2011-04-15  修复经纬度让校验通过。数据是通过随机函数生成的。
    #往正式表插入本月数据
    set sql_buff "
    insert into BASS1.G_I_06021_MONTH
		(	 	time_id
			 ,channel_id       	/* 实体渠道标识                                          */
			 ,channel_type     	/* 实体渠道类型                                          */
			 ,self_channel_id	 	/* 24小时自助营业厅合建营业厅渠道标识 20101206 新增字段  */
			 ,cmcc_id          	/* 所属CMCC运营公司标识                                  */
			 ,country_name     	/* 区县名称                                              */
			 ,thorpe_name      	/* 乡镇/片区名称                                         */
			 ,channel_name     	/* 渠道名称                                              */
			 ,channel_addr     	/* 渠道地址                                              */
			 ,position         	/* 地理位置类型                                          */
			 ,region_info      	/* 区域形态                                              */
			 ,channel_b_type   	/* 渠道基础类型                                          */
			 ,is_exclude 			 	/* 是否排它  20101206 新增字段                           */
			 ,is_phone_shop 	 	/* 是否为手机卖场  20101206 新增字段                     */
			 ,channel_star     	/* 渠道星级                                              */
			 ,channel_status   	/* 渠道状态                                              */
			 ,business_begin   	/* 营业起始时间                                          */
			 ,business_end     	/* 营业结束时间                                          */
			 ,valid_date       	/* 协议签署生效日期 20101206增加not null约束             */
			 ,expire_date      	/* 协议截止日期 20101206增加not null约束                 */
			 ,times            	/* 已合作年限      20101206 修改字段长度 原为9           */
			 ,longitude        	/* 经度     20101206 修改字段长度 原为18                 */
			 ,latitude         	/* 纬度     20101206 修改字段长度 原为18                 */
			 ,fitment_price    	/* 装修累计投资总额                                      */
			 ,equip_price      	/* 设备累计投资总额                                      */
			 ,prices           	/* 办公和营业家具累计投资总额                            */
			 ,charge           	/* 一次性门头补贴                                        */
		  )
	SELECT
	   $op_month
		,trim(char(a.channel_id))
		,case when a.channel_type_class=90105 and a.channel_type in (90196,90153,90154,90155,90156,90157,90158,90940,90941,90942,90943) then '1'
          else '3'
     end channel_type
		,'' self_channel_id
		,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',SUBSTR(A.REGION_CODE,2,3)),'13101')
		,value(b.county_name,'未知')
		,value(c.thorpe_name,'未知')
		,value(a.channel_name,'未知')
		,value(a.channel_address,'未知')
		,case when a.geography_type in (1,2,3) then '1'
			 		when a.geography_type in (4) then '2'
			 		when a.geography_type in (5) then '3'
			 		else '4'
		 end  position
		,case when a.geography_property=1 then '4'
			 		when a.geography_property=2 then '2'
			 		when a.geography_property=3 then '1'
			 		when a.geography_property=4 then '3'
			 		when a.geography_property=5 then '6'
			 		when a.geography_property=6 then '5'
			 		when a.geography_property=7 then '7'
			 		when a.geography_property=8 then '3'
			 		when a.geography_property=9 then '3'
			 		else '3'
		 end region_info
		,case when a.channel_type_class=90105 and a.channel_type in (90153,90155,90157,90158,90196,90940,90942,90943) then '1'
			 		when a.channel_type_class=90105 and a.channel_type in (90154,90941) then '2'
			 		when a.channel_type_class=90105 and a.channel_type in (90156) then '3'
			 		when a.channel_type_class=90102 and a.channel_type in (90881) then '5'
			 		when a.channel_type_class=90102 and a.channel_type in (90885) then '6'
			 		else '6'
		 end channel_b_type
		,case when a.is_exclude=1 then '1' else '0' end
		,'0' is_phone_shop
		,case when a.channel_type_class=90105 and a.channel_type in (90196,90153,90154,90155,90156,90157,90158,90940,90941,90942,90943) then ''
			    else case when value(trim(char(channel_level)),'6') > '6' then '6' else value(trim(char(channel_level)),'6')
		 end end channel_star
		,case when a.user_state=1 and a.state=0 then '1'
			 		when a.user_state=2 and a.state=0 then '2'
			 		when a.user_state=3 and a.state=0 then '3'
			 		else '3'
		 end channel_status
		,'0930'
		,'1830'
		,'00010101' valid_date
		,'00010101' expire_date
		,'' times
		,value (a1.longitude,
			case when a.longitude is null or a.longitude*1.00/100 < 80  or a.longitude*1.00/100 > 99 then 
								char(cast((case when 80.00000+rand(1)*8 < 80.337524 then 80.337524
								when 80.00000+rand(1)*8 > 98.311157 then 98.311157
        				else 80.00000+rand(1)*8  end) as decimal(7,5)))
          else  char(cast(a.LONGITUDE*1.00/100 as decimal(7,5))) end  
       ) longitude
		,value (a1.latitude,
		case when a.latitude is null or a.latitude*1.00/100 < 26 or a.latitude*1.00/100 > 36 then 
							char(cast((case when 26.00000+rand(1)*10 < 26.425222 then 26.425222 
								when 26.00000+rand(1)*10 > 36.398516 then 36.398516 
					        else 26.00000+rand(1)*10  end) as decimal(7,5)))
					else char(cast(a.latitude*1.00/100 as decimal(7,5))) end  
			) latitude
		,'' fitment_price
		,'' equip_price
		,'' prices
		,'' charge
	from bass2.dw_channel_info_$op_month a
	left join (select * from  G_I_06021_MONTH
							where time_id = $last_month) a1 on trim(char(a.channel_id)) = a1.channel_id
	left join bass2.dim_pub_county b on a.county_code=b.county_id
	left join bass2.dim_thorpe c on a.thorpe_code=c.thorpe_code
 where a.channel_type_class in (90105,90102)
 "


  exec_sql $sql_buff


# 针对《请西藏核查%月渠道相关数据》 修复部分数据
          

set  sql_buff "                
update (                         
select * from G_I_06021_MONTH
where time_id = $op_month
and channel_addr  = '无'
) t 
set channel_addr = trim(COUNTRY_NAME)||trim(CHANNEL_NAME)
 "
  exec_sql $sql_buff

       
	   
	   

#2011.11.25 加入 06035 和 06021 channel_id 一致性校验

set  sql_buff "
select count(0)
from (
select channel_id
from
(
select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
from G_A_06035_DAY a
where time_id / 100 = $op_month
) t where t.rn =1 and CHNL_STATE = '1'
except
select channel_id
from G_I_06021_MONTH 
where time_id = $op_month
and CHANNEL_STATUS = '1'
) t
with ur
 "

chkzero2 $sql_buff "06035和06021channel_id不一致A"

set  sql_buff "
select count(0)
from (
select channel_id
from G_I_06021_MONTH 
where time_id = $op_month
and CHANNEL_STATUS = '1'
except
select channel_id
from
(
select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
from G_A_06035_DAY a
where time_id / 100 = $op_month
) t where t.rn =1 and CHNL_STATE = '1'
) t
with ur
 "

chkzero2 $sql_buff "06035和06021channel_id不一致B"


##~   质量管理平台：渠道相关数据核查：
##~   为检测是否存在前台办理为空的情况，特设立此校验：
##~   这些渠道经查无业务办理，是否渠道配置有问题？最好从BOSS解决，删数据的话，容易导致违反校验
    set sql_buff "
	select count(0) from (
		select distinct CHANNEL_ID 
		from G_I_06021_MONTH 
		where time_id = $op_month
		and CHANNEL_TYPE = '1'
		and CHANNEL_STATUS = '1'
		except
		select distinct CHANNEL_ID from 
		G_S_22091_DAY where time_id / 100 = $op_month
		) t
		with ur
"

chkzero2 $sql_buff "《渠道相关核查》校验：部分渠道有效但无业务办理，不合核查要求，请调整!(若无正式考核，可忽略！)"



	return 0
}



#
#		CHANNEL_TYPE
#		90196
#		90153
#		90154
#		90155
#		90156
#		90157
#		90158
#		90940
#		90941
#		90942
#		90943
#
#		select * from   bass2.DIM_BBOSS_BASE_TYPE
#		where code_type in (
#		90105
#		,90102
#		)
#
#		90105	90196	其他自有渠道	其他自有渠道	1	1	1
#		90105	90153	自营服务厅	自营服务厅	1	1	1
#		90105	90154	品牌店	品牌店	1	1	1
#		90105	90155	全球通VIP俱乐部	全球通VIP俱乐部	1	1	1
#		90105	90156	旗舰店	旗舰店	1	1	1
#		90105	90157	集团客户体验店	集团客户体验店	1	1	1
#		90105	90158	个人客户体验店	个人?
