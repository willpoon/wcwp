######################################################################################################
#接口名称：基础资费套餐
#接口编码：02018
#接口说明：资费套餐是以打包优惠为主要方式，针对目标用户长期使用通信业务设计的，将语音通话、短信、
#数据流量等通信产品和业务组合后的资费结构。资费套餐主要分为“基础资费套餐/必选包”和“叠加资费套餐/可选包”两类，
#以下简称“基础套餐”和“叠加套餐”。
#程序名称: G_I_02018_MONTH.tcl
#功能描述: 生成02018的数据
#运行粒度: 月
#源    表：1.
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuqf
#编写时间：2011-02-16
#问题记录：1.
#修改历史: 1. 1.7.1 规范
#修改历史: 1. 1.7.2 规范:
	#	3.	全球通全网统一基础资费套餐也在本接口上报，上报方式说明：
	#1)	其“基础套餐标识”必须按附件一中BASS_STD1_0114【全球通全网统一资费基础套餐标识】规定的填报。
	#2.2)	对应“基础套餐类型编码”，上网套餐“111”，商旅套餐“112”，本地套餐“113”。
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      #本月 yyyymm
      #set op_time 2011-01-01
      #set optime_month 2011-01
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      #本月最后一天 yyyymmdd
      set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
      puts $op_time
      puts $op_month

  #删除本期数据
	set sql_buff "delete from bass1.g_i_02018_month where time_id=$op_month"
 	puts $sql_buff
	exec_sql $sql_buff


  #建立中间临时表1
	set sql_buff "
	declare global temporary table session.g_i_02018_month_tmp
		(   
		  base_prod_id        bigint,
			base_prod_name      character(200),
			prod_status         character(1),
			prod_begin_time     character(8),
			prod_end_time       character(8),
			platform_id         int,
			trademark           bigint
		)
	partitioning key (base_prod_id) using hashing
	with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sql_buff
	exec_sql $sql_buff

    
  #抓取基础套餐基本信息 platform_id in (1,2)个人和集团
	set sql_buff "
	insert into session.g_i_02018_month_tmp
		  (
			base_prod_id
			,base_prod_name
			,prod_status
			,prod_begin_time
			,prod_end_time
			,platform_id
			,trademark
		  )
		select 
		    a.product_item_id                         base_prod_id,
		    a.name                                    base_prod_name,
		    case when a.del_flag='1' then '1'
		    else '2' end                              prod_status,
		    replace(char(date(a.create_date)),'-','') prod_begin_time,
		    replace(char(date(a.exp_date)),'-','')    prod_end_time,
		    a.platform_id,
		    b.trademark
		from bass2.dim_prod_up_product_item a,
		  bass2.Dim_prod_up_offer b
		where a.item_type = 'OFFER_PLAN' 
		and b.offer_type IN ('OFFER_PLAN')
		and a.product_item_id = b.offer_id
		and a.platform_id in (1,2)
		and replace(char(date(a.create_date)),'-','')<='$this_month_last_day'
		and replace(char(date(b.create_date)),'-','')<='$this_month_last_day'
   "

 	puts $sql_buff
	exec_sql $sql_buff

#2011-05-03 15:36:01 单独对全网统一资费套餐追加ID
#为了防止主键重复，去掉  extend_id in(90001348, 90001349, 90001350, 90001351, 90001352, 90001353, 90001354, 90001355, 90001356, 90001357, 90001358, 90001359, 90001360, 90001361, 90001362 )
set sql_buff "
	insert into session.g_i_02018_month_tmp
		  (
			base_prod_id
			,base_prod_name
			,prod_status
			,prod_begin_time
			,prod_end_time
			,platform_id
			,trademark
		  )
		select 
		    a.product_item_id                         base_prod_id,
		    a.name                                    base_prod_name,
		    case when a.del_flag='1' then '1'
		    else '2' end                              prod_status,
		    replace(char(date(a.create_date)),'-','') prod_begin_time,
		    replace(char(date(a.exp_date)),'-','')    prod_end_time,
		    a.platform_id,
		    b.trademark                
		from bass2.dim_prod_up_product_item a,
			bass2.Dim_prod_up_offer b
                where a.item_type = 'OFFER_PLAN' 
                and b.offer_type ='OFFER_PROM'
                and a.product_item_id = b.offer_id
                and a.platform_id in (1,2)
                and replace(char(date(a.create_date)),'-','')<='$this_month_last_day'
                and replace(char(date(b.create_date)),'-','')<='$this_month_last_day'
                and (extend_id  in(90001331, 90001332, 90001333, 90001334, 90001335
                , 90001336, 90001337, 90001338, 90001339, 90001340, 90001341
                , 90001342, 90001343, 90001344, 90001345 )
                )
                and a.product_item_id not in (select base_prod_id from session.g_i_02018_month_tmp)
        "
	exec_sql $sql_buff

								
###  #建立中间临时表2
###	set sql_buff "
###	declare global temporary table session.g_i_02018_month_tmp1
###		(   
###		  base_prod_id        bigint,
###			qqt_mark            character(3)
###		)
###	partitioning key (base_prod_id) using hashing
###	with replace on commit preserve rows not logged in tbs_user_temp"
###	puts $sql_buff
###	exec_sql $sql_buff
###
###
###  #处理全球通分类
###	set sql_buff "
###	insert into session.g_i_02018_month_tmp1
###		  (
###		   base_prod_id
###			,qqt_mark
###		  )
###		select
###		   base_prod_id
###			,case when base_prod_id 
###		from session.g_i_02018_month_tmp
###	 where platform_id=1 and trademark=161000000001
###   "
###
### 	puts $sql_buff
###	exec_sql $sql_buff
###
###
###  #建立中间临时表3
###	set sql_buff "
###	declare global temporary table session.g_i_02018_month_tmp2
###		(   
###		  base_prod_id        bigint,
###			szx_mark            character(3)
###		)
###	partitioning key (base_prod_id) using hashing
###	with replace on commit preserve rows not logged in tbs_user_temp"
###	puts $sql_buff
###	exec_sql $sql_buff
###
###
###  #处理神州行分类
###	set sql_buff "
###	insert into session.g_i_02018_month_tmp2
###		  (
###		   base_prod_id
###			,szx_mark
###		  )
###		select
###		   base_prod_id
###			,case when base_prod_id 
###		from session.g_i_02018_month_tmp
###	 where platform_id=1 and trademark=161000000005
###   "
###
### 	puts $sql_buff
###	exec_sql $sql_buff
###
###
###  #建立中间临时表4
###	set sql_buff "
###	declare global temporary table session.g_i_02018_month_tmp3
###		(   
###		  base_prod_id        bigint,
###			dgdd_mark           character(3)
###		)
###	partitioning key (base_prod_id) using hashing
###	with replace on commit preserve rows not logged in tbs_user_temp"
###	puts $sql_buff
###	exec_sql $sql_buff
###
###  #处理动感地带分类
###	set sql_buff "
###	insert into session.g_i_02018_month_tmp3
###		  (
###		   base_prod_id
###			,dgdd_mark
###		  )
###		select
###		   base_prod_id
###			,case when base_prod_id 
###		from session.g_i_02018_month_tmp
###	 where platform_id=1 and trademark=161000000004
###   "
###
### 	puts $sql_buff
###	exec_sql $sql_buff
###

  #建立中间临时表5
	set sql_buff "
	declare global temporary table session.g_i_02018_month_tmp4
		(   
		  base_prod_id        bigint,
			relat_base_prod_id  bigint
		)
	partitioning key (base_prod_id) using hashing
	with replace on commit preserve rows not logged in tbs_user_temp"
	puts $sql_buff
	exec_sql $sql_buff


  #处理绑定数据业务关系
	set sql_buff "
	insert into session.g_i_02018_month_tmp4
		  (
		   base_prod_id
			,relat_base_prod_id
		  )
		select 
		     b.product_item_id,b.relat_product_item_id
		  from bass2.dim_prod_up_product_item a,
		       bass2.dim_prod_up_plan_plan_rel b
		where a.product_item_id = b.relat_product_item_id
		  and b.prod_item_relat_kind_id = 'ORDER_A_JOIN_ORDER_B'
		  and b.del_flag = '1'
		  and replace(char(date(b.create_date)),'-','')<='$this_month_last_day'
		  and a.platform_id = 3 
		  and a.item_type = 'OFFER_PLAN'
   "

 	puts $sql_buff
	exec_sql $sql_buff

  #套餐分类映射入库 bass1.dim_base_prod_map 需要随时更新，不然新增有效的全部归为‘900’
  
  #2011-05-01 19:50:567 
  #1.一次性插入：
  #insert into   bass1.dim_base_prod_map
	#select bigint( xzbas_value ) 
	#,case 
	#when XZBAS_COLNAME like '%上网套餐%' then  '111'
	#when XZBAS_COLNAME like '%商旅套餐%' then  '112'
	#when XZBAS_COLNAME like '%本地套餐%' then  '113'
	#end 
	#from  BASS1.ALL_DIM_LKP 
	#where BASS1_TBID = 'BASS_STD1_0114'
	#	3.	全球通全网统一基础资费套餐也在本接口上报，上报方式说明：
	#1)	其“基础套餐标识”必须按附件一中BASS_STD1_0114【全球通全网统一资费基础套餐标识】规定的填报。
	#2.2)	对应“基础套餐类型编码”，上网套餐“111”，商旅套餐“112”，本地套餐“113”。

	#2.对全网统一资费套餐进行ID转换
	
  #处理各字段插入目标表
	set sql_buff "
	insert into bass1.g_i_02018_month
		  (
		   time_id
	    ,base_prod_id
			,base_prod_name
			,base_prod_type
			,prod_status
			,prod_begin_time
			,prod_end_time
			,data_cnts
		  )
		select
		   $op_month
	    ,value(e.new_pkg_id,char(a.base_prod_id)) base_prod_id
			,a.base_prod_name
			,value(c.base_prod_type,'900')
			,a.prod_status
			,a.prod_begin_time
			,a.prod_end_time
			,char(count(distinct b.relat_base_prod_id))
		from session.g_i_02018_month_tmp a
	left join session.g_i_02018_month_tmp4 b on a.base_prod_id=b.base_prod_id
	left join bass1.dim_base_prod_map c on a.base_prod_id=c.base_prod_id     
	left join (select xzbas_value  as offer_id ,bass1_value bass1_offer_id
							from  BASS1.ALL_DIM_LKP 
							where BASS1_TBID = 'BASS_STD1_0114'
					      and bass1_value like 'QW_QQT_JC%'
				      ) d on char(a.base_prod_id) = d.offer_id
	left join bass1.DIM_QW_QQT_PKGID e on  d.bass1_offer_id = e.old_pkg_id
	group by value(d.bass1_offer_id,char(a.base_prod_id)),
	         a.base_prod_name,
	         value(c.base_prod_type,'900'),
	         a.prod_status,
	         a.prod_begin_time,
	         a.prod_end_time
	with ur
 "

 	puts $sql_buff
	exec_sql $sql_buff

	
  #1.检查chkpkunique
	set tabname "G_I_02018_MONTH"
	set pk 			"base_prod_id"
	chkpkunique ${tabname} ${pk} ${op_month}
	


	return 0
}