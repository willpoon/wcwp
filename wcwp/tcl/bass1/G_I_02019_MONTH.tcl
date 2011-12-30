######################################################################################################
#接口名称：叠加资费套餐
#接口编码：02019
#接口说明：资费套餐是以打包优惠为主要方式，针对目标用户长期使用通信业务设计的，将语音通话、短信、数据流量等通信
#产品和业务组合后的资费结构。资费套餐主要分为“基础资费套餐/必选包”和“叠加资费套餐/可选包”两类，以下简称“基础套餐”和“叠加套餐”。
#1.	叠加套餐/可选包：在网用户为了满足自身需要，在基础套餐之外选取的叠加资费。
#2.	每月上传截止统计月末所有叠加套餐，包括在售和停售状态的套餐。
#程序名称: G_I_02019_MONTH.tcl
#功能描述: 生成02019的数据
#运行粒度: 月
#源    表：1.
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuqf
#编写时间：2011-02-18
#问题记录：1.
#修改历史: 1. 1.7.1 规范
#修改历史: 1. 1.7.2 规范
#2.
#3.	全球通全网统一资费专属叠加资费套餐也在本接口上报，上报方式说明：
#1)	其“叠加套餐标识”必须按附件一中BASS_STD1_0115【全球通全网统一资费专属叠加资费套餐标识】规定的填报。
#2.2)	对应“叠加套餐适用范围”中“1”。
#2011-05-03 16:51:19 更改session为实体表，以便给02021引用。
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
		global app_name
		set app_name "G_I_02019_MONTH.tcl"        

  #删除本期数据
	set sql_buff "delete from bass1.g_i_02019_month where time_id=$op_month"
	exec_sql $sql_buff

  #建立中间临时表(一次性)
	##drop table BASS1.g_i_02019_month_1;
	##CREATE TABLE BASS1.g_i_02019_month_1
	## (
	##	  base_prod_id        bigint,
	##		trademark           bigint
	## )
	##  DATA CAPTURE NONE
	## IN TBS_APP_BASS1
	## INDEX IN TBS_INDEX
	##  PARTITIONING KEY
	##   (base_prod_id
	##   ) USING HASHING;
	##
	##ALTER TABLE BASS1.g_i_02019_month_1
	##  LOCKSIZE ROW
	##  APPEND OFF
	##  NOT VOLATILE;

	
	set sql_buff "ALTER TABLE BASS1.g_i_02019_month_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

  #首先提取基础套餐信息
	set sql_buff "
	insert into BASS1.g_i_02019_month_1
		  (
	     base_prod_id
			,trademark
		  )
		select 
		    a.product_item_id                         base_prod_id,
		    b.trademark
		from bass2.dim_prod_up_product_item a,
		  bass2.Dim_prod_up_offer b
		where a.item_type = 'OFFER_PLAN' 
		and b.offer_type in ('OFFER_PLAN')
		and a.product_item_id = b.offer_id
		and a.platform_id in (1,2)
		and replace(char(date(a.create_date)),'-','')<='$this_month_last_day'
		and replace(char(date(b.create_date)),'-','')<='$this_month_last_day'
   "

	exec_sql $sql_buff

	  aidb_runstats bass1.g_i_02019_month_1 3


  #建立中间临时表2

  #建立中间临时表(一次性)
	##drop table BASS1.g_i_02019_month_2;
	##CREATE TABLE BASS1.g_i_02019_month_2
	## (
	##	  base_prod_id        bigint,
	##		base_prod_name      character(200),
	##		prod_status         character(1),
	##		prod_begin_time     character(8),
	##		prod_end_time       character(8),
	##		platform_id         int,
	##		trademark           int
	## )
	##  DATA CAPTURE NONE
	## IN TBS_APP_BASS1
	## INDEX IN TBS_INDEX
	##  PARTITIONING KEY
	##   (base_prod_id
	##   ) USING HASHING;
	##
	##ALTER TABLE BASS1.g_i_02019_month_2
	##  LOCKSIZE ROW
	##  APPEND OFF
	##  NOT VOLATILE;
  

##161000000004 	动感地带
##161000000001 	全球通
##161000000005 	神州行
##161000000031 	无线宽带
##161000000033 	小家庭宽带
##161000000032 	有线宽带
##161000000101 	预存费用赠送费用
##161000000102 	消费费用赠费用[预留]
##161000000103 	预存费用赠费赠物

###1 全球通/4 动感地带/9 神州行/20 其它
###1：仅可叠加在全球通基础套餐上的叠加套餐 =1
###2：仅可叠加在神州行基础套餐上           =9
###3：仅可叠加在动感地带基础套餐上         =4
###4：既可叠加在全球通基础套餐，也可叠加在神州行基础套餐上的叠加套餐 =10
###5：既可叠加在动感地带基础套餐，也可叠加在神州行基础套餐上         =13
###6：既可叠加在全球通基础套餐，也可叠加在动感地带套餐上             =5
###7：适用于全球通、动感地带、神州行的叠加套餐                       =14
###11：其他（包括面向集团客户的叠加套餐、上网本套餐）     

	set sql_buff "ALTER TABLE BASS1.g_i_02019_month_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff

  #提取和基础套餐存在关系的叠加套餐信息
	set sql_buff "
	insert into BASS1.g_i_02019_month_2
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
		    aa.product_item_id  base_prod_id,
		    aa.name             base_prod_name,
		    case when aa.del_flag='1' then '1'
		    else '2' end prod_status,
		    replace(char(date(aa.create_date)),'-','') prod_begin_time,
		    replace(char(date(aa.exp_date)),'-','')    prod_end_time,
		    aa.platform_id,
		    case when bb.trademark=161000000001 then 1 
		         when bb.trademark=161000000005 then 4
		         when bb.trademark=161000000004 then 9
		    else 20 end trademark
		from bass2.dim_prod_up_product_item aa,
		(
		select distinct b.relat_product_item_id,a.trademark
		from bass1.g_i_02019_month_1 a,
		     bass2.dim_prod_up_plan_plan_rel b
		where a.base_prod_id = b.product_item_id
		  and (b.extend_attr_g <> '0' or b.extend_attr_g is null)
		) bb
		where aa.product_item_id=bb.relat_product_item_id
   "

	exec_sql $sql_buff

	  aidb_runstats bass1.g_i_02019_month_2 3


#####********分类提取各个品牌的套餐标识*********######

	set sql_buff "
	declare global temporary table session.g_i_02019_month_tmp3
		(   
		  base_prod_id        bigint,
		  cnt                 int
		)
	partitioning key (base_prod_id) using hashing
	with replace on commit preserve rows not logged in tbs_user_temp"
	exec_sql $sql_buff


	set sql_buff "
	insert into session.g_i_02019_month_tmp3
		  (
	     base_prod_id
			,cnt
		  )
   select base_prod_id,sum(trademark) from bass1.g_i_02019_month_2 group by base_prod_id
   "

	exec_sql $sql_buff

	#2011-05-01 20:21:29
	#1)	其“叠加套餐标识”必须按附件一中BASS_STD1_0115【全球通全网统一资费专属叠加资费套餐标识】规定的填报。
	#2.2)	对应“叠加套餐适用范围”中“1”。
	#

  #插入目标表,状态ID根据失效时间进行再次判断
	set sql_buff "
	insert into bass1.g_i_02019_month
		  (
				time_id
				,over_prod_id
				,over_prod_name
				,over_prod_area
				,vac_prod_flag
				,city_prod_flag
				,prod_status
				,prod_begin_time
				,prod_end_time
		  )
		select 
		   $op_month
		  ,value(d.bass1_offer_id,char(a.base_prod_id))   over_prod_id
		  ,a.base_prod_name over_prod_name
		  ,case when b.cnt=1 then '1'
		        when b.cnt=9 then '2'
		        when b.cnt=4 then '3'
		        when b.cnt=10 then '4'
		        when b.cnt=13 then '5'
		        when b.cnt=5 then '6'
		        when b.cnt=14 then '7'
		   else '11' end over_prod_area
		   ,case when a.base_prod_name like '%假期%' then '1' else '0' end vac_prod_flag
		   ,case when a.base_prod_name like '%两城%' then '1' else '0' end city_prod_flag
		   ,case when a.prod_end_time<='$this_month_last_day' or a.prod_status='2' then '2' else '1' end prod_status
		   ,a.prod_begin_time
		   ,a.prod_end_time
		 from 
		    (
		    select distinct
		       base_prod_id
		      ,base_prod_name
		      ,prod_status
		      ,prod_begin_time
		      ,prod_end_time
		    from bass1.g_i_02019_month_2
		    ) a
		left join session.g_i_02019_month_tmp3 b on a.base_prod_id = b.base_prod_id
	left join (select xzbas_value  as offer_id ,bass1_value bass1_offer_id
							from  BASS1.ALL_DIM_LKP 
							where BASS1_TBID = 'BASS_STD1_0115'
					      and bass1_value like 'QW_QQT_DJ%'
				      ) d on char(a.base_prod_id) = d.offer_id
  "

	exec_sql $sql_buff

  aidb_runstats bass1.g_i_02019_month 3


#for 02021 :针对02021 建的表
  #建立中间临时表(一次性)
	##drop table BASS1.g_i_02019_month_4;
	##CREATE TABLE BASS1.g_i_02019_month_4
	## (
	##	  base_prod_id        bigint
	## )
	##  DATA CAPTURE NONE
	## IN TBS_APP_BASS1
	## INDEX IN TBS_INDEX
	##  PARTITIONING KEY
	##   (base_prod_id
	##   ) USING HASHING;
	##
	##ALTER TABLE BASS1.g_i_02019_month_4
	##  LOCKSIZE ROW
	##  APPEND OFF
	##  NOT VOLATILE;
	

	set sql_buff "ALTER TABLE BASS1.g_i_02019_month_4 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buff
			set sql_buff "
		insert into bass1.g_i_02019_month_4
		select distinct base_prod_id 
		from  bass1.g_i_02019_month_2
		"
	exec_sql $sql_buff
	
	  aidb_runstats bass1.g_i_02019_month_4 3


	
#进行主键唯一性检查
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select over_prod_id,count(*) cnt from bass1.g_i_02019_month
	              where time_id =$op_month
	             group by over_prod_id
	             having count(*)>1
	            ) as a
	            "
	puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	puts $DEC_RESULT_VAL1

	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent "02019月接口叠加资费套餐主键唯一性校验未通过"
	        WriteAlarm $app_name $optime_month $grade ${alarmcontent}
	   }



	return 0
}
