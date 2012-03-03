######################################################################################################
#接口名称：TD客户月汇总第二过程
#接口编码：每月统计
#接口说明：记录统计周期内TD客户汇总信息，包括TD分类客户数、TD客户计费时长、TD客户数据流量和TD客户的收入情况。
#程序名称: g_s_22204_month_two.tcl
#功能描述: 每个月生成TD客户指标以及收入,完成二个月数据的抓取，插入中间表 g_s_22204_month_tmp3
#运行粒度: 月
#源    表：1.
#          2.
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：
#编写时间：2010-06-23
#问题记录：1.
#修改历史:   2010-8-30    liuzhilong 修改tac码为从DIM_TERM_TAC获取
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#本月 yyyymm
	set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
	puts $op_month
	
	set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	#本月最后一天 yyyymmdd
	set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
	puts $this_month_last_day
	
	#本月第一天 yyyymmdd
	set this_month_first_day [string range $op_month 0 5]01
	puts $this_month_first_day

    #上个月 yyyymm
    set last_month [GetLastMonth [string range $op_month 0 5]]
    puts $last_month
    
    #上个月的第一天 yyyymmdd
    set two "01"
    set last_month_day  ${last_month}${two}
    puts $last_month_day    
    
    #上上个月 yyyymm
    set last_last_month [GetLastMonth [string range $last_month 0 5]]
    puts $last_last_month
    
    #上上个月的第一天 yyyymmdd
    set one "01"
    set last_last_month_day  ${last_last_month}${one}
    puts $last_last_month_day
    

##set op_month  "201007"
##set timestamp  "20100731"
##set this_month_last_day "20100731"
##set this_month_first_day  "20100701"
##set last_month  "201006"
##set last_month_day  "20100601"
##set last_last_month  "201005"
##set last_last_month_day  "20100501"


    #清空临时表表3
    set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_22204_month_tmp3 where cycle_id='2'"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



    #取最近1个月的通话次数大于0的用户终端使用情况表（02047接口）、和GPRS清单（不含上网本清单，即一经04002接口），
    #从中挑出IMEI字冠是TD终端（即用IMEI的前6位或前8位，与集团每月下发的“TAC码与终端的对应关系”全量表中的TAC码关联相等，
    #得到终端设备标识。再关联 “终端配置信息”表中取属性值为001001006（2GHz）、001012004（TD-SCDMA/GSM双模）的终端设备标识，
    #即为TD终端），形成：号码，substr(IMEI,1,14) as IMEI_14，sum(通信次数) as 通信次数_SUM的表1；
    #（对于04002，SUM 话单条数，对于02047，SUM通话次数，两者相加作为通信次数_SUM）
    
    #清空临时表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.g_s_04002_day_td activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



    #插入临时表bass1.g_s_04002_day_td
    set handle [aidb_open $conn]
    
#2011-03-31 12:26:49 
#1.去除：带bass1.G_S_04002_DAY_20100501bak 的子句
#2.取消子查询
    
#	set sql_buff "
#		insert into bass1.g_s_04002_day_td
#		select product_no,imei,count(*) from 
#		(
#			select product_no,imei from bass1.G_S_04002_DAY_20100501bak
#			where time_id between $last_month_day and $this_month_last_day
#			union all
#			select product_no,imei from bass1.G_S_04002_DAY
#			where time_id between $last_month_day and $this_month_last_day
#		) a
#		group by product_no,imei
#	  "
	set sql_buff "
		insert into bass1.g_s_04002_day_td
		select product_no,imei,count(0) from bass1.G_S_04002_DAY a
			where time_id between $last_month_day and $this_month_last_day
		group by product_no,imei
	  "
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.g_s_04002_day_td
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_04002_day_td(product_no,imei,call_cnt)
		select product_no,imei,sum(bigint(call_cnt)) call_cnt from bass1.G_S_02047_MONTH
		where time_id in ($last_month,$op_month)
		  and call_cnt is not null
		group by product_no,imei
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    exec db2 connect to bassdb user bass1 using bass1
    
    exec db2 runstats on table bass1.g_s_04002_day_td with distribution and detailed indexes all
    
    exec db2 terminate

    #清空临时表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_01 activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_01
    set handle [aidb_open $conn]
# 2010-8-30    liuzhilong 修改tac码为从DIM_TERM_TAC获取
##	set sql_buff "
##		insert into bass1.td_check_01
##		select a.product_no,substr(a.imei,1,14) as imei_14,sum(a.call_cnt) from bass1.g_s_04002_day_td a,
##		(
##		select distinct d.tac_num,d.dev_id from bass2.dim_tacnum_devid d
##		,bass2.dim_device_profile e
##		where d.time_id = $op_month
##		  and e.time_id = '$op_month'
##		  and d.dev_id = e.device_id
##		  and e.value in ('001001006','001012004')
##		) b
##		where substr(a.imei,1,6)=b.tac_num or substr(a.imei,1,8)=b.tac_num
##		group by a.product_no,substr(a.imei,1,14)
##	  "
	set sql_buff "
		insert into bass1.td_check_01
		select a.product_no,substr(a.imei,1,14) as imei_14,sum(a.call_cnt) from bass1.g_s_04002_day_td a,
		(
        select distinct tac_num
          from bass2.DIM_TERM_TAC
          where net_type='2'
		) b
		where substr(a.imei,1,6)=b.tac_num or substr(a.imei,1,8)=b.tac_num
		group by a.product_no,substr(a.imei,1,14)
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #对表1按终端和用户进行双向排重，即按IMEI_14分组，按通信次数_SUM合计从大到小排序，取每个分组中通信次数_SUM合计
    #最大的用户号码，即选取同一终端中，通信次数最大的用户，如果同一终端，存在多个通话次数最大且相等的用户，则取用户
    #号码最大的一条记录；然后，如果上述集合中，如果存在同一个用户号码对应多个终端的情况，再取用户通话次数最大的一款
    #终端作为其终端，如果同一用户号码，存在多个通话次数最大且相等的终端，取IMEI_14最大的一条记录(即再按照用户号码分组，
    #按通话次数合计从大到小排序，取每个分组中通话次数合计最大的IMEI_14，如果存在多个通话次数最大的终端，取IMEI_14最大
    #的一条记录)，最终计算成一个TD客户
    
    #清空临时表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_ls_02 activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_ls_02
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.td_check_ls_02
		select product_no,imei_14,call_cnt
		from
		(
		select product_no,imei_14,call_cnt,
		row_number() over(partition by product_no order by call_cnt desc,imei_14 desc) row_id
		from (
		    select product_no,imei_14,call_cnt from
		    (
		    select product_no,imei_14,call_cnt,
		    row_number() over(partition by imei_14 order by call_cnt desc,product_no desc ) row_id 
		    from bass1.td_check_01
		    ) a
		    where row_id=1
		    ) b
		 ) z
		where row_id=1
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #取所有使用TD网络的客户：取最近1个月的TD语音清单（一经04017接口）、GPRS清单（不含上网本清单，即一经04002接口），
    #且网络标识为TD网络的所有记录，形成：号码，substr(IMEI,1,14) as IMEI_14，sum(话单条数) as 通信次数_SUM的表2_TMP；
    #再取表2_TMP中号码不在表1号码中的所有记录形成表2；此时IMEI_14允许为空或非正规IMEI，例如全1或1357924680等，
    #且表2号码均不在表1中

    #清空临时表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.g_s_04002_day_td1 activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #清空临时表, 为后面T网标志做准备，所以多了一步中间表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.g_s_04002_day_tmp activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.g_s_04002_day_td1
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_04002_day_td1
		select product_no,substr(imei,1,14) imei,count(*) call_cnt
		  from bass1.g_s_04017_day
		where time_id between $last_month_day and $this_month_last_day
		  and mns_type='1'
		group by product_no,substr(imei,1,14)
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #抓取TD GPRS用户情况
    set handle [aidb_open $conn]
#	set sql_buff "
#		insert into bass1.g_s_04002_day_tmp
#		select product_no,imei,count(*) from 
#		(
#		select product_no,imei from bass1.G_S_04002_DAY_20100501bak
#		where time_id between $last_month_day and $this_month_last_day
#		  and mns_type='1'
#		union all
#		select product_no,imei from bass1.G_S_04002_DAY
#		where time_id between $last_month_day and $this_month_last_day
#		  and mns_type='1'
#		) a
#		group by product_no,imei
#	  "

	set sql_buff "
		insert into bass1.g_s_04002_day_tmp
		select product_no,imei,count(0)  
		from bass1.G_S_04002_DAY a
		where time_id between $last_month_day and $this_month_last_day
		  and mns_type='1'
		group by product_no,imei
	  "
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #中间表数据插入目标临时表
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_04002_day_td1
		select product_no,imei,call_cnt from bass1.g_s_04002_day_tmp
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



    #清空临时表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.g_s_04002_day_td2 activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.g_s_04002_day_td2
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_04002_day_td2
		select product_no,substr(imei,1,14) as imei,sum(call_cnt)
		  from bass1.g_s_04002_day_td1
		group by product_no,substr(imei,1,14)
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



    #清空临时表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_02 activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_02
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.td_check_02
		select * from bass1.g_s_04002_day_td2
		where product_no in 
		(
		select distinct product_no from bass1.g_s_04002_day_td2
		except
		select distinct product_no from bass1.td_check_01
		)
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    
    #对表2中IMEI_14是35或86（35和86开头的IMEI是正常IMEI）开头的记录，按终端和用户进行双向排重，即按IMEI_14分组，
    #按通信次数_SUM合计从大到小排序，取每个分组中通信次数_SUM合计最大的用户号码，即选取同一终端中，通信次数最大的用户，
    #如果同一终端，存在多个通话次数最大且相等的用户，则取用户号码最大的一条记录；然后，如果上述集合中，如果存在同一
    #个用户号码对应多个终端的情况，再取用户通话次数最大的一款终端作为其终端，如果同一用户号码，存在多个通话次数最大
    #且相等的终端，取IMEI_14最大的一条记录(即再按照用户号码分组，按通话次数合计从大到小排序，取每个分组中通话次数合计
    #最大的IMEI_14，如果存在多个通话次数最大的终端，取IMEI_14最大的一条记录)，最终计算成一个TD客户；
    #
    #对表2中IMEI_14不是35和86开头的记录（含IMEI为空），按用户号码进行排重，最终计算成一个TD客户

    #清空临时表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_03 activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_03
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.td_check_03
		select product_no,imei_14,call_cnt
		from
		(
		    select product_no,imei_14,call_cnt,
		    row_number() over(partition by product_no order by call_cnt desc,imei_14 desc ) row_id
		    from 
		    (
		        select product_no,imei_14,call_cnt
		        from 
		        (
		        select product_no,imei_14,call_cnt,
		        row_number() over(partition by imei_14 order by call_cnt desc,product_no desc ) row_id
		        from bass1.td_check_02
		        where imei_14 like '35%' or imei_14 like '86%'
		        ) a
		        where row_id=1
		    ) b
		) c
		where row_id=1
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #清空临时表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_04 activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_04
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.td_check_04
		select * from bass1.td_check_02
		where imei_14 not like '35%' and imei_14 not like '86%'
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    
    #取连续1个月的上网本话单（上网本产生的GPRS话单，不含CMLAP，即一经04018接口），统计流量之和>0的所有用户（绑定号码），

    #清空临时表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_05 activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_05
    set handle [aidb_open $conn]
#1.去掉带bass1.G_S_04018_DAY_20100501bak的子句
#2.去掉子查询        
#	set sql_buff "
#		insert into bass1.td_check_05
#		select product_no,imei,call_cnt
#		from
#		(
#		    select product_no,imei,call_cnt,
#		    row_number() over(partition by product_no order by call_cnt desc) row_id
#		    from
#		    (
#		    select product_no,imei,sum(bigint(up_flows)+bigint(down_flows)) call_cnt from 
#		   (
#		    select product_no,imei,serv_data_flow_up up_flows,serv_data_flow_down down_flows
#		      from bass1.G_S_04018_DAY_20100501bak
#		     where time_id between $last_month_day and $this_month_last_day
#		       and upper(apnni)<>'CMLAP'
#		       and (bigint(serv_data_flow_up)+bigint(serv_data_flow_down))>0
#		    union all
#		    select product_no,imei,up_flows,down_flows
#		      from bass1.G_S_04018_DAY
#		     where time_id between $last_month_day and $this_month_last_day
#		       and upper(apnni)<>'CMLAP'
#		       and (bigint(up_flows)+bigint(down_flows))>0    
#		   )  a
#		    group by product_no,imei
#		    ) a
#		) b
#		where row_id=1
#	  "
	set sql_buff "
		insert into bass1.td_check_05
		select product_no,imei,call_cnt
		from
		(
		    select product_no,imei,call_cnt,
		    row_number() over(partition by product_no order by call_cnt desc) row_id
		    from
		    (
		    select product_no,imei,sum(bigint(up_flows)+bigint(down_flows)) call_cnt 
		    from bass1.G_S_04018_DAY a
		     where time_id between $last_month_day and $this_month_last_day
		       and upper(apnni)<>'CMLAP'
		       and (bigint(up_flows)+bigint(down_flows))>0    
		    group by product_no,imei
		    ) a
		) b
		where row_id=1
	  "
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #如果内置卡号段不在147345—147349，“上网本客户标志”标为1，如果内置卡号段在147345—147349，标为“特殊终端标志”标为1
    #清空临时表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_05_2 activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_05_2
    set handle [aidb_open $conn]
#1.去掉带bass1.G_S_04018_DAY_20100501bak的子句
#2.去掉子查询        
#	set sql_buff "
#		insert into bass1.td_check_05_2(product_no,intra_product_no)
#		select a.product_no,b.intra_product_no
#		from bass1.td_check_05 a,
#		(select distinct intra_product_no,product_no 
#		  from 
#		  (
#		  select distinct intra_product_no,product_no from bass1.G_S_04018_DAY_20100501bak
#		  where time_id between $last_month_day and $this_month_last_day
#		  union all
#		  select distinct intra_product_no,product_no from bass1.G_S_04018_DAY
#		  where time_id between $last_month_day and $this_month_last_day
#		  ) a  
#		 ) b
#		where a.product_no=b.product_no
#	  "

	set sql_buff "
		insert into bass1.td_check_05_2(product_no,intra_product_no)
		select a.product_no,b.intra_product_no
		from bass1.td_check_05 a,
		(select distinct intra_product_no,product_no 
		  from bass1.G_S_04018_DAY a
		  where time_id between $last_month_day and $this_month_last_day
		 ) b
		where a.product_no=b.product_no
	  "
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
    
    #更新特殊终端标识
    set handle [aidb_open $conn]
	set sql_buff "update bass1.td_check_05_2 set special_terminal_mark='1' 
	    where bigint(substr(intra_product_no,1,6)) between 147345 and 147349
	   "
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
    
    #更新上网本客户标识
    set handle [aidb_open $conn]
	set sql_buff "update bass1.td_check_05_2 set shangwangben_mark='1'
		where bigint(substr(intra_product_no,1,6))<147345 
		   or bigint(substr(intra_product_no,1,6))>147349
       "
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    
    #汇总第2步、第4步、第5步的客户组合，关联在网客户（新的在网客户口径，即包含主动预销、进入保留期和数据卡）后，
    #就是最终的TD客户数（2010新口径）
    #清空临时表    
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_06 activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_06
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.td_check_06
		select a.product_no,substr(a.imei_14,1,14) imei_14,a.call_cnt 
		from
		(
		select product_no,imei_14,call_cnt from bass1.td_check_ls_02
		union 
		select product_no,imei_14,call_cnt from bass1.td_check_03
		union 
		select product_no,imei_14,call_cnt from bass1.td_check_04
		union
		select product_no,imei_14,call_cnt from bass1.td_check_05
		)a,
		(
		select distinct product_no from bass1.td_check_user_status_ls
		where usertype_id not in ('2010','2020','2030','9000')
		  and test_flag='0'
		) b
		where a.product_no=b.product_no
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #TD客户中间表生成完成,下面对各号码打标。

    #第7步，对第6步的每个TD客户，进行分类：
    #如果在第2步用户集合中，则打上“TD终端客户标志”；bass1.td_check_ls_02
    #如果在第4步用户集合中，则打上“最终非终端使用T网客户标志”；bass1.td_check_03和bass1.td_check_04
    #如果是在第3步表2_TMP中的用户，则打上“使用TD网络客户标志”；bass1.g_s_04002_day_td2
    #如果是在第3步表2_TMP中，且有过TD网语音清单的用户，则打上“使用语音TD网络客户标志”；g_s_04017_day
    #如果是在第3步表2_TMP中，且有过TD网GPRS清单(04002)的用户，则标记为“使用GPRSTD网络客户标志”；g_s_04002_day_tmp
    #如果是在第5步中，且“特殊终端标志”标为1的用户，则打上“特殊终端标志”；bass1.td_check_05_2
    #如果是在第5步中，且有过TD网络的上网本GPRS话单的用户，
    #则打上“使用上网本TD网络客户标志”；bass1.td_check_05_2

    #清空临时表    
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_06_2010 activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_06_2010
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.td_check_06_2010(product_no)
		select distinct product_no from bass1.td_check_06
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #更新 TD终端客户标志
    set handle [aidb_open $conn]
	set sql_buff "
		update bass1.td_check_06_2010 set td_terminal_mark='1'
		where product_no in (select distinct product_no from bass1.td_check_ls_02)
	"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #更新 最终非终端使用T网客户标志
    set handle [aidb_open $conn]
	set sql_buff "
		update bass1.td_check_06_2010 set notnet_terminal_mark='1'
		where product_no in 
		(
		select distinct product_no from bass1.td_check_03
		union
		select distinct product_no from bass1.td_check_04
		)
	"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle    

    #更新 使用TD网络客户标志
    set handle [aidb_open $conn]
	set sql_buff "
		update bass1.td_check_06_2010 set td_net_mark='1'
		where product_no in (select distinct product_no from bass1.g_s_04002_day_td2)
	"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle        


    #更新 使用语音TD网络客户标志
    set handle [aidb_open $conn]
	set sql_buff "
		update bass1.td_check_06_2010 set td_cdr_net_mark='1'
		where product_no in 
		  (
		  select distinct product_no from bass1.g_s_04017_day
		   where time_id between $last_month_day and $this_month_last_day
		     and mns_type='1'
		  )
		 and td_net_mark='1'
	"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle        

    #更新 使用GPRS TD网络客户标志
    set handle [aidb_open $conn]
	set sql_buff "
		update bass1.td_check_06_2010 set td_gprs_net_mark='1'
		where product_no in (select distinct product_no from bass1.g_s_04002_day_tmp)
		  and td_net_mark='1'
	"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle    

    #更新 特殊终端标志
    set handle [aidb_open $conn]
	set sql_buff "
		update bass1.td_check_06_2010 set special_terminal_mark='1'
		where product_no in 
		 (select distinct product_no from bass1.td_check_05_2 where special_terminal_mark='1')
	   "
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle    


    #更新 使用上网本TD网络客户标志
    set handle [aidb_open $conn]
#1.去掉带bass1.G_S_04018_DAY_20100501bak的子句    
#	set sql_buff "
#		update bass1.td_check_06_2010 set shangwangben_mark='1'
#		where product_no in 
#		  (
#		    select distinct product_no from bass1.G_S_04018_DAY_20100501bak
#		     where time_id between $last_month_day and $this_month_last_day
#		       and upper(apnni)<>'CMLAP'
#		       and mns_type='1'
#		       and (bigint(serv_data_flow_up)+bigint(serv_data_flow_down))>0
#		    union
#		    select product_no from bass1.G_S_04018_DAY
#		     where time_id between $last_month_day and $this_month_last_day
#		       and upper(apnni)<>'CMLAP'
#		       and mns_type='1'
#		       and (bigint(up_flows)+bigint(down_flows))>0		 
#		  )
#	   "

	set sql_buff "
		update bass1.td_check_06_2010 set shangwangben_mark='1'
		where product_no in 
		  (
		    select distinct product_no from bass1.G_S_04018_DAY
		     where time_id between $last_month_day and $this_month_last_day
		       and upper(apnni)<>'CMLAP'
		       and mns_type='1'
		       and (bigint(up_flows)+bigint(down_flows))>0		 
		  )
	   "	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle    



    #清空临时表    
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_user activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_user
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.td_check_user
		select distinct product_no from bass1.td_check_06_2010
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #在最终的TD客户数中，且属于上述TD客户数统计流程“第二步”结果中挑出的最终使用TD终端的客户，根据其IMEI_14所对应的TAC码，
    #在集团下发的终端参数（一经下发）中，属性值为006099001的TAC码为TD手机客户，属性值为006099002的TAC码为TD数据卡客户，
    #属性值为006099005的TAC码为TD无线座机客户；没有上述三个属性TAC码的客户归为TD手机客户
    #在最终的TD客户数中，且属于上述TD客户数统计流程“第四步”结果中挑出的最终的使用TD网络的非TD终端客户，
    #如果一经用户资料02004中“数据SIM卡用户标识”为1，则统计为TD数据卡用户，否则统计为TD手机客户
    
    #TD客户总数  03
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_cust)
		select
		  '2'
		  ,bigint(count(distinct product_no))
		  from bass1.td_check_user
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #清空临时表    
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_user_mobile activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_user_mobile
    set handle [aidb_open $conn]
#2010-8-30    liuzhilong 修改tac码为从DIM_TERM_TAC获取
##	set sql_buff "
##		insert into bass1.td_check_user_mobile
##		select distinct a.product_no
##		from  bass1.td_check_ls_02 a 
##		inner join bass1.td_check_06 b 
##		on a.product_no=b.product_no
##		inner join bass2.dim_tacnum_devid c on c.TAC_NUM=substr(a.imei_14,1,8)
##		where c.DEV_ID not in 
##		   (select d.device_id 
##		     from bass2.dim_device_profile d 
##		    where value in ('006099002','006099005')
##		   )
##	  "
	set sql_buff "
		insert into bass1.td_check_user_mobile
		select distinct a.product_no
		from  bass1.td_check_ls_02 a 
		inner join bass1.td_check_06 b 
		on a.product_no=b.product_no
		inner join bass2.DIM_TERM_TAC c on c.TAC_NUM=substr(a.imei_14,1,8)
		where term_type not in ('2','4')
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.td_check_user_mobile
		select distinct a.product_no
		from
		(
		select product_no from bass1.td_check_03 
		union 
		select product_no from bass1.td_check_04
		)a,
		bass1.td_check_06 b,
		(
		select * from bass1.g_a_02004_day
		where sim_code<>'1'
		) c
		where a.product_no=b.product_no
		  and a.product_no=c.product_no
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #总的TD手机客户数  04
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_mobile_cust)
		select
		  '2'
		  ,bigint(count(distinct product_no))
		  from bass1.td_check_user_mobile
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #清空临时表    
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_user_datacard activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_user_datacard
    set handle [aidb_open $conn]
# 2010-8-30    liuzhilong 修改tac码为从DIM_TERM_TAC获取
#	set sql_buff "
#		insert into td_check_user_datacard
#		select distinct product_no from
#		(
#		select distinct a.product_no,a.imei_14
#		from  bass1.td_check_ls_02 a,bass1.td_check_06 b
#		where a.product_no=b.product_no
#		) a,
#		(
#		select distinct d.tac_num,d.dev_id from bass2.dim_tacnum_devid d
#		,bass2.dim_device_profile e
#		where d.time_id = $op_month
#		  and e.time_id = '$op_month'
#		  and d.dev_id = e.device_id
#		  and e.value in ('006099002')
#		) b
#		where substr(a.imei_14,1,6)=b.tac_num or substr(a.imei_14,1,8)=b.tac_num
#	  "
	set sql_buff "
		insert into td_check_user_datacard
		select distinct product_no from
		(
		select distinct a.product_no,a.imei_14
		from  bass1.td_check_ls_02 a,bass1.td_check_06 b
		where a.product_no=b.product_no
		) a,
		(
        select distinct tac_num
        from bass2.DIM_TERM_TAC
        where term_type='2'
		) b
		where substr(a.imei_14,1,6)=b.tac_num or substr(a.imei_14,1,8)=b.tac_num
	  "
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.td_check_user_datacard
		select distinct a.product_no
		from
		(
		select product_no from bass1.td_check_03 
		union 
		select product_no from bass1.td_check_04
		)a,
		bass1.td_check_06 b,
		(
		select * from bass1.g_a_02004_day
		where sim_code='1'
		) c
		where a.product_no=b.product_no
		  and a.product_no=c.product_no
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #总的数据卡用户数    05
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_datacard_cust)
		select
		  '2'
		  ,bigint(count(distinct product_no))
		  from bass1.td_check_user_datacard
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #清空临时表    
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_user_wire activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_user_wire
    set handle [aidb_open $conn]
# 2010-8-30    liuzhilong 修改tac码为从DIM_TERM_TAC获取
#	set sql_buff "
#		insert into td_check_user_wire
#		select distinct product_no from
#		(
#		select distinct a.product_no,a.imei_14
#		from  bass1.td_check_ls_02 a,bass1.td_check_06 b
#		where a.product_no=b.product_no
#		) a,
#		(
#		select distinct d.tac_num,d.dev_id from bass2.dim_tacnum_devid d
#		,bass2.dim_device_profile e
#		where d.time_id = $op_month
#		  and e.time_id = '$op_month'
#		  and d.dev_id = e.device_id
#		  and e.value in ('006099005')
#		) b
#		where substr(a.imei_14,1,6)=b.tac_num or substr(a.imei_14,1,8)=b.tac_num
#	  "
	set sql_buff "
		insert into td_check_user_wire
		select distinct product_no from
		(
		select distinct a.product_no,a.imei_14
		from  bass1.td_check_ls_02 a,bass1.td_check_06 b
		where a.product_no=b.product_no
		) a,
		(
        select distinct tac_num
        from bass2.DIM_TERM_TAC
        where term_type='4'
		) b
		where substr(a.imei_14,1,6)=b.tac_num or substr(a.imei_14,1,8)=b.tac_num
	  "
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #TD无线座机客户    06
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_wire_cust)
		select
		  '2'
		  ,bigint(count(distinct product_no))
		  from bass1.td_check_user_wire
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #即在最终的TD客户数中，且属于上述TD客户数统计流程“第五步”结果中挑出的TD上网本客户。
    #4.	说明:
    #由于上网本是捆绑后才能使用，故TD上网本客户与其它三类TD分类客户（TD手机客户、TD无线座机客户、TD数据卡客户）
    #可能出现重复，从而TD客户数≤TD手机客户数+TD无线座机客户数+TD数据卡客户数+TD上网本客户数


    #清空临时表    
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_user_net activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_user_net
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.td_check_user_net
		select distinct a.product_no from bass1.td_check_06_2010 a,
		(select product_no from bass1.td_check_05_2 where shangwangben_mark='1') b
		where a.product_no=b.product_no
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #TD上网本客户数    07
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_book_cust)
		select
		  '2'
		  ,bigint(count(distinct product_no))
		  from bass1.td_check_user_net
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #使用TD网络的客户数   08
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_net_cust)
		select
		  '2'
		  ,bigint(count(distinct product_no))
		  from bass1.td_check_06_2010
		 where td_cdr_net_mark='1' or td_gprs_net_mark='1' or shangwangben_mark='1'
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #使用TD网络的手机客户数   09
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_net_mobile_cust)
		select
		  '2'
		  ,bigint(count(distinct aa.product_no))
		  from 
		   (
			select distinct product_no from bass1.td_check_06_2010
			where td_cdr_net_mark='1' or td_gprs_net_mark='1' or shangwangben_mark='1'
			intersect
			select distinct product_no from bass1.td_check_user_mobile
		   ) as aa
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #使用TD网络的数据卡客户数   10
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_net_datacard_cust)
		select
		  '2'
		  ,bigint(count(distinct aa.product_no))
		  from 
		   (
			select distinct product_no from bass1.td_check_06_2010
			where td_cdr_net_mark='1' or td_gprs_net_mark='1' or shangwangben_mark='1'
			intersect
			select distinct product_no from bass1.td_check_user_datacard
		   ) as aa
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #使用TD网络的上网本客户数   11
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_net_book_cust)
		select
		  '2'
		  ,bigint(count(distinct product_no))
		  from bass1.td_check_06_2010 
		 where shangwangben_mark='1'
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #使用TD网络的无线座机客户数   12
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_net_wire_cust)
		select
		  '2'
		  ,bigint(count(distinct aa.product_no))
		  from 
		   (
			select distinct product_no from bass1.td_check_06_2010
			where td_cdr_net_mark='1' or td_gprs_net_mark='1' or shangwangben_mark='1'
			intersect
			select distinct product_no from bass1.td_check_user_wire
		   ) as aa
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #TD终端客户数    13
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_term_cust)
		select
		  '2'
		  ,bigint(count(distinct product_no))
		  from bass1.td_check_06_2010 where td_terminal_mark='1'
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #TD手机终端客户数    14
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_term_mobile_cust)
		select
		  '2'
		  ,bigint(count(distinct aa.product_no))
		  from 
		    (
		     select distinct product_no from bass1.td_check_06_2010 where td_terminal_mark='1'
		     intersect
		     select distinct product_no from bass1.td_check_user_mobile
		    ) as aa
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #TD数据卡终端客户数    15
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_term_datacard_cust)
		select
		  '2'
		  ,bigint(count(distinct aa.product_no))
		  from 
		    (
		     select distinct product_no from bass1.td_check_06_2010 where td_terminal_mark='1'
		     intersect
		     select distinct product_no from bass1.td_check_user_datacard
		    ) as aa
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #使用可视电话的TD客户数    16
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_phone_cust)
		select
		  '2'
		  ,bigint(count(distinct product_no))
		  from bass1.g_s_04017_day
		where time_id between $last_month_day and $this_month_last_day
		  and mns_type='1' and video_type='1' and bigint(base_bill_duration)>0
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


#-----------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------
	#第二步，统计所有TD客户的语音业务量。从“GSM/TD普通语音业务月使用”（TB_SUM_GSM_VBUSN_MON_USG）、
	#“智能网业务月使用”（TB_SUM_INTN_BUSN_MON_USG）、“智能网VPMN业务使用”（ TB_SUM_VPMN_BUSN）表中取数据入库日期为本月，
	#漫游类型不等于122（漫游来访省际漫游）、202（漫游来访国际漫游）、302（漫游来访港澳台）、401（漫游来访边界漫游），
	#号码在第一步TD客户集合中的记录，按号码、无线网络标识、漫游类型、长途类型、呼叫类型汇总通话次数、计费时长、通话费用，
	#得到语音业务通话情况

    #清空临时表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.g_s_21003_month_tmp activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.g_s_21003_month_tmp 进行简单过滤，减少数据量，保存当月数据
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_21003_month_tmp
		select time_id,product_no,svc_type_id,toll_type_id,roam_type_id,call_type_id,call_counts,
		base_bill_duration,toll_bill_duration,call_duration,call_fee,mns_type
		from bass1.g_s_21003_month
		where time_id=$op_month
		  and roam_type_id not in ('122','202','302','401')
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    exec db2 connect to bassdb user bass1 using bass1
    
    exec db2 runstats on table bass1.g_s_21003_month_tmp with distribution and detailed indexes all
    
    exec db2 runstats on table bass1.td_check_user with distribution and detailed indexes all
    
    exec db2 terminate


    #清空临时表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.g_s_21003_month_td activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表,对清单进行汇总，抓取各项指标
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_21003_month_td(product_no,mns_type,bill_duration)
		select 
		   a.product_no,
		   a.mns_type,
		   sum(bigint(a.base_bill_duration))
		 from bass1.g_s_21003_month_tmp a,bass1.td_check_user b
		where a.product_no=b.product_no
		group by 
		   a.product_no,
		   a.mns_type
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    exec db2 connect to bassdb user bass1 using bass1
    
    exec db2 runstats on table bass1.g_s_21003_month_td with distribution and detailed indexes all
    
    exec db2 terminate

    #TD客户的总计费时长    17
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_cust_dur)
		select
		  '2'
		  ,sum(bill_duration) 
		from bass1.g_s_21003_month_td
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #TD客户在T网上的计费时长    18
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_cust_t_dur)
		select
		  '2'
		  ,sum(bill_duration) 
		from bass1.g_s_21003_month_td
	   where mns_type='1'
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #TD手机客户的总计费时长    19
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_mobile_cust_dur)
		select
		  '2'
		  ,sum(a.bill_duration) 
		from bass1.g_s_21003_month_td a,
		 (select distinct product_no from bass1.td_check_user_mobile) b
	   where a.product_no=b.product_no
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #TD手机客户在T网上的计费时长    20
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_mobile_cust_t_dur)
		select
		  '2'
		  ,sum(a.bill_duration) 
		from bass1.g_s_21003_month_td a,
		 (select distinct product_no from bass1.td_check_user_mobile) b
	   where a.product_no=b.product_no
	     and a.mns_type='1'
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #TD无线座机客户的总计费时长    21
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_wire_cust_dur)
		select
		  '2'
		  ,sum(a.bill_duration) 
		from bass1.g_s_21003_month_td a,
		 (select distinct product_no from bass1.td_check_user_wire) b
	   where a.product_no=b.product_no
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #TD无线座机客户在T网上的计费时长    22
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_wire_cust_t_dur)
		select
		  '2'
		  ,sum(a.bill_duration) 
		from bass1.g_s_21003_month_td a,
		 (select distinct product_no from bass1.td_check_user_wire) b
	   where a.product_no=b.product_no
	     and a.mns_type='1'
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


	#再从“TD普通语音业务话单”（TB_NET_TD_COMM_CDR）中取数据入库日期为统计月内，无线网络类型为1(TD无线网络)，
	#视频标识为1(视频呼叫)话单，按号码、无线网络标识、漫游类型、长途类型、呼叫类型汇总通话次数、计费时长、通话费用，
	#得到可视电话通话情况。将语音业务通话情况左外关联可视电话通话情况结果汇总可视电话时长，再左外关联第一步TD客户集合，
	#提取各类TD客户标识信息，插入目标表“TD用户语音使用月中间表（DM_SUM_TD_VOIC_MON）”中


    #清空临时表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.g_s_04017_day_td activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表,对清单进行汇总，抓取各项指标
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_04017_day_td(product_no,mns_type,call_counts,bill_duration)
		select
		  a.product_no,
		  a.mns_type,
		  count(*),
		  sum(bigint(a.base_bill_duration))
		from bass1.g_s_04017_day a,bass1.td_check_user b
		where a.product_no=b.product_no
		  and a.time_id/100=$op_month
		  and a.mns_type='1'
		  and a.video_type='1'
		group by
		  a.product_no,
		  a.mns_type
		"

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #可视电话计费时长    23
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_phone_cust_dur)
		select
		  '2'
		  ,sum(bill_duration) 
		from bass1.g_s_04017_day_td
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


	#第三步，统计TD客户的移动数据流量（不含TD上网本产生的）。从“GPRS话单”(TB_NET_GPRS_CDR)中取数据入库时间为本月内，
	#号码在第一步TD客户集合中的话单，按号码、GPRS网络类型、接入点，漫游类型汇总上行数据流量、下行数据流量、通话时长、
	#总费用、话单条数。计算各类移动数据流量指标，形成移动数据流量宽表（不含上网本）

    #清空临时表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.g_s_04002_day_flows activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表,对清单进行汇总，抓取各项指标
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_04002_day_flows(product_no,mns_type,flows,call_duration,all_fee)
		select 
			a.product_no,
			a.mns_type,
			sum(bigint(a.up_flows)+bigint(a.down_flows)),
			sum(bigint(a.call_duration)),
			sum(bigint(a.all_fee))
		from bass1.G_S_04002_DAY a,bass1.td_check_user b
		where a.product_no = b.product_no
		  and a.time_id/100 = $op_month
		group by a.product_no,a.mns_type
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


	#统计所有TD上网本客户的移动数据流量。从“上网本GPRS话单” （04018）(TB_NET_NTBK_GPRS_CDR_NEW)表中取入库时间在本月内，
	#剔除APN为‘CMLAP’的话单，按绑定号码、2G/3G网络标识计算各类上网本数据流量指标，形成上网本移动数据流量宽表
    
    #清空临时表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.g_s_04018_day_flows activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表,对清单进行汇总，抓取各项指标
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_04018_day_flows(product_no,mns_type,flows,duration)
		select 
			a.product_no,
			a.mns_type,
			sum(bigint(a.up_flows)+bigint(a.down_flows)),
			sum(bigint(a.duration))
		from bass1.g_s_04018_day a,bass1.td_check_user b
		where a.product_no = b.product_no
		  and a.time_id/100 = $op_month
		group by a.product_no,a.mns_type
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #清空临时表    
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_user_flow activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_user_flow
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.td_check_user_flow
		select distinct product_no from bass1.td_check_user_net
		except
		(
		select distinct product_no from bass1.td_check_user_mobile
		union 
		select distinct product_no from bass1.td_check_user_datacard
		union 
		select distinct product_no from bass1.td_check_user_wire
		)
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    
    #TD客户的总数据流量      24
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select 
		sum(bigint(flows))
		from bass1.g_s_04002_day_flows
	  "
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL1 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle


	set handle [ aidb_open $conn ]
	set sqlbuf "
		select 
		sum(bigint(flows))
		from bass1.g_s_04018_day_flows
	  "
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL2 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle


	set handle [ aidb_open $conn ]
	set sqlbuf "
		select 
		sum(bigint(a.flows))
		from bass1.g_s_04002_day_flows a,
		bass1.td_check_user_flow b
		where a.product_no=b.product_no
	  "
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL3 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

    #指标数值插入目标表
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_cust_flows)
		values('2',decimal(1.0*($RESULT_VAL1+$RESULT_VAL2-$RESULT_VAL3)/1024/1024,20,2))
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



    #TD客户在T网上的数据流量      25
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select 
		sum(bigint(flows))
		from bass1.g_s_04002_day_flows
	   where mns_type='1'
	  "
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL4 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle


	set handle [ aidb_open $conn ]
	set sqlbuf "
		select 
		sum(bigint(flows))
		from bass1.g_s_04018_day_flows
	   where mns_type='1'
	  "
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL5 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle


	set handle [ aidb_open $conn ]
	set sqlbuf "
		select 
		value(sum(bigint(a.flows)),0)
		from bass1.g_s_04002_day_flows a,
		bass1.td_check_user_flow b
		where a.product_no=b.product_no
		  and a.mns_type='1'
	  "
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL6 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

    #指标数值插入目标表
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_cust_t_flows)
		values('2',decimal(1.0*($RESULT_VAL4+$RESULT_VAL5-$RESULT_VAL6)/1024/1024,20,2))
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #TD手机客户的总数据流量       26
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select value(sum(a.flows),0) 
	  from g_s_04002_day_flows a,
		(select distinct product_no from bass1.td_check_user_mobile) b
	where a.product_no=b.product_no
	  "
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL7 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

    
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select value(sum(a.flows),0) 
	  from g_s_04018_day_flows a,
		(
		select distinct product_no from bass1.td_check_06_2010 where special_terminal_mark='1'
		intersect
		select product_no from bass1.td_check_user_mobile
		) b
	where a.product_no=b.product_no
	  "
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL8 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

    #指标数值插入目标表
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_mobile_flows)
		values('2',decimal(1.0*($RESULT_VAL7+$RESULT_VAL8)/1024/1024,20,2))
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



    #TD手机客户在T网上的数据流量       27
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select value(sum(a.flows),0) 
	  from g_s_04002_day_flows a,
		(select distinct product_no from bass1.td_check_user_mobile) b
	where a.product_no=b.product_no
	  and a.mns_type='1'
	  "
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL9 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

    
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select value(sum(a.flows),0) 
	  from g_s_04018_day_flows a,
		(
		select distinct product_no from bass1.td_check_06_2010 where special_terminal_mark='1'
		intersect
		select product_no from bass1.td_check_user_mobile
		) b
	where a.product_no=b.product_no
	  and a.mns_type='1'
	  "
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL10 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

    #指标数值插入目标表
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_mobile_t_flows)
		values('2',decimal(1.0*($RESULT_VAL9+$RESULT_VAL10)/1024/1024,20,2))
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #TD数据卡客户的总数据流量    28   TD数据卡客户在T网上的数据流量    29
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_datacard_flows,td_datacard_t_flows)
		select
		  '2'
		  ,decimal(1.0*sum(a.flows)/1024/1024,20,2)
		  ,decimal(1.0*sum(case when a.mns_type='1' then a.flows else 0 end)/1024/1024,20,2)
		from bass1.g_s_04002_day_flows a,
		    (select distinct product_no from bass1.td_check_user_datacard) b
	   where a.product_no = b.product_no
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



    #TD上网本客户的总数据流量     30   TD上网本客户的总数据流量     31
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_book_flows,td_book_t_flows)
		select
		  '2'
		  ,decimal(1.0*sum(a.flows)/1024/1024,20,2)
		  ,decimal(1.0*sum(case when a.mns_type='1' then a.flows else 0 end)/1024/1024,20,2)
		from bass1.g_s_04018_day_flows a,
		    (select distinct product_no from bass1.td_check_user_net) b
	   where a.product_no = b.product_no
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#统计所有TD客户的账单收入(本步骤只用于先计算各项费用，然后提供给后面的各项收入指标统计时使用，
	#不作为“TD客户的总收入”指标的统计口径，“TD客户的总收入”指标统计口径见后)。从“明细帐单”(TB_FIN_DTL_BILL)
	#和“智能网用户明细收入”（ TB_FIN_INET_USR_DTL_INC）取数据入库日期为统计日，用户标识在第一步TD客户集合中
	#的账单记录，计算各类费用
    
    #清空临时表
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.g_s_03004_month_td activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表,对清单进行汇总，提取总收入和每个用户对应的上网本费用
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_03004_month_td(product_no,all_fee,book_fee)
       select
           b.product_no,
           sum(bigint(fee_receivable)),
           sum(case when acct_item_id in ('0631','0632') then bigint(fee_receivable) else 0 end)
        from bass1.g_s_03004_month a,
			(
			select a.user_id,a.product_no from bass1.td_check_user_status_ls a,
			(select product_no from bass1.td_check_user) b
			where a.product_no=b.product_no
			) b
		where a.user_id=b.user_id
		  and a.time_id=$op_month
		group by b.product_no
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    exec db2 connect to bassdb user bass1 using bass1
    
    exec db2 runstats on table bass1.g_s_03004_month_td with distribution and detailed indexes all
    
    exec db2 terminate


    #清空临时表    
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_user_fee1 activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #插入临时表bass1.td_check_user_fee1,抓取仅当该客户为TD上网本客户数据
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.td_check_user_fee1
		select distinct product_no from bass1.td_check_user_net
		except
		(
		select distinct product_no from bass1.td_check_user_mobile
		union 
		select distinct product_no from bass1.td_check_user_datacard
		union 
		select distinct product_no from bass1.td_check_user_wire
		)
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #清空临时表    
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_user_fee2 activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_user_fee2,抓取 仅当该客户为非TD上网本客户 数据
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.td_check_user_fee2
		(
		select distinct product_no from bass1.td_check_user_mobile
		union 
		select distinct product_no from bass1.td_check_user_datacard
		union 
		select distinct product_no from bass1.td_check_user_wire
		)
		except
		select distinct product_no from bass1.td_check_user_net
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #清空临时表    
    set handle [aidb_open $conn]
	set sql_buff "alter table bass1.td_check_user_fee3 activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #插入临时表bass1.td_check_user_fee2,抓取 余下的TD客户 数据
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.td_check_user_fee3
		select distinct product_no from bass1.td_check_user
		except
		(
		select distinct product_no from bass1.td_check_user_fee1
		union 
		select distinct product_no from bass1.td_check_user_fee2
		)
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #TD客户收入            32
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select value(sum(a.book_fee),0) 
	  from g_s_03004_month_td a,bass1.td_check_user_fee1 b
	where a.product_no=b.product_no
	  "
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL11 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle


	set handle [ aidb_open $conn ]
	set sqlbuf "
	select value(sum(a.all_fee-a.book_fee),0) 
	  from g_s_03004_month_td a,bass1.td_check_user_fee2 b
	where a.product_no=b.product_no
	  "
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL12 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

	set handle [ aidb_open $conn ]
	set sqlbuf "
	select value(sum(a.all_fee),0) 
	  from g_s_03004_month_td a,bass1.td_check_user_fee3 b
	where a.product_no=b.product_no
	  "
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL13 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle


    #指标数值插入目标表,把单位分换算为元
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_cust_fee)
		values('2',decimal(1.0*($RESULT_VAL11+$RESULT_VAL12+$RESULT_VAL13)/100,20,2))
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #TD手机客户收入     33
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_mobile_cust_fee)
		select
		  '2'
		  ,decimal(1.0*sum(a.all_fee-a.book_fee)/100,20,2)
		from bass1.g_s_03004_month_td a,
		    (select distinct product_no from bass1.td_check_user_mobile) b
	   where a.product_no = b.product_no
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #TD数据卡客户收入     34
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_datacard_cust_fee)
		select
		  '2'
		  ,decimal(1.0*sum(a.all_fee-a.book_fee)/100,20,2)
		from bass1.g_s_03004_month_td a,
		    (select distinct product_no from bass1.td_check_user_datacard) b
	   where a.product_no = b.product_no
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #TD上网本客户收入     35
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_book_cust_fee)
		select
		  '2'
		  ,decimal(1.0*sum(a.book_fee)/100,20,2)
		from bass1.g_s_03004_month_td a,
		    (select distinct product_no from bass1.td_check_user_net) b
	   where a.product_no = b.product_no
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #TD无线座机客户收入     36
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_22204_month_tmp3(cycle_id,td_wire_cust_fee)
		select
		  '2'
		  ,decimal(1.0*sum(a.all_fee-a.book_fee)/100,20,2)
		from bass1.g_s_03004_month_td a,
		    (select distinct product_no from bass1.td_check_user_wire) b
	   where a.product_no = b.product_no
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle






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





