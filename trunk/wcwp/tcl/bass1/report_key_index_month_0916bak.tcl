######################################################################################################
#接口名称：27个指标形成前台报表数据
#接口编码：每月统计
#接口说明：
#程序名称: report_key_index_month.tcl
#功能描述: 每个月生成27个指标数据
#运行粒度: 月
#源    表：1.
#          2.
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：
#编写时间：2010-06-18
#问题记录：1.集团短信计费量未关联在网用户资料
#修改历史: 
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
    
    #上上个月 yyyymm
    set last_last_month [GetLastMonth [string range $last_month 0 5]]
    puts $last_last_month
    
    #上上个月的第一天 yyyymmdd
    set one "01"
    set last_last_month_day  ${last_last_month}${one}
    puts $last_last_month_day
    

    #删除目标表本期数据
    set handle [aidb_open $conn]
	set sql_buff "delete from bass1.report_key_index_month where time_id=$op_month"
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
	set sql_buff "alter table bass1.td_check_user_status_ls activate not logged initially with empty table"
	
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
    
    #抓取最新的用户资料信息
    #插入临时表bass1.td_check_user_status_ls
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.td_check_user_status_ls (
		     user_id    
		    ,product_no 
		    ,test_flag  
		    ,sim_code   
		    ,usertype_id  
		    ,create_date
		    ,time_id )
		select e.user_id
		    ,e.product_no  
		    ,case when e.usertype_id in ('1','2') then '0' else '1' end  test_flag
		    ,e.sim_code
		    ,f.usertype_id  
		    ,e.create_date  
		    ,f.time_id       
		from (select user_id,create_date,product_no,sim_code,usertype_id
		                ,row_number() over(partition by user_id order by time_id desc ) row_id   
		from bass1.g_a_02004_day
		where time_id<=$this_month_last_day ) e
		inner join ( select user_id,usertype_id,time_id,row_number() over(partition by user_id order by time_id desc ) row_id   
		           from bass1.g_a_02008_day
		           where time_id<=$this_month_last_day ) f on f.user_id=e.user_id
		where e.row_id=1 and f.row_id=1
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #取最近三个月的通话次数大于0的用户终端使用情况表（02047接口）、和GPRS清单（不含上网本清单，即一经04002接口），
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
	set sql_buff "
		insert into bass1.g_s_04002_day_td
		select product_no,imei,count(*) from 
		(
			select product_no,imei from bass1.G_S_04002_DAY_20100501bak
			where time_id between $last_last_month_day and $this_month_last_day
			union all
			select product_no,imei from bass1.G_S_04002_DAY
			where time_id between $last_last_month_day and $this_month_last_day
		) a
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
		where time_id in ($op_month,$last_month,$last_last_month)
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
	set sql_buff "
		insert into bass1.td_check_01
		select a.product_no,substr(a.imei,1,14) as imei_14,sum(a.call_cnt) from bass1.g_s_04002_day_td a,
		(
		select distinct d.tac_num,d.dev_id from bass2.dim_tacnum_devid d
		,bass2.dim_device_profile e
		where d.time_id = $op_month
		  and e.time_id = '$op_month'
		  and d.dev_id = e.device_id
		  and e.value in ('001001006','001012004')
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


    #取所有使用TD网络的客户：取最近三个月的TD语音清单（一经04017接口）、GPRS清单（不含上网本清单，即一经04002接口），
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


    #插入临时表bass1.g_s_04002_day_td1
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_04002_day_td1
		select product_no,substr(imei,1,14) imei,count(*) call_cnt
		  from bass1.g_s_04017_day
		where time_id between $last_last_month_day and $this_month_last_day
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


    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.g_s_04002_day_td1
		select product_no,imei,count(*) from 
		(
		select product_no,imei from bass1.G_S_04002_DAY_20100501bak
		where time_id between $last_last_month_day and $this_month_last_day
		  and mns_type='1'
		union all
		select product_no,imei from bass1.G_S_04002_DAY
		where time_id between $last_last_month_day and $this_month_last_day
		  and mns_type='1'
		) a
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

    
    #取连续三个月的上网本话单（上网本产生的GPRS话单，不含CMLAP，即一经04018接口），统计流量之和>0的所有用户（绑定号码），

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
	set sql_buff "
		insert into bass1.td_check_05
		select product_no,imei,call_cnt
		from
		(
		    select product_no,imei,call_cnt,
		    row_number() over(partition by product_no order by call_cnt desc) row_id
		    from
		    (
		    select product_no,imei,sum(bigint(up_flows)+bigint(down_flows)) call_cnt from 
		   (
		    select product_no,imei,serv_data_flow_up up_flows,serv_data_flow_down down_flows
		      from bass1.G_S_04018_DAY_20100501bak
		     where time_id between $last_last_month_day and $this_month_last_day
		       and upper(apnni)<>'CMLAP'
		       and (bigint(serv_data_flow_up)+bigint(serv_data_flow_down))>0
		    union all
		    select product_no,imei,up_flows,down_flows
		      from bass1.G_S_04018_DAY
		     where time_id between $last_last_month_day and $this_month_last_day
		       and upper(apnni)<>'CMLAP'
		       and (bigint(up_flows)+bigint(down_flows))>0    
		   )  a
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
	set sql_buff "
		insert into bass1.td_check_05_2(product_no,intra_product_no)
		select a.product_no,b.intra_product_no
		from bass1.td_check_05 a,
		(select distinct intra_product_no,product_no 
		  from 
		  (
		  select distinct intra_product_no,product_no from bass1.G_S_04018_DAY_20100501bak
		  where time_id between $last_last_month_day and $this_month_last_day
		  union all
		  select distinct intra_product_no,product_no from bass1.G_S_04018_DAY
		  where time_id between $last_last_month_day and $this_month_last_day
		  ) a  
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
    #如果是在第3步表2_TMP中，且有过TD网语音清单的用户，则打上“使用语音TD网络客户标志”；
    #如果是在第3步表2_TMP中，且有过TD网GPRS清单(04002)的用户，则标记为“使用GPRSTD网络客户标志”；
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


    #插入临时表bass1.td_check_06
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
    
    #TD客户总数
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'007'
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
	set sql_buff "
		insert into bass1.td_check_user_mobile
		select distinct a.product_no
		from  bass1.td_check_ls_02 a 
		inner join bass1.td_check_06 b 
		on a.product_no=b.product_no
		inner join bass2.dim_tacnum_devid c on c.TAC_NUM=substr(a.imei_14,1,8)
		where c.DEV_ID not in 
		   (select d.device_id 
		     from bass2.dim_device_profile d 
		    where value in ('006099002','006099005')
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


    #总的TD手机客户数
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'008'
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
	set sql_buff "
		insert into td_check_user_datacard
		select distinct product_no from
		(
		select distinct a.product_no,a.imei_14
		from  bass1.td_check_ls_02 a,bass1.td_check_06 b
		where a.product_no=b.product_no
		) a,
		(
		select distinct d.tac_num,d.dev_id from bass2.dim_tacnum_devid d
		,bass2.dim_device_profile e
		where d.time_id = $op_month
		  and e.time_id = '$op_month'
		  and d.dev_id = e.device_id
		  and e.value in ('006099002')
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


    #总的数据卡用户数
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'009'
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
	set sql_buff "
		insert into td_check_user_wire
		select distinct product_no from
		(
		select distinct a.product_no,a.imei_14
		from  bass1.td_check_ls_02 a,bass1.td_check_06 b
		where a.product_no=b.product_no
		) a,
		(
		select distinct d.tac_num,d.dev_id from bass2.dim_tacnum_devid d
		,bass2.dim_device_profile e
		where d.time_id = $op_month
		  and e.time_id = '$op_month'
		  and d.dev_id = e.device_id
		  and e.value in ('006099005')
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

    #TD无线座机客户
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'011'
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


    #TD上网本客户数
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'010'
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


    #使用TD网络的客户在T网上计费时长
    #取TD客户中间表中，日期为统计月的TD客户的“T网语音基本计费时长”之和
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'022'
          ,sum(bigint(a.base_bill_duration)) from bass1.g_s_21003_month a,
		(select product_no from bass1.td_check_06_2010 where td_net_mark='1') b
		where a.time_id = $op_month
		  and a.mns_type = '1'
		  and a.product_no = b.product_no
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #使用TD网络的客户在T网上的数据流量
    #取TD客户中间表中日期为统计月的TD客户的普通GPRS话单（04002）T网上\下行流量、上网本GPRS话单T网上\下行流量之和，
    #并去掉如下流量（如果“TD上网本客户标志”为1，但“TD手机客户标志”、“TD数据卡客户标志”、“TD无线座机客户标志”都为0
    #的客户的GPRS话单（04002）的T网上下行流量）。

	set handle [ aidb_open $conn ]
	set sqlbuf "
		select 
		decimal(1.0*(sum(bigint(a.up_flows))+sum(bigint(a.down_flows)))/1024/1024,20,2)
		from bass1.G_S_04002_DAY a,
		(select product_no from bass1.td_check_06_2010 ) b
		where a.time_id/100 = $op_month
		  and a.product_no=b.product_no
		  and a.mns_type='1'
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
		decimal(1.0*(sum(bigint(a.up_flows))+sum(bigint(a.down_flows)))/1024/1024,20,2)
		from bass1.G_S_04018_DAY a,
		(select product_no from bass1.td_check_06_2010) b
		where a.time_id/100=$op_month
		  and a.product_no=b.product_no
		  and a.mns_type='1'
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


	set handle [ aidb_open $conn ]
	set sqlbuf "
		select 
		decimal(1.0*(sum(bigint(a.up_flows))+sum(bigint(a.down_flows)))/1024/1024,20,2)
		from bass1.G_S_04002_DAY a,
		bass1.td_check_user_flow b
		where a.time_id/100=$op_month
		  and a.product_no=b.product_no
		  and a.mns_type='1'
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


    #使用TD网络的客户在T网上的数据流量
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
        values($op_month,'023',$RESULT_VAL1+$RESULT_VAL2-$RESULT_VAL3)
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



    #新增客户数
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'001'
          ,count(distinct user_id) from bass1.td_check_user_status_ls
		  where bigint(create_date) between $this_month_first_day and $this_month_last_day
			and test_flag='0'
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #客户到达数
    set handle [aidb_open $conn]
	set sqlbuf "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'002'
          ,count(distinct user_id) from bass1.td_check_user_status_ls
		 where usertype_id NOT IN ('2010','2020','2030','9000')
		   and test_flag='0'
	     "

	puts $sqlbuf
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #离网客户数
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'003'
          ,count(distinct user_id) from bass1.td_check_user_status_ls
		where usertype_id IN ('2010','2020','2030','9000')
		  and test_flag='0'
		  and time_id between $this_month_first_day and $this_month_last_day
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #净增客户数=本月客户到达数-上月客户到达数
    set handle [aidb_open $conn]
	set sqlbuf "
        select index_values from bass1.report_key_index_month where time_id=$op_month and index_code='002'
	     "

    puts $sqlbuf
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL4 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle


    
    set handle [aidb_open $conn]
	set sqlbuf "
        select index_values from bass1.report_key_index_month where time_id=$last_month and index_code='002'
	     "

    puts $sqlbuf
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL5 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle


    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		 values($op_month,'004',$RESULT_VAL4-$RESULT_VAL5)
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #通信客户数
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'005'
          ,sum(bigint(m_comm_users)) from bass1.g_s_22038_day
          where time_id = $this_month_last_day
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #通话客户数
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'006'
          ,count(distinct product_no) from bass1.g_s_21003_month
         where time_id=$op_month
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    
    #联通移动客户总数
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'012'
          ,bigint(union_mobile_arrive_cnt) from bass1.G_S_22073_DAY
         where time_id = $this_month_last_day
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #电信移动客户总数
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'013'
          ,bigint(tel_mobile_arrive_cnt) from bass1.G_S_22073_DAY
         where time_id= $this_month_last_day
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #联通移动新增客户数
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'014'
          ,sum(bigint(union_mobile_new_add_cnt)) from bass1.G_S_22073_DAY
         where time_id between $this_month_first_day and $this_month_last_day
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #电信移动新增客户数
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'015'
          ,sum(bigint(tel_mobile_new_add_cnt)) from bass1.G_S_22073_DAY
         where time_id between $this_month_first_day and $this_month_last_day
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #联通移动离网客户数
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'016'
          ,sum(bigint(union_mobile_lost_cnt)) from bass1.G_S_22073_DAY
         where time_id between $this_month_first_day and $this_month_last_day
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #电信移动离网客户数
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'017'
          ,sum(bigint(tel_mobile_lost_cnt)) from bass1.G_S_22073_DAY
         where time_id between $this_month_first_day and $this_month_last_day
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #计费时长
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'018'
          ,sum(bigint(base_bill_duration)) from bass1.g_s_21003_month
         where time_id = $op_month
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #移动数据流量
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'019'
          ,decimal(1.0*(sum(bigint(up_flows))+sum(bigint(down_flows)))/1024/1024,20,2)
		  from bass1.G_S_04002_DAY
		 where time_id/100= $op_month
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
    
    #梦网短信量
    set handle [aidb_open $conn]
	set sqlbuf "
       select count(*) from bass1.g_s_04005_day 
		where time_id/100=$op_month
		  and calltype_id in ('00','01','10','11')
		  and sms_status='0'
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

    #普通短信
    set handle [aidb_open $conn]
	set sqlbuf "
		select sum(bigint(sms_counts)) cnt from bass1.g_s_21007_day
		where time_id/100=$op_month
		  and end_status='0'
		  and cdr_type_id in ('00','10','21')
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

    #音信互动短信
    set handle [aidb_open $conn]
	set sqlbuf "
       select count(*) from bass1.g_s_04014_day
		where time_id/100=$op_month
		  and sms_send_state='0'
		  and sms_bill_type in ('00','01','10','11')
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

    #短信计费量
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		 values($op_month,'020',$RESULT_VAL6+$RESULT_VAL7+$RESULT_VAL8)
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #彩信计费量
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'021'
          ,sum(cnt) 
          from
			(
			select count(*) cnt from bass1.g_s_04004_day
			where time_id/100=$op_month
			  and mm_bill_type in ('00')
			  and applcn_type='0'
			union all
			select count(*) cnt from bass1.g_s_04004_day
			where time_id/100=$op_month
			  and applcn_type in('1','2','3','4')
			 ) as a
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #出账客户数
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'024'
          ,count(distinct user_id) from bass1.g_s_03005_month
         where time_id=$op_month
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

    #业务收入（账单收入）
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
		select
		  $op_month
		  ,'025'
          ,decimal(1.0*sum(bigint(should_fee))/100,20,2)
          from bass1.g_s_03005_month
         where time_id=$op_month
	     "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #满足【TD客户总数】定义的TD客户当月产生的账单收入，不包括仅产生了上网本TD-GPRS话单的上网本客户的非上网本科目费用，
    #和未产生上网本TD-GPRS话单的非上网本客户的上网本科目费用

    #TD客户总数的收入
    set handle [aidb_open $conn]
	set sqlbuf "
		select decimal(1.0*sum(bigint(fee_receivable))/100,20,2) from bass1.g_s_03004_month a,
		(
		select a.user_id from bass1.td_check_user_status_ls a,
		(select product_no from bass1.td_check_user) b
		where a.product_no=b.product_no
		) b
		where a.user_id=b.user_id
		  and a.time_id=$op_month
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


    #插入临时表bass1.td_check_user_fee1
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


    #仅产生了上网本TD-GPRS话单的上网本客户的非上网本科目费用
    set handle [aidb_open $conn]
	set sqlbuf "
		select decimal(1.0*sum(bigint(fee_receivable))/100,20,2) from bass1.g_s_03004_month a,
		(
		select a.user_id from bass1.td_check_user_status_ls a,
		(select product_no from bass1.td_check_user_fee1) b
		where a.product_no=b.product_no
		) b
		where a.user_id=b.user_id
		  and a.time_id=$op_month
		  and a.acct_item_id not in ('0631','0632')
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


    #插入临时表bass1.td_check_user_fee2
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


    #未产生上网本TD-GPRS话单的非上网本客户的上网本科目费用
    set handle [aidb_open $conn]
	set sqlbuf "
		select decimal(1.0*sum(bigint(fee_receivable))/100,20,2) from bass1.g_s_03004_month a,
		(
		select a.user_id from bass1.td_check_user_status_ls a,
		(select product_no from bass1.td_check_user_fee2) b
		where a.product_no=b.product_no
		) b
		where a.user_id=b.user_id
		  and a.time_id=$op_month
		  and a.acct_item_id in ('0631','0632')
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


    #TD客户收入
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
        values($op_month,'026',$RESULT_VAL9-$RESULT_VAL10-$RESULT_VAL11)
	  "

	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    #满足【TD客户总数】定义的TD手机客户当月产生的账单收入，不包括同时为上网本客户的上网本科目费用
    set handle [aidb_open $conn]
	set sqlbuf "
		select decimal(1.0*sum(bigint(fee_receivable))/100,20,2) from bass1.g_s_03004_month a,
		(
		select a.user_id from bass1.td_check_user_status_ls a,
		(select distinct product_no from bass1.td_check_user_mobile
		 ) b
		where a.product_no=b.product_no
		  and a.usertype_id not in ('2010','2020','2030','9000')
		  and a.test_flag='0'
		) b
		where a.user_id=b.user_id
		  and a.time_id=$op_month
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

    #上网本客户的上网本科目费用
    set handle [aidb_open $conn]
	set sqlbuf "
		select decimal(1.0*sum(bigint(fee_receivable))/100,20,2) from bass1.g_s_03004_month a,
		(
		select a.user_id from bass1.td_check_user_status_ls a,
		(select distinct product_no from bass1.td_check_user_mobile
		 intersect
		 select distinct product_no from bass1.td_check_user_net
		) b
		where a.product_no=b.product_no
		  and a.usertype_id not in ('2010','2020','2030','9000')
		  and a.test_flag='0'
		) b
		where a.user_id=b.user_id
		  and a.acct_item_id in ('0631','0632')
		  and a.time_id=$op_month
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


    #TD手机客户收入
    set handle [aidb_open $conn]
	set sql_buff "
		insert into bass1.report_key_index_month(time_id,index_code,index_values)
        values($op_month,'027',$RESULT_VAL12-$RESULT_VAL13)
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





