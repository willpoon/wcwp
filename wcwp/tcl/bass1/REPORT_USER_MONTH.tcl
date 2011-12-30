######################################################################################################
#报表名称：用户资料中间表
#程序名称: REPORT_USER_MONTH.tcl
#功能描述: 根据集团下发的中间表统计口径在本地生成这些数据值
#运行粒度: 月
#源    表：1.
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-07-05
#问题记录：1.
#修改历史:
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本年 yyyy
        set op_year [string range $optime_month 0 3]
        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.report_user_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#step 1
        #创建临时表T0
        set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.T0
		 (
		  BRAND_ID               INTEGER,
		  USER_TYPE_ID           VARCHAR(10),
		  USER_STATUS_ID         VARCHAR(10),
		  USER_BUSI_TYPE         VARCHAR(10),
		  CHANNEL_ID             VARCHAR(20),
		  CREATE_DATE            INTEGER,
		  CUST_ID                VARCHAR(20),
		  PAY_TYPE_ID            VARCHAR(10),
		  DATE_SIM_FLAG          INTEGER,
		  CREATE_MODE            VARCHAR(10),
		  MSISDN                 VARCHAR(20),
		  USER_STATUS_DATE       INTEGER,
		  USER_ID                VARCHAR(20),
		  on_time                bigint,
		  OFF_MONTH_FLAG         INTEGER
		 )
                 partitioning key
                 (user_id)
                 using hashing
                 with replace on commit preserve rows not logged in tbs_user_temp;"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#创建临时表T1
        set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.T1
	    (
             BRAND_ID               INTEGER,
             USER_TYPE_ID           VARCHAR(10),
             USER_STATUS_ID         VARCHAR(10),
             allowance_flag         integer,
             USER_BUSI_TYPE         VARCHAR(10),
             CHANNEL_ID             VARCHAR(20),
             CREATE_DATE            INTEGER,
             PHOTO_FLAG             INTEGER,
             CUST_ID                VARCHAR(20),
             PAY_TYPE_ID            VARCHAR(10),
             DATE_SIM_FLAG          INTEGER,
             HIGH_PHOTO_FLAG        INTEGER,
             NEW_MONTH_FLAG         INTEGER,
             YEAR_INTEGRAL_SCALAR   VARCHAR(10),
             CREATE_MODE            VARCHAR(10),
             OFF_MONTH_FLAG         INTEGER,
             NEW_YEAR_FLAG          INTEGER,
             CONSUME_SCALAR         VARCHAR(10),
             CALL_SCALAR            VARCHAR(10),
             MSISDN                 VARCHAR(20),
             age_scalar             varchar(10),
             USER_STATUS_DATE       INTEGER,
             USER_ID                VARCHAR(20),
             ZERO_CALL_FLAG         INTEGER,
             ZERO_ROAM_FLAG         INTEGER,
             ZERO_TOLL_FLAG         INTEGER,
             AVAILAB_FLAG           INTEGER,
             YEAR_VALUE_INTEGRAL    BIGINT,
             CONSUME_FEE            DECIMAL(18,2),
             CALL_DURN              BIGINT,
             age                    integer,
             on_time                bigint,
             VIP_LVL                varchar(10),
             VIP_FLAG               INTEGER,
             LAST_CONSUME_FEE       DECIMAL(18,2),
             OFF_HIGH_PHOTO_FLAG   INTEGER,
             ENTERPRISE_FLAG        INTEGER,
             ENTERPRISE_INDUS_TYPE  VARCHAR(10),
             UNITE_PAY_FLAG         INTEGER
           )
           partitioning key
           (user_id)
           using hashing
           with replace on commit preserve rows not logged in tbs_user_temp;"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#创建用户群T0
	#A:插入非离网用户,剔除数据sim卡用户和测试卡用户
	set sql_buff "insert into  session.T0
        select
          int(t.brand_id)
          ,t.usertype_id
          ,m.usertype_id
          ,t.user_bus_typ_id
          ,t.channel_id
          ,int(t.create_date)
          ,t.cust_id
          ,t.prompt_type
          ,int(t.sim_code)
          ,t.subs_style_id
          ,t.product_no
          ,t.time_id
          ,t.user_id
          ,int($this_month_last_day)-int(t.create_date)
          ,0
    	from
          (  select
 	       a.*
    	     from bass1.g_a_02004_day  a,
    	     (select user_id,max(time_id) as time_id from bass1.g_a_02004_day
    	      where time_id <=$this_month_last_day
    	      group by user_id
    	      ) b
    	     where a.user_id = b.user_id and a.time_id = b.time_id
    	  )t,
    	  (  select
               a.*
             from bass1.g_a_02008_day a,
             (select user_id,max(time_id) as time_id from bass1.g_a_02008_day
              where time_id <=$this_month_last_day
              group by user_id
             ) b
             where a.user_id = b.user_id and a.time_id = b.time_id
           ) m
        where
           t.time_id <=$this_month_last_day
           and m.time_id <=$this_month_last_day
           and t.usertype_id <> '3'
           and t.sim_code <> '1'
           and m.usertype_id not in ('2010','2020','2030','1040','1021','9000')
           and t.user_id = m.user_id ;"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#B:插入本月离网的用户,剔除数据sim卡用户和测试卡用户
	set sql_buff "insert into  session.T0
        select
          int(t.brand_id)
          ,t.usertype_id
          ,m.usertype_id
          ,t.user_bus_typ_id
          ,t.channel_id
          ,int(t.create_date)
          ,t.cust_id
          ,t.prompt_type
          ,int(t.sim_code)
          ,t.subs_style_id
          ,t.product_no
          ,t.time_id
          ,t.user_id
          ,int(t.time_id)-int(t.create_date)
          ,1
    	from
          (  select
 	       a.*
    	     from bass1.g_a_02004_day  a,
    	     (select user_id,max(time_id) as time_id from bass1.g_a_02004_day
    	      where time_id <=$this_month_last_day
    	      group by user_id
    	      ) b
    	     where a.user_id = b.user_id and a.time_id = b.time_id
    	  )t,
    	  bass1.g_a_02008_day m
        where
           t.time_id/100=$op_month
           and t.usertype_id <> '3'
           and t.sim_code <> '1'
           and m.usertype_id not in('1010','1021','1022','1031','1032','1033','1034','1039','1040','9000')
           and t.user_id = m.user_id ; "
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

      #对临时表T0创建索引
       set sql_buff "create index session.idx_ui
                        on session.T0(user_id);"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

     #将
     set sql_buff "insert into session.T1
          select
            BRAND_ID
            ,USER_TYPE_ID
            ,USER_STATUS_ID
            ,0
            ,USER_BUSI_TYPE
            ,CHANNEL_ID
            ,CREATE_DATE
            ,0
            ,CUST_ID
            ,PAY_TYPE_ID
            ,DATE_SIM_FLAG
            ,0
            ,0
            ,'-1'
            ,CREATE_MODE
            ,OFF_MONTH_FLAG
            ,0
            ,'-1'
            ,'-1'
            ,MSISDN
            ,'-1'
            ,USER_STATUS_DATE
            ,USER_ID
            ,0
            ,0
            ,0
            ,0
            ,0
            ,0
            ,0
            ,0
            ,on_time
            ,'-1'
            ,0
            ,0
            ,0
            ,0
            ,'-1'
            ,0
          from
            session.T0 ;"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

        #月新增标志
        set sql_buff "update  session.T1
          set NEW_MONTH_FLAG=1
          where
             CREATE_DATE/100=$op_month; "
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#年新增标志
        set sql_buff "update session.T1
          set NEW_YEAR_FLAG=1
          where
            CREATE_DATE/10000=$op_year; "
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

       #个人大客户标志、个人客户级别编码
	set sql_buff "insert into session.T1
          select
            a.BRAND_ID
            ,a.USER_TYPE_ID
            ,a.USER_STATUS_ID
            ,0
            ,a.USER_BUSI_TYPE
            ,a.CHANNEL_ID
            ,a.CREATE_DATE
            ,0
            ,a.CUST_ID
            ,a.PAY_TYPE_ID
            ,a.DATE_SIM_FLAG
            ,0
            ,0
            ,'-1'
            ,a.CREATE_MODE
            ,a.OFF_MONTH_FLAG
            ,0
            ,'-1'
            ,'-1'
            ,a.MSISDN
            ,'-1'
            ,a.USER_STATUS_DATE
            ,a.USER_ID
            ,0
            ,0
            ,0
            ,0
            ,0
            ,0
            ,0
            ,0
            ,a.on_time
            ,b.CUSTCLASS_ID
            ,1
            ,0
            ,0
            ,0
            ,'-1'
            ,0
          from
            session.T0 a,
            bass1.g_i_02005_month b
          where
            b.time_id=$op_month
            and  a.user_id=b.user_id "
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

        #月消费金额、月消费层次分档
	set sql_buff "insert into session.T1
          select
            a.BRAND_ID
            ,a.USER_TYPE_ID
            ,a.USER_STATUS_ID
            ,0
            ,a.USER_BUSI_TYPE
            ,a.CHANNEL_ID
            ,a.CREATE_DATE
            ,0
            ,a.CUST_ID
            ,a.PAY_TYPE_ID
            ,a.DATE_SIM_FLAG
            ,0
            ,0
            ,'-1'
            ,a.CREATE_MODE
            ,a.OFF_MONTH_FLAG
            ,0
            ,case
               when bigint(b.SHOULD_FEE)=0                           then '1'
               when bigint(b.SHOULD_FEE) between 1 and 499           then '2'
               when bigint(b.SHOULD_FEE) between 500 and 999         then '3'
               when bigint(b.SHOULD_FEE) between 1000 and 1499       then '4'
               when bigint(b.SHOULD_FEE) between 1500 and 1999       then '5'
               when bigint(b.SHOULD_FEE) between 2000 and 2499       then '6'
               when bigint(b.SHOULD_FEE) between 2500 and 4999       then '7'
               when bigint(b.SHOULD_FEE) between 5000 and 7999       then '8'
               when bigint(b.SHOULD_FEE) between 8000 and 9999       then '9'
               when bigint(b.SHOULD_FEE) between 10000 and 11999     then '10'
               when bigint(b.SHOULD_FEE) between 12000 and 14999     then '11'
               when bigint(b.SHOULD_FEE) between 15000 and 19999     then '12'
               when bigint(b.SHOULD_FEE) between 20000 and 29999     then '13'
               when bigint(b.SHOULD_FEE) between 30000 and 39999     then '14'
               when bigint(b.SHOULD_FEE) between 40000 and 49999     then '15'
               when bigint(b.SHOULD_FEE) between 50000 and 59999     then '16'
               when bigint(b.SHOULD_FEE) between 60000 and 79999     then '17'
               when bigint(b.SHOULD_FEE) between 80000 and 99999     then '18'
               when bigint(b.SHOULD_FEE) between 100000 and 149999   then '19'
               else '20'
             end
            ,'-1'
            ,a.MSISDN
            ,'-1'
            ,a.USER_STATUS_DATE
            ,a.USER_ID
            ,0
            ,0
            ,0
            ,0
            ,0
            ,bigint(b.SHOULD_FEE)
            ,0
            ,0
            ,a.on_time
            ,'-1'
            ,0
            ,0
            ,0
            ,0
            ,'-1'
            ,0
          from
            session.T0 a,
            bass1.g_s_03005_month b
          where
            b.time_id=$op_month
            and  a.user_id=b.user_id "
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#月基本计费时长、月通话时长分档、零次通话标志、
	#零次长途标志、零次漫游标志
	set sql_buff "insert into session.T1
          select
            a.BRAND_ID
            ,a.USER_TYPE_ID
            ,a.USER_STATUS_ID
            ,0
            ,a.USER_BUSI_TYPE
            ,a.CHANNEL_ID
            ,a.CREATE_DATE
            ,0
            ,a.CUST_ID
            ,a.PAY_TYPE_ID
            ,a.DATE_SIM_FLAG
            ,0
            ,0
            ,'-1'
            ,a.CREATE_MODE
            ,a.OFF_MONTH_FLAG
            ,0
            ,'-1'
            ,case 
               when sum(bigint(b.base_bill_duration))+sum(bigint(c.base_bill_duration)) between 1 and 50          then '1'
               when sum(bigint(b.base_bill_duration))+sum(bigint(c.base_bill_duration)) between 51 and 100        then '2'
               when sum(bigint(b.base_bill_duration))+sum(bigint(c.base_bill_duration)) between 101 and 200       then '3'
               when sum(bigint(b.base_bill_duration))+sum(bigint(c.base_bill_duration)) between 201 and 250       then '4'
               when sum(bigint(b.base_bill_duration))+sum(bigint(c.base_bill_duration)) between 251 and 300       then '5'
               when sum(bigint(b.base_bill_duration))+sum(bigint(c.base_bill_duration)) between 301 and 450       then '6'
               when sum(bigint(b.base_bill_duration))+sum(bigint(c.base_bill_duration)) between 451 and 600       then '7'
               when sum(bigint(b.base_bill_duration))+sum(bigint(c.base_bill_duration)) between 601 and 750       then '8'
               when sum(bigint(b.base_bill_duration))+sum(bigint(c.base_bill_duration)) between 751 and 900       then '9'
               when sum(bigint(b.base_bill_duration))+sum(bigint(c.base_bill_duration)) between 901 and 1200      then '10'
               when sum(bigint(b.base_bill_duration))+sum(bigint(c.base_bill_duration)) between 1201 and 1500     then '11'
               else '12'
             end
            ,a.MSISDN
            ,'-1'
            ,a.USER_STATUS_DATE
            ,a.USER_ID
            ,case
               when sum(bigint(b.base_bill_duration))+sum(bigint(c.base_bill_duration))=0 then 1
               else 0
             end
            ,case
               when sum(case when b.roam_type_id<'500' or c.roam_type_id<'500' then 1 else 0 end)=0 then 1
               else 0
             end
            ,case
               when sum(case when b.toll_type_id>'010' or c.toll_type_id>'010' then 1 else 0 end)=0 then 1
               else 0
             end
            ,0
            ,0
            ,0
            ,sum(bigint(b.base_bill_duration))+sum(bigint(c.base_bill_duration))
            ,0
            ,a.on_time
            ,'-1'
            ,0
            ,0
            ,0
            ,0
            ,'-1'
            ,0
          from
            session.T0 a
          left join
            bass1.g_s_21003_month b
          on
            a.MSISDN=b.product_no
          left join
           (select * from  bass1.g_s_21009_day where time_id/100=$op_month
           )c
          on a.MSISDN=b.product_no
          group by
            a.BRAND_ID
            ,a.USER_TYPE_ID
            ,a.USER_STATUS_ID
            ,a.USER_BUSI_TYPE
            ,a.CHANNEL_ID
            ,a.CREATE_DATE
            ,a.CUST_ID
            ,a.PAY_TYPE_ID
            ,a.DATE_SIM_FLAG
            ,a.CREATE_MODE
            ,a.OFF_MONTH_FLAG
            ,a.MSISDN
            ,a.USER_STATUS_DATE
            ,a.USER_ID
            ,a.on_time
            ,b.roam_type_id
            ,c.roam_type_id
            ,b.toll_type_id
            ,c.toll_type_id "
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#有效客户标志
        set sql_buff "update  session.T1
          set AVAILAB_FLAG=1
          where
            CONSUME_FEE>0 or CALL_DURN>0 "
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


       #集团客户标志、集团行业分类编码、集团用户统一付费标志
	set sql_buff "insert into session.T1
          select
            a.BRAND_ID
            ,a.USER_TYPE_ID
            ,a.USER_STATUS_ID
            ,0
            ,a.USER_BUSI_TYPE
            ,a.CHANNEL_ID
            ,a.CREATE_DATE
            ,0
            ,a.CUST_ID
            ,a.PAY_TYPE_ID
            ,a.DATE_SIM_FLAG
            ,0
            ,0
            ,'-1'
            ,a.CREATE_MODE
            ,a.OFF_MONTH_FLAG
            ,0
            ,'-1'
            ,'-1'
            ,a.MSISDN
            ,'-1'
            ,a.USER_STATUS_DATE
            ,a.USER_ID
            ,0
            ,0
            ,0
            ,0
            ,0
            ,0
            ,0
            ,0
            ,a.on_time
            ,'-1'
            ,0
            ,0
            ,0
            ,int(c.ent_def_mode)
            ,case
               when c.ent_industry_id=' ' then '99'
               else c.ent_industry_id
             end
            ,int(c.unite_pay_flag)
          from
            session.T0 a
          left join
            (select enterprise_id,user_id from bass1.g_i_02049_month 
             where time_id=$op_month
             )b
          on  a.user_id=b.user_id
          left join
            (select a.enterprise_id,a.ent_def_mode,a.ent_industry_id,a.unite_pay_flag from bass1.g_a_01004_day a,
                            (select max(time_id) as time_id,enterprise_id from bass1.g_a_01004_day
                             where time_id<=$this_month_last_day
                             group by enterprise_id
                            )b
             where a.time_id=b.time_id and a.enterprise_id=b.enterprise_id
            )c
          on b.enterprise_id=c.enterprise_id "
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

       #汇总到结果表
	set sql_buff "insert into bass1.report_user_month
           select
             $op_month
             ,BRAND_ID
             ,USER_TYPE_ID
             ,USER_STATUS_ID
             ,max(allowance_flag)
             ,max(USER_BUSI_TYPE)
             ,max(CHANNEL_ID)
             ,max(CREATE_DATE)
             ,max(PHOTO_FLAG)
             ,CUST_ID
             ,max(PAY_TYPE_ID)
             ,max(DATE_SIM_FLAG)
             ,max(HIGH_PHOTO_FLAG)
             ,max(NEW_MONTH_FLAG)
             ,max(YEAR_INTEGRAL_SCALAR)
             ,max(CREATE_MODE)
             ,max(OFF_MONTH_FLAG)
             ,max(NEW_YEAR_FLAG)
             ,max(CONSUME_SCALAR)
             ,max(CALL_SCALAR)
             ,MSISDN
             ,max(age_scalar)
             ,max(USER_STATUS_DATE)
             ,USER_ID
             ,max(ZERO_CALL_FLAG)
             ,max(ZERO_ROAM_FLAG)
             ,max(ZERO_TOLL_FLAG)
             ,max(AVAILAB_FLAG)
             ,max(YEAR_VALUE_INTEGRAL)
             ,max(CONSUME_FEE)
             ,max(CALL_DURN)
             ,max(age)
             ,max(on_time)
             ,max(VIP_LVL)
             ,max(VIP_FLAG)
             ,max(LAST_CONSUME_FEE)
             ,max(OFF_HIGH_PHOTO_FLAG)
             ,max(ENTERPRISE_FLAG)
             ,max(ENTERPRISE_INDUS_TYPE)
             ,max(UNITE_PAY_FLAG)
           from
             session.T1
           group by
             BRAND_ID
             ,USER_TYPE_ID
             ,USER_STATUS_ID
             ,CUST_ID
             ,MSISDN
             ,USER_ID "
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
######################end####################
        aidb_close $handle
	return 0;
}      