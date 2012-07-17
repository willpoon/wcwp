#======================================================================================
#版权归属：Copyright (c) 2010,AsiaInfo-Linkage Inc.
#程序名称: App_xysc_new_user_ms.tcl
#程序功能: 西藏移动校园市场专题-3.校园新增客户识别
#核心思想: 用部分已知真实的客户去寻找其它最接近真实客户,并逐步扩大已知真实的客户,
#          如此反复迭代最终完成对所有校园客户的识别
#          移动当月新增识别方法:
#           	经分当月新增客户中,在校园基站通话次数占全市基站通话次数>=0.3的,当月和本学校已识别客户通话次数>=1或短信次数>=1
#          竞争对手当月新增识别方法:
#          	  暂时按照当月到达同上月到达比较得出,圈定当月竞争对手新增的那部分
#数据流向: dw_product_yyyymm/dw_call_cell_yyyymm/dw_call_opposite_yyyymm/dw_newbusi_sms_yyyymm/
#          dw_xysc_school_real_user_dt_yyyymm/dw_xysc_school_comp_user_dt_yyyymm/dw_comp_cust_yyyymm
#          -->dw_xysc_school_new_user_mm
#运行粒度: 月
#运行示例: ds App_xysc_new_user_ms.tcl 2010-09-01
#创建时间: 2010-12-2
#创 建 人: AsiaInfo	Liwei
#存在问题: 略
#修改历史: 1.
#          2.
#=======================================================================================

 set db_schema     "bass2"

 proc deal {p_optime p_timestamp} {

 	   global conn
 	   global handle

 	   if [catch {set handle [aidb_open $conn]} errmsg] {
 	   	trace_sql $errmsg 1000
 	   	return -1
 	   }
 	   scan   $p_optime "%04s-%02s-%02s" year month day

     #Step1:当月新增客户校园区域基站通话次数及占比处理
		 puts "Step1:当月新增客户校园区域基站通话次数及占比处理-Begin...\n"
 	   if {[Do_xysc_new_user_xycall_rate $p_optime]  != 0} {
 	   	  aidb_roll $conn
 	   	  aidb_close $handle
 	   	 return -1
 	   }
     #Step2:当月新增客户唯一归属校园处理(校园基站占比不小于0.3)
		 puts "Step2:当月新增客户唯一归属校园处理-Begin...\n"
 	   if {[Do_xysc_new_user_belong_school $p_optime]  != 0} {
 	   	  aidb_roll $conn
 	   	  aidb_close $handle
 	   	 return -1
 	   }
     #Step3:当月新增客户与校园客户通信(CALL/SMS)处理
		 puts "Step3:当月新增客户与校园客户通信(CALL/SMS)处理-Begin...\n"
 	   if {[Do_xysc_new_user_comm_school $p_optime]  != 0} {
 	   	  aidb_roll $conn
 	   	  aidb_close $handle
 	   	 return -1
 	   }

     #Step4:当月新增校园客户识别处理
		 puts "Step4:当月新增校园客户识别处理-Begin...\n"
 	   if {[Do_xysc_new_user_mm_info $p_optime]  != 0} {
 	   	  aidb_roll $conn
 	   	  aidb_close $handle
 	   	 return -1
 	   }
 	    	    	   	    	   
 	   #Step5:07/08月校园客户及校园竞争对手识别处理
     if { $month == "07" || $month == "08" } {
		    puts "Step5:${month}月校园客户及校园竞争对手识别处理-Begin...\n"
 	      if {[Do_xysc_school_comp_user $p_optime]  != 0} {
 	      	  aidb_roll $conn
 	      	  aidb_close $handle
 	      	 return -1
 	      }
     }

     #Step6:当月拍照校园客户处理
		 puts "Step6:当月拍照校园客户处理-Begin...\n"
 	   if {[Do_xysc_school_user_snapshot $p_optime]  != 0} {
 	   	  aidb_roll $conn
 	   	  aidb_close $handle
 	   	 return -1
 	   }
     #StepX.删除所有无需保留的临时表
		 puts "StepX.删除所有无需保留的临时表-Begin...\n"
 	   if {[Do_xysc_school_drop_tmp_table $p_optime]  != 0} {
 	   	  aidb_roll $conn
 	   	  aidb_close $handle
 	   	 return -1
 	   }

 	   aidb_commit $conn

 	   return 0
 }

 proc Do_xysc_new_user_xycall_rate {p_optime} {
 	   global conn
 	   global handle
	   global db_schema
	   source	./report.cfg

	   aidb_close $handle
	   if [catch {set handle [aidb_open $conn]} errmsg] {
	   	      trace_sql $errmsg 1302
	   	      return -1
	   }
 	   scan   $p_optime "%04s-%02s-%02s" year month day

 	   #源表
 	   set dw_product_yyyymm                    "$db_schema.dw_product_$year$month"
 	   set dw_call_cell_yyyymm                  "$db_schema.dw_call_cell_$year$month"
 	   set dim_xysc_lac_cell_info               "$db_schema.Dim_xysc_lac_cell_info"

 	   #1.新建临时表xysc_new_user_xycall_tmp,生成当月新增客户基站通话次数信息
	   set sql_buf "drop TABLE bass2.xysc_new_user_xycall_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_xycall_tmp(
			   user_id		  varchar(20),
			   product_no	  varchar(15),
			   lac_id		    varchar(10),
			   cell_id		  varchar(10),
			   call_counts	integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_xycall_tmp
         select a.user_id, a.product_no, b.lac_id, b.cell_id, sum(b.call_counts)
          from (select user_id, product_no
                  from $dw_product_yyyymm
                 where userstatus_id in ($rep_online_userstatus_id)
                   and month_new_mark = 1
                 group by user_id, product_no) a
          left join (select user_id,
                            lac_id,
                            cell_id,
                            sum(call_counts) as call_counts
                       from $dw_call_cell_yyyymm
                      group by user_id, lac_id, cell_id) b
            on a.user_id = b.user_id
         group by a.user_id, a.product_no, b.lac_id, b.cell_id
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #2.新建临时表xysc_new_user_xycall_tmp2,当月新增客户基站通话次数区分校园基站通话
	   set sql_buf "drop TABLE bass2.xysc_new_user_xycall_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_xycall_tmp2(
			   user_id		  varchar(20),
			   product_no	  varchar(15),
			   lac_id		    varchar(10),
			   cell_id		  varchar(10),
			   call_counts	integer,
			   xycall_flag  smallint
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into bass2.xysc_new_user_xycall_tmp2
	       select a.user_id,
               a.product_no,
               a.lac_id,
               a.cell_id,
               a.call_counts,
               case
                 when a.lac_id = b.lac_id and a.cell_id = b.cell_id then
                  1
                 else
                  0
               end as xycall_flag
          from xysc_new_user_xycall_tmp as a
          left outer join (select distinct lac_id, cell_id
                             from $dim_xysc_lac_cell_info) as b
            on a.lac_id = b.lac_id
           and a.cell_id = b.cell_id
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #3.新建临时表xysc_new_user_xycall_tmp3,当月新增客户校园基站通话占比
	   set sql_buf "drop TABLE bass2.xysc_new_user_xycall_tmp3"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_xycall_tmp3(
			   user_id		          varchar(20),
			   product_no	          varchar(15),
			   xy_call_counts       integer,
			   call_counts	        integer,
			   xy_call_counts_rate  decimal(10,2)
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into bass2.xysc_new_user_xycall_tmp3
	       select user_id,
               product_no,
               xy_call_counts,
               call_counts,
               case
                 when call_counts = 0 then
                  0.00
                 else
                  xy_call_counts * 1.0 / call_counts
               end
          from (select user_id, product_no, sum(xy_call_counts) as xy_call_counts, sum(call_counts) as call_counts
                  from (select user_id,
                               product_no,
                               sum(call_counts) as xy_call_counts,
                               0 as call_counts
                          from xysc_new_user_xycall_tmp2
                         where xycall_flag = 1
                         group by user_id, product_no
                        union all
                        select user_id,
                               product_no,
                               0 as xy_call_counts,
                               sum(call_counts) as call_counts
                          from xysc_new_user_xycall_tmp2
                         group by user_id, product_no) a
                 group by user_id, product_no) a
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step1:当月新增客户校园区域基站通话次数及占比处理-End...\n"

	return 0
}

 proc Do_xysc_new_user_belong_school {p_optime} {
 	   global conn
 	   global handle
	   global db_schema

	   aidb_close $handle
	   if [catch {set handle [aidb_open $conn]} errmsg] {
	   	      trace_sql $errmsg 1302
	   	      return -1
	   }
 	   scan   $p_optime "%04s-%02s-%02s" year month day

 	   #源表
 	   set xysc_new_user_xycall_tmp3            "$db_schema.xysc_new_user_xycall_tmp3"
 	   set xysc_new_user_xycall_tmp2            "$db_schema.xysc_new_user_xycall_tmp2"
 	   set dim_xysc_lac_cell_info               "$db_schema.Dim_xysc_lac_cell_info"

 	   #1.新建临时表xysc_new_user_belong_school_tmp,筛选新增客户校园基站占比不小于0.3信息
	   set sql_buf "drop TABLE bass2.xysc_new_user_belong_school_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_belong_school_tmp(
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   lac_id		       varchar(10),
			   cell_id		     varchar(10),
			   call_counts	   integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_belong_school_tmp
         select a.user_id, a.product_no, b.lac_id, b.cell_id, b.call_counts
          from $xysc_new_user_xycall_tmp3 as a
          left outer join (select *
                             from $xysc_new_user_xycall_tmp2
                            where xycall_flag = 1) as b
            on a.user_id = b.user_id
           and a.product_no = b.product_no
         where a.xy_call_counts_rate >= 0.3
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #2.新建临时表xysc_new_user_belong_school_tmp2,按校园基本信息表,初步将新增客户归属到学校
	   set sql_buf "drop TABLE bass2.xysc_new_user_belong_school_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_belong_school_tmp2(
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   lac_id		       varchar(10),
			   cell_id		     varchar(10),
			   call_counts	   integer,
			   school_name     varchar(128)
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_belong_school_tmp2
         select a.user_id,
               a.product_no,
               a.lac_id,
               a.cell_id,
               a.call_counts,
               b.school_name
          from xysc_new_user_belong_school_tmp as a
          left outer join (select distinct lac_id, cell_id, school_name
                             from $dim_xysc_lac_cell_info) as b
            on a.lac_id = b.lac_id
           and a.cell_id = b.cell_id
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #3.新建临时表xysc_new_user_belong_school_tmp3,按通话次数倒序排列新增客户归属学校信息
	   set sql_buf "drop TABLE bass2.xysc_new_user_belong_school_tmp3"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_belong_school_tmp3(
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   school_name	   varchar(128),
			   call_counts	   integer,
			   sn		           integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_belong_school_tmp3
         select user_id,
              product_no,
              school_name,
              call_counts,
              row_number() over(partition by user_id order by call_counts desc) as sn
         from (select user_id,
                      product_no,
                      school_name,
                      sum(call_counts) as call_counts
                 from xysc_new_user_belong_school_tmp2
                group by user_id, product_no, school_name) a
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #4.新建临时表xysc_new_user_belong_school_tmp4,将新增客户唯一归属到学校
	   set sql_buf "drop TABLE bass2.xysc_new_user_belong_school_tmp4"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_belong_school_tmp4(
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   school_name	   varchar(128),
			   xy_call_counts	 integer,
			   call_counts		 integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_belong_school_tmp4
         select a.user_id, a.product_no, a.school_name, a.call_counts, b.call_counts
          from xysc_new_user_belong_school_tmp3 as a
          left outer join $xysc_new_user_xycall_tmp3 as b
            on a.user_id = b.user_id
           and a.product_no = b.product_no
         where a.sn = 1
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_new_up"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_new_up on xysc_new_user_belong_school_tmp4 (user_id,product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step2:当月新增客户唯一归属校园处理-End...\n"

	return 0
}

 proc Do_xysc_new_user_comm_school {p_optime} {
 	   global conn
 	   global handle
	   global db_schema
	   source	./report.cfg

	   aidb_close $handle
	   if [catch {set handle [aidb_open $conn]} errmsg] {
	   	      trace_sql $errmsg 1302
	   	      return -1
	   }
 	   scan   $p_optime "%04s-%02s-%02s" year month day
 	   set lastmonth [GetLastMonth $year$month]

 	   #源表
 	   set xysc_new_user_belong_school_tmp4     "$db_schema.xysc_new_user_belong_school_tmp4"
 	   set dw_call_opposite_yyyymm              "$db_schema.dw_call_opposite_$year$month"
 	   set dw_xysc_school_real_user_dt_yyyymm1  "$db_schema.dw_xysc_school_real_user_dt_$lastmonth"
 	   set dw_xysc_school_comp_user_dt_yyyymm1  "$db_schema.dw_xysc_school_comp_user_dt_$lastmonth"
 	   set dw_newbusi_sms_yyyymm                "$db_schema.dw_newbusi_sms_$year$month"

 	   #1.新建临时表xysc_new_user_comm_school_calltmp,筛选新增客户通话对端信息
	   set sql_buf "drop TABLE bass2.xysc_new_user_comm_school_calltmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_comm_school_calltmp(
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   opposite_no	   varchar(32),
			   call_counts     integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_comm_school_calltmp
         select a.user_id, a.product_no, a.opp_number, sum(a.call_counts)
          from $dw_call_opposite_yyyymm as a
         inner join $xysc_new_user_belong_school_tmp4 as b
            on a.user_id = b.user_id
           and a.product_no = b.product_no
         group by a.user_id, a.product_no, a.opp_number
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #2.新建临时表xysc_new_user_comm_school_calltmp2,汇总新增客户与校园客户通话信息
	   set sql_buf "drop TABLE bass2.xysc_new_user_comm_school_calltmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_comm_school_calltmp2(
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   school_name	   varchar(128),
			   xy_call_counts  integer,
			   call_counts		 integer,
			   school_cnt	     integer,
			   school_users	   integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_comm_school_calltmp2
         select a.user_id,
               a.product_no,
               a.school_name,
               a.xy_call_counts,
               a.call_counts,
               value(b.school_cnt, 0),
               value(b.school_users, 0)
          from $xysc_new_user_belong_school_tmp4 as a
          left outer join (select a.product_no,
                                  a.school_name,
                                  sum(b.call_counts) as school_cnt,
                                  count(distinct b.opposite_no) as school_users
                             from $xysc_new_user_belong_school_tmp4 as a
                            inner join xysc_new_user_comm_school_calltmp as b
                               on a.user_id = b.user_id
                              and a.product_no = b.product_no
                            inner join (select distinct product_no, school_name
                                         from $dw_xysc_school_real_user_dt_yyyymm1
                                       union all
                                       select distinct comp_no as product_no,
                                                       school_name
                                         from $dw_xysc_school_comp_user_dt_yyyymm1) as c
                               on b.opposite_no = c.product_no
                              and a.school_name = c.school_name
                            group by a.product_no, a.school_name) as b
            on a.product_no = b.product_no
           and a.school_name = b.school_name
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_new_up2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_new_up2 on xysc_new_user_comm_school_calltmp2 (user_id,product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #3.新建临时表xysc_new_user_comm_school_smstmp,筛选新增客户短信对端信息
	   set sql_buf "drop TABLE bass2.xysc_new_user_comm_school_smstmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_comm_school_smstmp(
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   opposite_no	   varchar(32),
			   sms_counts      integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_comm_school_smstmp
         select a.user_id, a.product_no, a.opp_number, sum(a.counts)
          from $dw_newbusi_sms_yyyymm as a
         inner join $xysc_new_user_belong_school_tmp4 as b
            on a.user_id = b.user_id
           and a.product_no = b.product_no
           and a.svcitem_id in (200001, 200002, 200003, 200005)
         group by a.user_id, a.product_no, a.opp_number
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #4.新建临时表xysc_new_user_comm_school_commtmp,汇总新增客户与校园客户通信(CALL/SMS)信息
	   set sql_buf "drop TABLE bass2.xysc_new_user_comm_school_commtmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_comm_school_commtmp(
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   school_name	   varchar(128),
			   xy_call_counts	 integer,
			   call_counts		 integer,
			   school_cnt	     integer,
			   school_users	   integer,
			   school_sms_cnt	 integer,
			   sms_cnt		     integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_comm_school_commtmp
         select a.user_id,
               a.product_no,
               a.school_name,
               a.xy_call_counts,
               a.call_counts,
               a.school_cnt,
               a.school_users,
               value(b.school_sms_cnt, 0),
               value(b.sms_cnt, 0)
          from xysc_new_user_comm_school_calltmp2 as a
          left outer join (select a.product_no,
                                  a.school_name,
                                  sum(b.sms_counts) as sms_cnt,
                                  sum(case
                                        when b.OPPOSITE_NO = c.product_no and
                                             a.school_name = c.school_name then
                                         1
                                        else
                                         0
                                      end) as school_sms_cnt
                             from xysc_new_user_comm_school_calltmp2 as a
                            inner join xysc_new_user_comm_school_smstmp as b
                               on a.product_no = b.product_no
                             left outer join (select distinct PRODUCT_NO, school_name
                                               from $dw_xysc_school_real_user_dt_yyyymm1
                                             union all
                                             select distinct comp_no as product_no,
                                                             SCHOOL_NAME
                                               from $dw_xysc_school_comp_user_dt_yyyymm1) as c
                               on b.OPPOSITE_NO = c.product_no
                              and a.school_name = c.school_name
                            group by a.product_no, a.school_name) as b
            on a.product_no = b.product_no
           and a.school_name = b.school_name
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_new_up3"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_new_up3 on xysc_new_user_comm_school_commtmp (user_id,product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step3:当月新增客户与校园客户通信(CALL/SMS)处理-End...\n"

	return 0
}

 proc Do_xysc_new_user_mm_info {p_optime} {
 	   global conn
 	   global handle
	   global db_schema
	   source	./report.cfg

	   aidb_close $handle
	   if [catch {set handle [aidb_open $conn]} errmsg] {
	   	      trace_sql $errmsg 1302
	   	      return -1
	   }
 	   scan   $p_optime "%04s-%02s-%02s" year month day
 	   set lastmonth [GetLastMonth $year$month]

 	   #源表
 	   set xysc_new_user_comm_school_commtmp    "$db_schema.xysc_new_user_comm_school_commtmp"
 	   set dw_comp_cust_yyyymm                  "$db_schema.dw_comp_cust_$year$month"
 	   set dw_xysc_school_comp_user_dt_yyyymm   "$db_schema.dw_xysc_school_comp_user_dt_$year$month"
 	   set dw_xysc_school_comp_user_dt_yyyymm1  "$db_schema.dw_xysc_school_comp_user_dt_$lastmonth"

 	   #1.将与本网校园客户当月通话次数>=1或者短信次数>=1的客户取出,即为本网真实新增校园客户
 	   set sql_buf "delete from dw_xysc_school_new_user_mm where op_time='$year-$month-$day' and user_type='CMCC'"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into dw_xysc_school_new_user_mm
         select '$year-$month-$day' as op_time,
                user_id,
                product_no,
                school_name,
                'CMCC' as user_type
           from $xysc_new_user_comm_school_commtmp
          where school_cnt >= 1
             or school_sms_cnt >= 1
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #2 将竞争对手当月同上月比较,同时限定当月新增，即为竞争对手真实新增校园客户
 	   set sql_buf "delete from dw_xysc_school_new_user_mm where op_time='$year-$month-$day' and user_type='COMP'"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into dw_xysc_school_new_user_mm
         select distinct '$year-$month-$day' as op_time,
                         '-1',
                         a.COMP_NO as product_no,
                         c.school_name,
                         'COMP' as user_type
           from (select case
                              when length(rtrim(comp_product_no))=10 then
                               '0' || rtrim(comp_product_no)
                              else
                               rtrim(comp_product_no)
                            end as COMP_NO
                       from $dw_comp_cust_yyyymm where comp_userstatus_id=1 and comp_month_new_mark=1) as a
          inner join (select distinct COMP_NO
                        from $dw_xysc_school_comp_user_dt_yyyymm except
                              select distinct COMP_NO
                                from $dw_xysc_school_comp_user_dt_yyyymm1
                      ) as b
             on a.COMP_NO = b.COMP_NO
           left outer join $dw_xysc_school_comp_user_dt_yyyymm as c
             on a.COMP_NO = c.COMP_NO
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step4:当月新增校园客户识别处理-End...\n"

	return 0
}

 proc Do_xysc_school_comp_user {p_optime} {
 	   global conn
 	   global handle
	   global db_schema
	   source	./report.cfg

	   aidb_close $handle
	   if [catch {set handle [aidb_open $conn]} errmsg] {
	   	      trace_sql $errmsg 1302
	   	      return -1
	   }
 	   scan   $p_optime "%04s-%02s-%02s" year month day
     set  last_month  [string range [clock format [ clock scan "${year}${month}${day} - 1 month" ] -format "%Y-%m-%d"] 0 9]
 	   set sixmonth  "${year}06"
 	   puts $last_month
 	   puts $sixmonth

 	   #源表
 	   set dw_xysc_school_new_user_mm           "$db_schema.dw_xysc_school_new_user_mm"
 	   set dw_xysc_school_real_user_dt_yyyymm6  "$db_schema.dw_xysc_school_real_user_dt_$sixmonth"
 	   set dw_xysc_school_comp_user_dt_yyyymm6  "$db_schema.dw_xysc_school_comp_user_dt_$sixmonth"
 	   set dw_comp_cust_yyyymm                  "$db_schema.dw_comp_cust_$year$month"

	   #1.新建目标表-dw_xysc_school_real_user_dt_$year$month/bass2.dw_xysc_school_comp_user_dt_$year$month
	   set sql_buf "DROP TABLE bass2.dw_xysc_school_real_user_dt_$year$month
         "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.dw_xysc_school_real_user_dt_$year$month like bass2.dw_xysc_school_real_user_dt_yyyymm in tbs_bass_miner
					        index in tbs_index partitioning key (user_id,product_no) using hashing not logged initially
        "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "DROP TABLE bass2.dw_xysc_school_comp_user_dt_$year$month
         "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.dw_xysc_school_comp_user_dt_$year$month like bass2.dw_xysc_school_comp_user_dt_yyyymm in tbs_bass_miner
					        index in tbs_index partitioning key (comp_no) using hashing not logged initially
        "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #2.生成目标表数据
	   if { $month == "07" } {
	      #2_1.dw_xysc_school_real_user_dt_$year$month
	      set sql_buf "insert into dw_xysc_school_real_user_dt_$year$month (user_id,product_no,school_name)
            select user_id, product_no, school_name
               from $dw_xysc_school_real_user_dt_yyyymm6
             union all
             select user_id, product_no, school_name
               from $dw_xysc_school_new_user_mm
              where user_type = 'CMCC'
                and op_time in ('$year-$month-$day')
           "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      #2_2.dw_xysc_school_comp_user_dt_$year$month
	      set sql_buf "insert into dw_xysc_school_comp_user_dt_$year$month (comp_no,school_name,comp_city_id)
            select comp_no, school_name, comp_city_id
               from $dw_xysc_school_comp_user_dt_yyyymm6
             union all
             select a.product_no as comp_no, a.school_name, value(b.comp_city_id, '-1')
               from $dw_xysc_school_new_user_mm a
               left join (select case
                                   when length(rtrim(comp_product_no)) = 10 then
                                    '0' || rtrim(comp_product_no)
                                   else
                                    rtrim(comp_product_no)
                                 end as comp_no,
                                 comp_city_id
                            from $dw_comp_cust_yyyymm
                           where comp_userstatus_id = 1) b
                 on a.product_no = b.comp_no
              where a.user_type = 'COMP'
                and a.op_time in ('$year-$month-$day')
           "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	   } else {
	      #2_1.dw_xysc_school_real_user_dt_$year$month
	      set sql_buf "insert into dw_xysc_school_real_user_dt_$year$month (user_id,product_no,school_name)
            select user_id, product_no, school_name
               from $dw_xysc_school_real_user_dt_yyyymm6
             union all
             select user_id, product_no, school_name
               from $dw_xysc_school_new_user_mm
              where user_type = 'CMCC'
                and op_time in ('$year-$month-$day','$last_month')
           "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      #2_2.dw_xysc_school_comp_user_dt_$year$month
	      set sql_buf "insert into dw_xysc_school_comp_user_dt_$year$month (comp_no,school_name,comp_city_id)
            select comp_no, school_name, comp_city_id
               from $dw_xysc_school_comp_user_dt_yyyymm6
             union all
             select a.product_no as comp_no, a.school_name, value(b.comp_city_id, '-1')
               from $dw_xysc_school_new_user_mm a
               left join (select case
                                   when length(rtrim(comp_product_no)) = 10 then
                                    '0' || rtrim(comp_product_no)
                                   else
                                    rtrim(comp_product_no)
                                 end as comp_no,
                                 comp_city_id
                            from $dw_comp_cust_yyyymm
                           where comp_userstatus_id = 1) b
                 on a.product_no = b.comp_no
              where a.user_type = 'COMP'
                and a.op_time in ('$year-$month-$day', '$last_month')
           "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }

	   }


		 puts "Step5:${month}月校园客户及校园竞争对手识别处理-End...\n"

	return 0
}

 proc Do_xysc_school_user_snapshot {p_optime} {
 	   global conn
 	   global handle
	   global db_schema
	   source	./report.cfg

	   aidb_close $handle
	   if [catch {set handle [aidb_open $conn]} errmsg] {
	   	      trace_sql $errmsg 1302
	   	      return -1
	   }
 	   scan   $p_optime "%04s-%02s-%02s" year month day
 	   set  op_month  "201009"
 	   set  last_day  "2010-09-30"

 	   #源表
 	   set dw_xysc_school_new_user_mm           "$db_schema.dw_xysc_school_new_user_mm"
 	   set dw_xysc_school_real_user_dt_yyyymm   "$db_schema.dw_xysc_school_real_user_dt_$op_month"
 	   set dw_xysc_school_comp_user_dt_yyyymm   "$db_schema.dw_xysc_school_comp_user_dt_$op_month"

 	   #以201009月的校园客户为拍照基准客户,生成数据插入dw_xysc_school_snapshot_mm
 	   #1.删除本月拍照数据
	   set sql_buf "delete from bass2.dw_xysc_school_snapshot_mm where op_time='$year-$month-$day'
         "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #2.插入拍照客户数据
 	   #2_1.移动
	   set sql_buf "insert into bass2.dw_xysc_school_snapshot_mm
	       select '$year-$month-$day', user_id, product_no, school_name, 'CMCC' as user_type
          from (select distinct user_id, product_no, school_name
                  from $dw_xysc_school_real_user_dt_yyyymm
                union all
                select distinct user_id, product_no, school_name
                  from $dw_xysc_school_new_user_mm
                 where user_type = 'CMCC'
                   and op_time > '$last_day') as a
         group by user_id, product_no, school_name
         "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #2_2.竞争对手
	   set sql_buf "insert into bass2.dw_xysc_school_snapshot_mm
	       select '$year-$month-$day', '-1' as user_id, product_no, school_name, 'COMP' as user_type
          from (select distinct comp_no as product_no, school_name
                  from $dw_xysc_school_comp_user_dt_yyyymm
                union all
                select distinct product_no, school_name
                  from $dw_xysc_school_new_user_mm
                 where user_type = 'COMP'
                   and op_time > '$last_day') as a
         group by product_no, school_name
         "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step6:当月拍照校园客户处理-End...\n"

	return 0
}

 proc Do_xysc_school_drop_tmp_table {p_optime} {
 	   global conn
 	   global handle

	   aidb_close $handle
	   if [catch {set handle [aidb_open $conn]} errmsg] {
	   	      trace_sql $errmsg 1302
	   	      return -1
	   }
     #Step1.Do_xysc_new_user_xycall_rate
	   set sql_buf "drop table xysc_new_user_xycall_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_xycall_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_xycall_tmp3"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
     #Step2.Do_xysc_new_user_belong_school
	   set sql_buf "drop table xysc_new_user_belong_school_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_belong_school_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_belong_school_tmp3"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_belong_school_tmp4"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
     #Step3.Do_xysc_new_user_comm_school
	   set sql_buf "drop table xysc_new_user_comm_school_calltmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_comm_school_calltmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_comm_school_smstmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
     #Step4.Do_xysc_new_user_mm_info<无>
     #Step5.Do_xysc_school_comp_user<无>
     #Step6.Do_xysc_school_user_snapshot<无>

		 puts "StepX.删除所有无需保留的临时表-End...\n"

	return 0
}