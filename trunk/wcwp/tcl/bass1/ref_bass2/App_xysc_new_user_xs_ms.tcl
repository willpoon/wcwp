#======================================================================================
#版权归属：Copyright (c) 2010,AsiaInfo-Linkage Inc.
#程序名称: App_xysc_new_user_xs_ms.tcl
#程序功能: 西藏移动校园市场专题-4.校园新生客户识别
#核心思想: 用部分已知真实的客户去寻找其它最接近真实客户,并逐步扩大已知真实的客户,
#          如此反复迭代最终完成对所有校园客户的识别
#          从当月新增用户中,计算各用户同上月校园到达用户的通信占比情况,确定新生用户
#数据流向: dw_call_opposite_yyyymm/dw_newbusi_sms_yyyymm/dw_xysc_school_real_user_dt_yyyymm
#          /dw_xysc_school_comp_user_dt_yyyymm/dw_xysc_school_new_user_mm
#          -->dw_xysc_school_new_user_xs_mm
#运行粒度: 月
#运行示例: ds App_xysc_new_user_xs_ms.tcl 2010-09-01
#创建时间: 2010-12-8
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

     #Step1:当月校园新增客户对端通信(CALL/SMS)处理
		 puts "Step1:当月校园新增客户对端通信(CALL/SMS)处理-Begin...\n"
 	   if {[Do_xysc_new_user_comm_info $p_optime]  != 0} {
 	   	  aidb_roll $conn
 	   	  aidb_close $handle
 	   	 return -1
 	   }
     #Step2:当月校园客户归属学校级别变动处理
		 puts "Step2:当月校园客户归属学校级别变动处理-Begin...\n"
 	   if {[Do_xysc_new_user_school_change $p_optime]  != 0} {
 	   	  aidb_roll $conn
 	   	  aidb_close $handle
 	   	 return -1
 	   }
     #Step3:当月校园新生客户识别处理
		 puts "Step3:当月校园新生客户识别处理-Begin...\n"
 	   if {[Do_xysc_new_user_xs_info $p_optime]  != 0} {
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

 proc Do_xysc_new_user_comm_info {p_optime} {
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
 	   set dw_xysc_school_new_user_mm           "$db_schema.dw_xysc_school_new_user_mm"
 	   set dw_call_opposite_yyyymm              "$db_schema.dw_call_opposite_$year$month"
 	   set dw_xysc_school_real_user_dt_yyyymm1  "$db_schema.dw_xysc_school_real_user_dt_$lastmonth"
 	   set dw_xysc_school_comp_user_dt_yyyymm1  "$db_schema.dw_xysc_school_comp_user_dt_$lastmonth"
 	   set dw_newbusi_sms_yyyymm                "$db_schema.dw_newbusi_sms_$year$month"

 	   #1.新建临时表xysc_new_user_comm_calltmp,筛选校园新增移动客户通信对端信息
 	   #1_1.CALL
	   set sql_buf "drop TABLE bass2.xysc_new_user_comm_calltmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_comm_calltmp(
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
	   set sql_buf "insert into xysc_new_user_comm_calltmp
         select b.user_id, b.product_no, a.opp_number, sum(a.call_counts)
          from $dw_call_opposite_yyyymm as a
         inner join (select user_id, product_no
                       from $dw_xysc_school_new_user_mm
                      where op_time = '$year-$month-$day'
                        and user_type = 'CMCC') as b
            on a.user_id = b.user_id
           and a.product_no = b.product_no
         group by b.user_id, b.product_no, a.opp_number
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #1_2.SMS
	   set sql_buf "drop TABLE bass2.xysc_new_user_comm_smstmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_comm_smstmp(
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
	   set sql_buf "insert into xysc_new_user_comm_smstmp
         select b.user_id, b.product_no, a.opp_number, sum(a.counts)
          from $dw_newbusi_sms_yyyymm as a
         inner join (select user_id, product_no
                       from $dw_xysc_school_new_user_mm
                      where op_time = '$year-$month-$day'
                        and user_type = 'CMCC') as b
            on a.user_id = b.user_id
           and a.product_no = b.product_no
           and a.svcitem_id in (200001, 200002, 200003, 200005)
         group by b.user_id, b.product_no, a.opp_number
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #2.新建临时表xysc_new_user_comm_calltmp2,筛选校园新增竞争对手客户通信对端信息
 	   #2_1.CALL
	   set sql_buf "drop TABLE bass2.xysc_new_user_comm_calltmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_comm_calltmp2(
			   product_no	     varchar(15),
			   opposite_no	   varchar(32),
			   call_counts     integer
     )
     PARTITIONING KEY (product_no,opposite_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_comm_calltmp2
         select b.product_no, a.product_no, sum(a.call_counts)
          from $dw_call_opposite_yyyymm as a
         inner join (select product_no
                       from $dw_xysc_school_new_user_mm
                      where op_time = '$year-$month-$day'
                        and user_type = 'COMP') as b
            on a.opp_number = b.product_no
         group by b.product_no, a.product_no
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #2_2.SMS
	   set sql_buf "drop TABLE bass2.xysc_new_user_comm_smstmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_comm_smstmp2(
			   product_no	     varchar(15),
			   opposite_no	   varchar(32),
			   sms_counts      integer
     )
     PARTITIONING KEY (product_no,opposite_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_comm_smstmp2
         select b.product_no, a.product_no, sum(a.counts)
          from $dw_newbusi_sms_yyyymm as a
         inner join (select product_no
                       from $dw_xysc_school_new_user_mm
                      where op_time = '$year-$month-$day'
                        and user_type = 'COMP') as b
            on a.opp_number = b.product_no
           and a.svcitem_id in (200001, 200002, 200003, 200005)
         group by b.product_no, a.product_no
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #3.新建临时表xysc_new_user_cmcc_commtmp,标记校园新增移动客户对端与上月校园客户通信(CALL/SMS)信息
	   set sql_buf "drop TABLE bass2.xysc_new_user_cmcc_commtmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_cmcc_commtmp(
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   opposite_no	   varchar(32),
			   opp_call_counts integer,
			   opp_sms_counts	 integer,
			   opp_school_mark smallint
     )
     PARTITIONING KEY (user_id,product_no,opposite_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_cmcc_commtmp
         select a.user_id,
               a.product_no,
               a.opposite_no,
               a.opp_call_counts,
               a.opp_sms_counts,
               case
                 when a.opposite_no = b.product_no then
                  1
                 else
                  0
               end as opp_school_mark
          from (select user_id,
                       product_no,
                       opposite_no,
                       sum(opp_call_counts) as opp_call_counts,
                       sum(opp_sms_counts) as opp_sms_counts
                  from (select user_id,
                               product_no,
                               opposite_no,
                               call_counts as opp_call_counts,
                               0 as opp_sms_counts
                          from xysc_new_user_comm_calltmp
                        union all
                        select user_id,
                               product_no,
                               opposite_no,
                               0 as opp_call_counts,
                               sms_counts as opp_sms_counts
                          from xysc_new_user_comm_smstmp) as a
                 group by user_id, product_no, opposite_no) as a
          left outer join (select distinct product_no
                             from $dw_xysc_school_real_user_dt_yyyymm1
                           union all
                           select distinct comp_no as product_no
                             from $dw_xysc_school_comp_user_dt_yyyymm1) as b
            on a.opposite_no = b.product_no
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #4.新建临时表xysc_new_user_cmcc_tmp,汇总校园新增移动客户与对端通信(CALL/SMS)信息
	   set sql_buf "drop TABLE bass2.xysc_new_user_cmcc_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_cmcc_tmp(
			   user_id		         varchar(20),
			   product_no	         varchar(15),
			   opp_call_counts     integer,
			   opp_sms_counts	     integer,
			   sch_opp_call_counts integer,
			   sch_opp_sms_counts	 integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_cmcc_tmp
	       select user_id,
                product_no,
                sum(opp_call_counts),
                sum(opp_sms_counts),
                sum(case
                      when opp_school_mark = 1 then
                       opp_call_counts
                      else
                       0
                    end),
                sum(case
                      when opp_school_mark = 1 then
                       opp_sms_counts
                      else
                       0
                    end)
           from xysc_new_user_cmcc_commtmp
          group by user_id, product_no
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #5.新建临时表xysc_new_user_comp_commtmp,标记校园新增竞争对手客户对端与上月校园客户通信(CALL/SMS)信息
	   set sql_buf "drop TABLE bass2.xysc_new_user_comp_commtmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_comp_commtmp(
			   product_no	     varchar(15),
			   opposite_no	   varchar(32),
			   opp_call_counts integer,
			   opp_sms_counts	 integer,
			   opp_school_mark smallint
     )
     PARTITIONING KEY (product_no,opposite_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_comp_commtmp
         select a.product_no,
               a.opposite_no,
               a.opp_call_counts,
               a.opp_sms_counts,
               case
                 when a.opposite_no = b.product_no then
                  1
                 else
                  0
               end as opp_school_mark
          from (select product_no,
                       opposite_no,
                       sum(opp_call_counts) as opp_call_counts,
                       sum(opp_sms_counts) as opp_sms_counts
                  from (select product_no,
                               opposite_no,
                               call_counts as opp_call_counts,
                               0 as opp_sms_counts
                          from xysc_new_user_comm_calltmp2
                        union all
                        select product_no,
                               opposite_no,
                               0 as opp_call_counts,
                               sms_counts as opp_sms_counts
                          from xysc_new_user_comm_smstmp2) as a
                 group by product_no, opposite_no) as a
          left outer join (select distinct product_no
                             from $dw_xysc_school_real_user_dt_yyyymm1
                           union all
                           select distinct comp_no as product_no
                             from $dw_xysc_school_comp_user_dt_yyyymm1) as b
            on a.opposite_no = b.product_no
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #6.新建临时表xysc_new_user_comp_tmp,汇总校园新增竞争对手客户与对端通信(CALL/SMS)信息
	   set sql_buf "drop TABLE bass2.xysc_new_user_comp_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_comp_tmp(
			   product_no	         varchar(15),
			   opp_call_counts     integer,
			   opp_sms_counts	     integer,
			   sch_opp_call_counts integer,
			   sch_opp_sms_counts	 integer
     )
     PARTITIONING KEY (product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_comp_tmp
	       select product_no,
                sum(opp_call_counts),
                sum(opp_sms_counts),
                sum(case
                      when opp_school_mark = 1 then
                       opp_call_counts
                      else
                       0
                    end),
                sum(case
                      when opp_school_mark = 1 then
                       opp_sms_counts
                      else
                       0
                    end)
           from xysc_new_user_comp_commtmp
          group by product_no
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #7.新建临时表xysc_new_user_cmcc_ratetmp,校园新增移动客户通信对端及占比
	   set sql_buf "drop TABLE bass2.xysc_new_user_cmcc_ratetmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_cmcc_ratetmp(
			   user_id		         varchar(20),
			   product_no	         varchar(15),
			   opp_call_counts     integer,
			   opp_sms_counts	     integer,
			   sch_opp_call_counts integer,
			   sch_opp_sms_counts	 integer,
			   sch_opp_call_rate	 decimal(10,2),
			   sch_opp_sms_rate		 decimal(10,2)
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into bass2.xysc_new_user_cmcc_ratetmp
	       select user_id,
               product_no,
               opp_call_counts,
               opp_sms_counts,
               sch_opp_call_counts,
               sch_opp_sms_counts,
               case
                 when opp_call_counts = 0 then
                  0.00
                 else
                  sch_opp_call_counts * 1.0 / opp_call_counts
               end,
               case
                 when opp_sms_counts = 0 then
                  0.00
                 else
                  sch_opp_sms_counts * 1.0 / opp_sms_counts
               end
          from xysc_new_user_cmcc_tmp
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index bass2.idx_xysc_new_xs_up"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index bass2.idx_xysc_new_xs_up on bass2.xysc_new_user_cmcc_ratetmp (user_id,product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #8.新建临时表xysc_new_user_comp_ratetmp,校园新增竞争对手客户通信对端及占比
	   set sql_buf "drop TABLE bass2.xysc_new_user_comp_ratetmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_comp_ratetmp(
			   product_no	         varchar(15),
			   opp_call_counts     integer,
			   opp_sms_counts	     integer,
			   sch_opp_call_counts integer,
			   sch_opp_sms_counts	 integer,
			   sch_opp_call_rate	 decimal(10,2),
			   sch_opp_sms_rate		 decimal(10,2)
     )
     PARTITIONING KEY (product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into bass2.xysc_new_user_comp_ratetmp
	       select product_no,
               opp_call_counts,
               opp_sms_counts,
               sch_opp_call_counts,
               sch_opp_sms_counts,
               case
                 when opp_call_counts = 0 then
                  0.00
                 else
                  sch_opp_call_counts * 1.0 / opp_call_counts
               end,
               case
                 when opp_sms_counts = 0 then
                  0.00
                 else
                  sch_opp_sms_counts * 1.0 / opp_sms_counts
               end
          from xysc_new_user_comp_tmp
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index bass2.idx_xysc_new_xs_up2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index bass2.idx_xysc_new_xs_up2 on bass2.xysc_new_user_comp_ratetmp (product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step1:当月校园新增客户对端通信(CALL/SMS)处理-End...\n"

	return 0
}

 proc Do_xysc_new_user_school_change {p_optime} {
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
 	   set dw_xysc_school_real_user_dt_yyyymm   "$db_schema.dw_xysc_school_real_user_dt_$year$month"
 	   set dw_xysc_school_real_user_dt_yyyymm1  "$db_schema.dw_xysc_school_real_user_dt_$lastmonth"
 	   set dw_xysc_school_comp_user_dt_yyyymm   "$db_schema.dw_xysc_school_comp_user_dt_$year$month"
 	   set dw_xysc_school_comp_user_dt_yyyymm1  "$db_schema.dw_xysc_school_comp_user_dt_$lastmonth"
 	   set dim_xysc_lac_cell_info               "$db_schema.Dim_xysc_lac_cell_info"

 	   #1.查找当月校园客户和上月校园客户中的学校性质
 	   #1_1.本网(CMCC)
	   set sql_buf "drop TABLE bass2.xysc_new_user_school_change_cmcctmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_school_change_cmcctmp(
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   school_name	   varchar(128),
			   school_type	   varchar(16),
			   pre_school_name varchar(128),
			   pre_school_type varchar(16)
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_school_change_cmcctmp
         select distinct a.user_id,
                         a.product_no,
                         a.school_name,
                         c.school_type,
                         b.school_name,
                         d.school_type
           from $dw_xysc_school_real_user_dt_yyyymm as a
          inner join $dw_xysc_school_real_user_dt_yyyymm1 as b
             on a.product_no = b.product_no
            and a.user_id = b.user_id
           left outer join (select distinct school_name, school_type
                              from $dim_xysc_lac_cell_info) as c
             on a.school_name = c.school_name
           left outer join (select distinct school_name, school_type
                              from $dim_xysc_lac_cell_info) as d
             on b.school_name = d.school_name
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #1_2.他网(COMP)
	   set sql_buf "drop TABLE bass2.xysc_new_user_school_change_comptmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_school_change_comptmp(
			   product_no	     varchar(15),
			   school_name	   varchar(128),
			   school_type	   varchar(16),
			   pre_school_name varchar(128),
			   pre_school_type varchar(16)
     )
     PARTITIONING KEY (product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_school_change_comptmp
         select distinct a.comp_no,
                         a.school_name,
                         c.school_type,
                         b.school_name,
                         d.school_type
           from $dw_xysc_school_comp_user_dt_yyyymm as a
          inner join $dw_xysc_school_comp_user_dt_yyyymm1 as b
             on a.comp_no = b.comp_no
           left outer join (select distinct school_name, school_type
                              from $dim_xysc_lac_cell_info) as c
             on a.school_name = c.school_name
           left outer join (select distinct school_name, school_type
                              from $dim_xysc_lac_cell_info) as d
             on b.school_name = d.school_name
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #2.对查找出当月校园客户和上月校园客户的学校性质信息,判定是否变动
 	   #2_1.本网(CMCC)
	   set sql_buf "drop TABLE bass2.xysc_new_user_school_change_cmcctmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_school_change_cmcctmp2(
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   school_name	   varchar(128),
			   school_type	   varchar(16),
			   pre_school_name varchar(128),
			   pre_school_type varchar(16),
			   change_flag	   smallint
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_school_change_cmcctmp2
         select distinct a.user_id,
                        a.product_no,
                        a.school_name,
                        a.school_type,
                        a.pre_school_name,
                        a.pre_school_type,
                        case
                          when int(a.school_type) = int(a.pre_school_type) then
                           0
                          else
                           1
                        end
          from xysc_new_user_school_change_cmcctmp a
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #2_2.他网(COMP)
	   set sql_buf "drop TABLE bass2.xysc_new_user_school_change_comptmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_school_change_comptmp2(
			   product_no	     varchar(15),
			   school_name	   varchar(128),
			   school_type	   varchar(16),
			   pre_school_name varchar(128),
			   pre_school_type varchar(16),
			   change_flag	   smallint
     )
     PARTITIONING KEY (product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_school_change_comptmp2
         select distinct a.product_no,
                        a.school_name,
                        a.school_type,
                        a.pre_school_name,
                        a.pre_school_type,
                        case
                          when int(a.school_type) = int(a.pre_school_type) then
                           0
                          else
                           1
                        end
          from xysc_new_user_school_change_comptmp a
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step2:当月校园客户归属学校级别变动处理-End...\n"

	return 0
}

 proc Do_xysc_new_user_xs_info {p_optime} {
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
 	   set xysc_new_user_school_change_cmcctmp2 "$db_schema.xysc_new_user_school_change_cmcctmp2"
 	   set xysc_new_user_school_change_comptmp2 "$db_schema.xysc_new_user_school_change_comptmp2"
 	   set xysc_new_user_cmcc_ratetmp           "$db_schema.xysc_new_user_cmcc_ratetmp"
 	   set xysc_new_user_comp_ratetmp           "$db_schema.xysc_new_user_comp_ratetmp"
 	   set dw_xysc_school_real_user_dt_yyyymm   "$db_schema.dw_xysc_school_real_user_dt_$year$month"
 	   set dw_xysc_school_comp_user_dt_yyyymm   "$db_schema.dw_xysc_school_comp_user_dt_$year$month"

 	   #1.将当月校园客户和上月校园客户中的学校性质变动的客户,定义为校园新生客户群1
 	   #1_1.本网(CMCC)
	   set sql_buf "drop TABLE bass2.xysc_new_user_xs_info_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_new_user_xs_info_tmp(
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   school_name	   varchar(128),
			   user_type	     varchar(5)
     )
     PARTITIONING KEY (product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_new_user_xs_info_tmp
         select distinct a.user_id,
                         a.product_no,
                         a.school_name,
                         'CMCC'
           from $xysc_new_user_school_change_cmcctmp2 a
         where a.change_flag=1
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #1_2.他网(COMP)
	   set sql_buf "insert into xysc_new_user_xs_info_tmp
         select distinct '-1',
                         a.product_no,
                         a.school_name,
                         'COMP'
           from $xysc_new_user_school_change_comptmp2 a
         where a.change_flag=1
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #2.校园新增客户通信(CALL/SMS)对端占比小于0.3,定义为校园新生客户群2
 	   #2_1.本网(CMCC)
	   set sql_buf "insert into xysc_new_user_xs_info_tmp
         select a.user_id, a.product_no, b.school_name, 'CMCC'
          from $xysc_new_user_cmcc_ratetmp as a
          left outer join $dw_xysc_school_real_user_dt_yyyymm as b
            on a.user_id = b.user_id
           and a.product_no = b.product_no
         where a.sch_opp_call_rate < 0.3
           and a.sch_opp_sms_rate < 0.3
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #2_2.他网(COMP)
	   set sql_buf "insert into xysc_new_user_xs_info_tmp
	       select '-1' as user_id, a.product_no, b.school_name, 'COMP' as user_type
           from $xysc_new_user_comp_ratetmp as a
           left outer join $dw_xysc_school_comp_user_dt_yyyymm as b
             on a.product_no = b.comp_no
          where a.sch_opp_call_rate < 0.3
            and a.sch_opp_sms_rate < 0.3
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #3.汇总校园新生客户群1/2,并插入目标表-校园新增客户新生识别表
	   set sql_buf "delete from dw_xysc_school_new_user_xs_mm where op_time='$year-$month-$day'
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into dw_xysc_school_new_user_xs_mm
         select '$year-$month-$day', user_id, product_no, school_name, user_type
           from (select distinct user_id, product_no, school_name, user_type
                   from xysc_new_user_xs_info_tmp) a
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step3:当月校园新生客户识别处理-End...\n"

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
     #Step1.Do_xysc_new_user_comm_info
	   set sql_buf "drop table xysc_new_user_comm_calltmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_comm_smstmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_comm_calltmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_comm_smstmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_cmcc_commtmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_cmcc_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_comp_commtmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_comp_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_cmcc_ratetmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_comp_ratetmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
     #Step2.Do_xysc_new_user_school_change
	   set sql_buf "drop table xysc_new_user_school_change_cmcctmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_school_change_comptmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_school_change_cmcctmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_new_user_school_change_comptmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
     #Step3.Do_xysc_new_user_xs_info
	   set sql_buf "drop table xysc_new_user_xs_info_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }

		 puts "StepX.删除所有无需保留的临时表-End...\n"

	return 0
}