#======================================================================================
#��Ȩ������Copyright (c) 2010,AsiaInfo-Linkage Inc.
#��������: App_xysc_comp_user_ms.tcl
#������: �����ƶ�У԰�г�ר��-2.У԰�������ֿͻ�ʶ��
#����˼��: �ò�����֪��ʵ���û�ȥѰ��������ӽ���ʵ�û�,����������֪��ʵ���û�,
#          ��˷�������������ɶ�����У԰�ͻ���ʶ��
#��������: dw_xysc_school_real_user_dt_yyyymm/dw_call_opposite_yyyymm/dw_newbusi_sms_yyyymm/
#          dw_newbusi_mms_yyyymm/dw_comp_cust_yyyymm -->dw_xysc_school_comp_user_dt_yyyymm
#��������: ��
#����ʾ��: ds App_xysc_comp_user_ms.tcl 2010-09-01
#����ʱ��: 2010-11-30
#�� �� ��: AsiaInfo	Liwei
#��������: <ע��!>������ٵĹ�ϵ,ÿ���7�¡�8��У԰�������ֿͻ����ܰ����������㷨����,��Ҫ�޸ģ�
#	         ���統ǰ����8��,����7�µ׵�У԰�������ֿͻ�,Ϊ6�µ׵�У԰�������ֿͻ�+7�µ���������У԰�������ֿͻ�;
#          8�µ׵�У԰�������ֿͻ�,��Ϊ6�µ׵�У԰�������ֿͻ�+7�µ���������У԰�������ֿͻ�+8�µ���������У԰�������ֿͻ�
#�޸���ʷ: 1.
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

     if { $month != "07" && $month != "08" } {

        #Step1:У԰�ͻ���ͨ�ŶԶ˿ͻ���У԰�ͻ�����ȫ���ͨ��(CALL/SMS/MMS)����
		    puts "Step1:У԰�ͻ���ͨ�ŶԶ˿ͻ���У԰�ͻ�����ȫ���ͨ��(CALL/SMS/MMS)����-Begin...\n"
 	      if {[Do_xysc_comp_comn_info $p_optime]  != 0} {
 	      	  aidb_roll $conn
 	      	  aidb_close $handle
 	      	 return -1
 	      }
        #Step2:У԰�ͻ���ͨ�ŶԶ˿ͻ����ƶ��ͻ�����ȫ���ͨ��(CALL/SMS/MMS)����
		    puts "Step2:У԰�ͻ���ͨ�ŶԶ˿ͻ����ƶ��ͻ�����ȫ���ͨ��(CALL/SMS/MMS)����-Begin...\n"
 	      if {[Do_xysc_comp_comn_info2 $p_optime]  != 0} {
 	      	  aidb_roll $conn
 	      	  aidb_close $handle
 	      	 return -1
 	      }
        #Step3:�������ֹ���ΨһѧУ����
		    puts "Step3:�������ֹ���ΨһѧУ����-Begin...\n"
 	      if {[Do_xysc_comp_belong_school $p_optime]  != 0} {
 	      	  aidb_roll $conn
 	      	  aidb_close $handle
 	      	 return -1
 	      }
        #Step4:��������ͨ�ż�ռ�ȼ���������ʶ����
		    puts "Step4:��������ͨ�ż�ռ�ȼ���������ʶ����-Begin...\n"
 	      if {[Do_xysc_comp_real_user $p_optime]  != 0} {
 	      	  aidb_roll $conn
 	      	  aidb_close $handle
 	      	 return -1
 	      }
        #Step5:���������н�ʦ��ѧ��ʶ����
		    puts "Step5:���������н�ʦ��ѧ��ʶ����-Begin...\n"
 	      if {[Do_xysc_comp_TeacherandStudent_user $p_optime]  != 0} {
 	      	  aidb_roll $conn
 	      	  aidb_close $handle
 	      	 return -1
 	      }
        #StepX.ɾ���������豣������ʱ��
		    puts "StepX.ɾ���������豣������ʱ��-Begin...\n"
 	      if {[Do_xysc_school_drop_tmp_table $p_optime]  != 0} {
 	      	  aidb_roll $conn
 	      	  aidb_close $handle
 	      	 return -1
 	      }
 	      aidb_commit $conn
     } else {
 	      puts "Dw_xysc_school_comp_user_dt_yyyymm-${month}�±�,��App_xysc_new_user_ms.tcl-����${month}������ʱ����!"
     }

 	   return 0
 }

 proc Do_xysc_comp_comn_info {p_optime} {
 	   global conn
 	   global handle
	   global db_schema

	   aidb_close $handle
	   if [catch {set handle [aidb_open $conn]} errmsg] {
	   	      trace_sql $errmsg 1302
	   	      return -1
	   }
 	   scan   $p_optime "%04s-%02s-%02s" year month day
 	   set    city_list "891,892,893,894,895,896,897"

 	   #Դ��
 	   set dw_xysc_school_real_user_dt_yyyymm   "$db_schema.dw_xysc_school_real_user_dt_$year$month"
 	   set dw_call_opposite_yyyymm              "$db_schema.dw_call_opposite_$year$month"
 	   set dw_newbusi_sms_yyyymm                "$db_schema.dw_newbusi_sms_$year$month"
 	   set dw_newbusi_mms_yyyymm                "$db_schema.dw_newbusi_mms_$year$month"
 	   set dim_gsm_hlr_info                     "$db_schema.dim_gsm_hlr_info"

 	   #1.�½�ͨ��(CALL/SMS/MMS)��ʱ��
	   set sql_buf "drop TABLE bass2.xysc_comp_comn_info_calltmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_comn_info_calltmp(
			   product_no	      varchar(15),
			   comp_no	        varchar(32),
			   call_counts	    integer
     )
     PARTITIONING KEY (product_no,comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop TABLE bass2.xysc_comp_comn_info_smstmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_comn_info_smstmp(
			   product_no	      varchar(15),
			   comp_no	        varchar(32),
			   sms_counts	      integer
     )
     PARTITIONING KEY (product_no,comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop TABLE bass2.xysc_comp_comn_info_mmstmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_comn_info_mmstmp(
			   product_no	      varchar(15),
			   comp_no	        varchar(32),
			   mms_counts	      integer
     )
     PARTITIONING KEY (product_no,comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #2.У԰�ͻ��뾺�����ֵ���ȫ���ͨ��(CALL/SMS/MMS)����
	   #2_1.CALL
	   set sql_buf "insert into xysc_comp_comn_info_calltmp
         select b.product_no, a.opp_number, sum(a.call_counts)
           from $dw_call_opposite_yyyymm a
          inner join $dw_xysc_school_real_user_dt_yyyymm b
             on a.user_id = b.user_id
          where a.opp_city_id in ('891', '892', '893', '894', '895', '896', '897')
            and a.opposite_id in (1, 2, 4, 13, 14, 17, 115, 116)
          group by b.product_no, a.opp_number
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #2_2.SMS
	   set sql_buf "insert into xysc_comp_comn_info_smstmp
         select a.product_no, a.opp_number, sum(cnt)
          from (select b.product_no,
                       a.opp_number,
                       substr(case
                                when length(rtrim(substr(opp_number, 1, 13))) = 13 then
                                 substr(opp_number, 3, 13)
                                else
                                 rtrim(substr(opp_number, 1, 13))
                              end,
                              1,
                              7) as hlr_code,
                       sum(counts) as cnt
                  from $dw_newbusi_sms_yyyymm              a,
                       $dw_xysc_school_real_user_dt_yyyymm b
                 where a.user_id = b.user_id
                   and a.svcitem_id in (200001, 200002, 200003, 200005)
                 group by b.product_no,
                          a.opp_number,
                          substr(case
                                   when length(rtrim(substr(opp_number, 1, 13))) = 13 then
                                    substr(opp_number, 3, 13)
                                   else
                                    rtrim(substr(opp_number, 1, 13))
                                 end,
                                 1,
                                 7)) a
         where (hlr_code in (select hlr_code
                                  from $dim_gsm_hlr_info
                                 where prov_code in ('891')
                                   and hlr_type in (0, 2))) or (int(substr(opp_number,1,4)) in ($city_list))
         group by a.product_no, a.opp_number
       "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #2_3.MMS
	   set sql_buf "insert into xysc_comp_comn_info_mmstmp
         select a.product_no, a.opp_number, sum(cnt)
          from (select b.product_no,
                       a.opp_number,
                       substr(case
                                when length(rtrim(substr(opp_number, 1, 13))) = 13 then
                                 substr(opp_number, 3, 13)
                                else
                                 rtrim(substr(opp_number, 1, 13))
                              end,
                              1,
                              7) as hlr_code,
                       sum(counts) as cnt
                  from $dw_newbusi_mms_yyyymm              a,
                       $dw_xysc_school_real_user_dt_yyyymm b
                 where a.user_id = b.user_id
                   and a.svcitem_id in (400001)
                 group by b.product_no,
                          a.opp_number,
                          substr(case
                                   when length(rtrim(substr(opp_number, 1, 13))) = 13 then
                                    substr(opp_number, 3, 13)
                                   else
                                    rtrim(substr(opp_number, 1, 13))
                                 end,
                                 1,
                                 7)) a
         where (hlr_code in (select hlr_code
                                  from $dim_gsm_hlr_info
                                 where prov_code in ('891')
                                   and hlr_type in (0, 2))) or (int(substr(opp_number,1,4)) in ($city_list))
         group by a.product_no, a.opp_number
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #3.����������У԰�ͻ�ͨ��(CALL/SMS/MMS)�������
	   set sql_buf "drop TABLE bass2.xysc_comp_comn_info_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_comn_info_tmp(
			   comp_no		     varchar(32),
			   xy_call_counts	 integer,
			   xy_call_user	   integer,
			   xy_sms_counts	 integer,
			   xy_sms_user	   integer,
			   xy_mms_counts	 integer,
			   xy_mms_user	   integer
     )
     PARTITIONING KEY (comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into bass2.xysc_comp_comn_info_tmp
	       select comp_no,
               sum(xy_call_counts),
               sum(xy_call_user),
               sum(xy_sms_counts),
               sum(xy_sms_user),
               sum(xy_mms_counts),
               sum(xy_mms_user)
          from (select comp_no,
                       sum(call_counts) xy_call_counts,
                       count(distinct product_no) xy_call_user,
                       0 xy_sms_counts,
                       0 xy_sms_user,
                       0 xy_mms_counts,
                       0 xy_mms_user
                  from xysc_comp_comn_info_calltmp
                 group by comp_no
                union all
                select comp_no,
                       0 xy_call_counts,
                       0 xy_call_user,
                       sum(sms_counts) xy_sms_counts,
                       count(distinct product_no) xy_sms_user,
                       0 xy_mms_counts,
                       0 xy_mms_user
                  from xysc_comp_comn_info_smstmp
                 group by comp_no
                union all
                select comp_no,
                       0 xy_call_counts,
                       0 xy_call_user,
                       0 xy_sms_counts,
                       0 xy_sms_user,
                       sum(mms_counts) xy_mms_counts,
                       count(distinct product_no) xy_mms_user
                  from xysc_comp_comn_info_mmstmp
                 group by comp_no) x
         group by comp_no
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_comp_no"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_comp_no on xysc_comp_comn_info_tmp (comp_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step1:У԰�ͻ���ͨ�ŶԶ˿ͻ���У԰�ͻ�����ȫ���ͨ��(CALL/SMS/MMS)����-End...\n"

	return 0
}

 proc Do_xysc_comp_comn_info2 {p_optime} {
 	   global conn
 	   global handle
	   global db_schema

	   aidb_close $handle
	   if [catch {set handle [aidb_open $conn]} errmsg] {
	   	      trace_sql $errmsg 1302
	   	      return -1
	   }
 	   scan   $p_optime "%04s-%02s-%02s" year month day
	   set sql_buf "select day(date('$p_optime')+ 1 month-1 day) from dual"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 while {[set this_row [aidb_fetch $handle]] != ""} {
		    set	    last_days    	        [lindex $this_row 0]
		 }
	   aidb_close $handle
	   if [catch {set handle [aidb_open $conn]} errmsg] {
	   	trace_sql $errmsg 1302
	   	return -1
	   }

 	   #Դ��
 	   set xysc_comp_comn_info_tmp              "$db_schema.xysc_comp_comn_info_tmp"
 	   set dw_call_opposite_yyyymm              "$db_schema.dw_call_opposite_$year$month"
 	   set dw_newbusi_sms_yyyymm                "$db_schema.dw_newbusi_sms_$year$month"
 	   set dw_newbusi_mms_yyyymm                "$db_schema.dw_newbusi_mms_$year$month"

 	   #1.�½�ͨ��(CALL/SMS/MMS)��ʱ��
	   set sql_buf "drop TABLE bass2.xysc_comp_comn_info2_calltmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_comn_info2_calltmp(
			   comp_no	        varchar(32),
			   product_no	      varchar(15),
			   call_counts	    integer
     )
     PARTITIONING KEY (product_no,comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop TABLE bass2.xysc_comp_comn_info2_smstmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_comn_info2_smstmp(
			   comp_no	        varchar(32),
			   product_no	      varchar(15),
			   sms_counts	      integer
     )
     PARTITIONING KEY (product_no,comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop TABLE bass2.xysc_comp_comn_info2_mmstmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_comn_info2_mmstmp(
			   comp_no	        varchar(32),
			   product_no	      varchar(15),
			   mms_counts	      integer
     )
     PARTITIONING KEY (product_no,comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #2.�����������ƶ��ͻ�����ȫ���ͨ��(CALL/SMS/MMS)����
	   #2_1.CALL
	   set sql_buf "insert into xysc_comp_comn_info2_calltmp
         select b.comp_no, a.product_no, sum(a.call_counts)
           from $dw_call_opposite_yyyymm a
          inner join $xysc_comp_comn_info_tmp b
             on a.opp_number = b.comp_no
          group by b.comp_no, a.product_no
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #2_2.SMS
	   set sql_buf "insert into xysc_comp_comn_info2_smstmp
         select b.comp_no, a.product_no, sum(a.counts)
           from $dw_newbusi_sms_yyyymm a
          inner join $xysc_comp_comn_info_tmp b
             on a.opp_number = b.comp_no
          group by b.comp_no, a.product_no
       "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #2_3.MMS
	   set sql_buf "insert into xysc_comp_comn_info2_mmstmp
         select b.comp_no, a.product_no, sum(a.counts)
           from $dw_newbusi_mms_yyyymm a
          inner join $xysc_comp_comn_info_tmp b
             on a.opp_number = b.comp_no
          group by b.comp_no, a.product_no
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #3.�����������ƶ��ͻ�ͨ��(CALL/SMS/MMS)�������
	   set sql_buf "drop TABLE bass2.xysc_comp_comn_info2_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_comn_info2_tmp(
			   comp_no		     varchar(32),
			   call_counts	   integer,
			   call_user	     integer,
			   sms_counts	     integer,
			   sms_user	       integer,
			   mms_counts	     integer,
			   mms_user	       integer
     )
     PARTITIONING KEY (comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into bass2.xysc_comp_comn_info2_tmp
	       select comp_no,
               sum(call_counts),
               sum(call_user),
               sum(sms_counts),
               sum(sms_user),
               sum(mms_counts),
               sum(mms_user)
          from (select comp_no,
                       sum(call_counts) call_counts,
                       count(distinct product_no) call_user,
                       0 sms_counts,
                       0 sms_user,
                       0 mms_counts,
                       0 mms_user
                  from xysc_comp_comn_info2_calltmp
                 group by comp_no
                union all
                select comp_no,
                       0 call_counts,
                       0 call_user,
                       sum(sms_counts) sms_counts,
                       count(distinct product_no) sms_user,
                       0 mms_counts,
                       0 mms_user
                  from xysc_comp_comn_info2_smstmp
                 group by comp_no
                union all
                select comp_no,
                       0 call_counts,
                       0 call_user,
                       0 sms_counts,
                       0 sms_user,
                       sum(mms_counts) mms_counts,
                       count(distinct product_no) mms_user
                  from xysc_comp_comn_info2_mmstmp
                 group by comp_no) x
         group by comp_no
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_comp_no2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_comp_no2 on xysc_comp_comn_info2_tmp (comp_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step2:У԰�ͻ���ͨ�ŶԶ˿ͻ����ƶ��ͻ�����ȫ���ͨ��(CALL/SMS/MMS)����-End...\n"

	return 0
}

 proc Do_xysc_comp_belong_school {p_optime} {
 	   global conn
 	   global handle
	   global db_schema

	   aidb_close $handle
	   if [catch {set handle [aidb_open $conn]} errmsg] {
	   	      trace_sql $errmsg 1302
	   	      return -1
	   }
 	   scan   $p_optime "%04s-%02s-%02s" year month day

 	   #Դ��
 	   set dw_xysc_school_real_user_dt_yyyymm   "$db_schema.dw_xysc_school_real_user_dt_$year$month"
 	   set xysc_comp_comn_info_calltmp          "$db_schema.xysc_comp_comn_info_calltmp"
 	   set xysc_comp_comn_info_smstmp           "$db_schema.xysc_comp_comn_info_smstmp"
 	   set xysc_comp_comn_info_mmstmp           "$db_schema.xysc_comp_comn_info_mmstmp"

 	   #�������ֹ���ΨһѧУ:�Ȱ�����ͨ�ŵ�У԰�û���������,��ͨ�Ŵ������ٵ���
 	   #1.�½���ʱ��xysc_comp_belong_school_tmp,���ܾ���������У԰�ͻ�ͨ�Ŵ�����Ϣ
	   set sql_buf "drop TABLE bass2.xysc_comp_belong_school_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_belong_school_tmp(
			   comp_no		     varchar(32),
			   product_no		   varchar(32),
			   xy_comm_counts	 integer
     )
     PARTITIONING KEY (comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into bass2.xysc_comp_belong_school_tmp
	       select comp_no,
	             product_no,
               sum(comm_counts)
          from (select comp_no,product_no,
                       sum(call_counts) comm_counts
                  from $xysc_comp_comn_info_calltmp
                 group by comp_no,product_no
                union all
                select comp_no,product_no,
                       sum(sms_counts) comm_counts
                  from $xysc_comp_comn_info_smstmp
                 group by comp_no,product_no
                union all
                select comp_no,product_no,
                       sum(mms_counts) comm_counts
                  from $xysc_comp_comn_info_mmstmp
                 group by comp_no,product_no) x
         group by comp_no,product_no
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #2.�½���ʱ��xysc_comp_belong_school_tmp2,���ܾ���������У԰�ͻ�ͨ�Ŵ�����Ϣ
	   set sql_buf "drop TABLE bass2.xysc_comp_belong_school_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_belong_school_tmp2(
			   comp_no		     varchar(32),
			   school_name	   varchar(128),
			   xy_comm_user	   integer,
			   xy_comm_counts	 integer
     )
     PARTITIONING KEY (comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into bass2.xysc_comp_belong_school_tmp2
	       select comp_no, school_name, xy_comm_user, xy_comm_counts
          from (select a.comp_no,
                       b.school_name,
                       count(1) xy_comm_user,
                       sum(a.xy_comm_counts) xy_comm_counts,
                       row_number() over(partition by a.comp_no order by count(1) desc, sum(a.xy_comm_counts) desc) sn
                  from xysc_comp_belong_school_tmp a
                 inner join $dw_xysc_school_real_user_dt_yyyymm b
                    on a.product_no = b.product_no
                 group by comp_no, school_name) x
         where x.sn = 1
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }


		 puts "Step3:�������ֹ���ΨһѧУ����-End...\n"

	return 0
}

 proc Do_xysc_comp_real_user {p_optime} {
 	   global conn
 	   global handle
	   global db_schema

	   aidb_close $handle
	   if [catch {set handle [aidb_open $conn]} errmsg] {
	   	      trace_sql $errmsg 1302
	   	      return -1
	   }
 	   scan   $p_optime "%04s-%02s-%02s" year month day
 	   set    city_list "891,892,893,894,895,896,897"

 	   #Դ��
 	   set dw_comp_cust_yyyymm                  "$db_schema.dw_comp_cust_$year$month"
 	   set xysc_comp_comn_info_tmp              "$db_schema.xysc_comp_comn_info_tmp"
 	   set xysc_comp_comn_info2_tmp             "$db_schema.xysc_comp_comn_info2_tmp"
 	   set xysc_comp_belong_school_tmp2         "$db_schema.xysc_comp_belong_school_tmp2"

 	   set xysc_comp_comn_info_calltmp          "$db_schema.xysc_comp_comn_info_calltmp"
 	   set xysc_comp_comn_info_smstmp           "$db_schema.xysc_comp_comn_info_smstmp"
 	   set xysc_comp_comn_info_mmstmp           "$db_schema.xysc_comp_comn_info_mmstmp"
 	   set xysc_comp_comn_info2_calltmp         "$db_schema.xysc_comp_comn_info2_calltmp"
 	   set xysc_comp_comn_info2_smstmp          "$db_schema.xysc_comp_comn_info2_smstmp"
 	   set xysc_comp_comn_info2_mmstmp          "$db_schema.xysc_comp_comn_info2_mmstmp"

 	   #1.�½���ʱ��xysc_comp_comm_rate_tmp,���ܾ�������ͨ����Ϣ
	   set sql_buf "drop TABLE bass2.xysc_comp_comm_rate_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_comm_rate_tmp(
			   comp_no		      varchar(32),
			   call_counts	    integer,
			   xy_call_counts	  integer,
			   call_user	      integer,
			   xy_call_user     integer,
			   sms_counts	      integer,
			   xy_sms_counts	  integer,
			   sms_user	        integer,
			   xy_sms_user      integer,
			   mms_counts	      integer,
			   xy_mms_counts	  integer,
			   mms_user	        integer,
			   xy_mms_user	    integer,
			   user_cnt	    integer,
			   xy_user_cnt	  integer,
			   school_name	    varchar(128)
     )
     PARTITIONING KEY (comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_comp_comm_rate_tmp
         select a.comp_no,
                sum(b.call_counts),
                sum(a.xy_call_counts),
                sum(b.call_user),
                sum(a.xy_call_user),
                sum(b.sms_counts),
                sum(a.xy_sms_counts),
                sum(b.sms_user),
                sum(a.xy_sms_user),
                sum(b.mms_counts),
                sum(a.xy_mms_counts),
                sum(b.mms_user),
                sum(a.xy_mms_user),
                sum(d.user_cnt),
                sum(c.xy_user_cnt),
                max(e.school_name)
           from $xysc_comp_comn_info_tmp a
          inner join $xysc_comp_comn_info2_tmp b
             on a.comp_no = b.comp_no
          inner join (select comp_no, count(distinct product_no) xy_user_cnt
                        from (select comp_no, product_no
                                from $xysc_comp_comn_info_calltmp
                              union all
                              select comp_no, product_no
                                from $xysc_comp_comn_info_smstmp
                              union all
                              select comp_no, product_no from $xysc_comp_comn_info_mmstmp) x
                       group by comp_no) c
             on a.comp_no = c.comp_no
          inner join (select comp_no, count(distinct product_no) user_cnt
                        from (select comp_no, product_no
                                from $xysc_comp_comn_info2_calltmp
                              union all
                              select comp_no, product_no
                                from $xysc_comp_comn_info2_smstmp
                              union all
                              select comp_no, product_no
                                from $xysc_comp_comn_info2_mmstmp) x
                       group by comp_no) d
             on a.comp_no = d.comp_no
           left join $xysc_comp_belong_school_tmp2 e
             on a.comp_no = e.comp_no
         group by a.comp_no
        "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #2.�½���ʱ��xysc_comp_comm_rate_tmp2,���ܾ�������ͨ��ռ����Ϣ
	   set sql_buf "drop TABLE bass2.xysc_comp_comm_rate_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_comm_rate_tmp2(
			   comp_no		          varchar(32),
			   call_counts	        integer,
			   xy_call_counts	      integer,
			   xy_call_counts_rate	decimal(12,2),
			   call_user	          integer,
			   xy_call_user         integer,
			   xy_call_user_rate	  decimal(12,2),
			   sms_counts	          integer,
			   xy_sms_counts	      integer,
			   xy_sms_counts_rate	  decimal(12,2),
			   sms_user	            integer,
			   xy_sms_user          integer,
			   xy_sms_user_rate	    decimal(12,2),
			   user_cnt	            integer,
			   xy_user_cnt	        integer,
			   xy_user_cnt_rate	    decimal(12,2),
			   school_name	        varchar(128)
     )
     PARTITIONING KEY (comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_comp_comm_rate_tmp2
         select comp_no,
                call_counts,
                xy_call_counts,
                case
                  when call_counts <> 0 then
                   xy_call_counts * 1.0 / call_counts
                  else
                   0
                end as xy_call_counts_rate,
                call_user,
                xy_call_user,
                case
                  when call_user <> 0 then
                   xy_call_user * 1.0 / call_user
                  else
                   0
                end as xy_call_user_rate,
                sms_counts,
                xy_sms_counts,
                case
                  when sms_counts <> 0 then
                   xy_sms_counts * 1.0 / sms_counts
                  else
                   0
                end as xy_sms_counts_rate,
                sms_user,
                xy_sms_user,
                case
                  when sms_user <> 0 then
                   xy_sms_user * 1.0 / sms_user
                  else
                   0
                end as xy_sms_user_rate,
                user_cnt,
                xy_user_cnt,
                case
                  when user_cnt <> 0 then
                   xy_user_cnt * 1.0 / user_cnt
                  else
                   0
                end as xy_user_cnt_rate,
                school_name
           from xysc_comp_comm_rate_tmp
        "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #3.�½���ʱ��xysc_comp_comm_rate_tmp3,ɸѡ��������ͨ��ռ����Ϣ
	   set sql_buf "drop TABLE bass2.xysc_comp_comm_rate_tmp3"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_comm_rate_tmp3(
			   comp_no		          varchar(32),
			   call_counts	        integer,
			   xy_call_counts	      integer,
			   xy_call_counts_rate	decimal(12,2),
			   call_user	          integer,
			   xy_call_user         integer,
			   xy_call_user_rate	  decimal(12,2),
			   sms_counts	          integer,
			   xy_sms_counts	      integer,
			   xy_sms_counts_rate	  decimal(12,2),
			   sms_user	            integer,
			   xy_sms_user          integer,
			   xy_sms_user_rate	    decimal(12,2),
			   user_cnt	            integer,
			   xy_user_cnt	        integer,
			   xy_user_cnt_rate	    decimal(12,2),
			   school_name	        varchar(128)
     )
     PARTITIONING KEY (comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_comp_comm_rate_tmp3
         select a.*
           from xysc_comp_comm_rate_tmp2 a
          where (a.xy_call_counts_rate >= 0.25 and xy_call_counts > 1)
             or (a.xy_call_user_rate >= 0.25 and xy_call_user > 1)
             or (a.xy_sms_counts_rate >= 0.25 and xy_sms_counts > 1)
             or (a.xy_sms_user_rate >= 0.25 and xy_sms_user > 1)
        "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #4.�½���ʱ��xysc_comp_comm_rate_tmp4,��һ��ɸѡ��������ͨ��ռ����Ϣ
	   set sql_buf "drop TABLE bass2.xysc_comp_comm_rate_tmp4"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_comm_rate_tmp4(
			   comp_no		          varchar(32),
			   call_counts	        integer,
			   xy_call_counts	      integer,
			   xy_call_counts_rate	decimal(12,2),
			   call_user	          integer,
			   xy_call_user         integer,
			   xy_call_user_rate	  decimal(12,2),
			   sms_counts	          integer,
			   xy_sms_counts	      integer,
			   xy_sms_counts_rate	  decimal(12,2),
			   sms_user	            integer,
			   xy_sms_user          integer,
			   xy_sms_user_rate	    decimal(12,2),
			   user_cnt	            integer,
			   xy_user_cnt	        integer,
			   xy_user_cnt_rate	    decimal(12,2),
			   school_name	        varchar(128),
			   comp_city_id         varchar(7)
     )
     PARTITIONING KEY (comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_comp_comm_rate_tmp4
         select a.*, value(b.comp_city_id,'-1')
           from xysc_comp_comm_rate_tmp3 as a
          inner join (select case
                              when length(rtrim(comp_product_no))=10 then
                               '0' || rtrim(comp_product_no)
                              else
                               rtrim(comp_product_no)
                            end as comp_product_no,
                            comp_city_id
                       from $dw_comp_cust_yyyymm where comp_userstatus_id=1
                      ) b
             on a.comp_no = b.comp_product_no
          where ((a.xy_call_counts_rate >= 0.45 and a.xy_call_user_rate >= 0.45) or
                (a.xy_sms_counts_rate >= 0.45 and a.xy_sms_user_rate >= 0.45))
        "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #5.�½�bass2.dw_xysc_school_comp_user_dt_$year$month,������У԰�������ֿͻ�
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
	   set sql_buf "insert into dw_xysc_school_comp_user_dt_$year$month (comp_no,school_name,comp_city_id)
         select comp_no, school_name, comp_city_id
           from xysc_comp_comm_rate_tmp4
        "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_comp_no3"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_comp_no3 on dw_xysc_school_comp_user_dt_$year$month (comp_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step4:��������ͨ�ż�ռ�ȼ���������ʶ����-End...\n"

	return 0
}

 proc Do_xysc_comp_TeacherandStudent_user {p_optime} {
 	   global conn
 	   global handle
	   global db_schema

	   aidb_close $handle
	   if [catch {set handle [aidb_open $conn]} errmsg] {
	   	      trace_sql $errmsg 1302
	   	      return -1
	   }
 	   scan   $p_optime "%04s-%02s-%02s" year month day

 	   #Դ��
 	   set dw_xysc_school_real_user_dt_yyyymm   "$db_schema.dw_xysc_school_real_user_dt_$year$month"
 	   set dw_xysc_school_comp_user_dt_yyyymm   "$db_schema.dw_xysc_school_comp_user_dt_$year$month"
 	   set dw_call_opposite_yyyymm              "$db_schema.dw_call_opposite_$year$month"
 	   set dw_newbusi_sms_yyyymm                "$db_schema.dw_newbusi_sms_$year$month"
 	   set xysc_comp_comm_rate_tmp4             "$db_schema.xysc_comp_comm_rate_tmp4"

 	   #1.�½���ʱ��xysc_comp_TeacherandStudent_user_calltmp,���ܾ���������У԰������ʦ�ͻ�ͨ����Ϣ
	   set sql_buf "drop TABLE bass2.xysc_comp_TeacherandStudent_user_calltmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_TeacherandStudent_user_calltmp(
			   comp_no		      varchar(32),
			   school_name      varchar(128),
			   opposite_no	    varchar(15),
			   call_counts	    integer,
			   call_duration_m  integer
     )
     PARTITIONING KEY (comp_no,opposite_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_comp_TeacherandStudent_user_calltmp
	       select a.comp_no,
                a.school_name,
                b.product_no,
                sum(b.call_counts),
                sum(b.call_duration_m)
           from $dw_xysc_school_comp_user_dt_yyyymm a
          inner join (select product_no,opp_number,
                             sum(call_counts) as call_counts,
                             sum(call_duration_m) as call_duration_m
                        from $dw_call_opposite_yyyymm
                      group by product_no,opp_number) b
             on a.comp_no = b.opp_number
          inner join $dw_xysc_school_real_user_dt_yyyymm c
             on b.product_no = c.product_no and c.phone_type='T'
          group by a.comp_no, a.school_name, b.product_no
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

 	   #2.�½���ʱ��xysc_comp_TeacherandStudent_user_smstmp,���ܾ���������У԰������ʦ�ͻ�������Ϣ
	   set sql_buf "drop TABLE bass2.xysc_comp_TeacherandStudent_user_smstmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_TeacherandStudent_user_smstmp(
			   comp_no		      varchar(32),
			   school_name      varchar(128),
			   opposite_no	    varchar(15),
			   sms_counts	      integer
     )
     PARTITIONING KEY (comp_no,opposite_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_comp_TeacherandStudent_user_smstmp
	       select a.comp_no,
                a.school_name,
                b.product_no,
                sum(b.sms_counts)
           from $dw_xysc_school_comp_user_dt_yyyymm a
          inner join (select product_no,opp_number,
                             sum(counts) as sms_counts
                        from $dw_newbusi_sms_yyyymm
                      group by product_no,opp_number) b
             on a.comp_no = b.opp_number
          inner join $dw_xysc_school_real_user_dt_yyyymm c
             on b.product_no = c.product_no and c.phone_type='T'
          group by a.comp_no, a.school_name, b.product_no
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

 	   #3.�½���ʱ��xysc_comp_TeacherandStudent_user_commtmp,���ܾ���������У԰������ʦ�ͻ�ͨ��(CALL/SMS)��Ϣ
	   set sql_buf "drop TABLE bass2.xysc_comp_TeacherandStudent_user_commtmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_TeacherandStudent_user_commtmp(
			   comp_no			          varchar(32),
			   school_name		        varchar(128),
			   xy_teacher_call_counts	integer,
			   xy_teacher_call_user	  integer,
			   xy_teacher_sms_counts	integer,
			   xy_teacher_sms_user	  integer
     )
     PARTITIONING KEY (comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_comp_TeacherandStudent_user_commtmp
	       select comp_no,
               school_name,
               sum(xy_teacher_call_counts),
               sum(xy_teacher_call_user),
               sum(xy_teacher_sms_counts),
               sum(xy_teacher_sms_user)
          from (select comp_no,
                       school_name,
                       sum(call_counts) as xy_teacher_call_counts,
                       count(distinct opposite_no) as xy_teacher_call_user,
                       0 as xy_teacher_sms_counts,
                       0 as xy_teacher_sms_user
                  from xysc_comp_TeacherandStudent_user_calltmp
                 group by comp_no, school_name
                union all
                select comp_no,
                       school_name,
                       0 as xy_teacher_call_counts,
                       0 as xy_teacher_call_user,
                       sum(sms_counts) as xy_teacher_sms_counts,
                       count(distinct opposite_no) as xy_teacher_sms_user
                  from xysc_comp_TeacherandStudent_user_smstmp
                 group by comp_no, school_name) a
         group by comp_no, school_name
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

 	   #4.�½���ʱ��xysc_comp_TeacherandStudent_user_tmp,���ܾ�������У԰��У԰��ʦ�ͻ���ͨ��(CALL/SMS)��Ϣ
	   set sql_buf "drop TABLE bass2.xysc_comp_TeacherandStudent_user_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_TeacherandStudent_user_tmp(
			   comp_no			          varchar(32),
			   school_name		        varchar(128),
			   xy_call_counts		      integer,
			   xy_teacher_call_counts	integer,
			   xy_call_user		        integer,
			   xy_teacher_call_user	  integer,
			   xy_sms_counts		      integer,
			   xy_teacher_sms_counts	integer,
			   xy_sms_user		        integer,
			   xy_teacher_sms_user	  integer
     )
     PARTITIONING KEY (comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_comp_TeacherandStudent_user_tmp
	       select a.comp_no,
               a.school_name,
               value(b.xy_call_counts, 0),
               a.xy_teacher_call_counts,
               value(b.xy_call_user, 0),
               a.xy_teacher_call_user,
               value(b.xy_sms_counts, 0),
               a.xy_teacher_sms_counts,
               value(b.xy_sms_user, 0),
               a.xy_teacher_sms_user
          from xysc_comp_TeacherandStudent_user_commtmp a
          left outer join $xysc_comp_comm_rate_tmp4 b
            on a.comp_no = b.comp_no
           and a.school_name = b.school_name
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

 	   #5.�½���ʱ��xysc_comp_TeacherandStudent_user_ratetmp,���ܾ�������У԰��У԰��ʦ�ͻ���ͨ��(CALL/SMS)ռ����Ϣ
	   set sql_buf "drop TABLE bass2.xysc_comp_TeacherandStudent_user_ratetmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_TeacherandStudent_user_ratetmp(
			   comp_no			                varchar(32),
			   school_name		              varchar(128),
			   xy_call_counts		            integer,
			   xy_teacher_call_counts	      integer,
			   xy_teacher_call_counts_rate	decimal(12,2),
			   xy_call_user		              integer,
			   xy_teacher_call_user	        integer,
			   xy_teacher_call_user_rate	  decimal(12,2),
			   xy_sms_counts		            integer,
			   xy_teacher_sms_counts	      integer,
			   xy_teacher_sms_counts_rate	  decimal(12,2),
			   xy_sms_user		              integer,
			   xy_teacher_sms_user	        integer,
			   xy_teacher_sms_user_rate	    decimal(12,2)
     )
     PARTITIONING KEY (comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_comp_TeacherandStudent_user_ratetmp
	       select comp_no,
               school_name,
               xy_call_counts,
               xy_teacher_call_counts,
               case
                 when xy_call_counts = 0 then
                  0.00
                 else
                  round(decimal(xy_teacher_call_counts, 12, 2) /
                        decimal(xy_call_counts, 12, 2) * 100.00,
                        2)
               end as xy_teacher_call_counts_rate,
               xy_call_user,
               xy_teacher_call_user,
               case
                 when xy_call_user = 0 then
                  0.00
                 else
                  round(decimal(xy_teacher_call_user, 12, 2) /
                        decimal(xy_call_user, 12, 2) * 100.00,
                        2)
               end as xy_teacher_call_user_rate,
               xy_sms_counts,
               xy_teacher_sms_counts,
               case
                 when xy_sms_counts = 0 then
                  0.00
                 else
                  round(decimal(xy_teacher_sms_counts, 12, 2) /
                        decimal(xy_sms_counts, 12, 2) * 100.00,
                        2)
               end as xy_teacher_sms_counts_rate,
               xy_sms_user,
               xy_teacher_sms_user,
               case
                 when xy_sms_user = 0 then
                  0.00
                 else
                  round(decimal(xy_teacher_sms_user, 12, 2) /
                        decimal(xy_sms_user, 12, 2) * 100.00,
                        2)
               end as xy_teacher_sms_user_rate
          from xysc_comp_TeacherandStudent_user_tmp
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

 	   #6.�½���ʱ��xysc_comp_TeacherandStudent_tuser_tmp,ʶ��������У԰��ʦ�ͻ�
	   set sql_buf "drop TABLE bass2.xysc_comp_TeacherandStudent_tuser_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_comp_TeacherandStudent_tuser_tmp(
			   comp_no			                varchar(32),
			   school_name		              varchar(128)
     )
     PARTITIONING KEY (comp_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_comp_TeacherandStudent_tuser_tmp
	       select distinct comp_no, school_name
          from xysc_comp_TeacherandStudent_user_ratetmp
         where xy_teacher_call_counts_rate >= 50
            or xy_teacher_call_user_rate >= 50
            or xy_teacher_sms_counts_rate >= 50
            or xy_teacher_sms_user_rate >= 50
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

 	   #7.����dw_xysc_school_comp_user_dt_yyyymm,ʶ��������У԰��ʦ��ѧ���ͻ�
	   #�������-S:ѧ������;T:��ʦ
	   set sql_buf "update $dw_xysc_school_comp_user_dt_yyyymm a
	       set phone_type=
	           (select case when b.comp_no is not null then 'T' end
	            from xysc_comp_TeacherandStudent_tuser_tmp b
	            where a.comp_no=b.comp_no
	           )
        "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "update $dw_xysc_school_comp_user_dt_yyyymm a
	       set phone_type='S'
	       where phone_type is null
        "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step5:���������н�ʦ��ѧ��ʶ����-End...\n"

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
     #Step1.Do_xysc_comp_comn_info
	   set sql_buf "drop table xysc_comp_comn_info_calltmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_comp_comn_info_smstmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_comp_comn_info_mmstmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
     #Step2.Do_xysc_comp_comn_info2
	   set sql_buf "drop table xysc_comp_comn_info2_calltmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_comp_comn_info2_smstmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_comp_comn_info2_mmstmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
     #Step3.Do_xysc_comp_belong_school
	   set sql_buf "drop table xysc_comp_belong_school_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
     #Step4.Do_xysc_comp_real_user
	   set sql_buf "drop table xysc_comp_comm_rate_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_comp_comm_rate_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_comp_comm_rate_tmp3"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
     #Step5.Do_xysc_comp_TeacherandStudent_user
	   set sql_buf "drop table xysc_comp_TeacherandStudent_user_calltmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_comp_TeacherandStudent_user_smstmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_comp_TeacherandStudent_user_commtmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_comp_TeacherandStudent_user_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_comp_TeacherandStudent_user_ratetmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_comp_TeacherandStudent_tuser_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }

		 puts "StepX.ɾ���������豣������ʱ��-End...\n"

	return 0
}