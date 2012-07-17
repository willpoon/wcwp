#======================================================================================
#��Ȩ������Copyright (c) 2010,AsiaInfo-Linkage Inc.
#��������: App_xysc_school_user_ms_1.tcl
#������: �����ƶ�У԰�г�ר��-1.У԰�ƶ��ͻ�ʶ��
#����˼��: �ò�����֪��ʵ���û�ȥѰ��������ӽ���ʵ�û�,����������֪��ʵ���û�,
#          ��˷�������������ɶ�����У԰�û���ʶ��
#��������: dw_product_yyyymm/cdr_call_dtl_$today/cdr_sms_dtl_$today -->dw_xysc_school_real_user_dt_yyyymm
#��������: ��
#����ʾ��: ds App_xysc_school_user_ms_1.tcl 2010-09-01
#����ʱ��: 2010-11-18
#�� �� ��: AsiaInfo	Liwei
#��������: <ע��!>������ٵĹ�ϵ,ÿ���7�¡�8��У԰�ͻ����ܰ����������㷨����,��Ҫ�޸ģ�
#	         ���統ǰ����8��,����7�µ׵�У԰�û�,Ϊ6�µ׵�У԰�û�+7�µ���������У԰�û�;
#          8�µ׵�У԰�û�,��Ϊ6�µ׵�У԰�û�+7�µ���������У԰�û�+8�µ���������У԰�û�
#�޸���ʷ: 1.2011-1-10 By Liwei ȡ��Do_dim_xysc_lac_cell_info��ע��
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
        #Step1:��վά�����ݴ���
        #Step1_1:��վά�����ݴ���
		    puts "Step1_1:��վά�����ݴ���-Begin...\n"
 	      if {[Do_dim_xysc_lac_cell_info $p_optime]  != 0} {
 	      	  aidb_roll $conn
 	      	  aidb_close $handle
 	      	 return -1
 	      }
        #Step1_2:��ʵ�����������ݴ���
		    puts "Step1_2:��ʵ�����������ݴ���-Begin...\n"
 	      if {[Do_dim_xysc_pno_example_info $p_optime]  != 0} {
 	      	  aidb_roll $conn
 	      	  aidb_close $handle
 	      	 return -1
 	      }
        #Step2:У԰Ǳ�ڿͻ�ʶ��(����Ϊ����A)����
        #Step2-1:����ѧУ��վ����,Ȧ��У԰��վ����(����ĩ)ͨ�������ͻ�
		    puts "Step2-1:����ѧУ��վ����,Ȧ��У԰��վ����(����ĩ)ͨ�������ͻ�-Begin...\n"
 	      if {[Do_xysc_school_latency_userA_1 $p_optime]  != 0} {
 	      	  aidb_roll $conn
 	      	  aidb_close $handle
 	      	 return -1
 	      }
        #Step2-2:У԰��վ����(����ĩ)ͨ���ͻ��еĿͻ�Ψһ����ѧУ����
		    puts "Step2-2:У԰��վ����(����ĩ)ͨ���ͻ��еĿͻ�Ψһ����ѧУ����-Begin...\n"
 	      if {[Do_xysc_school_latency_userA_2 $p_optime]  != 0} {
 	      	  aidb_roll $conn
 	      	  aidb_close $handle
 	      	 return -1
 	      }
        ##Step2-3:ʶ��У԰��վ����(����ĩ)ͨ���ͻ��еĿͻ�Ψһ����ѧУ����-���û�վ(������,�ݲ�����)
		    #puts "Step2-3:ʶ��У԰��վ����(����ĩ)ͨ���ͻ��еĿͻ�Ψһ����ѧУ����-���û�վ-Begin...\n"
 	      #if {[Do_xysc_school_latency_userA_3 $p_optime]  != 0} {
 	      #	  aidb_roll $conn
 	      #	  aidb_close $handle
 	      #	 return -1
 	      #}
        #Step2-4:ʶ��У԰��վ����(����ĩ)ͨ���ͻ��е�Ǳ��У԰�ͻ�
		    puts "Step2-4:ʶ��У԰��վ����(����ĩ)ͨ���ͻ��е�Ǳ��У԰�ͻ�-Begin...\n"
 	      if {[Do_xysc_school_latency_userA_4 $p_optime]  != 0} {
 	      	  aidb_roll $conn
 	      	  aidb_close $handle
 	      	 return -1
 	      }
 	      #Step3:ʶ��У԰��ʵ�ͻ�(����Ϊ����B)����
        #Step3_1:Ǳ��У԰�ͻ�����(����ĩ)У԰����ͨ��ռ��30%�����ϵ�ͨ���Զ����
		    puts "Step3_1:Ǳ��У԰�ͻ�����(����ĩ)У԰����ͨ��ռ��30%�����ϵ�ͨ���Զ����-Begin...\n"
 	      if {[Do_xysc_school_latency_userB_1 $p_optime]  != 0} {
 	      	  aidb_roll $conn
 	      	  aidb_close $handle
 	      	 return -1
 	      }
        #Step3_2:Ǳ��У԰�ͻ�����(����ĩ)У԰����ͨ��ռ��30%�����ϵĶ��ŶԶ����
		    puts "Step3_2:Ǳ��У԰�ͻ�����(����ĩ)У԰����ͨ��ռ��30%�����ϵĶ��ŶԶ����-Begin...\n"
 	      if {[Do_xysc_school_latency_userB_2 $p_optime]  != 0} {
 	      	  aidb_roll $conn
 	      	  aidb_close $handle
 	      	 return -1
 	      }
        #Step4:ʶ����ʵУ԰�ͻ�
        ##Step4_1.������ʵ����ʶ��-ͨ�����������ŵ�������
		    #puts "Step4_1.������ʵ����ʶ��-ͨ�����������ŵ�������-Begin...\n"
 	      #if {[Do_xysc_school_real_user_1 $p_optime]  != 0} {
 	      #	  aidb_roll $conn
 	      #	  aidb_close $handle
 	      #	 return -1
 	      #}
        #Step4_2.��������ʶ��-ͨ�����������ŵ�������
		    puts "Step4_2.��������ʶ��-ͨ�����������ŵ�������-Begin...\n"
 	      if {[Do_xysc_school_real_user_2 $p_optime]  != 0} {
 	      	  aidb_roll $conn
 	      	  aidb_close $handle
 	      	 return -1
 	      }
        #Step5.ʶ����ʵѧ���ͽ�ʦУ԰�ͻ�
        #Step5_1.ʶ����ʵѧ���ͽ�ʦУ԰�ͻ�Ԥ����
		    puts "Step5_1.ʶ����ʵѧ���ͽ�ʦУ԰�ͻ�Ԥ����-Begin...\n"
 	      if {[Do_xysc_school_TeacherandStudent_user_1 $p_optime]  != 0} {
 	      	  aidb_roll $conn
 	      	  aidb_close $handle
 	      	 return -1
 	      }
        #Step5_2.ʶ����ʵѧ���ͽ�ʦУ԰�ͻ�
		    puts "Step5_2.ʶ����ʵѧ���ͽ�ʦУ԰�ͻ�-Begin...\n"
 	      if {[Do_xysc_school_TeacherandStudent_user_2 $p_optime]  != 0} {
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
 	      puts "Dw_xysc_school_real_user_dt_yyyymm-${month}�±�,��App_xysc_new_user_ms.tcl-����${month}������ʱ����!"
     }

 	   return 0
 }

 proc Do_dim_xysc_lac_cell_info {p_optime} {
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
 	   set dim_xysc_maintenance_info   "$db_schema.Dim_xysc_maintenance_info"
     set dim_stat_cell_yyyymm        "dim_stat_cell_$year$month"

 	   #1.����У԰ά����Ϣ¼���(dim_xysc_maintenance_info)��cell_name,city_id
 	   set sql_buf "Select count(1) from SYSIBM.SYSTABLES
     		 Where lower(name)='$dim_stat_cell_yyyymm' and lower(creator)='$db_schema'"

     if [catch {aidb_sql $handle $sql_buf} errmsg] {
     	     trace_sql $errmsg 1300
     	     puts "Errmsg:$errmsg"
     	     return -1
     }
     set cond [lindex [aidb_fetch $handle] 0]
     aidb_commit $conn
     aidb_close $handle
     set handle [aidb_open $conn]
     if { $cond==1 } {
          set dim_stat_cell_yyyymm   "bass2.dim_stat_cell_$year$month"
 	   } else {
 	        set lastmonth [GetLastMonth $year$month]
          set dim_stat_cell_yyyymm   "bass2.dim_stat_cell_$lastmonth"
 	   }
	   set sql_buf "update $dim_xysc_maintenance_info a
	       set (cell_name,cell_type,city_id)=
	           (select value(b.cell_name,a.cell_name),value(b.locntype_id,a.cell_type),value(b.city_id,a.city_id)
	            from $dim_stat_cell_yyyymm b
	            where SUBSTR(HEX(INT(RTRIM(a.lac_id))),5,4)=b.lac_id and SUBSTR(HEX(INT(RTRIM(a.cell_id))),5,4)=b.cell_id
	           )
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #2.����������У԰������Ϣ��(dim_xysc_lac_cell_info)
	   set sql_buf "alter table dim_xysc_lac_cell_info activate not logged initially with empty table
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into dim_xysc_lac_cell_info (
	         school_id
	         ,school_name
           ,lac_id
           ,cell_id
           ,cell_name
           ,cell_type
           ,city_id
           ,school_type
           ,over_flag
	          )
         select a.school_id,
               a.school_name,
               SUBSTR(HEX(INT(RTRIM(a.lac_id))), 5, 4) as lac_id,
               SUBSTR(HEX(INT(RTRIM(a.cell_id))), 5, 4) as cell_id,
               a.cell_name,
               a.cell_type,
               a.city_id,
               a.school_type,
               case
                 when a.lac_id = b.lac_id and a.cell_id = b.cell_id then
                  1
                 else
                  0
               end as over_flag
          from $dim_xysc_maintenance_info a
          left join (select lac_id, cell_id, count(1)
                       from $dim_xysc_maintenance_info
                      group by lac_id, cell_id
                     having count(3) > 1) b
            on a.lac_id = b.lac_id
           and a.cell_id = b.cell_id
	   "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step1_1:��վά�����ݴ���-End...\n"

	return 0
}

 proc Do_dim_xysc_pno_example_info {p_optime} {
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

 	   #Դ��
 	   set dim_xysc_pno_example_info   "$db_schema.dim_xysc_pno_example_info"
 	   set dw_product_yyyymm           "$db_schema.dw_product_$year$month"

 	   #1.�½���ʵ����������Ϣ��ʱ��(dim_xysc_pno_example_info_tmp),����������
	   set sql_buf "drop TABLE bass2.dim_xysc_pno_example_info_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.dim_xysc_pno_example_info_tmp(
			     user_id			     varchar(20),
           product_no		     varchar(15),
           cmcc_active_flag	 smallint

     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into dim_xysc_pno_example_info_tmp (
	          user_id
           ,product_no
           ,cmcc_active_flag
	          )
         select value(b.user_id,'-1'),a.product_no,
            case when c.product_no is not null then 1 else 0 end
         from $dim_xysc_pno_example_info a left join
              (select user_id,product_no,row_number() over(partition by product_no order by max(create_date) desc ) as rank
               from $dw_product_yyyymm
               group by user_id,product_no
              )b on a.product_no=b.product_no left join
              (select product_no from $dw_product_yyyymm
               where userstatus_id in ($rep_online_userstatus_id)
               group by product_no
              ) c on a.product_no=c.product_no
         where b.rank=1
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "update $dim_xysc_pno_example_info a
	       set (user_id,cmcc_active_flag)=
	           (select b.user_id,b.cmcc_active_flag
	            from dim_xysc_pno_example_info_tmp b
	            where a.product_no=b.product_no
	            )
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step1_2:��ʵ�����������ݴ���-End...\n"

	return 0
}
 proc Do_xysc_school_latency_userA_1 {p_optime} {
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
     set dw_product_yyyymm            "$db_schema.dw_product_$year$month"
     set dw_call_cell_yyyymm          "$db_schema.dw_call_cell_$year$month"
     set dim_xysc_lac_cell_info       "$db_schema.dim_xysc_lac_cell_info"

     #1.�½���ʱ��(xysc_school_latency_userA_1_tmp),������ѧУ��վ����,Ȧ��У԰��վ����ͨ�������ͻ�
	   set sql_buf "drop TABLE bass2.xysc_school_latency_userA_1_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userA_1_tmp(
			     user_id	    varchar(20),
           product_no   varchar(15)
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_latency_userA_1_tmp (
	          user_id
           ,product_no
	          )
         select a.user_id,c.product_no
         from $dw_call_cell_yyyymm a inner join $dim_xysc_lac_cell_info b
              on a.lac_id=b.lac_id and a.cell_id=b.cell_id inner join
              (select user_id,product_no from $dw_product_yyyymm
               where userstatus_id in ($rep_online_userstatus_id)
               group by user_id,product_no
              )c on a.user_id=c.user_id and a.product_no=c.product_no
         group by a.user_id,c.product_no
	   "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up on xysc_school_latency_userA_1_tmp (user_id,product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

     #2.�½���ʱ��xysc_school_latency_userA_1_tmp2,ѭ������У԰��վ����(����ĩ)ͨ������
     set sql_buf "drop TABLE bass2.xysc_school_latency_userA_1_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userA_1_tmp2(
			   op_time		      date,
			   user_id		      varchar(20),
			   product_no	      varchar(15),
			   lac_id		        varchar(10),
			   cell_id		      varchar(10),
			   call_counts	    integer,
			   call_duration_m	integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   for {set i 1 } { $i <= $last_days } { incr i } {
		    set today  [format "%04s%02s%02s" $year $month $i]
	      set to_day [format "%04s-%02s-%02s" $year $month $i]

	      set sql_buf "select dayofweek('$to_day') from dual"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }

		    while {[set this_row [aidb_fetch $handle]] != ""} {
		       set	    weekdays    	        [lindex $this_row 0]
		    }
	      aidb_close $handle
	      if [catch {set handle [aidb_open $conn]} errmsg] {
	      	trace_sql $errmsg 1302
	      	return -1
	      }
	      ##�жϽ������ڼ�,�������ĩ��������ѭ��
	      if {$weekdays == 1 || $weekdays == 7} {
	         continue
	      }
		    puts "$today-����ĩͨ���������ʼ......"
	      set sql_buf "insert into xysc_school_latency_userA_1_tmp2
		    	select
		    		 '$to_day',
		    		 b.user_id,
		    		 b.product_no,
		    		 a.lac_id,
		    		 a.cell_id,
		    		 count(1),
		    		 sum(a.call_duration_m)
		    	from cdr_call_dtl_$today a inner join xysc_school_latency_userA_1_tmp b
		    	  on a.user_id=b.user_id and a.product_no=b.product_no
		    	group by b.user_id,
		    		 b.product_no,
		    		 a.lac_id,
		    		 a.cell_id"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }

		 }
     #3.�½���ʱ��xysc_school_latency_userA_1_$year$month,Ȧ��У԰��վ����(����ĩ)ͨ�������ͻ�
     set sql_buf "drop TABLE bass2.xysc_school_latency_userA_1_$year$month"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userA_1_$year$month like bass2.xysc_school_latency_userA_1_yyyymm in tbs_bass_miner
					        index in tbs_index partitioning key (user_id,product_no) using hashing not logged initially
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_latency_userA_1_$year$month
		     select
		    	  user_id,
		    	  product_no,
		    	  lac_id,
		    	  cell_id,
		    	  0 as callmoment,
		    	  sum(call_counts),
		    	  sum(call_duration_m)
		     from xysc_school_latency_userA_1_tmp2
		     group by user_id,
		     	  product_no,
		     	  lac_id,
		     	  cell_id"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up2 on xysc_school_latency_userA_1_$year$month (user_id,product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step2-1:����ѧУ��վ����,Ȧ��У԰��վ����(����ĩ)ͨ�������ͻ�-End...\n"

	return 0
}
 proc Do_xysc_school_latency_userA_2 {p_optime} {
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
 	   #Դ��
 	   set    xysc_school_latency_userA_1_yyyymm    "$db_schema.xysc_school_latency_userA_1_$year$month"
 	   set    dim_xysc_lac_cell_info                "$db_schema.dim_xysc_lac_cell_info"

 	   #1.�½���ʱ��xysc_school_latency_userA_2_tmp,��ѧУ���Ƽ�ѧУ��վ����Ϣ����ͨ�����
 	   set sql_buf "drop TABLE bass2.xysc_school_latency_userA_2_tmp
 	   "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
 	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userA_2_tmp (
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   lac_id		       varchar(10),
			   cell_id		     varchar(10),
			   school_name	   varchar(128),
			   over_flag	     smallint,
			   call_counts	   integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_latency_userA_2_tmp
		     select
		    	  a.user_id,
			      a.product_no,
			      a.lac_id,
			      a.cell_id,
			      b.school_name,
			      b.over_flag,
			      sum(a.call_counts)
		     from $xysc_school_latency_userA_1_yyyymm a left join
		          (select distinct lac_id,cell_id,school_name,over_flag from $dim_xysc_lac_cell_info
		          )b on a.lac_id=b.lac_id and a.cell_id=b.cell_id
		     group by a.user_id,
			      a.product_no,
			      a.lac_id,
			      a.cell_id,
			      b.school_name,
			      b.over_flag"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up3"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up3 on xysc_school_latency_userA_2_tmp (user_id,product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #2.�½���ʱ��xysc_school_latency_userA_2_tmp2,���ͻ���ͨ���������������
 	   set sql_buf "drop TABLE bass2.xysc_school_latency_userA_2_tmp2
 	   "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
 	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userA_2_tmp2 (
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   school_name	   varchar(128),
			   call_counts	   integer,
			   sn              integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_latency_userA_2_tmp2
		     select
		    	  a.user_id,
			      a.product_no,
			      a.school_name,
			      a.call_counts,
			      row_number() over(partition by user_id order by call_counts desc) sn
		     from (select user_id,product_no,school_name,sum(call_counts) as call_counts
		           from xysc_school_latency_userA_2_tmp
		           where school_name is not null
		           group by user_id,product_no,school_name
		          )a"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up4"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up4 on xysc_school_latency_userA_2_tmp2 (user_id,product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #3.�½���ʱ��xysc_school_latency_userA_2_tmp3,���ͻ���ͨ��������������ֵ�ѧУ
 	   set sql_buf "drop TABLE bass2.xysc_school_latency_userA_2_tmp3
 	   "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
 	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userA_2_tmp3 (
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   school_name	   varchar(128),
			   call_counts	   integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_latency_userA_2_tmp3
		     select
		    	  a.user_id,
			      a.product_no,
			      a.school_name,
			      a.call_counts
		     from xysc_school_latency_userA_2_tmp2 a
		     where a.sn=1"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up5"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up5 on xysc_school_latency_userA_2_tmp3 (user_id,product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step2-2:У԰��վ����(����ĩ)ͨ���ͻ��еĿͻ�Ψһ����ѧУ����-End...\n"

	return 0
}
 proc Do_xysc_school_latency_userA_3 {p_optime} {
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

 	   #Դ��
 	   set   dim_xysc_lac_cell_info             "$db_schema.Dim_xysc_lac_cell_info"
 	   set   xysc_school_latency_userA_2_tmp    "$db_schema.xysc_school_latency_userA_2_tmp"
 	   set   xysc_school_latency_userA_2_tmp3   "$db_schema.xysc_school_latency_userA_2_tmp3"


 	   #���ڹ��û�վ,�������ٰ���Щ�й��л�վѧУ�Ŀͻ����¹���ѧУ
 	   #1.�½���ʱ��xysc_school_latency_userA_3_tmp,Ȧ�����л�վѧУ�Ŀͻ�ͨ������
 	   set sql_buf "drop TABLE bass2.xysc_school_latency_userA_3_tmp
 	   "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
 	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userA_3_tmp (
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   school_name	   varchar(128)
			   call_counts	   integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_latency_userA_3_tmp
		     select
		    	  a.user_id,
			      a.product_no,
			      a.school_name,
			      a.call_counts
		     from $xysc_school_latency_userA_2_tmp3
		     where school_name in (select school_name from $dim_xysc_lac_cell_info where over_flag=1 group by school_name)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up5_1"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up5_1 on xysc_school_latency_userA_3_tmp (user_id,product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #2.�½���ʱ��xysc_school_latency_userA_3_tmp2,Ȧ�����л�վѧУ�Ŀͻ�У԰���л�վͨ������
 	   set sql_buf "drop TABLE bass2.xysc_school_latency_userA_3_tmp2
 	   "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
 	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userA_3_tmp2 (
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   lac_id		       varchar(10),
			   cell_id		     varchar(10),
			   school_name	   varchar(128),
			   over_flag	     smallint,
			   call_counts	   integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_latency_userA_3_tmp2
		     select
		    	  a.user_id,a.product_no,a.lac_id,a.cell_id,a.school_name,a.over_flag,a.call_counts
		     from $xysc_school_latency_userA_2_tmp a inner join
		         (select lac_id,cell_id from $dim_xysc_lac_cell_info where over_flag=1 group by lac_id,cell_id
		          )b on a.lac_id=b.lac_id and a.cell_id=b.cell_id
		     where a.user_id in (select user_id from xysc_school_latency_userA_3_tmp group by user_id)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up5_2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up5_2 on xysc_school_latency_userA_3_tmp2 (user_id,product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #3.�½���ʱ��xysc_school_latency_userA_3_tmp3,XXXȦ�����л�վѧУ�Ŀͻ�ͨ�����
 	   set sql_buf "drop TABLE bass2.xysc_school_latency_userA_3_tmp3
 	   "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
 	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userA_3_tmp3 (
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   lac_id		       varchar(10),
			   cell_id		     varchar(10),
			   school_name	   varchar(128),
			   over_flag	     smallint,
			   call_counts	   integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_latency_userA_3_tmp3
		     select
		     	 a.user_id,a.product_no,a.lac_id,a.cell_id,a.school_name,a.over_flag,a.call_counts
		     from	xysc_school_latency_userA_3_tmp2 a left outer join
		     	    xysc_school_latency_userA_3_tmp b
		        on a.user_id=b.user_id and a.product_no=b.product_no and a.school_name=b.school_name
		     where b.product_no is null"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up5_3"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up5_3 on xysc_school_latency_userA_3_tmp3 (user_id,product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
 	   #4.�½���ʱ��xysc_school_latency_userA_3_tmp4,Ȧ�����л�վѧУ�Ŀͻ�ͨ�����
 	   #�����������Ҫ����ѧУ���û�,�����û�վ��ͨ�������Ƿ����У԰�������
 	   #������,��˵��ֻ�ڹ��û�վͨ��,��У԰������վδͨ��,���û��Ͳ����ٹ鵽���ѧУ
 	   set sql_buf "drop TABLE bass2.xysc_school_latency_userA_3_tmp4
 	   "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
 	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userA_3_tmp4 (
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   school_name	   varchar(128),
			   call_counts	   integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_latency_userA_3_tmp4
		     select a.user_id, a.product_no, a.school_name, a.call_counts
          from (selecct user_id, product_no, school_name, sum(call_counts) as
                call_counts from xysc_school_latency_userA_3_tmp3 where
                over_flag = 1) a
          left outer join $xysc_school_latency_userA_2_tmp b
            on a.user_id = b.user_id
           and a.product_no = b.product_no
           and a.school_name = b.school_name
           and a.call_counts = b.call_counts
         where b.product_no is null
        "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up5_4"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up5_4 on xysc_school_latency_userA_3_tmp4 (user_id,product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #5.�½���ʱ��xysc_school_latency_userA_3_tmp5,���ɹ��û�վͨ������
 	   set sql_buf "drop TABLE bass2.xysc_school_latency_userA_3_tmp5
 	   "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
 	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userA_3_tmp5 (
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   school_name	   varchar(128),
			   call_counts	   integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_latency_userA_3_tmp5
		     select a.user_id, a.product_no, a.school_name, a.call_counts
          from $xysc_school_latency_userA_2_tmp a
          left outer join xysc_school_latency_userA_3_tmp4 b
            on a.user_id = b.user_id
           and a.product_no = b.product_no
           and a.school_name = b.school_name
        "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up5_5"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up5_5 on xysc_school_latency_userA_3_tmp4 (user_id,product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #6.�½���ʱ��xysc_school_latency_userA_3_tmp6,�ϲ�У԰��վ(����+�ǹ���)ͨ������
 	   set sql_buf "drop TABLE bass2.xysc_school_latency_userA_3_tmp6
 	   "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
 	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userA_3_tmp6 (
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   school_name	   varchar(128),
			   call_counts	   integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_latency_userA_3_tmp6
		     select user_id, product_no, school_name, call_counts
           from $xysc_school_latency_userA_2_tmp3 a
         union all
         select user_id, product_no, school_name, call_counts
           from xysc_school_latency_userA_3_tmp5 a
        "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up5_6"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up5_6 on xysc_school_latency_userA_3_tmp4 (user_id,product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step2-3:ʶ��У԰��վ����(����ĩ)ͨ���ͻ��еĿͻ�Ψһ����ѧУ����-���û�վ-End...\n"

	return 0
}

 proc Do_xysc_school_latency_userA_4 {p_optime} {
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
 	   #Դ��
 	   set    xysc_school_latency_userA_2_tmp    "$db_schema.xysc_school_latency_userA_2_tmp"
 	   set    xysc_school_latency_userA_2_tmp3   "$db_schema.xysc_school_latency_userA_2_tmp3"

 	   #�½�Ŀ���xysc_school_latency_userA_4_$year$month,����Ǳ��У԰�ͻ�(����A)����
 	   #ע�������ڹ��û�վ����,��˴���:xysc_school_latency_userA_2_tmp3Ӧ�滻Ϊ
 	   #��:xysc_school_latency_userA_3_tmp6
 	   set sql_buf "drop TABLE bass2.xysc_school_latency_userA_4_$year$month
 	   "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
 	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userA_4_$year$month like bass2.xysc_school_latency_userA_4_yyyymm in tbs_bass_miner
					        index in tbs_index partitioning key (user_id,product_no) using hashing not logged initially
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_latency_userA_4_$year$month
		     select
		     	  a.user_id,
		     	  a.product_no,
		     	  a.call_counts,
		     	  value(b.total,0),
		     	  case when b.total=0 then 0 else round(a.call_counts*1.0/b.total,2) end,
		     	  a.school_name
		     from xysc_school_latency_userA_2_tmp3 a left join
		     	   (select user_id,product_no,sum(call_counts) as total
		     	    from xysc_school_latency_userA_2_tmp group by user_id,product_no
		     	   )b on a.user_id=b.user_id and a.product_no=b.product_no"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up6"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up6 on xysc_school_latency_userA_4_$year$month (user_id,product_no,xy_call_rate)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }


		 puts "Step2-4:ʶ��У԰��վ����(����ĩ)ͨ���ͻ��е�Ǳ��У԰�ͻ�-End...\n"

	return 0
}

 proc Do_xysc_school_latency_userB_1 {p_optime} {
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

 	   #Դ��
 	   set   xysc_school_latency_userA_4_yyyymm   "$db_schema.xysc_school_latency_userA_4_$year$month"

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

     #1.�½���ʱ��(xysc_school_latency_userB_1_call_tmp),Ǳ��У԰�ͻ�����ĩͨ���Զ��������
	   set sql_buf "drop TABLE bass2.xysc_school_latency_userB_1_call_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userB_1_call_tmp(
			   op_time		     date,
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   school_name	   varchar(128),
			   opposite_no	   varchar(32),
			   call_counts	   integer,
			   call_duration_m integer
     )
     PARTITIONING KEY (user_id,product_no,opposite_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   for {set i 1 } { $i <= $last_days } { incr i } {
		    set today  [format "%04s%02s%02s" $year $month $i]
	      set to_day [format "%04s-%02s-%02s" $year $month $i]

	      set sql_buf "select dayofweek('$to_day') from dual"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }

		    while {[set this_row [aidb_fetch $handle]] != ""} {
		       set	    weekdays    	        [lindex $this_row 0]
		    }
	      aidb_close $handle
	      if [catch {set handle [aidb_open $conn]} errmsg] {
	      	trace_sql $errmsg 1302
	      	return -1
	      }
	      ##�жϽ������ڼ�,�������ĩ��������ѭ��
	      if {$weekdays == 1 || $weekdays == 7} {
	         continue
	      }
		    puts "$today-����ĩͨ���Զ��������ʼ......"
	      set sql_buf "insert into xysc_school_latency_userB_1_call_tmp
		    	  select
		    	  	 '$to_day',
		    	  	 b.user_id,
		    	  	 b.product_no,
		    	  	 b.school_name,
		    	  	 a.opp_regular_number,
		    	  	 count(1),
		    	  	 sum(a.call_duration_m)
		    	  from cdr_call_dtl_$today a inner join $xysc_school_latency_userA_4_yyyymm b
		    	    on a.user_id=b.user_id and a.product_no=b.product_no
		    	  where b.xy_call_rate>=0.3
		    	  group by b.user_id,
		    	  	 b.product_no,
		    	  	 b.school_name,
		    	  	 a.opp_regular_number"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }

		 }
	   #2.�½���ʱ��(xysc_school_latency_userB_1_call_$year$month),����Ǳ��У԰�ͻ�����ĩͨ���Զ�����
     set sql_buf "drop TABLE bass2.xysc_school_latency_userB_1_call_$year$month"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userB_1_call_$year$month like bass2.xysc_school_latency_userB_1_call_yyyymm in tbs_bass_miner
					        index in tbs_index partitioning key (user_id,product_no,opposite_no) using hashing not logged initially
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_latency_userB_1_call_$year$month
		     select
		    	  user_id,
		    	  product_no,
		    	  school_name,
		    	  opposite_no,
		    	  sum(call_counts),
		    	  sum(call_duration_m)
		     from xysc_school_latency_userB_1_call_tmp
		     group by user_id,
		    	  product_no,
		    	  school_name,
		    	  opposite_no"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up7"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up7 on xysc_school_latency_userB_1_call_$year$month (user_id,product_no,school_name,opposite_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step3_1:Ǳ��У԰�ͻ�����(����ĩ)У԰����ͨ��ռ��30%�����ϵ�ͨ���Զ����-End...\n"

	return 0
}

 proc Do_xysc_school_latency_userB_2 {p_optime} {
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

 	   #Դ��
 	   set   xysc_school_latency_userA_4_yyyymm   "$db_schema.xysc_school_latency_userA_4_$year$month"

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

     #1.�½���ʱ��(xysc_school_latency_userB_2_sms_tmp),Ǳ��У԰�ͻ�����ĩ���ŶԶ��������
	   set sql_buf "drop TABLE bass2.xysc_school_latency_userB_2_sms_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userB_2_sms_tmp(
			   op_time		     date,
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   school_name	   varchar(128),
			   opposite_no	   varchar(32),
			   sms_counts	     integer
     )
     PARTITIONING KEY (user_id,product_no,opposite_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   for {set i 1 } { $i <= $last_days } { incr i } {
		    set today  [format "%04s%02s%02s" $year $month $i]
	      set to_day [format "%04s-%02s-%02s" $year $month $i]

	      set sql_buf "select dayofweek('$to_day') from dual"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }

		    while {[set this_row [aidb_fetch $handle]] != ""} {
		       set	    weekdays    	        [lindex $this_row 0]
		    }
	      aidb_close $handle
	      if [catch {set handle [aidb_open $conn]} errmsg] {
	      	trace_sql $errmsg 1302
	      	return -1
	      }
	      ##�жϽ������ڼ�,�������ĩ��������ѭ��
	      if {$weekdays == 1 || $weekdays == 7} {
	         continue
	      }
		    puts "$today-����ĩ���ŶԶ��������ʼ......"
	      set sql_buf "insert into xysc_school_latency_userB_2_sms_tmp
		    	  select
		    	  	 '$to_day',
		    	  	 b.user_id,
		    	  	 b.product_no,
		    	  	 b.school_name,
		    	  	 a.opp_number,
		    	  	 count(1)
		    	  from cdr_sms_dtl_$today a inner join $xysc_school_latency_userA_4_yyyymm b
		    	    on a.user_id=b.user_id and a.product_no=b.product_no
		    	  where b.xy_call_rate>=0.3
		    	  group by b.user_id,
		    	  	 b.product_no,
		    	  	 b.school_name,
		    	  	 a.opp_number"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }

		 }
	   #2.�½���ʱ��(xysc_school_latency_userB_2_sms_$year$month),����Ǳ��У԰�ͻ�����ĩ���ŶԶ�����
     set sql_buf "drop TABLE bass2.xysc_school_latency_userB_2_sms_$year$month"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userB_2_sms_$year$month like bass2.xysc_school_latency_userB_2_sms_yyyymm in tbs_bass_miner
					        index in tbs_index partitioning key (user_id,product_no,opposite_no) using hashing not logged initially
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_latency_userB_2_sms_$year$month
		     select
		    	  user_id,
		    	  product_no,
		    	  school_name,
		    	  opposite_no,
		    	  sum(sms_counts)
		     from xysc_school_latency_userB_2_sms_tmp
		     group by user_id,
		    	  product_no,
		    	  school_name,
		    	  opposite_no"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up8"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up8 on xysc_school_latency_userB_2_sms_$year$month (user_id,product_no,school_name,opposite_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step3_2:Ǳ��У԰�ͻ�����(����ĩ)У԰����ͨ��ռ��30%�����ϵĶ��ŶԶ����-End...\n"

	return 0
}
 proc Do_xysc_school_real_user_1 {p_optime} {
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
 	   #Դ��
 	   set    dim_xysc_pno_example_info                 "$db_schema.dim_xysc_pno_example_info"
 	   set    dim_prod_up_product_item                  "$db_schema.dim_prod_up_product_item"
 	   set    xysc_school_latency_userA_1_tmp           "$db_schema.xysc_school_latency_userA_1_tmp"
 	   set    xysc_school_latency_userB_1_call_yyyymm   "$db_schema.xysc_school_latency_userB_1_call_$year$month"
 	   set    xysc_school_latency_userB_2_sms_yyyymm    "$db_schema.xysc_school_latency_userB_2_sms_$year$month"
 	   set    dw_product_yyyymm                         "$db_schema.dw_product_$year$month"

     #1.�½���ʱ��(xysc_school_latency_userB_tmp),������ʵ��������
     #ע�����ߵ�����δ��У԰��վͨ���Ŀͻ�
	   set sql_buf "drop TABLE bass2.xysc_school_latency_userB_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_latency_userB_tmp(
			   user_id		     varchar(20),
			   product_no	     varchar(15),
			   school_name	   varchar(128)
     )
     PARTITIONING KEY (user_id,product_no,school_name) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_latency_userB_tmp
	       select distinct a.user_id,a.product_no,a.school_name
		     from $dim_xysc_pno_example_info a inner join
			        $xysc_school_latency_userA_1_tmp b
		        on a.user_id=b.user_id
	      "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	   #2.��������
	   set flag 1
	   set i    0
	   while { $flag>0 } {
	      #2_1.�Զ�ͨ��(�Զ˺���ΪУ԰��ʵ��������)
	      #�Զ�ͨ������ / ����У԰����
	      #�Զ�ͨ������ / ����У԰����
	      #�Զ�ͨ��ʱ�� / ����У԰ʱ��
	      set sql_buf "drop TABLE bass2.xysc_school_latency_userB_3_tmp"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_latency_userB_3_tmp(
			        user_id			          varchar(20),
				      product_no		        varchar(15),
				      school_name		        varchar(128),
				      opp_call_user_num	    integer,
				      opp_call_xyuser_num	  integer,
				      opp_call_cnt		      integer,
				      opp_call_xy_cnt	      integer,
				      opp_call_duration	    integer,
				      opp_call_xy_duration	integer
        )
        PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
        "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "insert into xysc_school_latency_userB_3_tmp (
	             user_id
              ,product_no
              ,school_name
              ,opp_call_user_num
              ,opp_call_xyuser_num
              ,opp_call_cnt
              ,opp_call_xy_cnt
              ,opp_call_duration
              ,opp_call_xy_duration
	             )
           select distinct
				      a.user_id,
				      a.product_no,
				      a.school_name,
				      value(a.opp_call_user_num,0),
				      value(b.opp_call_xyuser_num,0),
				      value(a.opp_call_cnt,0),
				      value(b.opp_call_xy_cnt,0),
				      value(a.opp_call_duration,0),
				      value(b.opp_call_xy_duration,0)
			     from (select user_id,product_no,school_name,
			  		        count(distinct opposite_no) as opp_call_user_num,
			  		        sum(call_counts) as opp_call_cnt,
			  		        sum(call_duration_m) as opp_call_duration
			  	       from $xysc_school_latency_userB_1_call_yyyymm
			  	       where opposite_no not like '10086%'
			  	       group by user_id,product_no,school_name
			  	      )a left outer join
			  	     (select a.user_id,a.product_no,b.school_name,
			  		       count(distinct a.opposite_no) as opp_call_xyuser_num,
			  		       sum(a.call_counts) as opp_call_xy_cnt,
			  		       sum(a.call_duration_m) as opp_call_xy_duration
			  	      from $xysc_school_latency_userB_1_call_yyyymm a inner join
			  		        (select distinct product_no,school_name from xysc_school_latency_userB_tmp) b
			  	          on a.opposite_no=b.product_no and a.school_name=b.school_name and a.opposite_no not like '10086%'
			  	      group by a.user_id,a.product_no,b.school_name
			  	     )b on a.user_id=b.user_id and a.product_no=b.product_no and a.school_name=b.school_name
	      "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      #2_2.�Զ˶���(�Զ˺���ΪУ԰��ʵ��������)
	      #�Զ˶������� / ����У԰����
	      #�Զ˶��Ŵ��� / ����У԰����
	      set sql_buf "drop TABLE bass2.xysc_school_latency_userB_3_tmp2"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_latency_userB_3_tmp2(
			        user_id			          varchar(20),
				      product_no		        varchar(15),
				      school_name		        varchar(128),
				      opp_sms_user_num	    integer,
				      opp_sms_xyuser_num	  integer,
				      opp_sms_cnt		        integer,
				      opp_sms_xy_cnt	      integer
        )
        PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
        "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "insert into xysc_school_latency_userB_3_tmp2 (
	             user_id
               ,product_no
               ,school_name
               ,opp_sms_user_num
               ,opp_sms_xyuser_num
               ,opp_sms_cnt
               ,opp_sms_xy_cnt
	             )
           select distinct
				      a.user_id,
				      a.product_no,
				      a.school_name,
				      value(a.opp_sms_user_num,0),
				      value(b.opp_sms_xyuser_num,0),
				      value(a.opp_sms_cnt,0),
				      value(b.opp_sms_xy_cnt,0)
			     from (select user_id,product_no,school_name,
			  		        count(distinct opposite_no) as opp_sms_user_num,
			  		        sum(sms_counts) as opp_sms_cnt
			  	       from $xysc_school_latency_userB_2_sms_yyyymm
			  	       where opposite_no not like '10086%'
			  	       group by user_id,product_no,school_name
			  	      )a left outer join
			  	     (select a.user_id,a.product_no,b.school_name,
			  		       count(distinct a.opposite_no) as opp_sms_xyuser_num,
			  		       sum(a.sms_counts) as opp_sms_xy_cnt
			  	      from $xysc_school_latency_userB_2_sms_yyyymm a inner join
			  		        (select distinct product_no,school_name from xysc_school_latency_userB_tmp) b
			  	          on a.opposite_no=b.product_no and a.school_name=b.school_name and a.opposite_no not like '10086%'
			  	      group by a.user_id,a.product_no,b.school_name
			  	     )b on a.user_id=b.user_id and a.product_no=b.product_no and a.school_name=b.school_name
	      "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      #3.�½���ʱ��(xysc_school_latency_userB_3_tmp3),���ܶԶ�ͨ���Ͷ���
	      set sql_buf "drop TABLE bass2.xysc_school_latency_userB_3_tmp3"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_latency_userB_3_tmp3(
			        user_id			          varchar(20),
				      product_no		        varchar(15),
				      school_name		        varchar(128),
				      opp_call_user_num	    integer,
				      opp_call_xyuser_num	  integer,
				      opp_call_cnt		      integer,
				      opp_call_xy_cnt	      integer,
				      opp_call_duration	    integer,
				      opp_call_xy_duration	integer,
				      opp_sms_user_num	    integer,
				      opp_sms_xyuser_num	  integer,
				      opp_sms_cnt		        integer,
				      opp_sms_xy_cnt	      integer
        )
        PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
        "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "insert into xysc_school_latency_userB_3_tmp3 (
	             user_id
              ,product_no
              ,school_name
              ,opp_call_user_num
              ,opp_call_xyuser_num
              ,opp_call_cnt
              ,opp_call_xy_cnt
              ,opp_call_duration
              ,opp_call_xy_duration
              ,opp_sms_user_num
              ,opp_sms_xyuser_num
              ,opp_sms_cnt
              ,opp_sms_xy_cnt
	            )
           select a.user_id,a.product_no,a.school_name,
				      sum(a.opp_call_user_num),
				      sum(a.opp_call_xyuser_num),
				      sum(a.opp_call_cnt),
				      sum(a.opp_call_xy_cnt),
				      sum(a.opp_call_duration),
				      sum(a.opp_call_xy_duration),
				      sum(a.opp_sms_user_num),
				      sum(a.opp_sms_xyuser_num),
				      sum(a.opp_sms_cnt),
				      sum(a.opp_sms_xy_cnt)
			     from (select user_id,product_no,school_name,
			  		        opp_call_user_num,opp_call_xyuser_num,opp_call_cnt,opp_call_xy_cnt,opp_call_duration,opp_call_xy_duration,
			  		        0 as opp_sms_user_num,0 as opp_sms_xyuser_num,0 as opp_sms_cnt,0 as opp_sms_xy_cnt
			  	       from xysc_school_latency_userB_3_tmp union all
			  	       select user_id,product_no,school_name,
			  		        0 as opp_call_user_num,0 as opp_call_xyuser_num,0 as opp_call_cnt,0 as opp_call_xy_cnt,0 as opp_call_duration,0 as opp_call_xy_duration,
			  		        opp_sms_user_num,opp_sms_xyuser_num,opp_sms_cnt,opp_sms_xy_cnt
			  	       from xysc_school_latency_userB_3_tmp2
			  	      )a
			  	 group by a.user_id,a.product_no,a.school_name
	      "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      #4.�½���ʱ��(xysc_school_latency_userB_3_tmp4),�Զ�ͨ���Ͷ���ռ�ȴ���
	      #�ߵ���ʵ��������
	      set sql_buf "drop TABLE bass2.xysc_school_latency_userB_3_tmp4"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_latency_userB_3_tmp4(
			        user_id			                varchar(20),
				      product_no		              varchar(15),
				      school_name		              varchar(128),
				      opp_call_user_num	          integer,
				      opp_call_xyuser_num	        integer,
				      opp_call_xyuser_num_rate	  decimal(12,2),
				      opp_call_cnt		            integer,
				      opp_call_xy_cnt	            integer,
				      opp_call_xy_cnt_rate	      decimal(12,2),
				      opp_call_duration	          integer,
				      opp_call_xy_duration	      integer,
				      opp_call_xy_duration_rate	  decimal(12,2),
				      opp_sms_user_num	          integer,
				      opp_sms_xyuser_num	        integer,
				      opp_sms_xyuser_num_rate	    decimal(12,2),
				      opp_sms_cnt		              integer,
				      opp_sms_xy_cnt	            integer,
				      opp_sms_xy_cnt_rate	        decimal(12,2)
        )
        PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
        "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "insert into xysc_school_latency_userB_3_tmp4 (
	             user_id
              ,product_no
              ,school_name
              ,opp_call_user_num
              ,opp_call_xyuser_num
              ,opp_call_xyuser_num_rate
              ,opp_call_cnt
              ,opp_call_xy_cnt
              ,opp_call_xy_cnt_rate
              ,opp_call_duration
              ,opp_call_xy_duration
              ,opp_call_xy_duration_rate
              ,opp_sms_user_num
              ,opp_sms_xyuser_num
              ,opp_sms_xyuser_num_rate
              ,opp_sms_cnt
              ,opp_sms_xy_cnt
              ,opp_sms_xy_cnt_rate
	             )
           select distinct a.user_id,a.product_no,a.school_name,
			     	  opp_call_user_num,opp_call_xyuser_num,
			     	  case when opp_call_user_num=0 then 0.00 else round(decimal(opp_call_xyuser_num,12,2)/decimal(opp_call_user_num,12,2)*100.0,2) end as opp_call_xyuser_num_rate,
			     	  opp_call_cnt,opp_call_xy_cnt,
			     	  case when opp_call_cnt=0 then 0.00 else round(decimal(opp_call_xy_cnt,12,2)/decimal(opp_call_cnt,12,2)*100.0,2) end as opp_call_xy_cnt_rate,
			     	  opp_call_duration,opp_call_xy_duration,
			     	  case when opp_call_duration=0 then 0.00 else round(decimal(opp_call_xy_duration,12,2)/decimal(opp_call_duration,12,2)*100.0,2) end as opp_call_xy_duration_rate,
			     	  opp_sms_user_num,opp_sms_xyuser_num,
			     	  case when opp_sms_user_num=0 then 0.00 else round(decimal(opp_sms_xyuser_num,12,2)/decimal(opp_sms_user_num,12,2)*100.0,2) end as opp_sms_xyuser_num_rate,
			     	  opp_sms_cnt,opp_sms_xy_cnt,
			     	  case when opp_sms_cnt=0 then 0.00 else round(decimal(opp_sms_xy_cnt,12,2)/decimal(opp_sms_cnt,12,2)*100.0,2) end as opp_sms_xy_cnt_rate
			     from xysc_school_latency_userB_3_tmp3 a left outer join
			     	    xysc_school_latency_userB_tmp b on a.product_no=b.product_no
			     where b.product_no is null
	      "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      #5.ѭ����ֹ��ʶ����-�ߵ�IP���к�TD����
	      set sql_buf "select count(distinct a.product_no)
			      from xysc_school_latency_userB_3_tmp4 a,$dw_product_yyyymm b
			      where a.product_no=b.product_no and b.month_call_mark=1 and b.userstatus_id in ($rep_online_userstatus_id)
			           and b.plan_id not in (select extend_id from $dim_prod_up_product_item where (name like '%IP����%' or name like '%TD�̻�%') and item_type='OFFER_PLAN')
			           and a.school_name is not null
			           and (a.opp_call_xy_cnt_rate>=40.00
			              	or
			              	a.opp_call_xyuser_num_rate>=20.00
			              	or
			              	a.opp_call_xyuser_num>=4
			              	or
			              	a.opp_call_xy_cnt>=50
			              	or
			              	a.opp_sms_xy_cnt_rate>=30.00
			              	or
			              	a.opp_sms_xyuser_num_rate>=15.00
			              	or
			              	a.opp_sms_xyuser_num>=4
			              	or
			              	a.opp_sms_xy_cnt>=30
			               )
	         "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
		    while {[set this_row [aidb_fetch $handle]] != ""} {
		       set	    table_count         [lindex $this_row 0]
		    }
	      aidb_close $handle
	      if [catch {set handle [aidb_open $conn]} errmsg] {
	      	trace_sql $errmsg 1302
	      	return -1
	      }
	      if { $table_count==0 } {
        		set flag 0
        }
        #6.����ʶ������ʵУ԰�ͻ�(����ʵ��������)��xysc_school_latency_userB_tmp
	      set sql_buf "insert into xysc_school_latency_userB_tmp
	          select distinct a.user_id,a.product_no,a.school_name
			      from xysc_school_latency_userB_3_tmp4 a,$dw_product_yyyymm b
			      where a.product_no=b.product_no and b.month_call_mark=1 and b.userstatus_id in ($rep_online_userstatus_id)
			           and b.plan_id not in (select extend_id from $dim_prod_up_product_item where (name like '%IP����%' or name like '%TD�̻�%') and item_type='OFFER_PLAN')
			           and a.school_name is not null
			           and (a.opp_call_xy_cnt_rate>=40.00
			              	or
			              	a.opp_call_xyuser_num_rate>=20.00
			              	or
			              	a.opp_call_xyuser_num>=4
			              	or
			              	a.opp_call_xy_cnt>=50
			              	or
			              	a.opp_sms_xy_cnt_rate>=30.00
			              	or
			              	a.opp_sms_xyuser_num_rate>=15.00
			              	or
			              	a.opp_sms_xyuser_num>=4
			              	or
			              	a.opp_sms_xy_cnt>=30
			               )
	         "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      incr i
		    puts "��������:$i��........ \n"

	   }
	   #7.�½�bass2.dw_xysc_school_real_user_dt_$year$month,������ȫ��ʶ����ʵУ԰�ͻ�
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
	   set sql_buf "insert into dw_xysc_school_real_user_dt_$year$month (
	              user_id,product_no,school_name
	              )
	      select distinct a.user_id,a.product_no,a.school_name
		    from xysc_school_latency_userB_tmp a,$dw_product_yyyymm b
		    where a.product_no=b.product_no and b.month_call_mark=1 and b.userstatus_id in ($rep_online_userstatus_id)
		          and b.plan_id not in (select extend_id from $dim_prod_up_product_item where (name like '%IP����%' or name like '%TD�̻�%') and item_type='OFFER_PLAN')
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }


		 puts "Step4_1.������ʵ����ʶ��-ͨ�����������ŵ�������-End...\n"

	return 0
}

 proc Do_xysc_school_real_user_2 {p_optime} {
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
 	   set    var_rate     "0.3"
 	   set    var_rate2    "0.6"
 	   set    var_rate3    "0.8"

 	   #Դ��
 	   set    xysc_school_latency_userA_4_yyyymm        "$db_schema.xysc_school_latency_userA_4_$year$month"
 	   set    xysc_school_latency_userB_1_call_yyyymm   "$db_schema.xysc_school_latency_userB_1_call_$year$month"
 	   set    xysc_school_latency_userB_2_sms_yyyymm    "$db_schema.xysc_school_latency_userB_2_sms_$year$month"
 	   set    dw_product_yyyymm                         "$db_schema.dw_product_$year$month"
 	   set    dim_xysc_pno_example_info                 "$db_schema.Dim_xysc_pno_example_info"
 	   set    dim_xysc_lac_cell_info                    "$db_schema.Dim_xysc_lac_cell_info"
 	   set    dim_prod_up_product_item                  "$db_schema.dim_prod_up_product_item"

	   #1.�½�Ǳ����ʵУ԰�û���ʱ��(xysc_school_real_user_2_tmp),���ڲ���������֪��
     set sql_buf "drop TABLE bass2.xysc_school_real_user_2_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_real_user_2_tmp (
			   user_id		       varchar(20),
			   product_no	       varchar(15),
			   school_name	     varchar(128)
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

	   #2.�½���ʱ��(xysc_school_real_user_2_tmp2),ѡȡ����(����ĩ)Ǳ��У԰�ͻ�У԰��վͨ��ռ��(xy_call_rate>=0.3),
	   #��У԰��վͨ���ͻ���ѧУ��Ψһ������
     set sql_buf "drop TABLE bass2.xysc_school_real_user_2_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_real_user_2_tmp2 (
			   user_id		       varchar(20),
			   product_no	       varchar(15),
			   xy_call_counts    integer,
			   total_call_counts integer,
			   xy_call_rate	     decimal(10,2),
			   school_name	     varchar(128)
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into bass2.xysc_school_real_user_2_tmp2 (
			   user_id
        ,product_no
        ,xy_call_counts
        ,total_call_counts
        ,xy_call_rate
        ,school_name
        )
        select distinct a.user_id,
                        a.product_no,
                        a.xy_call_counts,
                        a.total_call_counts,
                        a.xy_call_rate,
                        a.school_name
          from $xysc_school_latency_userA_4_yyyymm a
         inner join (select user_id, product_no, count(1)
                       from $xysc_school_latency_userA_4_yyyymm
                      where xy_call_rate >= $var_rate
                      group by user_id, product_no
                     having count(3) = 1) b
            on a.user_id = b.user_id
           and a.product_no = b.product_no
         where a.xy_call_rate >= $var_rate

     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

	   #3.�½���ʱ��(xysc_school_real_user_2_tmp3),ѡȡ����(����ĩ)Ǳ��У԰�ͻ�У԰��վͨ��ռ��(xy_call_rate>=0.6),
	   #��У԰��վͨ���ͻ���ѧУ��Ψһ������
     set sql_buf "drop TABLE bass2.xysc_school_real_user_2_tmp3"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_real_user_2_tmp3 (
			   user_id		       varchar(20),
			   product_no	       varchar(15),
			   xy_call_counts    integer,
			   total_call_counts integer,
			   xy_call_rate	     decimal(10,2),
			   school_name	     varchar(128)
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into bass2.xysc_school_real_user_2_tmp3 (
			   user_id
        ,product_no
        ,xy_call_counts
        ,total_call_counts
        ,xy_call_rate
        ,school_name
        )
        select distinct a.user_id,
                        a.product_no,
                        a.xy_call_counts,
                        a.total_call_counts,
                        a.xy_call_rate,
                        a.school_name
          from $xysc_school_latency_userA_4_yyyymm a
         inner join (select user_id, product_no, count(1)
                       from $xysc_school_latency_userA_4_yyyymm
                      where xy_call_rate >= $var_rate2
                      group by user_id, product_no
                     having count(3) = 1) b
            on a.user_id = b.user_id
           and a.product_no = b.product_no
         where a.xy_call_rate >= $var_rate2

     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #4.������-�Զ˴���
	   #4_1.������-����(�Զ�ͨ��Ϊ����У԰ͨ��ռ�ȴ���30%�еĿͻ���ͨ������ռ�ȴ��ڻ����80%,
	   #    ����ͨ������ռ�ȴ��ڻ����80%,��ͨ������������10��)
	   set sql_buf "insert into bass2.xysc_school_real_user_2_tmp (
			   user_id
        ,product_no
        ,school_name
        )
        select distinct a.user_id, a.product_no, a.school_name
         from (select a.user_id,
                      a.product_no,
                      a.school_name,
                      sum(case
                            when b.product_no is not null then
                             a.CALL_COUNTS
                            else
                             0
                          end) as call1,
                      sum(a.CALL_COUNTS) as call,
                      count(distinct case
                              when b.product_no is not null then
                               a.OPPOSITE_NO
                            end) as calluser1,
                      count(distinct a.OPPOSITE_NO) as calluser
                 from $xysc_school_latency_userB_1_call_yyyymm a
                inner join xysc_school_real_user_2_tmp2 x
                   on a.user_id = x.user_id
                  and a.product_no = x.product_no
                  and a.school_name = x.school_name
                 left join (select distinct product_no, school_name
                             from xysc_school_real_user_2_tmp2) b
                   on a.OPPOSITE_NO = b.product_no
                  and a.school_name = b.school_name
                where a.SCHOOL_NAME in
                      (select distinct school_name
                         from $dim_xysc_lac_cell_info
                        where SCHOOL_TYPE in ('2', '3'))
                group by a.user_id, a.product_no, a.school_name) a
        where (a.call1 * 1.0 / call >= 0.8 or a.calluser1 * 1.0 / calluser >= 0.8)
          and a.calluser1 >= 10

     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #4_2.������-����(�Զ˶���Ϊ����У԰����ռ�ȴ���30%�еĿͻ��Ķ��Ŵ���ռ�ȴ��ڻ����80%,
	   #    ���߶�������ռ�ȴ��ڻ����80%,�Ҷ�������������10��)
	   set sql_buf "insert into bass2.xysc_school_real_user_2_tmp (
			   user_id
        ,product_no
        ,school_name
        )
        select distinct a.user_id, a.product_no, a.school_name
          from (select a.user_id,
                       a.product_no,
                       a.school_name,
                       sum(case
                             when b.product_no is not null then
                              a.sms_COUNTS
                             else
                              0
                           end) as call1,
                       sum(a.sms_COUNTS) as call,
                       count(distinct case
                               when b.product_no is not null then
                                a.OPPOSITE_NO
                             end) as calluser1,
                       count(distinct a.OPPOSITE_NO) as calluser
                  from $xysc_school_latency_userB_2_sms_yyyymm a
                 inner join xysc_school_real_user_2_tmp2 x
                    on a.user_id = x.user_id
                   and a.product_no = x.product_no
                   and a.school_name = x.school_name
                  left join (select distinct product_no, school_name
                              from xysc_school_real_user_2_tmp2) b
                    on a.OPPOSITE_NO = b.product_no
                   and a.school_name = b.school_name
                 where a.SCHOOL_NAME in
                       (select distinct school_name
                          from $dim_xysc_lac_cell_info
                         where SCHOOL_TYPE in ('2', '3'))
                 group by a.user_id, a.product_no, a.school_name) a
         where (a.call1 * 1.0 / call >= 0.8 or a.calluser1 * 1.0 / calluser >= 0.8)
           and a.calluser1 >= 10

     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #5.��ѧ/��ְ-����У԰��վͨ��ռ��>=0.6,����ѧУ����>=2000;����У԰��վͨ��ռ��>=0.6,����ѧУ����<2000
	   #5_1.��ѧ/��ְ-����(>=2000)(�Զ�ͨ��Ϊ����У԰ͨ��ռ�ȴ���30%�еĿͻ���ͨ������ռ�ȴ��ڻ����40%,
	   #    ����ͨ������ռ�ȴ��ڻ����40%,��ͨ������������10�˼�У԰��վͨ��ռ��>=0.6��ѧУ����������2000)
	   set sql_buf "insert into bass2.xysc_school_real_user_2_tmp (
			   user_id
        ,product_no
        ,school_name
        )
        select distinct a.user_id, a.product_no, a.school_name
          from (select a.user_id,
                       a.product_no,
                       a.school_name,
                       sum(case
                             when b.product_no is not null then
                              a.CALL_COUNTS
                             else
                              0
                           end) as call1,
                       sum(a.CALL_COUNTS) as call,
                       count(distinct case
                               when b.product_no is not null then
                                a.OPPOSITE_NO
                             end) as calluser1,
                       count(distinct a.OPPOSITE_NO) as calluser
                  from $xysc_school_latency_userB_1_call_yyyymm a
                 inner join xysc_school_real_user_2_tmp2 x
                    on a.user_id = x.user_id
                   and a.product_no = x.product_no
                   and a.school_name = x.school_name
                  left join (select distinct product_no, school_name
                              from xysc_school_real_user_2_tmp2) b
                    on a.OPPOSITE_NO = b.product_no
                   and a.school_name = b.school_name
                 inner join (select school_name, count(distinct product_no) as cnt
                              from xysc_school_real_user_2_tmp2
                             group by school_name
                            having count(2) >= 2000) d
                    on a.school_name = d.school_name
                 where a.SCHOOL_NAME in
                       (select distinct school_name
                          from Dim_xysc_lac_cell_info
                         where SCHOOL_TYPE in ('1', '4', '5'))
                 group by a.user_id, a.product_no, a.school_name) a
         where (a.call1 * 1.0 / call >= 0.4 or a.calluser1 * 1.0 / calluser >= 0.4)
           and a.calluser1 >= 10
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #5_2.��ѧ/��ְ-����(>=2000)(�Զ˶���Ϊ����У԰ͨ��ռ�ȴ���30%�еĿͻ��Ķ��Ŵ���ռ�ȴ��ڻ����40%,
	   #    ���߶�������ռ�ȴ��ڻ����40%,�Ҷ�������������10�˼�У԰��վͨ��ռ��>=0.3��ѧУ����������2000)
	   set sql_buf "insert into bass2.xysc_school_real_user_2_tmp (
			   user_id
        ,product_no
        ,school_name
        )
        select distinct a.user_id, a.product_no, a.school_name
          from (select a.user_id,
                       a.product_no,
                       a.school_name,
                       sum(case
                             when b.product_no is not null then
                              a.sms_COUNTS
                             else
                              0
                           end) as call1,
                       sum(a.sms_COUNTS) as call,
                       count(distinct case
                               when b.product_no is not null then
                                a.OPPOSITE_NO
                             end) as calluser1,
                       count(distinct a.OPPOSITE_NO) as calluser
                  from $xysc_school_latency_userB_2_sms_yyyymm a
                 inner join xysc_school_real_user_2_tmp2 x
                    on a.user_id = x.user_id
                   and a.product_no = x.product_no
                   and a.school_name = x.school_name
                  left join (select distinct product_no, school_name
                              from xysc_school_real_user_2_tmp2) b
                    on a.OPPOSITE_NO = b.product_no
                   and a.school_name = b.school_name
                 inner join (select school_name, count(distinct product_no) as cnt
                              from xysc_school_real_user_2_tmp2
                             group by school_name
                            having count(2) >= 2000) d
                    on a.school_name = d.school_name
                 where a.SCHOOL_NAME in
                       (select distinct school_name
                          from Dim_xysc_lac_cell_info
                         where SCHOOL_TYPE in ('1', '4', '5'))
                 group by a.user_id, a.product_no, a.school_name) a
         where (a.call1 * 1.0 / call >= 0.4 or a.calluser1 * 1.0 / calluser >= 0.4)
           and a.calluser1 >= 10
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #5_3.��ѧ/��ְ-����(<2000)(�Զ�ͨ��Ϊ����У԰ͨ��ռ�ȴ���60%�еĿͻ���ͨ������ռ�ȴ��ڻ����40%,
	   #    ����ͨ������ռ�ȴ��ڻ����40%,��ͨ������������5�˼�У԰��վͨ��ռ��>=0.6��ѧУ��������2000)
	   set sql_buf "insert into bass2.xysc_school_real_user_2_tmp (
			   user_id
        ,product_no
        ,school_name
        )
        select distinct a.user_id, a.product_no, a.school_name
          from (select a.user_id,
                       a.product_no,
                       a.school_name,
                       sum(case
                             when b.product_no is not null then
                              a.CALL_COUNTS
                             else
                              0
                           end) as call1,
                       sum(a.CALL_COUNTS) as call,
                       count(distinct case
                               when b.product_no is not null then
                                a.OPPOSITE_NO
                             end) as calluser1,
                       count(distinct a.OPPOSITE_NO) as calluser
                  from $xysc_school_latency_userB_1_call_yyyymm a
                 inner join xysc_school_real_user_2_tmp3 x
                    on a.user_id = x.user_id
                   and a.product_no = x.product_no
                   and a.school_name = x.school_name
                  left join (select distinct product_no, school_name
                              from xysc_school_real_user_2_tmp3) b
                    on a.OPPOSITE_NO = b.product_no
                   and a.school_name = b.school_name
                 inner join (select school_name, count(distinct product_no) as cnt
                              from xysc_school_real_user_2_tmp3
                             group by school_name
                            having count(2) < 2000) d
                    on a.school_name = d.school_name
                 where a.SCHOOL_NAME in
                       (select distinct school_name
                          from Dim_xysc_lac_cell_info
                         where SCHOOL_TYPE in ('1', '4', '5'))
                 group by a.user_id, a.product_no, a.school_name) a
         where (a.call1 * 1.0 / call >= 0.4 or a.calluser1 * 1.0 / calluser >= 0.4)
           and a.calluser1 >= 5
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #5_4.��ѧ/��ְ-����(<2000)(�Զ˶���Ϊ����У԰ͨ��ռ�ȴ���60%�еĿͻ��Ķ��Ŵ���ռ�ȴ��ڻ����80%,
	   #    ���߶�������ռ�ȴ��ڻ����80%,�Ҷ�������������15�˼�У԰��վͨ��ռ��>=0.6��ѧУ��������2000)
	   set sql_buf "insert into bass2.xysc_school_real_user_2_tmp (
			   user_id
        ,product_no
        ,school_name
        )
        select distinct a.user_id, a.product_no, a.school_name
          from (select a.user_id,
                       a.product_no,
                       a.school_name,
                       sum(case
                             when b.product_no is not null then
                              a.sms_COUNTS
                             else
                              0
                           end) as call1,
                       sum(a.sms_COUNTS) as call,
                       count(distinct case
                               when b.product_no is not null then
                                a.OPPOSITE_NO
                             end) as calluser1,
                       count(distinct a.OPPOSITE_NO) as calluser
                  from $xysc_school_latency_userB_2_sms_yyyymm a
                 inner join xysc_school_real_user_2_tmp3 x
                    on a.user_id = x.user_id
                   and a.product_no = x.product_no
                   and a.school_name = x.school_name
                  left join (select distinct product_no, school_name
                              from xysc_school_real_user_2_tmp3) b
                    on a.OPPOSITE_NO = b.product_no
                   and a.school_name = b.school_name
                 inner join (select school_name, count(distinct product_no) as cnt
                              from xysc_school_real_user_2_tmp3
                             group by school_name
                            having count(2) < 2000) d
                    on a.school_name = d.school_name
                 where a.SCHOOL_NAME in
                       (select distinct school_name
                          from Dim_xysc_lac_cell_info
                         where SCHOOL_TYPE in ('1', '4', '5'))
                 group by a.user_id, a.product_no, a.school_name) a
         where (a.call1 * 1.0 / call >= 0.8 or a.calluser1 * 1.0 / calluser >= 0.8)
           and a.calluser1 >= 15
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #6.������ʵ����������Ϣ,�Բ�����֪��
	   #��ʵ���������赱��У԰��վͨ��ռ�ȴ���0
     set sql_buf "drop TABLE bass2.xysc_school_real_user_2_tmp4"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_real_user_2_tmp4 (
			   user_id		       varchar(20),
			   product_no	       varchar(15),
			   school_name	     varchar(128)
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into bass2.xysc_school_real_user_2_tmp4 (
			   user_id
        ,product_no
        ,school_name
        )
        select distinct a.user_id, a.product_no, a.school_name
          from $dim_xysc_pno_example_info a
         inner join (select distinct user_id, product_no, school_name
                       from $xysc_school_latency_userA_4_yyyymm
                      where xy_call_rate > 0) b
            on a.user_id = b.user_id
           and a.product_no = b.product_no
           and a.school_name = b.school_name
         where a.cmcc_active_flag = 1
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
     set sql_buf "drop TABLE bass2.xysc_school_real_user_2_tmp5"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_real_user_2_tmp5 (
			   user_id		       varchar(20),
			   product_no	       varchar(15),
			   school_name	     varchar(128)
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into bass2.xysc_school_real_user_2_tmp5 (
			   user_id
        ,product_no
        ,school_name
        )
        select distinct a.user_id, a.product_no, a.school_name
          from (select distinct user_id, product_no, school_name
                  from xysc_school_real_user_2_tmp4
                union all
                select distinct user_id, product_no, school_name
                  from xysc_school_real_user_2_tmp) a
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #7.������ʵ���������,��ѧУ����֪������(����ְ����20��,��ѧ����30��)
	   #��ȡУ԰ͨ���û�������10��,�Ҵ�ѧ/��ְУ԰ͨ��ռ�Ȳ�С��0.6(���߳�����У԰ͨ��ռ�Ȳ�С��0.3)
	   #����������ŵĴ���(��������)ռ�ȴ��ڵ���50%,����ռ������
	   #��ѧ/��ְ-ȡǰ50��,������-ȡǰ30��
     set sql_buf "drop TABLE bass2.xysc_school_real_user_2_tmp6"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_real_user_2_tmp6 (
			   user_id		       varchar(20),
			   product_no	       varchar(15),
			   school_name	     varchar(128),
			   rate1		         decimal(12,2),
			   rate2		         decimal(12,2),
			   school_type	     varchar(8)
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #7_1.��ѧ/��ְ-����
	   set sql_buf "insert into bass2.xysc_school_real_user_2_tmp6 (
			   user_id
        ,product_no
        ,school_name
        ,rate1
        ,rate2
        ,school_type
        )
        select distinct a.user_id,
                        a.product_no,
                        a.school_name,
                        a.rate1,
                        a.rate2,
                        '1'
          from (select distinct a.user_id,
                                a.product_no,
                                a.school_name,
                                call1 * 1.0 / call as rate1,
                                calluser1 * 1.0 / calluser as rate2,
                                calluser1
                  from (select a.user_id,
                               a.product_no,
                               a.school_name,
                               sum(case
                                     when b.product_no is not null then
                                      a.CALL_COUNTS
                                     else
                                      0
                                   end) as call1,
                               sum(a.CALL_COUNTS) as call,
                               count(distinct case
                                       when b.product_no is not null then
                                        a.OPPOSITE_NO
                                     end) as calluser1,
                               count(distinct a.OPPOSITE_NO) as calluser
                          from $xysc_school_latency_userB_1_call_yyyymm as a
                         inner join (select distinct product_no, school_name
                                      from $xysc_school_latency_userA_4_yyyymm
                                     where xy_call_rate >= 0.6) as c
                            on a.product_no = c.product_no
                           and a.school_name = c.school_name
                          left outer join (select distinct product_no, school_name
                                            from $xysc_school_latency_userA_4_yyyymm
                                           where xy_call_rate >= 0.6) as b
                            on a.OPPOSITE_NO = b.product_no
                           and a.school_name = b.school_name
                         where a.SCHOOL_NAME in
                               (select distinct school_name
                                  from $dim_xysc_lac_cell_info
                                 where SCHOOL_TYPE in ('1', '4', '5'))
                           and a.SCHOOL_NAME in
                               (select distinct school_name
                                  from (select school_name, count(1)
                                          from xysc_school_real_user_2_tmp5
                                         group by school_name
                                        having count(2) < 30) a)
                         group by a.user_id, a.product_no, a.school_name) as a) as a
          left outer join xysc_school_real_user_2_tmp5 b
            on a.product_no = b.product_no
           and a.school_name = b.school_name
         where b.product_no is null
           and (a.rate1 >= 0.5 or a.rate2 >= 0.5)
           and a.calluser1 >= 10
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #7_2.��ѧ/��ְ-����
	   set sql_buf "insert into bass2.xysc_school_real_user_2_tmp6 (
			   user_id
        ,product_no
        ,school_name
        ,rate1
        ,rate2
        ,school_type
        )
        select distinct a.user_id,
                        a.product_no,
                        a.school_name,
                        a.rate1,
                        a.rate2,
                        '1'
          from (select distinct a.user_id,
                                a.product_no,
                                a.school_name,
                                call1 * 1.0 / call as rate1,
                                calluser1 * 1.0 / calluser as rate2,
                                calluser1
                  from (select a.user_id,
                               a.product_no,
                               a.school_name,
                               sum(case
                                     when b.product_no is not null then
                                      a.SMS_COUNTS
                                     else
                                      0
                                   end) as call1,
                               sum(a.SMS_COUNTS) as call,
                               count(distinct case
                                       when b.product_no is not null then
                                        a.OPPOSITE_NO
                                     end) as calluser1,
                               count(distinct a.OPPOSITE_NO) as calluser
                          from $xysc_school_latency_userB_2_sms_yyyymm as a
                         inner join (select distinct product_no, school_name
                                      from $xysc_school_latency_userA_4_yyyymm
                                     where xy_call_rate >= 0.6) as c
                            on a.product_no = c.product_no
                           and a.school_name = c.school_name
                          left outer join (select distinct product_no, school_name
                                            from $xysc_school_latency_userA_4_yyyymm
                                           where xy_call_rate >= 0.6) as b
                            on a.OPPOSITE_NO = b.product_no
                           and a.school_name = b.school_name
                         where a.SCHOOL_NAME in
                               (select distinct school_name
                                  from $dim_xysc_lac_cell_info
                                 where SCHOOL_TYPE in ('1', '4', '5'))
                           and a.SCHOOL_NAME in
                               (select distinct school_name
                                  from (select school_name, count(1)
                                          from xysc_school_real_user_2_tmp5
                                         group by school_name
                                        having count(2) < 30) a)
                         group by a.user_id, a.product_no, a.school_name) as a) as a
          left outer join xysc_school_real_user_2_tmp5 b
            on a.product_no = b.product_no
           and a.school_name = b.school_name
         where b.product_no is null
           and (a.rate1 >= 0.5 or a.rate2 >= 0.5)
           and a.calluser1 >= 10
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #7_3.������-����
	   set sql_buf "insert into bass2.xysc_school_real_user_2_tmp6 (
			   user_id
        ,product_no
        ,school_name
        ,rate1
        ,rate2
        ,school_type
        )
        select distinct a.user_id,
                        a.product_no,
                        a.school_name,
                        a.rate1,
                        a.rate2,
                        '2'
          from (select distinct a.user_id,
                                a.product_no,
                                a.school_name,
                                call1 * 1.0 / call as rate1,
                                calluser1 * 1.0 / calluser as rate2,
                                calluser1
                  from (select a.user_id,
                               a.product_no,
                               a.school_name,
                               sum(case
                                     when b.product_no is not null then
                                      a.CALL_COUNTS
                                     else
                                      0
                                   end) as call1,
                               sum(a.CALL_COUNTS) as call,
                               count(distinct case
                                       when b.product_no is not null then
                                        a.OPPOSITE_NO
                                     end) as calluser1,
                               count(distinct a.OPPOSITE_NO) as calluser
                          from $xysc_school_latency_userB_1_call_yyyymm as a
                         inner join (select distinct product_no, school_name
                                      from $xysc_school_latency_userA_4_yyyymm
                                     where xy_call_rate >= 0.3) as c
                            on a.product_no = c.product_no
                           and a.school_name = c.school_name
                          left outer join (select distinct product_no, school_name
                                            from $xysc_school_latency_userA_4_yyyymm
                                           where xy_call_rate >= 0.3) as b
                            on a.OPPOSITE_NO = b.product_no
                           and a.school_name = b.school_name
                         where a.SCHOOL_NAME in
                               (select distinct school_name
                                  from $dim_xysc_lac_cell_info
                                 where SCHOOL_TYPE in ('2', '3'))
                           and a.SCHOOL_NAME in
                               (select distinct school_name
                                  from (select school_name, count(1)
                                          from xysc_school_real_user_2_tmp5
                                         group by school_name
                                        having count(2) < 20) a)
                         group by a.user_id, a.product_no, a.school_name) as a) as a
          left outer join xysc_school_real_user_2_tmp5 b
            on a.product_no = b.product_no
           and a.school_name = b.school_name
         where b.product_no is null
           and (a.rate1 >= 0.5 or a.rate2 >= 0.5)
           and a.calluser1 >= 10
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #7_4.��ѧ/��ְ-����
	   set sql_buf "insert into bass2.xysc_school_real_user_2_tmp6 (
			   user_id
        ,product_no
        ,school_name
        ,rate1
        ,rate2
        ,school_type
        )
        select distinct a.user_id,
                        a.product_no,
                        a.school_name,
                        a.rate1,
                        a.rate2,
                        '2'
          from (select distinct a.user_id,
                                a.product_no,
                                a.school_name,
                                call1 * 1.0 / call as rate1,
                                calluser1 * 1.0 / calluser as rate2,
                                calluser1
                  from (select a.user_id,
                               a.product_no,
                               a.school_name,
                               sum(case
                                     when b.product_no is not null then
                                      a.SMS_COUNTS
                                     else
                                      0
                                   end) as call1,
                               sum(a.SMS_COUNTS) as call,
                               count(distinct case
                                       when b.product_no is not null then
                                        a.OPPOSITE_NO
                                     end) as calluser1,
                               count(distinct a.OPPOSITE_NO) as calluser
                          from $xysc_school_latency_userB_2_sms_yyyymm as a
                         inner join (select distinct product_no, school_name
                                      from $xysc_school_latency_userA_4_yyyymm
                                     where xy_call_rate >= 0.3) as c
                            on a.product_no = c.product_no
                           and a.school_name = c.school_name
                          left outer join (select distinct product_no, school_name
                                            from $xysc_school_latency_userA_4_yyyymm
                                           where xy_call_rate >= 0.3) as b
                            on a.OPPOSITE_NO = b.product_no
                           and a.school_name = b.school_name
                         where a.SCHOOL_NAME in
                               (select distinct school_name
                                  from $dim_xysc_lac_cell_info
                                 where SCHOOL_TYPE in ('2', '3'))
                           and a.SCHOOL_NAME in
                               (select distinct school_name
                                  from (select school_name, count(1)
                                          from xysc_school_real_user_2_tmp5
                                         group by school_name
                                        having count(2) < 20) a)
                         group by a.user_id, a.product_no, a.school_name) as a) as a
          left outer join xysc_school_real_user_2_tmp5 b
            on a.product_no = b.product_no
           and a.school_name = b.school_name
         where b.product_no is null
           and (a.rate1 >= 0.5 or a.rate2 >= 0.5)
           and a.calluser1 >= 10
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #8.ѡȡռ����������
     set sql_buf "drop TABLE bass2.xysc_school_real_user_2_tmp7"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_real_user_2_tmp7 (
			   user_id		       varchar(20),
			   product_no	       varchar(15),
			   school_name	     varchar(128)
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into bass2.xysc_school_real_user_2_tmp7 (
			   user_id
        ,product_no
        ,school_name
        )
        select distinct a.user_id, a.product_no, a.school_name
          from (select user_id,
                       product_no,
                       school_name,
                       rate1,
                       rate2,
                       school_type,
                       row_number() over(partition by school_name order by rate1 desc, rate2 desc) sn
                  from (select distinct user_id,
                                        product_no,
                                        school_name,
                                        rate1,
                                        rate2,
                                        school_type
                          from xysc_school_real_user_2_tmp6) a) a
         where (a.school_type = '1' and a.sn <= 50)
            or (a.school_type = '2' and a.sn <= 30)
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

	   #9.�½���ʱ��(xysc_school_real_user_2_tmp8),�ϲ�Ǳ��У԰�ͻ���,����ѡȡ��ʵУ԰�ͻ�
     set sql_buf "drop TABLE bass2.xysc_school_real_user_2_tmp8"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_real_user_2_tmp8 (
			         user_id		  varchar(20),
			         product_no	  varchar(20),
			         school_name	varchar(128),
			         rank		      integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_real_user_2_tmp8
          (user_id, product_no, school_name, rank)
          select distinct user_id, product_no, school_name, 0
            from (select user_id, product_no, school_name
                    from xysc_school_real_user_2_tmp5
                  union all
                  select user_id, product_no, school_name
                    from xysc_school_real_user_2_tmp7) a
		    "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #10.��������
	   set flag 1
	   set i    0
	   while { $flag>0 } {
	      #10_1.�Զ�ͨ��
	      #�Զ�ͨ������ / ����У԰����
	      #�Զ�ͨ������ / ����У԰����
	      #�Զ�ͨ��ʱ�� / ����У԰ʱ��
	      set sql_buf "drop TABLE bass2.xysc_school_real_user_2_tmp9"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_real_user_2_tmp9(
			        user_id			          varchar(20),
				      product_no		        varchar(15),
				      school_name		        varchar(128),
				      opp_call_user_num	    integer,
				      opp_call_xyuser_num	  integer,
				      opp_call_cnt		      integer,
				      opp_call_xy_cnt	      integer,
				      opp_call_duration	    integer,
				      opp_call_xy_duration	integer
        )
        PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
        "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "insert into xysc_school_real_user_2_tmp9 (
	             user_id
              ,product_no
              ,school_name
              ,opp_call_user_num
              ,opp_call_xyuser_num
              ,opp_call_cnt
              ,opp_call_xy_cnt
              ,opp_call_duration
              ,opp_call_xy_duration
	              )
               select distinct a.user_id,
                               a.product_no,
                               a.school_name,
                               value(a.opp_call_user_num, 0),
                               value(b.opp_call_xyuser_num, 0),
                               value(a.opp_call_cnt, 0),
                               value(b.opp_call_xy_cnt, 0),
                               value(a.opp_call_duration, 0),
                               value(b.opp_call_xy_duration, 0)
                 from (select user_id,
                              product_no,
                              school_name,
                              count(distinct opposite_no) as opp_call_user_num,
                              sum(CALL_COUNTS) as opp_call_cnt,
                              sum(CALL_DURATION_M) as opp_call_duration
                         from $xysc_school_latency_userB_1_call_yyyymm
                        where opposite_no not like '10086%'
                        group by user_id, product_no, school_name) a
                 left outer join (select a.user_id,
                                         a.product_no,
                                         b.school_name,
                                         count(distinct a.opposite_no) as opp_call_xyuser_num,
                                         sum(a.CALL_COUNTS) as opp_call_xy_cnt,
                                         sum(a.CALL_DURATION_M) as opp_call_xy_duration
                                    from $xysc_school_latency_userB_1_call_yyyymm a
                                   inner join (select distinct product_no, school_name
                                                from xysc_school_real_user_2_tmp8) b
                                      on a.OPPOSITE_NO = b.product_no
                                     and a.school_name = b.school_name
                                     and a.opposite_no not like '10086%'
                                   group by a.user_id, a.product_no, b.school_name) b
                   on a.user_id = b.user_id
                  and a.product_no = b.product_no
                  and a.school_name = b.school_name
	      "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      #10_2.�Զ˶���
	      #�Զ˶������� / ����У԰����
	      #�Զ˶��Ŵ��� / ����У԰����
	      set sql_buf "drop TABLE bass2.xysc_school_real_user_2_tmp10"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_real_user_2_tmp10(
			        user_id			          varchar(20),
				      product_no		        varchar(15),
				      school_name		        varchar(128),
				      opp_sms_user_num	    integer,
				      opp_sms_xyuser_num	  integer,
				      opp_sms_cnt		        integer,
				      opp_sms_xy_cnt	      integer
        )
        PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
        "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "insert into xysc_school_real_user_2_tmp10 (
	             user_id
              ,product_no
              ,school_name
              ,opp_sms_user_num
              ,opp_sms_xyuser_num
              ,opp_sms_cnt
              ,opp_sms_xy_cnt
	              )
               select distinct a.user_id,
                               a.product_no,
                               a.school_name,
                               value(a.opp_sms_user_num, 0),
                               value(b.opp_sms_xyuser_num, 0),
                               value(a.opp_sms_cnt, 0),
                               value(b.opp_sms_xy_cnt, 0)
                 from (select user_id,
                              product_no,
                              school_name,
                              count(distinct opposite_no) as opp_sms_user_num,
                              sum(sms_COUNTS) as opp_sms_cnt
                         from $xysc_school_latency_userB_2_sms_yyyymm
                        where opposite_no not like '10086%'
                        group by user_id, product_no, school_name) a
                 left outer join (select a.user_id,
                                         a.product_no,
                                         b.school_name,
                                         count(distinct a.opposite_no) as opp_sms_xyuser_num,
                                         sum(a.sms_COUNTS) as opp_sms_xy_cnt
                                    from $xysc_school_latency_userB_2_sms_yyyymm a
                                   inner join (select distinct product_no, school_name
                                                from xysc_school_real_user_2_tmp8) b
                                      on a.OPPOSITE_NO = b.product_no
                                     and a.school_name = b.school_name
                                     and a.opposite_no not like '10086%'
                                   group by a.user_id, a.product_no, b.school_name) b
                   on a.user_id = b.user_id
                  and a.product_no = b.product_no
                  and a.school_name = b.school_name
	      "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      #10_3.�½���ʱ��(xysc_school_real_user_2_tmp11),��������(����+����)
	      set sql_buf "drop TABLE bass2.xysc_school_real_user_2_tmp11"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_real_user_2_tmp11(
			        user_id			          varchar(20),
				      product_no		        varchar(15),
				      school_name		        varchar(128),
				      opp_call_user_num	    integer,
				      opp_call_xyuser_num	  integer,
				      opp_call_cnt		      integer,
				      opp_call_xy_cnt	      integer,
				      opp_call_duration	    integer,
				      opp_call_xy_duration	integer,
				      opp_sms_user_num	    integer,
				      opp_sms_xyuser_num	  integer,
				      opp_sms_cnt		        integer,
				      opp_sms_xy_cnt	      integer
        )
        PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
        "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "
	          insert into bass2.xysc_school_real_user_2_tmp11
              select a.user_id,
                     a.product_no,
                     a.school_name,
                     sum(a.opp_call_user_num),
                     sum(a.opp_call_xyuser_num),
                     sum(a.opp_call_cnt),
                     sum(a.opp_call_xy_cnt),
                     sum(a.opp_call_duration),
                     sum(a.opp_call_xy_duration),
                     sum(a.opp_sms_user_num),
                     sum(a.opp_sms_xyuser_num),
                     sum(a.opp_sms_cnt),
                     sum(a.opp_sms_xy_cnt)
                from (select user_id,
                             product_no,
                             school_name,
                             opp_call_user_num,
                             opp_call_xyuser_num,
                             opp_call_cnt,
                             opp_call_xy_cnt,
                             opp_call_duration,
                             opp_call_xy_duration,
                             0                    as opp_sms_user_num,
                             0                    as opp_sms_xyuser_num,
                             0                    as opp_sms_cnt,
                             0                    as opp_sms_xy_cnt
                        from xysc_school_real_user_2_tmp9
                      union all
                      select user_id,
                             product_no,
                             school_name,
                             0                  as opp_call_user_num,
                             0                  as opp_call_xyuser_num,
                             0                  as opp_call_cnt,
                             0                  as opp_call_xy_cnt,
                             0                  as opp_call_duration,
                             0                  as opp_call_xy_duration,
                             opp_sms_user_num,
                             opp_sms_xyuser_num,
                             opp_sms_cnt,
                             opp_sms_xy_cnt
                        from xysc_school_real_user_2_tmp10) a
               group by a.user_id, a.product_no, a.school_name
	         "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      }
	      #10_4.�½���ʱ��(xysc_school_real_user_2_tmp12),��������(����+����)ռ�ȴ���
	      set sql_buf "drop TABLE bass2.xysc_school_real_user_2_tmp12"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_real_user_2_tmp12(
			        user_id			                varchar(20),
				      product_no		              varchar(15),
				      school_name		              varchar(128),
				      opp_call_user_num	          integer,
				      opp_call_xyuser_num	        integer,
				      opp_call_xyuser_num_rate	  decimal(12,2),
				      opp_call_cnt		            integer,
				      opp_call_xy_cnt	            integer,
				      opp_call_xy_cnt_rate	      decimal(12,2),
				      opp_call_duration	          integer,
				      opp_call_xy_duration	      integer,
				      opp_call_xy_duration_rate	  decimal(12,2),
				      opp_sms_user_num	          integer,
				      opp_sms_xyuser_num	        integer,
				      opp_sms_xyuser_num_rate	    decimal(12,2),
				      opp_sms_cnt		              integer,
				      opp_sms_xy_cnt	            integer,
				      opp_sms_xy_cnt_rate	        decimal(12,2)
        )
        PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
        "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "insert into xysc_school_real_user_2_tmp12 (
	             user_id
              ,product_no
              ,school_name
              ,opp_call_user_num
              ,opp_call_xyuser_num
              ,opp_call_xyuser_num_rate
              ,opp_call_cnt
              ,opp_call_xy_cnt
              ,opp_call_xy_cnt_rate
              ,opp_call_duration
              ,opp_call_xy_duration
              ,opp_call_xy_duration_rate
              ,opp_sms_user_num
              ,opp_sms_xyuser_num
              ,opp_sms_xyuser_num_rate
              ,opp_sms_cnt
              ,opp_sms_xy_cnt
              ,opp_sms_xy_cnt_rate
	             )
              select distinct a.user_id,
                              a.product_no,
                              a.school_name,
                              opp_call_user_num,
                              opp_call_xyuser_num,
                              case
                                when opp_call_user_num = 0 then
                                 0.00
                                else
                                 round(decimal(opp_call_xyuser_num, 12, 2) /
                                       decimal(opp_call_user_num, 12, 2) * 100.0,
                                       2)
                              end as opp_call_xyuser_num_rate,
                              opp_call_cnt,
                              opp_call_xy_cnt,
                              case
                                when opp_call_cnt = 0 then
                                 0.00
                                else
                                 round(decimal(opp_call_xy_cnt, 12, 2) /
                                       decimal(opp_call_cnt, 12, 2) * 100.0,
                                       2)
                              end as opp_call_xy_cnt_rate,
                              opp_call_duration,
                              opp_call_xy_duration,
                              case
                                when opp_call_duration = 0 then
                                 0.00
                                else
                                 round(decimal(opp_call_xy_duration, 12, 2) /
                                       decimal(opp_call_duration, 12, 2) * 100.0,
                                       2)
                              end as opp_call_xy_duration_rate,
                              opp_sms_user_num,
                              opp_sms_xyuser_num,
                              case
                                when opp_sms_user_num = 0 then
                                 0.00
                                else
                                 round(decimal(opp_sms_xyuser_num, 12, 2) /
                                       decimal(opp_sms_user_num, 12, 2) * 100.0,
                                       2)
                              end as opp_sms_xyuser_num_rate,
                              opp_sms_cnt,
                              opp_sms_xy_cnt,
                              case
                                when opp_sms_cnt = 0 then
                                 0.00
                                else
                                 round(decimal(opp_sms_xy_cnt, 12, 2) /
                                       decimal(opp_sms_cnt, 12, 2) * 100.0,
                                       2)
                              end as opp_sms_xy_cnt_rate
                from xysc_school_real_user_2_tmp11 a
                left join xysc_school_real_user_2_tmp8 b
                  on a.product_no = b.product_no
               where b.product_no is null
	      "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      #10_5.ѭ����ֹ��ʶ����
	      set sql_buf "
            select count(distinct a.product_no)
              from xysc_school_real_user_2_tmp12 a, $dw_product_yyyymm b
             where a.product_no = b.product_no
               and b.month_call_mark = 1
               and b.userstatus_id in ($rep_online_userstatus_id)
               and a.school_name is not null
               and (a.opp_call_xy_cnt_rate >= 40.00 or
                   a.opp_call_xyuser_num_rate >= 20.00 or a.opp_call_xyuser_num >= 4 or
                   a.opp_call_xy_cnt >= 50 or a.opp_sms_xy_cnt_rate >= 30.00 or
                   a.opp_sms_xyuser_num_rate >= 15.00 or a.opp_sms_xyuser_num >= 4 or
                   a.opp_sms_xy_cnt >= 30)
	         "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
		    while {[set this_row [aidb_fetch $handle]] != ""} {
		       set	    table_count         [lindex $this_row 0]
		    }
	      aidb_close $handle
	      if [catch {set handle [aidb_open $conn]} errmsg] {
	      	trace_sql $errmsg 1302
	      	return -1
	      }
	      if { $table_count==0 } {
        		set flag 0
        }
	      incr i
		    puts "��������:$i��........ \n"
	      set sql_buf "
            insert into bass2.xysc_school_real_user_2_tmp8
              select distinct a.user_id, a.product_no, a.school_name, $i
                from xysc_school_real_user_2_tmp12 a, $dw_product_yyyymm b
               where a.product_no = b.product_no
                 and b.month_call_mark = 1
                 and b.userstatus_id in ($rep_online_userstatus_id)
                 and a.school_name is not null
                 and (a.opp_call_xy_cnt_rate >= 40.00 or
                     a.opp_call_xyuser_num_rate >= 20.00 or
                     a.opp_call_xyuser_num >= 4 or a.opp_call_xy_cnt >= 50 or
                     a.opp_sms_xy_cnt_rate >= 30.00 or
                     a.opp_sms_xyuser_num_rate >= 15.00 or a.opp_sms_xyuser_num >= 4 or
                     a.opp_sms_xy_cnt >= 30)
           "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      }

	  }
	   #11.�½�bass2.dw_xysc_school_real_user_dt_$year$month,������ȫ��ʶ����ʵУ԰�ͻ�
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
	   set sql_buf "
         insert into dw_xysc_school_real_user_dt_$year$month
           (user_id, product_no, school_name)
           select distinct a.user_id, a.product_no, a.school_name
             from (select *
                     from (select a.*,
                                  value(xy_call_rate, 0) xy_call_rate,
                                  row_number() over(partition by a.user_id order by b.xy_call_rate desc) sn
                             from xysc_school_real_user_2_tmp8 a
                             left join $xysc_school_latency_userA_4_yyyymm b
                               on a.user_id = b.user_id
                              and a.SCHOOL_NAME = b.SCHOOL_NAME) a
                    where sn = 1) a,
                  $dw_product_yyyymm b
            where a.product_no = b.product_no
              and b.month_call_mark = 1
              and b.userstatus_id in ($rep_online_userstatus_id)
              and b.plan_id not in
                  (select extend_id
                     from $dim_prod_up_product_item
                    where (name like '%IP����%' or name like '%TD�̻�%')
                      and item_type = 'OFFER_PLAN')
	      "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step4_2.��������ʶ��-ͨ�����������ŵ�������-End...\n"

	return 0
}

 proc Do_xysc_school_TeacherandStudent_user_1 {p_optime} {
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
 	   set    dw_xysc_school_real_user_dt_yyyymm   "$db_schema.dw_xysc_school_real_user_dt_$year$month"
 	   set    dw_product_yyyymm                    "$db_schema.dw_product_$year$month"
 	   set    xysc_school_latency_userA_4_yyyymm   "$db_schema.xysc_school_latency_userA_4_$year$month"

     #1.�½���ʱ��(xysc_school_teacherandstudent_user_tmp),Ȧ��ȫ��ͨ�ͻ�������Ϊ��ʦǱ�ڿͻ�
	   set sql_buf "drop TABLE bass2.xysc_school_teacherandstudent_user_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_user_tmp(
		    user_id		     varchar(20),
		    product_no	   varchar(15),
		    school_name	   varchar(128)
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
     set sql_buf "insert into bass2.xysc_school_teacherandstudent_user_tmp
         select a.user_id,a.product_no,a.school_name
         from $dw_xysc_school_real_user_dt_yyyymm a inner join
              $dw_product_yyyymm b
              on a.user_id=b.user_id and b.brand_id=1 and b.userstatus_id in ($rep_online_userstatus_id)
        "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
     #2.�½���ʱ��(xysc_school_teacherandstudent_user_tmp2),�������Ƿ����100�����ֽ�ʦǱ�ڿͻ�
	   set sql_buf "drop TABLE bass2.xysc_school_teacherandstudent_user_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_user_tmp2(
		    user_id		     varchar(20),
		    product_no	     varchar(15),
		    school_name	   varchar(128)
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
     set sql_buf "insert into bass2.xysc_school_teacherandstudent_user_tmp2
         select distinct a.user_id,a.product_no,a.school_name
         from xysc_school_teacherandstudent_user_tmp a
         where a.school_name in (select school_name
               from xysc_school_teacherandstudent_user_tmp group by school_name having count(1)<=100
              )
        "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

     #3.�½���ʱ��(xysc_school_teacherandstudent_oppcall_tmp),У԰�ͻ�ͨ���Զ��������
	   set sql_buf "drop TABLE bass2.xysc_school_teacherandstudent_oppcall_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_oppcall_tmp(
		    op_time		     date,
		    user_id		     varchar(20),
		    product_no	     varchar(15),
		    school_name	   varchar(128),
		    opposite_no	   varchar(32),
		    call_counts	   integer,
		    call_duration_m integer
     )
     PARTITIONING KEY (user_id,product_no,opposite_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   for {set i 1 } { $i <= $last_days } { incr i } {
		    set today  [format "%04s%02s%02s" $year $month $i]
	      set to_day [format "%04s-%02s-%02s" $year $month $i]

		    puts "$today-У԰�ͻ�ͨ���Զ��������ʼ......"
	      set sql_buf "insert into xysc_school_teacherandstudent_oppcall_tmp
		    	  select
		    	  	 '$to_day',
		    	  	 b.user_id,
		    	  	 b.product_no,
		    	  	 b.school_name,
		    	  	 a.opp_regular_number,
		    	  	 count(1),
		    	  	 sum(a.call_duration_m)
		    	  from cdr_call_dtl_$today a inner join $xysc_school_latency_userA_4_yyyymm b
		    	    on a.user_id=b.user_id and a.product_no=b.product_no
		    	  where b.xy_call_rate>=0.3
		    	  group by b.user_id,
		    	  	 b.product_no,
		    	  	 b.school_name,
		    	  	 a.opp_regular_number"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }

		 }
	   set sql_buf "drop index idx_xysc_up9"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up9 on xysc_school_teacherandstudent_oppcall_tmp (user_id,product_no,school_name,opposite_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

     #4.�½���ʱ��(xysc_school_teacherandstudent_oppsms_tmp),У԰�ͻ����ŶԶ��������
	   set sql_buf "drop TABLE bass2.xysc_school_teacherandstudent_oppsms_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_oppsms_tmp(
		    op_time		     date,
		    user_id		     varchar(20),
		    product_no	   varchar(15),
		    school_name	   varchar(128),
		    opposite_no	   varchar(32),
		    sms_counts	   integer
     )
     PARTITIONING KEY (user_id,product_no,opposite_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   for {set i 1 } { $i <= $last_days } { incr i } {
		    set today  [format "%04s%02s%02s" $year $month $i]
	      set to_day [format "%04s-%02s-%02s" $year $month $i]

		    puts "$today-У԰�ͻ����ŶԶ��������ʼ......"
	      set sql_buf "insert into xysc_school_teacherandstudent_oppsms_tmp
		    	  select
		    	  	 '$to_day',
		    	  	 b.user_id,
		    	  	 b.product_no,
		    	  	 b.school_name,
		    	  	 a.opp_number,
		    	  	 count(1)
		    	  from cdr_sms_dtl_$today a inner join $xysc_school_latency_userA_4_yyyymm b
		    	    on a.user_id=b.user_id and a.product_no=b.product_no
		    	  where b.xy_call_rate>=0.3
		    	  group by b.user_id,
		    	  	 b.product_no,
		    	  	 b.school_name,
		    	  	 a.opp_number"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }

		 }
	   set sql_buf "drop index idx_xysc_up10"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up10 on xysc_school_teacherandstudent_oppsms_tmp (user_id,product_no,school_name,opposite_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #5.�½���ʱ��(xysc_school_teacherandstudent_oppcall_$year$month),����У԰�ͻ�ͨ���Զ�����
     set sql_buf "drop TABLE bass2.xysc_school_teacherandstudent_oppcall_$year$month"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_oppcall_$year$month like bass2.xysc_school_teacherandstudent_oppcall_yyyymm in tbs_bass_miner
					        index in tbs_index partitioning key (user_id,product_no,opposite_no) using hashing not logged initially
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_teacherandstudent_oppcall_$year$month
		     select
		    	  a.user_id,
		    	  a.product_no,
		    	  a.school_name,
		    	  a.opposite_no,
		    	  sum(a.call_counts),
		    	  sum(a.call_duration_m)
		     from xysc_school_teacherandstudent_oppcall_tmp a inner join
		          $dw_xysc_school_real_user_dt_yyyymm b
		          on a.product_no=b.product_no and a.school_name=b.school_name
		     group by a.user_id,
		    	  a.product_no,
		    	  a.school_name,
		    	  a.opposite_no"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up11"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up11 on xysc_school_teacherandstudent_oppcall_$year$month (user_id,product_no,school_name,opposite_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #6.�½���ʱ��(xysc_school_teacherandstudent_user_tmp3),����У԰�ͻ�ͨ������
     set sql_buf "drop TABLE bass2.xysc_school_teacherandstudent_user_tmp3"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_user_tmp3 (
			   product_no	     varchar(15),
			   call_counts1		 integer,
			   call_counts		 integer,
			   call_user1	     integer,
			   call_user	     integer
     )
     PARTITIONING KEY (product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_teacherandstudent_user_tmp3
		     select a.product_no,
			      sum(case when c.product_no is not null then a.call_counts else 0 end) as call_counts1,
			      sum(case when e.product_no is not null then a.call_counts end) as call_counts,
			      count(distinct case when c.product_no is not null then a.opposite_no end) as call_user1,
			      count(distinct case when e.product_no is not null then a.opposite_no end )as call_user
		     from xysc_school_teacherandstudent_oppcall_$year$month as a
		          left join xysc_school_teacherandstudent_user_tmp c
		          on a.opposite_no=c.product_no and a.school_name=c.school_name
		          left join $dw_xysc_school_real_user_dt_yyyymm e
		          on a.opposite_no=e.product_no and a.school_name=e.school_name
		          left join $xysc_school_latency_userA_4_yyyymm d
		          on a.product_no=d.product_no and a.school_name=d.school_name
		     where d.xy_call_rate>0.3
		     group by a.product_no
		     having count(distinct case when e.product_no is not null then a.opposite_no end)>1"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up12"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up12 on xysc_school_teacherandstudent_user_tmp3 (product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #7.�½���ʱ��(xysc_school_teacherandstudent_user_tmp4),Ǳ��У԰��ʦ�ͻ����봦��
     set sql_buf "drop TABLE bass2.xysc_school_teacherandstudent_user_tmp4"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_user_tmp4 (
			   product_no	     varchar(15)
     )
     PARTITIONING KEY (product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_teacherandstudent_user_tmp4
		     select distinct product_no
		     from xysc_school_teacherandstudent_user_tmp3
		     where (call_counts1*1.0/call_counts<0.5 and call_user1*1.0/call_user<0.5) or call_user1<=1"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "drop index idx_xysc_up13"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "create index idx_xysc_up13 on xysc_school_teacherandstudent_user_tmp4 (product_no)"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
     set sql_buf "insert into bass2.xysc_school_teacherandstudent_user_tmp2
         select distinct a.user_id,a.product_no,a.school_name
		     from xysc_school_teacherandstudent_user_tmp a
		          left join xysc_school_teacherandstudent_user_tmp4 b
		        on a.product_no=b.product_no
		     where b.product_no is null and
		     	  a.school_name in (select school_name from xysc_school_teacherandstudent_user_tmp
		     	                    group by school_name having count(1)>100)
        "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }

		 puts "Step5_1.ʶ����ʵѧ���ͽ�ʦУ԰�ͻ�Ԥ����-End...\n"

	return 0
}

 proc Do_xysc_school_TeacherandStudent_user_2 {p_optime} {
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
 	   set    xysc_school_teacherandstudent_user_tmp2   "$db_schema.xysc_school_teacherandstudent_user_tmp2"
 	   set    xysc_school_teacherandstudent_oppcall_tmp "$db_schema.xysc_school_teacherandstudent_oppcall_tmp"
 	   set    xysc_school_teacherandstudent_oppsms_tmp  "$db_schema.xysc_school_teacherandstudent_oppsms_tmp"
 	   set    dw_xysc_school_real_user_dt_yyyymm        "$db_schema.dw_xysc_school_real_user_dt_$year$month"
 	   set    dw_product_yyyymm                         "$db_schema.dw_product_$year$month"

	   #1.�½���ʱ��(xysc_school_teacherandstudent_user_tmp5),У԰��ʦ�ͻ����봦��
     set sql_buf "drop TABLE bass2.xysc_school_teacherandstudent_user_tmp5"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     puts "errmsg:$errmsg"
	   }
	   set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_user_tmp5 (
			         user_id		  varchar(20),
			         product_no	  varchar(20),
			         school_name	varchar(128),
			         rank		      integer
     )
     PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
     "

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   set sql_buf "insert into xysc_school_teacherandstudent_user_tmp5
		     select distinct user_id,product_no,school_name,0
		     from $xysc_school_teacherandstudent_user_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	   	     trace_sql $errmsg 1300
	   	     puts "errmsg:$errmsg"
	   	     return -1
	   }
	   #2.��������
	   set flag 1
	   set i    0
	   while { $flag>0 } {
	      #2_1.�Զ�ͨ��
	      #�Զ�ͨ������ / ����У԰����
	      #�Զ�ͨ������ / ����У԰����
	      #�Զ�ͨ��ʱ�� / ����У԰ʱ��
	      set sql_buf "drop TABLE bass2.xysc_school_teacherandstudent_user_tmp6"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_user_tmp6(
			        user_id			          varchar(20),
				      product_no		        varchar(15),
				      school_name		        varchar(128),
				      opp_call_user_num	    integer,
				      opp_call_xyuser_num	  integer,
				      opp_call_cnt		      integer,
				      opp_call_xy_cnt	      integer,
				      opp_call_duration	    integer,
				      opp_call_xy_duration	integer
        )
        PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
        "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "drop TABLE bass2.xysc_school_teacherandstudent_user_tmp6_01"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_user_tmp6_01(
			        user_id			          varchar(20),
				      product_no		        varchar(15),
				      school_name		        varchar(128),
				      opp_call_user_num	    integer,
				      opp_call_cnt		      integer,
				      opp_call_duration	    integer
        )
        PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
        "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "insert into xysc_school_teacherandstudent_user_tmp6_01 (
	             user_id
              ,product_no
              ,school_name
              ,opp_call_user_num
              ,opp_call_cnt
              ,opp_call_duration
	             )
           select a.user_id,a.product_no,a.school_name,
			     	  count(distinct a.opposite_no) as opp_call_user_num,
			     	  sum(a.call_counts) as opp_call_cnt,
			     	  sum(a.call_duration_m) as opp_call_duration
			     from $xysc_school_teacherandstudent_oppcall_tmp a
			          inner join $dw_xysc_school_real_user_dt_yyyymm b
			        on a.product_no=b.product_no and a.school_name=b.school_name
			     where a.opposite_no not like '10086%' and
			     	  a.opposite_no in (select distinct product_no from $dw_xysc_school_real_user_dt_yyyymm)
			     group by a.user_id,a.product_no,a.school_name
	      "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "drop TABLE bass2.xysc_school_teacherandstudent_user_tmp6_02"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_user_tmp6_02(
			        user_id			          varchar(20),
				      product_no		        varchar(15),
				      school_name		        varchar(128),
				      opp_call_xyuser_num	  integer,
				      opp_call_xy_cnt	      integer,
				      opp_call_xy_duration	integer
        )
        PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
        "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "insert into xysc_school_teacherandstudent_user_tmp6_02 (
	             user_id
              ,product_no
              ,school_name
              ,opp_call_xyuser_num
              ,opp_call_xy_cnt
              ,opp_call_xy_duration
	             )
           select a.user_id,a.product_no,a.school_name,
			     	  count(distinct a.opposite_no) as opp_call_xyuser_num,
			     	  sum(a.call_counts) as opp_call_xy_cnt,
			     	  sum(a.call_duration_m) as opp_call_xy_duration
			     from $xysc_school_teacherandstudent_oppcall_tmp a inner join
			         (select distinct product_no,school_name from xysc_school_teacherandstudent_user_tmp5
			          )b on a.opposite_no=b.product_no and a.school_name=b.school_name
			     where a.opposite_no not like '10086%'
			     group by a.user_id,a.product_no,a.school_name
	      "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "insert into xysc_school_teacherandstudent_user_tmp6 (
	             user_id
              ,product_no
              ,school_name
              ,opp_call_user_num
              ,opp_call_xyuser_num
              ,opp_call_cnt
              ,opp_call_xy_cnt
              ,opp_call_duration
              ,opp_call_xy_duration
	              )
           select a.user_id,a.product_no,a.school_name,
			     	  value(a.opp_call_user_num,0) as opp_call_xyuser_num,
			     	  value(b.opp_call_xyuser_num,0) as opp_call_xy_cnt,
			     	  value(a.opp_call_cnt,0) as opp_call_xy_duration,
			     	  value(b.opp_call_xy_cnt,0) as opp_call_xyuser_num,
			     	  value(a.opp_call_duration,0) as opp_call_duration,
			     	  value(b.opp_call_xy_duration,0) as opp_call_xy_duration
			     from xysc_school_teacherandstudent_user_tmp6_01 a left join
			          xysc_school_teacherandstudent_user_tmp6_02 b
			        on a.user_id=b.user_id and a.product_no=b.product_no and a.school_name=b.school_name
	      "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      #2_2.�Զ˶���
	      #�Զ˶������� / ����У԰����
	      #�Զ˶��Ŵ��� / ����У԰����
	      set sql_buf "drop TABLE bass2.xysc_school_teacherandstudent_user_tmp7"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_user_tmp7(
			        user_id			          varchar(20),
				      product_no		        varchar(15),
				      school_name		        varchar(128),
				      opp_sms_user_num	    integer,
				      opp_sms_xyuser_num	  integer,
				      opp_sms_cnt		        integer,
				      opp_sms_xy_cnt	      integer
        )
        PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
        "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "drop TABLE bass2.xysc_school_teacherandstudent_user_tmp7_01"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_user_tmp7_01(
			        user_id			          varchar(20),
				      product_no		        varchar(15),
				      school_name		        varchar(128),
				      opp_sms_user_num	    integer,
				      opp_sms_cnt		        integer
        )
        PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
        "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "insert into xysc_school_teacherandstudent_user_tmp7_01 (
	             user_id
              ,product_no
              ,school_name
              ,opp_sms_user_num
              ,opp_sms_cnt
	             )
           select a.user_id,a.product_no,a.school_name,
			     	  count(distinct a.opposite_no) as opp_sms_user_num,
			     	  sum(sms_counts) as opp_sms_cnt
			     from $xysc_school_teacherandstudent_oppsms_tmp a
			          inner join $dw_xysc_school_real_user_dt_yyyymm b
			        on a.product_no=b.product_no and a.school_name=b.school_name
			     where a.opposite_no not like '10086%' and
			     	  a.opposite_no in (select distinct product_no from $dw_xysc_school_real_user_dt_yyyymm)
			     group by a.user_id,a.product_no,a.school_name
	      "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "drop TABLE bass2.xysc_school_teacherandstudent_user_tmp7_02"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_user_tmp7_02(
			        user_id			          varchar(20),
				      product_no		        varchar(15),
				      school_name		        varchar(128),
				      opp_sms_xyuser_num	  integer,
				      opp_sms_xy_cnt	      integer
        )
        PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
        "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "insert into xysc_school_teacherandstudent_user_tmp7_02 (
	             user_id
              ,product_no
              ,school_name
              ,opp_sms_xyuser_num
              ,opp_sms_xy_cnt
	             )
           select a.user_id,a.product_no,a.school_name,
			     	  count(distinct a.opposite_no) as opp_sms_xyuser_num,
			     	  sum(a.sms_counts) as opp_sms_xy_cnt
			     from $xysc_school_teacherandstudent_oppsms_tmp a inner join
			         (select distinct product_no,school_name from xysc_school_teacherandstudent_user_tmp5
			          )b on a.opposite_no=b.product_no and a.school_name=b.school_name
			     where a.opposite_no not like '10086%'
			     group by a.user_id,a.product_no,a.school_name
	      "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "insert into xysc_school_teacherandstudent_user_tmp7 (
	             user_id
              ,product_no
              ,school_name
              ,opp_sms_user_num
              ,opp_sms_xyuser_num
              ,opp_sms_cnt
              ,opp_sms_xy_cnt
	              )
           select a.user_id,a.product_no,a.school_name,
			     	  value(a.opp_sms_user_num,0) as opp_sms_user_num,
			     	  value(b.opp_sms_xyuser_num,0) as opp_sms_xyuser_num,
			     	  value(a.opp_sms_cnt,0) as opp_sms_cnt,
			     	  value(b.opp_sms_xy_cnt,0) as opp_sms_xy_cnt
			     from xysc_school_teacherandstudent_user_tmp7_01 a left join
			          xysc_school_teacherandstudent_user_tmp7_02 b
			        on a.user_id=b.user_id and a.product_no=b.product_no and a.school_name=b.school_name
	      "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      #3.�½���ʱ��(xysc_school_teacherandstudent_user_tmp8),У԰��ʦ�ͻ����봦��(����+����)
	      set sql_buf "drop TABLE bass2.xysc_school_teacherandstudent_user_tmp8"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_user_tmp8(
			        user_id			          varchar(20),
				      product_no		        varchar(15),
				      school_name		        varchar(128),
				      opp_call_user_num	    integer,
				      opp_call_xyuser_num	  integer,
				      opp_call_cnt		      integer,
				      opp_call_xy_cnt	      integer,
				      opp_call_duration	    integer,
				      opp_call_xy_duration	integer,
				      opp_sms_user_num	    integer,
				      opp_sms_xyuser_num	  integer,
				      opp_sms_cnt		        integer,
				      opp_sms_xy_cnt	      integer
        )
        PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
        "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "insert into bass2.xysc_school_teacherandstudent_user_tmp8
	          select a.user_id,a.product_no,a.school_name,
				      sum(a.opp_call_user_num),
				      sum(a.opp_call_xyuser_num),
				      sum(a.opp_call_cnt),
				      sum(a.opp_call_xy_cnt),
				      sum(a.opp_call_duration),
				      sum(a.opp_call_xy_duration),
				      sum(a.opp_sms_user_num),
				      sum(a.opp_sms_xyuser_num),
				      sum(a.opp_sms_cnt),
				      sum(a.opp_sms_xy_cnt)
			     from (select user_id,product_no,school_name,
			  		        opp_call_user_num,opp_call_xyuser_num,opp_call_cnt,opp_call_xy_cnt,opp_call_duration,opp_call_xy_duration,
			  		        0 as opp_sms_user_num,0 as opp_sms_xyuser_num,0 as opp_sms_cnt,0 as opp_sms_xy_cnt
			  	       from xysc_school_teacherandstudent_user_tmp6 union all
			  	       select user_id,product_no,school_name,
			  		        0 as opp_call_user_num,0 as opp_call_xyuser_num,0 as opp_call_cnt,0 as opp_call_xy_cnt,0 as opp_call_duration,0 as opp_call_xy_duration,
			  		        opp_sms_user_num,opp_sms_xyuser_num,opp_sms_cnt,opp_sms_xy_cnt
			  	       from xysc_school_teacherandstudent_user_tmp7
			  	      )a
			  	 group by a.user_id,a.product_no,a.school_name
	         "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      }
	      #4.�½���ʱ��(xysc_school_teacherandstudent_user_tmp9),У԰��ʦ�ͻ����봦��(����+����)ռ�ȴ���
	      set sql_buf "drop TABLE bass2.xysc_school_teacherandstudent_user_tmp9"

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_user_tmp9(
			        user_id			                varchar(20),
				      product_no		              varchar(15),
				      school_name		              varchar(128),
				      opp_call_user_num	          integer,
				      opp_call_xyuser_num	        integer,
				      opp_call_xyuser_num_rate	  decimal(12,2),
				      opp_call_cnt		            integer,
				      opp_call_xy_cnt	            integer,
				      opp_call_xy_cnt_rate	      decimal(12,2),
				      opp_call_duration	          integer,
				      opp_call_xy_duration	      integer,
				      opp_call_xy_duration_rate	  decimal(12,2),
				      opp_sms_user_num	          integer,
				      opp_sms_xyuser_num	        integer,
				      opp_sms_xyuser_num_rate	    decimal(12,2),
				      opp_sms_cnt		              integer,
				      opp_sms_xy_cnt	            integer,
				      opp_sms_xy_cnt_rate	        decimal(12,2)
        )
        PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
        "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "insert into xysc_school_teacherandstudent_user_tmp9 (
	             user_id
              ,product_no
              ,school_name
              ,opp_call_user_num
              ,opp_call_xyuser_num
              ,opp_call_xyuser_num_rate
              ,opp_call_cnt
              ,opp_call_xy_cnt
              ,opp_call_xy_cnt_rate
              ,opp_call_duration
              ,opp_call_xy_duration
              ,opp_call_xy_duration_rate
              ,opp_sms_user_num
              ,opp_sms_xyuser_num
              ,opp_sms_xyuser_num_rate
              ,opp_sms_cnt
              ,opp_sms_xy_cnt
              ,opp_sms_xy_cnt_rate
	             )
           select distinct a.user_id,a.product_no,a.school_name,
			     	  opp_call_user_num,opp_call_xyuser_num,
			     	  case when opp_call_user_num=0 then 0.00 else round(decimal(opp_call_xyuser_num,12,2)/decimal(opp_call_user_num,12,2)*100.0,2) end as opp_call_xyuser_num_rate,
			     	  opp_call_cnt,opp_call_xy_cnt,
			     	  case when opp_call_cnt=0 then 0.00 else round(decimal(opp_call_xy_cnt,12,2)/decimal(opp_call_cnt,12,2)*100.0,2) end as opp_call_xy_cnt_rate,
			     	  opp_call_duration,opp_call_xy_duration,
			     	  case when opp_call_duration=0 then 0.00 else round(decimal(opp_call_xy_duration,12,2)/decimal(opp_call_duration,12,2)*100.0,2) end as opp_call_xy_duration_rate,
			     	  opp_sms_user_num,opp_sms_xyuser_num,
			     	  case when opp_sms_user_num=0 then 0.00 else round(decimal(opp_sms_xyuser_num,12,2)/decimal(opp_sms_user_num,12,2)*100.0,2) end as opp_sms_xyuser_num_rate,
			     	  opp_sms_cnt,opp_sms_xy_cnt,
			     	  case when opp_sms_cnt=0 then 0.00 else round(decimal(opp_sms_xy_cnt,12,2)/decimal(opp_sms_cnt,12,2)*100.0,2) end as opp_sms_xy_cnt_rate
			     from xysc_school_teacherandstudent_user_tmp8 a left join
			     	    xysc_school_teacherandstudent_user_tmp5 b on a.product_no=b.product_no
			     where b.product_no is null
	      "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      #5.ѭ����ֹ��ʶ����
	      set sql_buf "select count(distinct a.product_no)
			      from xysc_school_teacherandstudent_user_tmp9 a,$dw_product_yyyymm b
			      where a.product_no=b.product_no and b.month_call_mark=1 and b.userstatus_id in ($rep_online_userstatus_id)
			           and a.school_name is not null
			           and (
				              (
				              	(
				              	a.opp_call_xy_cnt_rate>=40.00
				              	or
				              	a.opp_call_xyuser_num_rate>=35.00
				              	)
				              	and
				              	a.opp_call_xyuser_num>1
				              )
                      or
				              (
				              	(
				              	a.opp_sms_xy_cnt_rate>=40.00
				              	or
				              	a.opp_sms_xyuser_num_rate>=30.00
				              	)
				              	and
				              	a.opp_sms_xyuser_num>1
				              )
			               )
			           and a.product_no in (select distinct product_no from $dw_xysc_school_real_user_dt_yyyymm)
	         "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
		    while {[set this_row [aidb_fetch $handle]] != ""} {
		       set	    table_count         [lindex $this_row 0]
		    }
	      aidb_close $handle
	      if [catch {set handle [aidb_open $conn]} errmsg] {
	      	trace_sql $errmsg 1302
	      	return -1
	      }
	      if { $table_count==0 } {
        		set flag 0
        }
	      incr i
		    puts "��������:$i��........ \n"
		    #����ѭ������Ϊ15-20������
	      if { $i==15 } {
        		set flag 0
        }
	      set sql_buf "insert into bass2.xysc_school_teacherandstudent_user_tmp5
	          select distinct a.user_id,a.product_no,a.school_name,$i
			      from xysc_school_teacherandstudent_user_tmp9 a,$dw_product_yyyymm b
			      where a.product_no=b.product_no and b.month_call_mark=1 and b.userstatus_id in ($rep_online_userstatus_id)
			           and a.school_name is not null
			           and (
				              (
				              	(
				              	a.opp_call_xy_cnt_rate>=40.00
				              	or
				              	a.opp_call_xyuser_num_rate>=35.00
				              	)
				              	and
				              	a.opp_call_xyuser_num>1
				              )
                      or
				              (
				              	(
				              	a.opp_sms_xy_cnt_rate>=40.00
				              	or
				              	a.opp_sms_xyuser_num_rate>=30.00
				              	)
				              	and
				              	a.opp_sms_xyuser_num>1
				              )
			               )
			           and a.product_no in (select distinct product_no from $dw_xysc_school_real_user_dt_yyyymm)
           "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	   	     return -1
	      }

	  }
	      #6.�½�(xysc_school_teacherandstudent_user_tmp10),������ʶ����ʵУ԰��ʦ�ͻ�
	      #�ߵ�IP���к�TD����
	      set sql_buf "DROP TABLE xysc_school_teacherandstudent_user_tmp10
            "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     puts "errmsg:$errmsg"
	      }
	      set sql_buf "CREATE TABLE bass2.xysc_school_teacherandstudent_user_tmp10(
			        user_id			                varchar(20),
				      product_no		              varchar(15),
				      school_name		              varchar(128)
            )
            PARTITIONING KEY (user_id,product_no) USING HASHING IN TBS_BASS_MINER INDEX IN TBS_INDEX NOT LOGGED INITIALLY
           "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "insert into xysc_school_teacherandstudent_user_tmp10 (
	             user_id
	            ,product_no
	            ,school_name
	           )
	          select distinct a.user_id,a.product_no,a.school_name
			      from xysc_school_teacherandstudent_user_tmp5 a,$dw_product_yyyymm b
			      where a.product_no=b.product_no and b.month_call_mark=1 and b.userstatus_id in ($rep_online_userstatus_id)
			           and b.plan_id not in (select extend_id from dim_prod_up_product_item where (name like '%IP����%' or name like '%TD�̻�%') and item_type='OFFER_PLAN')
			     "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      #7.����(dw_xysc_school_real_user_dt_yyyymm)��phone_type
	      #�������-S:ѧ������;T:��ʦ
	      set sql_buf "update $dw_xysc_school_real_user_dt_yyyymm a
	          set phone_type=
	              (select case when b.product_no is not null then 'T' end
	               from xysc_school_teacherandstudent_user_tmp10 b
	               where a.product_no=b.product_no
	              )
           "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      set sql_buf "update $dw_xysc_school_real_user_dt_yyyymm a
	          set phone_type='S'
	          where phone_type is null
           "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	     
	     #2011-09-29������ģ�ͳ��������ݽ�ʦ�û���Ƭ����ѧ���û�,���������µ��� 
	       set sql_buf "update $dw_xysc_school_real_user_dt_yyyymm a
	          set phone_type='H'
			  where a.phone_type = 'T' 
			        and a.user_id not in (select user_id from bass2.dim_xysc_pno_example_info)
           "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      
	      set sql_buf "update $dw_xysc_school_real_user_dt_yyyymm a
	          set phone_type='T'
			  where a.phone_type = 'S' 
			        and a.user_id not in (select user_id from bass2.dim_xysc_pno_example_info)
           "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }
	      
	        set sql_buf "update $dw_xysc_school_real_user_dt_yyyymm a
	          set phone_type='S'
			  where a.phone_type = 'H' 
			        and a.user_id not in (select user_id from bass2.dim_xysc_pno_example_info)
           "

	      if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	     trace_sql $errmsg 1300
	      	     puts "errmsg:$errmsg"
	      	     return -1
	      }

		 puts "Step5_2.ʶ����ʵѧ���ͽ�ʦУ԰�ͻ�-End...\n"

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
     #Step1_1.Do_dim_xysc_lac_cell_info<��>
     #Step1_2.Do_dim_xysc_pno_example_info
	   set sql_buf "drop table dim_xysc_pno_example_info_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
     #Step2-1.Do_xysc_school_latency_userA_1
	   set sql_buf "drop table xysc_school_latency_userA_1_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   #Step2-2.Do_xysc_school_latency_userA_2
	   set sql_buf "drop table xysc_school_latency_userA_2_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   #Step2-3.Do_xysc_school_latency_userA_3
	   set sql_buf "drop table xysc_school_latency_userA_3_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_latency_userA_3_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_latency_userA_3_tmp3"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_latency_userA_3_tmp4"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_latency_userA_3_tmp5"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   #Step2-4.Do_xysc_school_latency_userA_4<��>
	   #Step3_1.Do_xysc_school_latency_userB_1
	   set sql_buf "drop table xysc_school_latency_userB_1_call_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   #Step3-2.Do_xysc_school_latency_userB_2
	   set sql_buf "drop table xysc_school_latency_userB_2_sms_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   #Step4-1.Do_xysc_school_real_user_1
	   set sql_buf "drop table xysc_school_latency_userB_3_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_latency_userB_3_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_latency_userB_3_tmp3"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_latency_userB_3_tmp4"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   #Step4-2.Do_xysc_school_real_user_2
	   set sql_buf "drop table xysc_school_real_user_2_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_real_user_2_tmp3"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_real_user_2_tmp4"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_real_user_2_tmp6"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_real_user_2_tmp7"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_real_user_2_tmp9"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_real_user_2_tmp10"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_real_user_2_tmp11"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_real_user_2_tmp12"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   #Step5_1.Do_xysc_school_TeacherandStudent_user_1
	   set sql_buf "drop table xysc_school_teacherandstudent_user_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_teacherandstudent_user_tmp2"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_teacherandstudent_oppcall_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_teacherandstudent_oppsms_tmp"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_teacherandstudent_user_tmp3"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_teacherandstudent_user_tmp4"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   #Step5-2.Do_xysc_school_TeacherandStudent_user_2
	   set sql_buf "drop table xysc_school_teacherandstudent_user_tmp6_01"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_teacherandstudent_user_tmp6_02"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_teacherandstudent_user_tmp6"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_teacherandstudent_user_tmp7_01"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_teacherandstudent_user_tmp7_02"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_teacherandstudent_user_tmp7"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_teacherandstudent_user_tmp8"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }
	   set sql_buf "drop table xysc_school_teacherandstudent_user_tmp9"

	   if [catch {aidb_sql $handle $sql_buf} errmsg] {
	      	   puts "errmsg:$errmsg"
	   }

		 puts "StepX.ɾ���������豣������ʱ��-End...\n"

	return 0
}