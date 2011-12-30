#==============================================================================================#
#����:stat_ecustsvc_0032.tcl                ��������:2009��ȵ�������ͳ�����ݱ�-A02��վ        #
#                                                                                              #
#���:���ӿͷ����� 0032                     ��д��:AsiaInfo HeYeSheng 2009-05-14               #
#	2009-8-10	�ͷ�ϵͳ���ŷ��������и��죬���ָ����µĿھ����Զ����������ͳ�ƵĿھ������޸�,#
#				ԭ�� dw_kf_sms_dm_yyyymm ���ϣ��Ĵӱ� dw_kf_sms_cmd_receive_dm_yyyymm ��	   #
#				�� dw_kf_cmd_hint_def_dm_yyyymm ��ȡ����						by HeYeSheng   #
#	2009-8-17	����������ѯ�����ҵ��Ĵ����ʶ���޳��û���Ϊ����Ͳ�Ʒ��������			   #
#				ֻͳ��ϵͳԭ������Ĵ�����������ѯ�����ҵ��ĳɹ��ʡ�		by HeYeSheng   #
#	2009-8-19	����EMAIL����������EMAIL���Ϳͻ�����ָ���ͳ�ƿھ�				by HeYeSheng   #
#	2009-11-12	���ݺ��������󣬸���ʵ������(�����ɷ�)ҵ������ͳ�ƿھ�		by HeYeSheng   #
#	2009-11-17	BOSS���Ӷ���ҵ�����ָ�BIͳ�ƿھ�����Ӧ���޸�				by HeYeSheng   #
#	2009-11-23	BOSS���Ӷ����Ҹ���ͥ����ָ�BIͳ�ƿھ�����Ӧ���޸�			by HeYeSheng   #
#	2009-12-2	BOSS�޸�10086�˹�ҵ�����ھ���BIͳ�ƿھ�����Ӧ���޸�			by HeYesheng   #
#	2010-1-5	ע�͵�EMAIL���������ͽ��տͻ���ָ�꣬��Ϊ��Щָ��Ľӿ�Ҫ7�Ų��ܳ����������޴������ݱ�����4��ǰ#
#	2010-1-6	ʵ������(�����ɷ�)��ͳ�ƿھ����������ӣ�op_id not in (1,800)    by HeYeSheng   #
# 2010-1-14 ���Ӷ����Ҹ���ͥ�ƻ�ָ��('KTQQW','TJFK','SCFK','QXQQW','KTQYQ','QXQYQ','TJQYQKH ',     #
#           'SCQYQKH','KTWXSW50','QXWXSW50','KTWXSW80','QXWXSW80','KTWXSW120','QXWXSW120') By fuzl #
# 2010-01-25 BY Lichuncai �޸Ŀͻ��������ھ���userstatus_id in ($rep_online_userstatus_id) and test_mark <> 1#
# 2010-4-23 By Liwei ����"���Ͻɷ�"������
# 2010-06-01 �޸���ʱ���ֶ�brand_id bigint, paytype_id=�ַ����� by lichuncai
# 2010-06-04 �޸�so_mode=�ַ����� BY Lichuncai
# 2010/8/10 By fuzl ��ϵͳ���ߺ��op_id=8000�Ĳ���Ա��Ϣ����ά��������op_id=10000475���棬��˶������İ�������so_mode ='15' and op_id=8000 ��Ϊ a.so_mode ='15' and a.op_id=10000475  
# 2010-09-16 By heys �Ϸ�����NGBOSS�ͷ���ӵ��¾����������ݿھ������						   #
# 2010-11-29 by heys �����ɷ���ԭ����paytype_id='4162'������ '4864' [����]���������ն��ֽ�ɷ� #
# 2010-12-06 by heys ʵ��ɷ���ԭ����paytype_id='4101'������ '4801' [����]ǰ̨�ɷ�             #
#==============================================================================================#

proc deal {p_optime p_timestamp} {
	global conn
	global handle

	if [catch {set handle [aidb_open $conn]} errmsg] {
	  	trace_sql $errmsg  1000
	  	puts "Errmsg:$errmsg"
	  	return -1
	}

	if { [stat_ecustsvc_0032 $p_optime ] != 0 } {
		aidb_roll $conn
		aidb_close $handle
		return -1
	}

  aidb_commit $conn
 	aidb_close $handle
	return 0
}

proc stat_ecustsvc_0032 {p_optime} {

  #Global Definition
	global conn
	global handle
	#source
 	source  stat_insert_index.tcl
  source  report.cfg
  #Variable Definition
  scan $p_optime "%04s-%02s-%02s" year month day
	set op_time [ai_to_date $p_optime]
  puts $op_time
  set chr_surfix  "${year}${month}"
  puts $chr_surfix
  set sql_buf "Select date(char($op_time+ 1 month)) from dual"
	puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	set daynextmonth [lindex [aidb_fetch $handle] 0]
	puts $daynextmonth
	scan $daynextmonth "%04s-%02s-%02s" year1 month1 day1	
	set endday [GetLastDay [GetNextMonth $year$month]01]
	puts $endday
	aidb_commit $conn
	aidb_close $handle
	set handle [aidb_open $conn]

  set sql_buf "Select date(char($op_time- 2 month)) from dual"
	puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	set dayprev2month [lindex [aidb_fetch $handle] 0]
	puts $dayprev2month
	scan $dayprev2month "%04s-%02s-%02s" year month day
	set chr_surfix_2  "${year}${month}"
	puts $chr_surfix_2
	aidb_commit $conn
	aidb_close $handle
	set handle [aidb_open $conn]

  set sql_buf "Select date(char($op_time- 1 month)) from dual"
	puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	set dayprev1month [lindex [aidb_fetch $handle] 0]
	puts $dayprev1month
	scan $dayprev1month "%04s-%02s-%02s" year month day
	set chr_surfix_1  "${year}${month}"
	puts $chr_surfix_1
	aidb_commit $conn
	aidb_close $handle
	set handle [aidb_open $conn]

  #Regsub(�����ַ���)
  #regsub -all " " $brandid_mzone_all "," rep_brandid_mzone

  #Source Table Definition
  set dw_product_yyyymm       	   			"dw_product_$chr_surfix"
  set dim_sys_object                  		"dim_sys_object5"
  set dw_ow_loading_log_yyyymm        		"dw_ow_loading_log_$chr_surfix"
  set dw_product_busi_dm_yyyymm       		"dw_product_busi_dm_$chr_surfix"
  set dwd_wcc_complain_info_yyyymm    		"dwd_wcc_complain_info_$chr_surfix"
  set dw_wcc_user_action_yyyymm       		"dw_wcc_user_action_$chr_surfix"
  set dw_custsvc_ivr_log_dm_yyyymm    		"dw_custsvc_ivr_log_dm_$chr_surfix"
  set dw_kf_sms_cmd_receive_dm_yyyymm	  	"dw_kf_sms_cmd_receive_dm_$chr_surfix"
  set dw_kf_cmd_hint_def_dm_yyyymm		  	"dw_kf_cmd_hint_def_dm_$chr_surfix"
  set dw_product_regsp_yyyymm         		"dw_product_regsp_$chr_surfix"
  set dw_acct_payitem_dm_yyyymm       		"dw_acct_payitem_$chr_surfix"
  set dw_custsvc_agent_tele_dm_yyyymm 		"dw_custsvc_agent_tele_dm_$chr_surfix"
  set dw_ow_send_mail_yyyymm				      "dw_ow_send_mail_$chr_surfix"
  set dw_product_ord_so_log_dm_yyyymm	  	"dw_product_ord_so_log_dm_$chr_surfix"
  set dw_product_ord_cust_dm_yyyymm			"dw_product_ord_cust_dm_$chr_surfix"
  set ods_dim_so_busioptcode_mapping_yyyymmdd	"ods_dim_so_busioptcode_mapping_$year1$month1$day1"

  set dim_pub_channel                 		"dim_pub_channel"
  set dim_pub_channeltype_tmp         		"dim_pub_channeltype_tmp"
  set dim_boss_staff                  		"dim_boss_staff"

  set stat_ecustsvc_0032			   		      "stat_ecustsvc_0032"
  set stat_ecustsvc_0032_s            		"stat_ecustsvc_0032_s"

  set dw_product_yyyymm_2       	   		  "dw_product_$chr_surfix_2"
  set dw_product_yyyymm_1       	   		  "dw_product_$chr_surfix_1"
  set dw_wcc_user_action_yyyymm_2       	"dw_wcc_user_action_$chr_surfix_2"
  set dw_wcc_user_action_yyyymm_1       	"dw_wcc_user_action_$chr_surfix_1"

  #Proc Main Body
  #Create Target_TMP_Table OR TMP_Table
	#set sql_buf "declare global temporary table session.stat_ecustsvc_0032_tmp like $stat_ecustsvc_0032
	#		partitioning key (channel_id) using hashing with replace on commit preserve rows not logged in tbs_user_temp"
  #===================================================================================================
  set sql_buf "declare global temporary table session.stat_ecustsvc_0032_tmp(
                  s_index_id            smallint,
                  t_index_id            smallint,
                  result                decimal(18,2)
  	          ) partitioning key (s_index_id) using hashing
  	          with replace on commit preserve rows not logged in tbs_user_temp"

  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn
  set sql_buf "declare global temporary table session.stat_ecustsvc_brand_tmp(
                  parent_ob_id          smallint,
                  brand_id              bigint,
                  is_success            smallint,
                  flow                  bigint,
                  count                 bigint
  	          ) partitioning key (parent_ob_id,brand_id) using hashing
  	          with replace on commit preserve rows not logged in tbs_user_temp"

  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn
  #stat_ecustsvc_echl_tmp
  set sql_buf "declare global temporary table session.stat_ecustsvc_echl_tmp(
                  echl_type             smallint,
                  busi_code_type        smallint,
                  brand_id              bigint,
                  flow                  bigint,
                  product_no            varchar(15)
  	          ) partitioning key (echl_type,busi_code_type) using hashing
  	          with replace on commit preserve rows not logged in tbs_user_temp"

  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn
  set sql_buf "declare global temporary table session.stat_index_tmp(
        t_index_id  smallint
    )
    partitioning key(t_index_id) using hashing in tbs_user_temp
    with replace on commit preserve rows not logged"

	puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "errmsg:$errmsg"
		return -1
	}

	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
	  	trace_sql $errmsg 1300
	  	return -1
	  }
  aidb_commit $conn
  set sql_buf "insert into session.stat_index_tmp values
      (1),(2),(3),(4);"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "errmsg:$errmsg"
		return -1
	}

	aidb_close $handle
	if [catch {set handle [aidb_open $conn]} errmsg] {
	  	trace_sql $errmsg 1300
	  	return -1
	  }
  aidb_commit $conn
  set sql_buf "declare global temporary table session.product_brand_tmp(
                  t_index_id            smallint,
                  count                 bigint
  	          ) partitioning key (t_index_id) using hashing
  	          with replace on commit preserve rows not logged in tbs_user_temp"

  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn
  #===================================================================================================
  #Initialize Target_TMP_Table
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp
      select s_index_id,t_index_id,0
      from $stat_ecustsvc_0032_s,session.stat_index_tmp"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #===================================================================================================
  #Insert Data Into stat_ecustsvc_brand_tmp
  set sql_buf "insert into session.product_brand_tmp(t_index_id,count)
      select case when grouping(brand_id)=1 then 4 else brand_id end,sum(cnt)
      from (select case brand_id when 1 then 1 when 4 then 2 else 3 end as brand_id,
               count(distinct user_id) as cnt
            from $dw_product_yyyymm
            where userstatus_id in ($rep_online_userstatus_id) and test_mark <> 1 and usertype_id in (1,2,9)
            group by case brand_id when 1 then 1 when 4 then 2 else 3 end
           )a
      group by cube(brand_id) with ur"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn
  #===================================================================================================
  #Insert Data Into stat_ecustsvc_brand_tmp
  #parent_ob_id
  #21:���Ѳ�ѯ
  #22:ҵ������������ѣ�
  #23:���ֲ�ѯ
  #24:����Ͷ��
  #25:��վ(��½)
  #26:��վ����
  #27:��������pageview��
  #28:�ͻ�ƽ�����ҳ����
  #29:��������µ�½
  #30:���Ͻɷ�
  #is_success
  #0:�ǳɹ�
  #1:�ɹ�
  #2:ȫ��
  #===================================================================================================
  set sql_buf "insert into session.stat_ecustsvc_brand_tmp(parent_ob_id,brand_id,is_success,flow,count)
      select parent_ob_id,case brand_id when 1 then 1 when 4 then 2 else 3 end,is_success,
         sum(flow),sum(count)
      from (
             select b.parent_ob_id,a.brand_id,case when a.is_success in (0,1) or a.err_msg is null
             		or a.err_msg = 'action is null' or a.err_msg = '����ǰ��Ӫ�������޷�����ô���!'
             		or a.err_msg = '������ķ�������У��������벻��ȷ!' or a.err_msg = 'Broken pipe' then 1 else 0 end as is_success,
                count(1) as flow,count(distinct a.product_no) as count
             from $dw_ow_loading_log_yyyymm a,$dim_sys_object  b
             where  a.ob_type =1 and b.parent_ob_id in (21,23) and b.check_result = 5 and a.ob_id = b.ob_id
                and date(a.start_time)>='$p_optime' and date(a.start_time)<'$daynextmonth'
             group by b.parent_ob_id,a.brand_id,case when a.is_success in (0,1) or a.err_msg is null
             		or a.err_msg = 'action is null' or a.err_msg = '����ǰ��Ӫ�������޷�����ô���!'
             		or a.err_msg = '������ķ�������У��������벻��ȷ!' or a.err_msg = 'Broken pipe' then 1 else 0 end
             union all
             select 22 as parent_ob_id,b.brand_id,2 as is_success,count(1) as flow,0 as count
             from $dw_product_busi_dm_yyyymm a,$dw_product_yyyymm b
             where a.user_id=b.user_id and a.user_id in (
                select user_id from $dw_product_yyyymm
                where product_no in (select product_no from $dw_ow_loading_log_yyyymm
                                     where date(start_time)>='$p_optime' and date(start_time)<'$daynextmonth'
                                     ) and userstatus_id>0
                ) and a.so_mode ='15' and a.op_id=10000475
             group by b.brand_id
             union all
             select 22 as parent_ob_id,b.brand_id,1 as is_success,count(1) as flow,count(distinct a.user_id) as count
             from $dw_product_busi_dm_yyyymm a,$dw_product_yyyymm b
             where a.user_id=b.user_id and a.user_id in (
                select user_id from $dw_product_yyyymm
                where product_no in (select product_no from $dw_ow_loading_log_yyyymm
                                     where is_success in (0,1) and date(start_time)>='$p_optime' and date(start_time)<'$daynextmonth'
                                     ) and userstatus_id>0
                ) and a.so_mode ='15' and a.op_id=10000475
             group by b.brand_id
             union all
             select 24 as parent_ob_id,a.brand_id,2 as is_success,count(1) as flow,count(distinct a.product_no) as count
             from $dw_product_yyyymm a,$dwd_wcc_complain_info_yyyymm b
             where a.product_no=b.phone_id and a.userstatus_id>0 and b.complain_type=2
                and date(b.creat_date)>='$p_optime' and date(b.creat_date)<'$daynextmonth'
             group by a.brand_id
             union all
             select 25 as parent_ob_id,a.brand_id,1 as is_success,count(1) as flow,count(distinct login_product_no) as count
             from $dw_product_yyyymm a,$dw_wcc_user_action_yyyymm b
             where b.login_product_no=a.product_no and b.login_type=1 and b.is_success=1
                and a.userstatus_id>0
                and date(b.login_time)>='$p_optime' and date(b.login_time)<'$daynextmonth'
             group by a.brand_id
             union all
             select 26 as parent_ob_id,a.brand_id,2 as is_success,count(1) as flow,0 as count
             from $dw_wcc_user_action_yyyymm a
             where date(a.login_time)>='$p_optime' and date(a.login_time)<'$daynextmonth'
             group by a.brand_id
             union all
             select 27 as parent_ob_id,a.brand_id,2 as is_success,count(1) as flow,0 as count
             from $dw_product_yyyymm a,$dw_ow_loading_log_yyyymm b
             where b.product_no=a.product_no and a.userstatus_id>0
                and date(b.start_time)>='$p_optime' and date(b.start_time)<'$daynextmonth'
             group by a.brand_id
             union all
             select 28 as parent_ob_id,brand_id,2 as is_success,count(1) as flow,count(distinct product_no) as count
             from $dw_ow_loading_log_yyyymm
             where date(start_time)>='$p_optime' and date(start_time)<'$daynextmonth'
             group by brand_id
             union all
             select 29 as parent_ob_id,a.brand_id,1 as is_success,0 as flow,count(distinct login_product_no) as count
		         from (select product_no, userstatus_id,brand_id from $dw_product_yyyymm_2
	  		      	union select product_no, userstatus_id,brand_id from $dw_product_yyyymm_1
	  		      	union select product_no, userstatus_id,brand_id from $dw_product_yyyymm) a, (
	  		      	select * from $dw_wcc_user_action_yyyymm_2
	  		      	union select * from $dw_wcc_user_action_yyyymm_1
	  		      	union select * from $dw_wcc_user_action_yyyymm
	  		      	) b
		         where b.login_product_no=a.product_no and b.login_type=1 and b.is_success=1 and a.userstatus_id>0
		         and date(b.login_time)>='$dayprev2month' and date(b.login_time)<'$daynextmonth'
		         group by a.brand_id
             union all
             select 30 as parent_ob_id,b.brand_id,1 as is_success,count(1) as flow,count(distinct a.user_id) as count
             from $dw_acct_payitem_dm_yyyymm a,$dw_product_yyyymm b
             where a.user_id=b.user_id and a.paytype_id in ('4468')
             group by b.brand_id
            )a
      group by parent_ob_id,case brand_id when 1 then 1 when 4 then 2 else 3 end,is_success
      with ur"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn
  #===================================================================================================
  #Insert Data Into stat_ecustsvc_echl_tmp
  #echl_type:
  #1.����
  #2.��վ
  #3.����
  #4.ʵ����������
  #5.WAP
  #6.�����ն�
  #busi_code_type
  #21.���Ѳ�ѯ
  #24.ҵ������Զ���/ҵ����������ѣ�
  #23.���ֲ�ѯ
  #22.ҵ������˹���/ҵ������������ѣ�
  set sql_buf "insert into session.stat_ecustsvc_echl_tmp(echl_type,busi_code_type,brand_id,flow,product_no)
      select echl_type,busi_code_type,case brand_id when 1 then 1 when 4 then 2 else 3 end,
         sum(flow),product_no
      from (
            select 1 as echl_type,busi_code_type,brand_id,product_no,sum(flow) as flow
            from (
				  select 21 as busi_code_type, brand as brand_id, count(*) as flow, calling as product_no
				  from $dw_custsvc_ivr_log_dm_yyyymm
				  where operation_code = '1008611' or operation_code = '20102'
				  group by brand, calling
				  union all
				  select 23 as busi_code_type, brand as brand_id, count(*) as flow, calling as product_no
				  from $dw_custsvc_ivr_log_dm_yyyymm  
				  where operation_code = '20106'
				  group by brand, calling
				  union all
				  select 24 as busi_code_type, brand as brand_id, count(*) as flow, calling as product_no
				  from $dw_custsvc_ivr_log_dm_yyyymm 
				  where operation_type <> 6
				  group by brand, calling
            	  union all
				  select 22 as busi_code_type, case when brand_name like '%ȫ��ͨ%' then 1
								  					when brand_name like '%���еش�%' then 4 else 5 end as brand_id, 
						 count(*) as flow, bill_id as product_no
				  from $dw_product_ord_so_log_dm_yyyymm
				  where length(trim(char(op_id))) = 4 
				  and busi_id in (
				  select business_id from $ods_dim_so_busioptcode_mapping_yyyymmdd where opt_name not like '%��ѯ%')
				  group by case when brand_name like '%ȫ��ͨ%' then 1
								when brand_name like '%���еش�%' then 4 else 5 end, bill_id
                 )a
            group by busi_code_type,brand_id,product_no
            union all
            select 2 as echl_type,busi_code_type,brand_id,product_no,sum(flow) as flow
            from (select b.parent_ob_id as busi_code_type,a.brand_id,
                      count(1) as flow,a.product_no
                   from $dw_ow_loading_log_yyyymm a,$dim_sys_object  b
                   where a.ob_type =1 and b.parent_ob_id in (21,23) and b.check_result = 5 and a.ob_id = b.ob_id 
                      and date(a.start_time)>='$p_optime' and date(a.start_time)<'$daynextmonth'
					  and (a.is_success in (0,1) or a.err_msg is null 
             			   or a.err_msg = 'action is null' or a.err_msg = '����ǰ��Ӫ�������޷�����ô���!' 
             			   or a.err_msg = '������ķ�������У��������벻��ȷ!' or a.err_msg = 'Broken pipe')
                   group by b.parent_ob_id,a.brand_id,a.product_no
                   union all
                   select 22 as busi_code_type,b.brand_id,count(1) as flow,b.product_no
                   from $dw_product_busi_dm_yyyymm a,$dw_product_yyyymm b
                   where a.user_id=b.user_id and a.user_id in (
                      select user_id from $dw_product_yyyymm 
                      where product_no in (select product_no from $dw_ow_loading_log_yyyymm
                                           where is_success in (0,1) and date(start_time)>='$p_optime' and date(start_time)<'$daynextmonth'
                                           ) and userstatus_id>0  
                      ) and a.so_mode ='15' and a.op_id=10000475
                      and date(a.so_date)>='$p_optime' and date(a.so_date)<'$daynextmonth'
                   group by b.brand_id,b.product_no
                   union all
                   select 24 as busi_code_type,b.brand_id,count(1) as flow,b.product_no
                   from $dw_acct_payitem_dm_yyyymm a,$dw_product_yyyymm b
                   where a.user_id=b.user_id and a.paytype_id in ('4468')
                   group by b.brand_id,b.product_no
                  )a
            group by busi_code_type,brand_id,product_no
            union all
            select 3 as echl_type,busi_code_type,brand_id,phone_id as product_no,sum(flow) as flow
            from (select case when c.result_type in ('CXYE','CXZD','CXZE','CXKCYE') then 21 
	   						  else 23 end as busi_code_type, a.brand_id,
       					 count(1) as flow, b.phone_id
				  from $dw_product_yyyymm a, $dw_kf_sms_cmd_receive_dm_yyyymm b, $dw_kf_cmd_hint_def_dm_yyyymm c
				  where a.product_no  = b.phone_id and b.cmd_id = c.cmd_id and b.deal_result = c.cmd_result
				  and replace(char(c.op_time),'-','') = '$endday' and c.result_jieg in (0)
				  and c.result_type in ('CXYE','CXZD','CXZE','CXKCYE','CXJF')
				  and a.userstatus_id in ($rep_online_userstatus_id) and a.test_mark <> 1 and a.usertype_id in (1,2,9)
				  group by case when c.result_type in ('CXYE','CXZD','CXZE','CXKCYE') then 21 
	   		  			  		else 23 end, a.brand_id, b.phone_id
				  union all
				  select 22 as busi_code_type, a.brand_id,
				  		 count(1) as flow, b.phone_id
				  from $dw_product_yyyymm a, $dw_kf_sms_cmd_receive_dm_yyyymm b, $dw_kf_cmd_hint_def_dm_yyyymm c
				  where a.product_no  = b.phone_id and b.cmd_id = c.cmd_id and b.deal_result = c.cmd_result
				  and replace(char(c.op_time),'-','') = '$endday' and c.result_jieg in (0)
				  and c.result_type in (select sms_cmd from stat_ecustsvc_sms_cmd)
				  and a.userstatus_id in ($rep_online_userstatus_id) and a.test_mark <> 1 and a.usertype_id in (1,2,9)
				  group by a.brand_id, b.phone_id) a
            group by busi_code_type,brand_id,phone_id
            union all
			select 5 as echl_type,22 as busi_code_type,b.brand_id,b.product_no, count(1) as flow
			from $dw_product_ord_cust_dm_yyyymm a left join $dw_product_yyyymm b on a.product_instance_id=b.user_id
			where channel_type='6' and source_system_id in (3) and business_id=193000000001 and old_so_order_id is null
			group by b.brand_id, b.product_no
            union all
            select 6 as echl_type,24 as busi_code_type,a.brand_id,a.product_no,count(1) as flow
            from $dw_product_yyyymm a,$dw_acct_payitem_dm_yyyymm b
            where a.user_id=b.user_id and b.paytype_id in ('4162','4864') and b.rec_sts=0
            group by a.brand_id,a.product_no
            union all
            select 6 as echl_type,22 as busi_code_type,a.brand_id,a.product_no,count(1) as flow
            from $dw_product_yyyymm a,$dw_product_busi_dm_yyyymm b
            where a.user_id=b.user_id and b.so_mode='5' and b.op_id in (select op_id from $dim_boss_staff where op_name like '%����%')
            and b.process_id not in (2,3,22,14,24,12,13)
            group by a.brand_id,a.product_no
           )a
      group by echl_type,busi_code_type,case brand_id when 1 then 1 when 4 then 2 else 3 end,product_no"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn
  set sql_buf "insert into session.stat_ecustsvc_echl_tmp(echl_type,busi_code_type,brand_id,flow,product_no)
      select 4 as echl_type,24 as busi_code_type,case a.brand_id when 1 then 1 when 4 then 2 else 3 end as brand_id,
         count(1) as flow,a.product_no
      from $dw_product_yyyymm a,$dw_acct_payitem_dm_yyyymm b, $dim_pub_channel c
      where a.user_id=b.user_id and b.so_channel_id = c.channel_id
      and b.paytype_id in ('4101','4801') and c.channeltype_id in (1,2,3,4,12)
      group by case a.brand_id when 1 then 1 when 4 then 2 else 3 end,a.product_no
      union all
      select 4 as echl_type,22 as busi_code_type,case b.brand_id when 1 then 1 when 4 then 2 else 3 end as brand_id,
         count(1) as flow,b.product_no
      from $dw_product_busi_dm_yyyymm a,$dw_product_yyyymm b, $dim_pub_channel c
      where a.user_id=b.user_id and a.so_mode in('0','1','2') and a.so_org_id=c.channel_id
         and a.busi_code not in (1,4,5,7,1318,1315,201,1009,1017,1327,1101,205,204,2804,2808,2824,2842,2846,1005,2822,2844,2848,1018,205,1020,1010,204,1019,1326,1034,1035,205,2806,201,204,201,2802,2832,1655,1096)
         and a.busi_code not between 9000 and 9020 and a.op_id<>9999 and a.op_id not in (1,800)
         and c.channeltype_id in (1,2,3,4,12)
      group by case b.brand_id when 1 then 1 when 4 then 2 else 3 end,b.product_no
      with ur"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn
  #===================================================================================================
  #Initialize Target_TMP_Table
  #Insert Data Into stat_ecustsvc_0032_tmp
  #===================================================================================================
  #154.��¼�ͻ���
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 154,
         case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,sum(count)
      from session.stat_ecustsvc_brand_tmp
      where parent_ob_id = 25
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #107.���Ѳ�ѯ��������
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 107,
         case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,sum(flow)
      from session.stat_ecustsvc_brand_tmp
      where parent_ob_id = 21
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #101.���Ѳ�ѯ����
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 101,
         case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,sum(flow)
      from session.stat_ecustsvc_brand_tmp
      where parent_ob_id = 21 and is_success = 1
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #102.ռ����������
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 102,a.t_index_id,case when b.v2=0 then 0 else round(100*float(a.v1)/b.v2,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=101 group by t_index_id
           )a,
           (select case when grouping(brand_id)=1 then 4 else brand_id
               end as t_index_id,sum(flow) as v2 from session.stat_ecustsvc_echl_tmp
            where echl_type in (1,2,3,5,6) and busi_code_type=21 group by cube(brand_id)
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #103.�˾���
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 103,a.t_index_id,case when v2=0 then 0 else round(float(a.v1)/b.v2,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=101 group by t_index_id
           )a,
           (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=154 group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #104.ռ�ܷ��ʴ���֮��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 104,a.t_index_id,case when b.v1=0 then 0 else round(float(a.v2)*100/b.v1,2) end
      from (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=101
            group by t_index_id
            )a,
           (select sum(flow) as v1
            from session.stat_ecustsvc_brand_tmp
            where parent_ob_id = 26
           )b"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #105.�ɹ���
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 105,a.t_index_id,case when b.v2=0 then 0 else round(float(a.v1)*100/b.v2,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=101
            group by t_index_id
            )a,
           (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=107
            group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #106.�����������Ѳ�ѯ����
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 106,
	    case when grouping(brand_id)=1 then 4 else brand_id
               end as t_index_id,sum(flow)
      from session.stat_ecustsvc_echl_tmp
      where echl_type in (1,2,3,5,6) and busi_code_type=21
      group by cube(brand_id)"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #114.���ֲ�ѯ��������
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 114,
         case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,sum(flow)
      from session.stat_ecustsvc_brand_tmp
      where parent_ob_id = 23
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #108.���ֲ�ѯ����
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 108,
         case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,sum(flow)
      from session.stat_ecustsvc_brand_tmp
      where parent_ob_id = 23 and is_success = 1
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #109.ռ����������
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 109,a.t_index_id,case when b.v2=0 then 0 else round(100*float(a.v1)/b.v2,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=108 group by t_index_id
           )a,
           (select case when grouping(brand_id)=1 then 4 else brand_id
               end as t_index_id,sum(flow) as v2 from session.stat_ecustsvc_echl_tmp
            where echl_type in (1,2,3,5,6) and busi_code_type=23 group by cube(brand_id)
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #110.�˾���
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 110,a.t_index_id,case when b.v2=0 then 0 else round(float(a.v1)/b.v2,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=108 group by t_index_id
           )a,
           (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=154 group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #111.ռ�ܷ��ʴ���֮��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 111,a.t_index_id,case when b.v1=0 then 0 else round(float(a.v2)*100/b.v1,2) end
      from (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=108
            group by t_index_id
            )a,
           (select sum(flow) as v1
            from session.stat_ecustsvc_brand_tmp
            where parent_ob_id = 26
           )b"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #112.�ɹ���
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 112,a.t_index_id,case when b.v2=0 then 0 else round(float(a.v1)*100/b.v2,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=108
            group by t_index_id
            )a,
           (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=114
            group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #113.�����������Ѳ�ѯ����
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 113,
	    case when grouping(brand_id)=1 then 4 else brand_id
               end as t_index_id,sum(flow)
      from session.stat_ecustsvc_echl_tmp
      where echl_type in (1,2,3,5,6) and busi_code_type=23
      group by cube(brand_id)"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #123.ҵ�����������(������)
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 123,
         case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,sum(flow)
      from session.stat_ecustsvc_brand_tmp
      where (parent_ob_id = 22 and is_success = 2) or (parent_ob_id=30)
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #115.ҵ���������
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 115,
         case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,sum(flow)
      from session.stat_ecustsvc_brand_tmp
      where (parent_ob_id = 22 and is_success = 1) or (parent_ob_id=30)
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #116.ռ����������
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 116,a.t_index_id,case when b.v2=0 then 0 else round(100*float(a.v1)/b.v2,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=115 group by t_index_id
           )a,
           (select case when grouping(brand_id)=1 then 4 else brand_id
               end as t_index_id,sum(flow) as v2 from session.stat_ecustsvc_echl_tmp
            where echl_type in (1,2,3,5,6) and busi_code_type in (22,24) group by cube(brand_id)
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #117.ռ����������
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 117,a.t_index_id,case when b.v2=0 then 0 else round(100*float(a.v1)/b.v2,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=115 group by t_index_id
           )a,
           (select case when grouping(brand_id)=1 then 4 else brand_id
               end as t_index_id,sum(flow) as v2 from session.stat_ecustsvc_echl_tmp
            where busi_code_type in (22,24) group by cube(brand_id)
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #118.�˾���
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 118,a.t_index_id,case when b.v2=0 then 0 else round(float(a.v1)/b.v2,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=115 group by t_index_id
           )a,
           (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=154 group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #119.ռ�ܷ��ʴ���֮��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 119,a.t_index_id,case when b.v1=0 then 0 else round(float(a.v2)*100/b.v1,2) end
      from (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=115
            group by t_index_id
            )a,
           (select sum(flow) as v1
            from session.stat_ecustsvc_brand_tmp
            where parent_ob_id = 26
           )b"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #120.�ɹ���
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 120,a.t_index_id,case when b.v2=0 then 0 else round(float(a.v1)*100/b.v2,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=115
            group by t_index_id
            )a,
           (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=123
            group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #121.��������ҵ���������
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 121,
	    case when grouping(brand_id)=1 then 4 else brand_id
               end as t_index_id,sum(flow)
      from session.stat_ecustsvc_echl_tmp
      where echl_type in (1,2,3,5,6) and busi_code_type in (22,24)
      group by cube(brand_id)"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #122.��������ҵ���������
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 122,
	    case when grouping(brand_id)=1 then 4 else brand_id
               end as t_index_id,sum(flow)
      from session.stat_ecustsvc_echl_tmp
      where busi_code_type in (22,24)
      group by cube(brand_id)"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #127.Ͷ�ߴ���
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 127,
         case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,sum(flow)
      from session.stat_ecustsvc_brand_tmp
      where parent_ob_id = 24
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #128.Ͷ�߿ͻ���
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 128,
         case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,sum(count)
      from session.stat_ecustsvc_brand_tmp
      where parent_ob_id = 24
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #129.�˾�Ͷ����
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 129,a.t_index_id,case when b.v2=0 then 0 else round(float(a.v1)/b.v2,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=127
            group by t_index_id
            )a,
           (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=128
            group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #130.EMAIL��������
#  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
#      select 130,case when grouping(brand_id)=1 then 4 else brand_id end,sum(cnt)
#      from (select case b.brand_id when 1 then 1 when 4 then 2 else 3 end as brand_id, count(1) as cnt
#            from $dw_ow_send_mail_yyyymm a, $dw_product_yyyymm b
#            where a.phone_id = b.product_no
#			and b.userstatus_id in (1,2,3,6) and b.usertype_id in (1,2,9)
#			group by b.brand_id
#           )a
#      group by cube(brand_id)"
#  puts $sql_buf
#	if [catch {aidb_sql $handle $sql_buf} errmsg] {
#		trace_sql $errmsg 1300
#		puts "Errmsg:$errmsg"
#		return -1
#	}
#	aidb_close $handle
#  if [catch {set handle [aidb_open $conn]} errmsg] {
#      trace_sql $errmsg 1302
#      return -1
#    }
#  aidb_commit $conn
#
#  #131.EMAIL���Ϳͻ���
#  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
#      select 131,case when grouping(brand_id)=1 then 4 else brand_id end,sum(cnt)
#      from (select case b.brand_id when 1 then 1 when 4 then 2 else 3 end as brand_id, count(distinct a.phone_id) as cnt
#           from $dw_ow_send_mail_yyyymm a, $dw_product_yyyymm b
#            where a.phone_id = b.product_no
#			and b.userstatus_id in (1,2,3,6) and b.usertype_id in (1,2,9)
#			group by b.brand_id
#           )a
#      group by cube(brand_id)"
#  puts $sql_buf
#	if [catch {aidb_sql $handle $sql_buf} errmsg] {
#		trace_sql $errmsg 1300
#		puts "Errmsg:$errmsg"
#		return -1
#	}
#	aidb_close $handle
#  if [catch {set handle [aidb_open $conn]} errmsg] {
#      trace_sql $errmsg 1302
#      return -1
#    }
#  aidb_commit $conn

  #132.ƽ��ÿ�ͻ������ʼ���
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 132,a.t_index_id,case when b.v2=0 then 0 else round(float(a.v1)/b.v2,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=130 group by t_index_id
           )a,
           (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=131 group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #141.�ź�����
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 141,case when grouping(brand_id)=1 then 4 else brand_id end,sum(cnt)
      from (select 1 as brand_id,count(1) as cnt
            from bass2.dwd_book_msisdn_$endday
            where substr(char(date(so_date)),1,7) = '[string range $p_optime 0 6]' and op_id=8000 and msisdn_sts=3
           )a
      group by cube(brand_id)"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #142.���������ź�����
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 142,case when grouping(a.brand_id)=1 then 4 else a.brand_id end,sum(a.cnt)+sum(b.cnt)
      from (select case brand_id when 1 then 1 when 4 then 2 else 3 end as brand_id,count(1) as cnt
            from $dw_product_yyyymm a,$dim_pub_channel b
            where a.channel_id=b.channel_id
            and b.channeltype_id in (1,2,3,4,12) and a.month_new_mark=1
            group by a.brand_id
           )a,
           (select 1 as brand_id,count(1) as cnt
            from bass2.dwd_book_msisdn_$endday
            where substr(char(date(so_date)),1,7) = '[string range $p_optime 0 6]' and op_id=8000 and msisdn_sts=3
           )b
      where a.brand_id = b.brand_id
      group by cube(a.brand_id)"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #143.ռ�ź�����֮��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 143,a.t_index_id,case when b.v2=0 then 0 else round(100*float(a.v1)/b.v2,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=141 group by t_index_id
           )a,
           (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=142 group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #150.������(pageview)
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 150,
         case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,sum(flow)
      from session.stat_ecustsvc_brand_tmp
      where parent_ob_id = 27
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #153.�ͻ�ƽ�����ҳ����
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 153,
         case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,case when sum(count)=0 then 0 else round(float(sum(flow))/sum(count),2) end
      from session.stat_ecustsvc_brand_tmp
      where parent_ob_id = 28
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #155.��½��վ����
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 155,
         case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,sum(flow)
      from session.stat_ecustsvc_brand_tmp
      where parent_ob_id = 25
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #156.�ͻ�ƽ����½����
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 156,
         case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,case when sum(count)=0 then 0 else round(float(sum(flow))/sum(count),2) end
      from session.stat_ecustsvc_brand_tmp
      where parent_ob_id = 25
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #157.�����ͻ���
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 157,t_index_id,sum(count)
      from session.product_brand_tmp
      group by t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #158.��½�ͻ�ռ�����ͻ���֮��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 158,a.t_index_id,case when b.v1=0 then 0 else round(float(a.v2)*100/b.v1,2) end
      from (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=154
            group by t_index_id
            )a,
           (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=157
            group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #159.��������µ�½�ͻ���
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 159,
         case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,sum(count)
      from session.stat_ecustsvc_brand_tmp
      where parent_ob_id = 29
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #160.����������
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      values(160,4,470000)"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #161.��Ծ�ͻ�ռ��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 161,t_index_id,round(float(result)*100/470000,2)
      from session.stat_ecustsvc_0032_tmp
      where s_index_id=159 and t_index_id = 4"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #162.���Ѳ�ѯ�ͻ���
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 162,
         case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,sum(count)
      from session.stat_ecustsvc_brand_tmp
      where parent_ob_id = 21 and is_success = 1
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #163.ռ��¼�ͻ���֮��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 163,a.t_index_id,case when a.v1=0 then 0 else round(float(b.v2)*100/a.v1,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=154
            group by t_index_id
            )a,
           (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=162
            group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #164.ռ����������֮��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 164,t_index_id,round(float(result)*100/470000,2)
      from session.stat_ecustsvc_0032_tmp
      where s_index_id=162 and t_index_id = 4"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #165.ռ�����ͻ���֮��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 165,a.t_index_id,case when b.v1=0 then 0 else round(float(a.v2)*100/b.v1,2) end
      from (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=162
            group by t_index_id
            )a,
           (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=157
            group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #166.���ֲ�ѯ�ͻ���
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 166,
         case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,sum(count)
      from session.stat_ecustsvc_brand_tmp
      where parent_ob_id = 23 and is_success = 1
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #167.ռ��¼�ͻ���֮��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 167,a.t_index_id,case when a.v1=0 then 0 else round(float(b.v2)*100/a.v1,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=154
            group by t_index_id
            )a,
           (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=166
            group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #168.ռ����������֮��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 168,t_index_id,round(float(result)*100/470000,2)
      from session.stat_ecustsvc_0032_tmp
      where s_index_id=166 and t_index_id = 4"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #169.ռ�����ͻ���֮��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 169,a.t_index_id,case when b.v1=0 then 0 else round(float(a.v2)*100/b.v1,2) end
      from (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=166
            group by t_index_id
            )a,
           (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=157
            group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #170.ҵ�����ͻ���
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 170,case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,count(distinct a.user_id)
      from (select case b.brand_id when 1 then 1 when 4 then 2 else 3 end as brand_id,a.user_id
            from $dw_product_busi_dm_yyyymm a,$dw_product_yyyymm b
            where a.user_id=b.user_id and a.user_id in (
               select user_id from $dw_product_yyyymm
               where product_no in (select product_no from $dw_ow_loading_log_yyyymm
                                    where is_success in (0,1) and date(start_time)>='$p_optime' and date(start_time)<'$daynextmonth'
                                    ) and userstatus_id>0
               ) and a.so_mode ='15' and a.op_id=10000475
            union all
            select case b.brand_id when 1 then 1 when 4 then 2 else 3 end as brand_id,a.user_id
            from $dw_acct_payitem_dm_yyyymm a,$dw_product_yyyymm b
            where a.user_id=b.user_id and a.paytype_id in ('4468')
           )a
      group by cube(brand_id) "
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #171.ռ����������
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 171,a.t_index_id,case when b.v2=0 then 0 else round(100*float(a.v1)/b.v2,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=170 group by t_index_id
           )a,
           (select case when grouping(brand_id)=1 then 4 else brand_id
               end as t_index_id,count(distinct product_no) as v2 from session.stat_ecustsvc_echl_tmp
            where echl_type in (1,2,3,5,6) and busi_code_type in (22,24) group by cube(brand_id)
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #172.��������ҵ�����ͻ���
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 172,case when grouping(brand_id)=1 then 4 else brand_id
            end as t_index_id,count(distinct product_no) from session.stat_ecustsvc_echl_tmp
            where echl_type in (1,2,3,5,6) and busi_code_type in (22,24)
            group by cube(brand_id)"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #173.ռ��¼�ͻ���֮��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 173,a.t_index_id,case when a.v1=0 then 0 else round(float(b.v2)*100/a.v1,2) end
      from (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=154
            group by t_index_id
            )a,
           (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=170
            group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #174.ռ����������֮��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 174,t_index_id,round(float(result)*100/470000,2)
      from session.stat_ecustsvc_0032_tmp
      where s_index_id=170 and t_index_id = 4"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #175.ռ�����ͻ���֮��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 175,a.t_index_id,case when b.v1=0 then 0 else round(float(a.v2)*100/b.v1,2) end
      from (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=170
            group by t_index_id
            )a,
           (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=157
            group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn
  #133	ҵ����	���Ͻ���	�����ܶԪ��	��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 133,case when grouping(a.brand_id)=1 then 4 else a.brand_id end,sum(recv_cash)
      from (select case b.brand_id when 1 then 1 when 4 then 2 else 3 end as brand_id,sum(recv_cash) as recv_cash
            from $dw_acct_payitem_dm_yyyymm a,$dw_product_yyyymm b
            where a.user_id=b.user_id and a.paytype_id in ('4468')
            group by case b.brand_id when 1 then 1 when 4 then 2 else 3 end
           )a
      group by cube(a.brand_id)"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
  #134	ҵ����	���Ͻ���	���ѱ���	��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 134,case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,sum(flow)
      from session.stat_ecustsvc_brand_tmp
      where parent_ob_id = 30
      group by cube(brand_id)"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
  #135	ҵ����	���Ͻ���	ÿ�ʽ��ѽ�Ԫ��	��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 135,a.t_index_id,case when b.v1=0 then 0 else round(float(a.v2)/b.v1,2) end
      from (select t_index_id,sum(result) as v2 from session.stat_ecustsvc_0032_tmp
            where s_index_id=133
            group by t_index_id
            )a,
           (select t_index_id,sum(result) as v1 from session.stat_ecustsvc_0032_tmp
            where s_index_id=134
            group by t_index_id
           )b
      where a.t_index_id=b.t_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
  #136	ҵ����	���Ͻ���	���Ͻ��ѿͻ���	��
  set sql_buf "insert into session.stat_ecustsvc_0032_tmp(s_index_id,t_index_id,result)
      select 136,case when grouping(brand_id)=1 then 4 else brand_id
         end as t_index_id,sum(count)
      from session.stat_ecustsvc_brand_tmp
      where parent_ob_id = 30
      group by cube(brand_id)"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}

  #Summary Of Sub_value(�ϼƷ�**����)
  #===================================================================================================
  #Sumary Of All_value(�ϼ�ȫ��890����)
  #===================================================================================================
	#Clear Target_Table Value Of This Cycle
	set sql_buf "delete from $stat_ecustsvc_0032 where time_id=$op_time "
	puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn

  #===================================================================================================
	#Insert Data To Target_Table From Target_TMP_Table
	set sql_buf "insert into $stat_ecustsvc_0032
      select $op_time,s_index_id,sum(case when t_index_id=1 then result end),sum(case when t_index_id=2 then result end),
         sum(case when t_index_id=3 then result end),0,0,sum(case when t_index_id=4 then result end)
      from session.stat_ecustsvc_0032_tmp
      group by s_index_id"
  puts $sql_buf
	if [catch {aidb_sql $handle $sql_buf} errmsg] {
		trace_sql $errmsg 1300
		puts "Errmsg:$errmsg"
		return -1
	}
	aidb_close $handle
  if [catch {set handle [aidb_open $conn]} errmsg] {
      trace_sql $errmsg 1302
      return -1
    }
  aidb_commit $conn
	return 0
}