######################################################################################################
#�ӿ����ƣ�TD�ͻ��»��ܵڶ�����
#�ӿڱ��룺ÿ��ͳ��
#�ӿ�˵������¼ͳ��������TD�ͻ�������Ϣ������TD����ͻ�����TD�ͻ��Ʒ�ʱ����TD�ͻ�����������TD�ͻ������������
#��������: g_s_22204_month_two.tcl
#��������: ÿ��������TD�ͻ�ָ���Լ�����,��ɶ��������ݵ�ץȡ�������м�� g_s_22204_month_tmp3
#��������: ��
#Դ    ��1.
#          2.
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�
#��дʱ�䣺2010-06-23
#�����¼��1.
#�޸���ʷ:   2010-8-30    liuzhilong �޸�tac��Ϊ��DIM_TERM_TAC��ȡ
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymm
	set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
	puts $op_month
	
	set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	#�������һ�� yyyymmdd
	set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
	puts $this_month_last_day
	
	#���µ�һ�� yyyymmdd
	set this_month_first_day [string range $op_month 0 5]01
	puts $this_month_first_day

    #�ϸ��� yyyymm
    set last_month [GetLastMonth [string range $op_month 0 5]]
    puts $last_month
    
    #�ϸ��µĵ�һ�� yyyymmdd
    set two "01"
    set last_month_day  ${last_month}${two}
    puts $last_month_day    
    
    #���ϸ��� yyyymm
    set last_last_month [GetLastMonth [string range $last_month 0 5]]
    puts $last_last_month
    
    #���ϸ��µĵ�һ�� yyyymmdd
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


    #�����ʱ���3
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



    #ȡ���1���µ�ͨ����������0���û��ն�ʹ�������02047�ӿڣ�����GPRS�嵥�������������嵥����һ��04002�ӿڣ���
    #��������IMEI�ֹ���TD�նˣ�����IMEI��ǰ6λ��ǰ8λ���뼯��ÿ���·��ġ�TAC�����ն˵Ķ�Ӧ��ϵ��ȫ�����е�TAC�������ȣ�
    #�õ��ն��豸��ʶ���ٹ��� ���ն�������Ϣ������ȡ����ֵΪ001001006��2GHz����001012004��TD-SCDMA/GSM˫ģ�����ն��豸��ʶ��
    #��ΪTD�նˣ����γɣ����룬substr(IMEI,1,14) as IMEI_14��sum(ͨ�Ŵ���) as ͨ�Ŵ���_SUM�ı�1��
    #������04002��SUM ��������������02047��SUMͨ�����������������Ϊͨ�Ŵ���_SUM��
    
    #�����ʱ��
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



    #������ʱ��bass1.g_s_04002_day_td
    set handle [aidb_open $conn]
    
#2011-03-31 12:26:49 
#1.ȥ������bass1.G_S_04002_DAY_20100501bak ���Ӿ�
#2.ȡ���Ӳ�ѯ
    
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


    #������ʱ��bass1.g_s_04002_day_td
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

    #�����ʱ��
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


    #������ʱ��bass1.td_check_01
    set handle [aidb_open $conn]
# 2010-8-30    liuzhilong �޸�tac��Ϊ��DIM_TERM_TAC��ȡ
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


    #�Ա�1���ն˺��û�����˫�����أ�����IMEI_14���飬��ͨ�Ŵ���_SUM�ϼƴӴ�С����ȡÿ��������ͨ�Ŵ���_SUM�ϼ�
    #�����û����룬��ѡȡͬһ�ն��У�ͨ�Ŵ��������û������ͬһ�նˣ����ڶ��ͨ�������������ȵ��û�����ȡ�û�
    #��������һ����¼��Ȼ��������������У��������ͬһ���û������Ӧ����ն˵��������ȡ�û�ͨ����������һ��
    #�ն���Ϊ���նˣ����ͬһ�û����룬���ڶ��ͨ�������������ȵ��նˣ�ȡIMEI_14����һ����¼(���ٰ����û�������飬
    #��ͨ�������ϼƴӴ�С����ȡÿ��������ͨ�������ϼ�����IMEI_14��������ڶ��ͨ�����������նˣ�ȡIMEI_14���
    #��һ����¼)�����ռ����һ��TD�ͻ�
    
    #�����ʱ��
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


    #������ʱ��bass1.td_check_ls_02
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


    #ȡ����ʹ��TD����Ŀͻ���ȡ���1���µ�TD�����嵥��һ��04017�ӿڣ���GPRS�嵥�������������嵥����һ��04002�ӿڣ���
    #�������ʶΪTD��������м�¼���γɣ����룬substr(IMEI,1,14) as IMEI_14��sum(��������) as ͨ�Ŵ���_SUM�ı�2_TMP��
    #��ȡ��2_TMP�к��벻�ڱ�1�����е����м�¼�γɱ�2����ʱIMEI_14����Ϊ�ջ������IMEI������ȫ1��1357924680�ȣ�
    #�ұ�2��������ڱ�1��

    #�����ʱ��
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

    #�����ʱ��, Ϊ����T����־��׼�������Զ���һ���м��
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


    #������ʱ��bass1.g_s_04002_day_td1
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

    #ץȡTD GPRS�û����
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

    #�м�����ݲ���Ŀ����ʱ��
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



    #�����ʱ��
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


    #������ʱ��bass1.g_s_04002_day_td2
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



    #�����ʱ��
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


    #������ʱ��bass1.td_check_02
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

    
    #�Ա�2��IMEI_14��35��86��35��86��ͷ��IMEI������IMEI����ͷ�ļ�¼�����ն˺��û�����˫�����أ�����IMEI_14���飬
    #��ͨ�Ŵ���_SUM�ϼƴӴ�С����ȡÿ��������ͨ�Ŵ���_SUM�ϼ������û����룬��ѡȡͬһ�ն��У�ͨ�Ŵ��������û���
    #���ͬһ�նˣ����ڶ��ͨ�������������ȵ��û�����ȡ�û���������һ����¼��Ȼ��������������У��������ͬһ
    #���û������Ӧ����ն˵��������ȡ�û�ͨ����������һ���ն���Ϊ���նˣ����ͬһ�û����룬���ڶ��ͨ���������
    #����ȵ��նˣ�ȡIMEI_14����һ����¼(���ٰ����û�������飬��ͨ�������ϼƴӴ�С����ȡÿ��������ͨ�������ϼ�
    #����IMEI_14��������ڶ��ͨ�����������նˣ�ȡIMEI_14����һ����¼)�����ռ����һ��TD�ͻ���
    #
    #�Ա�2��IMEI_14����35��86��ͷ�ļ�¼����IMEIΪ�գ������û�����������أ����ռ����һ��TD�ͻ�

    #�����ʱ��
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


    #������ʱ��bass1.td_check_03
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


    #�����ʱ��
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


    #������ʱ��bass1.td_check_04
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

    
    #ȡ����1���µ�������������������������GPRS����������CMLAP����һ��04018�ӿڣ���ͳ������֮��>0�������û����󶨺��룩��

    #�����ʱ��
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


    #������ʱ��bass1.td_check_05
    set handle [aidb_open $conn]
#1.ȥ����bass1.G_S_04018_DAY_20100501bak���Ӿ�
#2.ȥ���Ӳ�ѯ        
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


    #������ÿ��Ŷβ���147345��147349�����������ͻ���־����Ϊ1��������ÿ��Ŷ���147345��147349����Ϊ�������ն˱�־����Ϊ1
    #�����ʱ��
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


    #������ʱ��bass1.td_check_05_2
    set handle [aidb_open $conn]
#1.ȥ����bass1.G_S_04018_DAY_20100501bak���Ӿ�
#2.ȥ���Ӳ�ѯ        
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
    
    #���������ն˱�ʶ
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
    
    #�����������ͻ���ʶ
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

    
    #���ܵ�2������4������5���Ŀͻ���ϣ����������ͻ����µ������ͻ��ھ�������������Ԥ�������뱣���ں����ݿ�����
    #�������յ�TD�ͻ�����2010�¿ھ���
    #�����ʱ��    
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


    #������ʱ��bass1.td_check_06
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


    #TD�ͻ��м���������,����Ը������ꡣ

    #��7�����Ե�6����ÿ��TD�ͻ������з��ࣺ
    #����ڵ�2���û������У�����ϡ�TD�ն˿ͻ���־����bass1.td_check_ls_02
    #����ڵ�4���û������У�����ϡ����շ��ն�ʹ��T���ͻ���־����bass1.td_check_03��bass1.td_check_04
    #������ڵ�3����2_TMP�е��û�������ϡ�ʹ��TD����ͻ���־����bass1.g_s_04002_day_td2
    #������ڵ�3����2_TMP�У����й�TD�������嵥���û�������ϡ�ʹ������TD����ͻ���־����g_s_04017_day
    #������ڵ�3����2_TMP�У����й�TD��GPRS�嵥(04002)���û�������Ϊ��ʹ��GPRSTD����ͻ���־����g_s_04002_day_tmp
    #������ڵ�5���У��ҡ������ն˱�־����Ϊ1���û�������ϡ������ն˱�־����bass1.td_check_05_2
    #������ڵ�5���У����й�TD�����������GPRS�������û���
    #����ϡ�ʹ��������TD����ͻ���־����bass1.td_check_05_2

    #�����ʱ��    
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


    #������ʱ��bass1.td_check_06_2010
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

    #���� TD�ն˿ͻ���־
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

    #���� ���շ��ն�ʹ��T���ͻ���־
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

    #���� ʹ��TD����ͻ���־
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


    #���� ʹ������TD����ͻ���־
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

    #���� ʹ��GPRS TD����ͻ���־
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

    #���� �����ն˱�־
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


    #���� ʹ��������TD����ͻ���־
    set handle [aidb_open $conn]
#1.ȥ����bass1.G_S_04018_DAY_20100501bak���Ӿ�    
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



    #�����ʱ��    
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


    #������ʱ��bass1.td_check_user
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


    #�����յ�TD�ͻ����У�����������TD�ͻ���ͳ�����̡��ڶ��������������������ʹ��TD�ն˵Ŀͻ���������IMEI_14����Ӧ��TAC�룬
    #�ڼ����·����ն˲�����һ���·����У�����ֵΪ006099001��TAC��ΪTD�ֻ��ͻ�������ֵΪ006099002��TAC��ΪTD���ݿ��ͻ���
    #����ֵΪ006099005��TAC��ΪTD���������ͻ���û��������������TAC��Ŀͻ���ΪTD�ֻ��ͻ�
    #�����յ�TD�ͻ����У�����������TD�ͻ���ͳ�����̡����Ĳ�����������������յ�ʹ��TD����ķ�TD�ն˿ͻ���
    #���һ���û�����02004�С�����SIM���û���ʶ��Ϊ1����ͳ��ΪTD���ݿ��û�������ͳ��ΪTD�ֻ��ͻ�
    
    #TD�ͻ�����  03
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


    #�����ʱ��    
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


    #������ʱ��bass1.td_check_user_mobile
    set handle [aidb_open $conn]
#2010-8-30    liuzhilong �޸�tac��Ϊ��DIM_TERM_TAC��ȡ
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


    #�ܵ�TD�ֻ��ͻ���  04
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


    #�����ʱ��    
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


    #������ʱ��bass1.td_check_user_datacard
    set handle [aidb_open $conn]
# 2010-8-30    liuzhilong �޸�tac��Ϊ��DIM_TERM_TAC��ȡ
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


    #�ܵ����ݿ��û���    05
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


    #�����ʱ��    
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


    #������ʱ��bass1.td_check_user_wire
    set handle [aidb_open $conn]
# 2010-8-30    liuzhilong �޸�tac��Ϊ��DIM_TERM_TAC��ȡ
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

    #TD���������ͻ�    06
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

    #�������յ�TD�ͻ����У�����������TD�ͻ���ͳ�����̡����岽�������������TD�������ͻ���
    #4.	˵��:
    #��������������������ʹ�ã���TD�������ͻ�����������TD����ͻ���TD�ֻ��ͻ���TD���������ͻ���TD���ݿ��ͻ���
    #���ܳ����ظ����Ӷ�TD�ͻ�����TD�ֻ��ͻ���+TD���������ͻ���+TD���ݿ��ͻ���+TD�������ͻ���


    #�����ʱ��    
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


    #������ʱ��bass1.td_check_user_net
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


    #TD�������ͻ���    07
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


    #ʹ��TD����Ŀͻ���   08
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


    #ʹ��TD������ֻ��ͻ���   09
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


    #ʹ��TD��������ݿ��ͻ���   10
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

    #ʹ��TD������������ͻ���   11
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


    #ʹ��TD��������������ͻ���   12
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

    #TD�ն˿ͻ���    13
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

    #TD�ֻ��ն˿ͻ���    14
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


    #TD���ݿ��ն˿ͻ���    15
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

    #ʹ�ÿ��ӵ绰��TD�ͻ���    16
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
	#�ڶ�����ͳ������TD�ͻ�������ҵ�������ӡ�GSM/TD��ͨ����ҵ����ʹ�á���TB_SUM_GSM_VBUSN_MON_USG����
	#��������ҵ����ʹ�á���TB_SUM_INTN_BUSN_MON_USG������������VPMNҵ��ʹ�á��� TB_SUM_VPMN_BUSN������ȡ�����������Ϊ���£�
	#�������Ͳ�����122����������ʡ�����Σ���202���������ù������Σ���302���������ø۰�̨����401���������ñ߽����Σ���
	#�����ڵ�һ��TD�ͻ������еļ�¼�������롢���������ʶ���������͡���;���͡��������ͻ���ͨ���������Ʒ�ʱ����ͨ�����ã�
	#�õ�����ҵ��ͨ�����

    #�����ʱ��
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


    #������ʱ��bass1.g_s_21003_month_tmp ���м򵥹��ˣ����������������浱������
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


    #�����ʱ��
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


    #������ʱ��,���嵥���л��ܣ�ץȡ����ָ��
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

    #TD�ͻ����ܼƷ�ʱ��    17
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


    #TD�ͻ���T���ϵļƷ�ʱ��    18
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


    #TD�ֻ��ͻ����ܼƷ�ʱ��    19
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


    #TD�ֻ��ͻ���T���ϵļƷ�ʱ��    20
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


    #TD���������ͻ����ܼƷ�ʱ��    21
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


    #TD���������ͻ���T���ϵļƷ�ʱ��    22
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


	#�ٴӡ�TD��ͨ����ҵ�񻰵�����TB_NET_TD_COMM_CDR����ȡ�����������Ϊͳ�����ڣ�������������Ϊ1(TD��������)��
	#��Ƶ��ʶΪ1(��Ƶ����)�����������롢���������ʶ���������͡���;���͡��������ͻ���ͨ���������Ʒ�ʱ����ͨ�����ã�
	#�õ����ӵ绰ͨ�������������ҵ��ͨ���������������ӵ绰ͨ�����������ܿ��ӵ绰ʱ���������������һ��TD�ͻ����ϣ�
	#��ȡ����TD�ͻ���ʶ��Ϣ������Ŀ���TD�û�����ʹ�����м��DM_SUM_TD_VOIC_MON������


    #�����ʱ��
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


    #������ʱ��,���嵥���л��ܣ�ץȡ����ָ��
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


    #���ӵ绰�Ʒ�ʱ��    23
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


	#��������ͳ��TD�ͻ����ƶ���������������TD�����������ģ����ӡ�GPRS������(TB_NET_GPRS_CDR)��ȡ�������ʱ��Ϊ�����ڣ�
	#�����ڵ�һ��TD�ͻ������еĻ����������롢GPRS�������͡�����㣬�������ͻ�������������������������������ͨ��ʱ����
	#�ܷ��á�������������������ƶ���������ָ�꣬�γ��ƶ������������������������

    #�����ʱ��
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


    #������ʱ��,���嵥���л��ܣ�ץȡ����ָ��
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


	#ͳ������TD�������ͻ����ƶ������������ӡ�������GPRS������ ��04018��(TB_NET_NTBK_GPRS_CDR_NEW)����ȡ���ʱ���ڱ����ڣ�
	#�޳�APNΪ��CMLAP���Ļ��������󶨺��롢2G/3G�����ʶ���������������������ָ�꣬�γ��������ƶ������������
    
    #�����ʱ��
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


    #������ʱ��,���嵥���л��ܣ�ץȡ����ָ��
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


    #�����ʱ��    
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


    #������ʱ��bass1.td_check_user_flow
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

    
    #TD�ͻ�������������      24
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

    #ָ����ֵ����Ŀ���
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



    #TD�ͻ���T���ϵ���������      25
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

    #ָ����ֵ����Ŀ���
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

    #TD�ֻ��ͻ�������������       26
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

    #ָ����ֵ����Ŀ���
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



    #TD�ֻ��ͻ���T���ϵ���������       27
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

    #ָ����ֵ����Ŀ���
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


    #TD���ݿ��ͻ�������������    28   TD���ݿ��ͻ���T���ϵ���������    29
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



    #TD�������ͻ�������������     30   TD�������ͻ�������������     31
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

	#ͳ������TD�ͻ����˵�����(������ֻ�����ȼ��������ã�Ȼ���ṩ������ĸ�������ָ��ͳ��ʱʹ�ã�
	#����Ϊ��TD�ͻ��������롱ָ���ͳ�ƿھ�����TD�ͻ��������롱ָ��ͳ�ƿھ�����)���ӡ���ϸ�ʵ���(TB_FIN_DTL_BILL)
	#�͡��������û���ϸ���롱�� TB_FIN_INET_USR_DTL_INC��ȡ�����������Ϊͳ���գ��û���ʶ�ڵ�һ��TD�ͻ�������
	#���˵���¼������������
    
    #�����ʱ��
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


    #������ʱ��,���嵥���л��ܣ���ȡ�������ÿ���û���Ӧ������������
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


    #�����ʱ��    
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

    #������ʱ��bass1.td_check_user_fee1,ץȡ�����ÿͻ�ΪTD�������ͻ�����
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

    #�����ʱ��    
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


    #������ʱ��bass1.td_check_user_fee2,ץȡ �����ÿͻ�Ϊ��TD�������ͻ� ����
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


    #�����ʱ��    
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


    #������ʱ��bass1.td_check_user_fee2,ץȡ ���µ�TD�ͻ� ����
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

    #TD�ͻ�����            32
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


    #ָ����ֵ����Ŀ���,�ѵ�λ�ֻ���ΪԪ
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


    #TD�ֻ��ͻ�����     33
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

    #TD���ݿ��ͻ�����     34
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


    #TD�������ͻ�����     35
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


    #TD���������ͻ�����     36
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

#�ڲ���������
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





