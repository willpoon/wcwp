######################################################################################################
#�ӿ����ƣ�27��ָ���γ�ǰ̨��������
#�ӿڱ��룺ÿ��ͳ��
#�ӿ�˵����
#��������: report_key_index_month.tcl
#��������: ÿ��������27��ָ������
#��������: ��
#Դ    ��1.
#          2.
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�
#��дʱ�䣺2010-06-18
#�����¼��1.���Ŷ��żƷ���δ���������û�����
#�޸���ʷ: 
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
    
    #���ϸ��� yyyymm
    set last_last_month [GetLastMonth [string range $last_month 0 5]]
    puts $last_last_month
    
    #���ϸ��µĵ�һ�� yyyymmdd
    set one "01"
    set last_last_month_day  ${last_last_month}${one}
    puts $last_last_month_day
    

    #ɾ��Ŀ���������
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


    #�����ʱ��
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
    
    #ץȡ���µ��û�������Ϣ
    #������ʱ��bass1.td_check_user_status_ls
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


    #ȡ��������µ�ͨ����������0���û��ն�ʹ�������02047�ӿڣ�����GPRS�嵥�������������嵥����һ��04002�ӿڣ���
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


    #������ʱ��bass1.g_s_04002_day_td
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


    #ȡ����ʹ��TD����Ŀͻ���ȡ��������µ�TD�����嵥��һ��04017�ӿڣ���GPRS�嵥�������������嵥����һ��04002�ӿڣ���
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


    #������ʱ��bass1.g_s_04002_day_td1
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

    
    #ȡ���������µ�������������������������GPRS����������CMLAP����һ��04018�ӿڣ���ͳ������֮��>0�������û����󶨺��룩��

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
    #������ڵ�3����2_TMP�У����й�TD�������嵥���û�������ϡ�ʹ������TD����ͻ���־����
    #������ڵ�3����2_TMP�У����й�TD��GPRS�嵥(04002)���û�������Ϊ��ʹ��GPRSTD����ͻ���־����
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


    #������ʱ��bass1.td_check_06
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
    
    #TD�ͻ�����
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


    #�ܵ�TD�ֻ��ͻ���
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


    #�ܵ����ݿ��û���
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

    #TD���������ͻ�
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


    #TD�������ͻ���
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


    #ʹ��TD����Ŀͻ���T���ϼƷ�ʱ��
    #ȡTD�ͻ��м���У�����Ϊͳ���µ�TD�ͻ��ġ�T�����������Ʒ�ʱ����֮��
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


    #ʹ��TD����Ŀͻ���T���ϵ���������
    #ȡTD�ͻ��м��������Ϊͳ���µ�TD�ͻ�����ͨGPRS������04002��T����\����������������GPRS����T����\��������֮�ͣ�
    #��ȥ�����������������TD�������ͻ���־��Ϊ1������TD�ֻ��ͻ���־������TD���ݿ��ͻ���־������TD���������ͻ���־����Ϊ0
    #�Ŀͻ���GPRS������04002����T����������������

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


    #ʹ��TD����Ŀͻ���T���ϵ���������
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



    #�����ͻ���
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


    #�ͻ�������
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


    #�����ͻ���
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

    #�����ͻ���=���¿ͻ�������-���¿ͻ�������
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


    #ͨ�ſͻ���
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


    #ͨ���ͻ���
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

    
    #��ͨ�ƶ��ͻ�����
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


    #�����ƶ��ͻ�����
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


    #��ͨ�ƶ������ͻ���
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


    #�����ƶ������ͻ���
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

    #��ͨ�ƶ������ͻ���
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

    #�����ƶ������ͻ���
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


    #�Ʒ�ʱ��
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


    #�ƶ���������
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
    
    #����������
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

    #��ͨ����
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

    #���Ż�������
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

    #���żƷ���
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

    #���żƷ���
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


    #���˿ͻ���
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

    #ҵ�����루�˵����룩
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


    #���㡾TD�ͻ������������TD�ͻ����²������˵����룬��������������������TD-GPRS�������������ͻ��ķ���������Ŀ���ã�
    #��δ����������TD-GPRS�����ķ��������ͻ�����������Ŀ����

    #TD�ͻ�����������
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


    #������ʱ��bass1.td_check_user_fee1
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


    #��������������TD-GPRS�������������ͻ��ķ���������Ŀ����
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


    #������ʱ��bass1.td_check_user_fee2
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


    #δ����������TD-GPRS�����ķ��������ͻ�����������Ŀ����
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


    #TD�ͻ�����
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


    #���㡾TD�ͻ������������TD�ֻ��ͻ����²������˵����룬������ͬʱΪ�������ͻ�����������Ŀ����
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

    #�������ͻ�����������Ŀ����
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


    #TD�ֻ��ͻ�����
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





