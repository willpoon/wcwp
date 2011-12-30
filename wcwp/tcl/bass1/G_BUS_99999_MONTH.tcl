######################################################################################################
#�ӿ����ƣ�ÿ���ṩ��ҵ��ָ��У���ļ�
#�ӿڱ��룺99999
#�ӿ�˵����
#��������: G_BUS_99999_MONTH.tcl
#��������: ����99999����
#��������: ��
#Դ    ��
#          
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��
#�޸���ʷ: 20100125 �����û��ھ��䶯 usertype_id not in ('2010','2020','2030','9000') �������ݿ�sim_code='1'
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env

	append op_time_month ${optime_month}-01
	set db_user $env(DB_USER)
        #�½ӿ�ʹ��
        set op_month [string range $op_time_month 0 3][string range $op_time_month 5 6]
        #���ϸ��� ��ʽ yyyymm
        set last_month [GetLastMonth $op_month]
        #----���������һ��---#,��ʽ yyyymmdd
        set last_month_day [GetLastDay [string range $op_month 0 5]01]
        #���¸��� ��ʽ yyyymm
        set next_month [GetNextMonth $op_month]
        #���¸��µ�һ��
        set next_month_firstday "${next_month}01"
        puts ${next_month_firstday}
        #���µ�һ��#
        set this_month_firstday "${op_month}01"
        puts $this_month_firstday
        
        #----��������-----#
        set thismonthdays [GetThisMonthDays ${op_month}01]
        puts $thismonthdays
        #puts $thismonthdays
        #----��������-----#
        set thisyeardays [GetThisYearDays ${op_month}01]
        puts $thisyeardays
        #puts $thisyeardays
        set day [string range $op_time 8 9]
        puts $day
#        #--------------------------------------
        
        
        
        set handle [aidb_open $conn]
	set sql_buff "\
		DELETE FROM $db_user.G_BUS_CHECK_BILL_MONTH where time_id=${op_month}"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
       
       
       
        #------�����û�ȫ����ʱ��-------#
        set handle [aidb_open $conn]

	set sql_buff "declare global temporary table session.G_BUS_99999_MID1
                     (
                       TIME_ID		INTEGER,		
                       USER_ID		VARCHAR(20),		
                       CREATE_DATE      VARCHAR(10),		
                       PRODUCT_NO	VARCHAR(15),            
                       USERTYPE_ID	VARCHAR(1),		
                       SIM_CODE		VARCHAR(1),		
                       BRAND_ID		VARCHAR(1)			
                      )
        with replace on commit preserve rows not logged in tbs_user_temp"
        
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
        
	aidb_commit $conn
	puts "�����û�ȫ����ʱ��G_BUS_99999_MID1"
	
	#----�����û�״̬ȫ����ʱ��G_BUS_99999_MID2-------#
	set handle [aidb_open $conn]

	set sql_buff "declare global temporary table session.G_BUS_99999_MID2
                     (
                       TIME_ID		        INTEGER,		
                       USER_ID			VARCHAR(20),            
                       USERTYPE_ID		VARCHAR(4)			
                      )
        with replace on commit preserve rows not logged in tbs_user_temp"

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}

	aidb_commit $conn
	puts "�����û�״̬ȫ����ʱ��G_BUS_99999_MID2"

	
	
	#---�����û�ȫ����ʱ��---#
       set handle [aidb_open $conn]

	set sql_buff "insert into session.G_BUS_99999_MID1
                     select
                       a.time_id		
                      ,a.user_id		
                      ,create_date		
                      ,product_no		
                      ,usertype_id	        
                      ,sim_code		
                      ,brand_id	
	              from bass1.g_a_02004_day a,
                      (select max(time_id) as time_id,user_id from bass1.g_a_02004_day where time_id<${next_month_firstday} group by user_id)b
	              where a.user_id=b.user_id and a.time_id=b.time_id"

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}

	aidb_commit $conn
	puts "�����û�ȫ����ʱ��G_BUS_99999_MID1"
	
	#-----�����û�״̬ȫ����ʱ��-----#
	set handle [aidb_open $conn]

	set sql_buff "insert into session.G_BUS_99999_MID2
                     select 
                       a.time_id
                      ,a.user_id
                      ,a.USERTYPE_ID 
                      from bass1.G_A_02008_DAY a,
                      (select max(time_id) as time_id,user_id from bass1.G_A_02008_DAY where time_id<${next_month_firstday} group by user_id)b 
                       where a.user_id = b.user_id and a.time_id=b.time_id"

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}

	aidb_commit $conn
	puts "�����û�״̬ȫ����ʱ��G_BUS_99999_MID2"
	
	#------�����û�ȫ����ʱ��-------#
        set handle [aidb_open $conn]

	set sql_buff "declare global temporary table session.G_BUS_99999_MID3
                     (
                       TIME_ID		INTEGER,		
                       USER_ID		VARCHAR(20),		
                       CREATE_DATE      VARCHAR(10),		
                       PRODUCT_NO	VARCHAR(15),            
                       USERTYPE_ID	VARCHAR(1),		
                       SIM_CODE		VARCHAR(1),		
                       BRAND_ID		VARCHAR(1)			
                      )
        with replace on commit preserve rows not logged in tbs_user_temp"
        
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
        
	aidb_commit $conn
	puts "�����û�ȫ����ʱ��G_BUS_99999_MID3"
	
	#----�����û�״̬ȫ����ʱ��-------#
	set handle [aidb_open $conn]

	set sql_buff "declare global temporary table session.G_BUS_99999_MID4
                     (
                       TIME_ID		        INTEGER,		
                       USER_ID			VARCHAR(20),            
                       USERTYPE_ID		VARCHAR(4)			
                      )
        with replace on commit preserve rows not logged in tbs_user_temp"

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}

	aidb_commit $conn
	puts "�����û�ȫ����ʱ��G_BUS_99999_MID4"

	
	
	#---�����û�ȫ����ʱ��---#
       set handle [aidb_open $conn]

	set sql_buff "insert into session.G_BUS_99999_MID3
                     select
                       a.time_id		
                      ,a.user_id		
                      ,create_date		
                      ,product_no		
                      ,usertype_id	        
                      ,sim_code		
                      ,brand_id	
	              from bass1.g_a_02004_day a,
                      (select max(time_id) as time_id,user_id from bass1.g_a_02004_day where time_id<${this_month_firstday} group by user_id)b
	              where a.user_id=b.user_id and a.time_id=b.time_id"

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}

	aidb_commit $conn
	puts "�����û�ȫ����ʱ��G_BUS_99999_MID3"
	
	#-----�����û�״̬ȫ����ʱ��-----#
	set handle [aidb_open $conn]

	set sql_buff "insert into session.G_BUS_99999_MID4
                     select 
                       a.time_id
                      ,a.user_id
                      ,a.USERTYPE_ID 
                      from bass1.G_A_02008_DAY a,
                      (select max(time_id) as time_id,user_id from bass1.G_A_02008_DAY where time_id<${this_month_firstday} group by user_id)b 
                       where a.user_id = b.user_id and a.time_id=b.time_id"

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}

	aidb_commit $conn
	puts "�����û�״̬ȫ����ʱ��G_BUS_99999_MID4"
	
	#VIP_CUST_NUM --���˴�ͻ���--#
	
	set handle [aidb_open $conn]
	set sqlbuf "\
            select
              count(distinct a.user_id)
              from bass1.g_i_02005_month a,session.G_BUS_99999_MID2 b
              where a.user_id = b.user_id
      	      and a.time_id = $op_month
     	      and b.time_id < ${next_month_firstday}
      	      and b.usertype_id not in ('2010','2020','2030','9000'); "

        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set VIP_CUST_NUM [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "���˴�ͻ���: $VIP_CUST_NUM"

	aidb_commit $conn
	
	#ADD_VIP_CUST_NUM �������˴�ͻ���#
	set handle [aidb_open $conn]
	set sqlbuf "\
            select
        count(distinct a.user_id)
        from 
        (
        select user_id from bass1.g_i_02005_month where time_id=$op_month
        except select user_id from bass1.g_i_02005_month where time_id=${last_month})a, session.g_bus_99999_mid2 b
        where a.user_id = b.user_id
      	and b.usertype_id not in ('2010','2020','2030','9000');	"

        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set ADD_VIP_CUST_NUM [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "�������˴�ͻ���: $ADD_VIP_CUST_NUM"

	aidb_commit $conn
	
	# AWY_VIP_CUST_NUM �������˴�ͻ���--#
	set handle [aidb_open $conn]
	set sqlbuf "\
            select
        count(distinct a.user_id)
        from 
        (
        select user_id from bass1.g_i_02005_month where time_id=${last_month}
        except select user_id from bass1.g_i_02005_month where time_id=${op_month})a, session.g_bus_99999_mid2 b
        where a.user_id = b.user_id
      	and b.usertype_id  in ('2010','2020','2030','9000');	"

        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set AWY_VIP_CUST_NUM [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "�������˴�ͻ���: $AWY_VIP_CUST_NUM"

	aidb_commit $conn
	
	
	#ADD_USER_NUM ���������û���--#
	set handle [aidb_open $conn]
	set sqlbuf "select
                      count(distinct a.user_id)
                      from session.g_bus_99999_mid1 a
                      where 
      	              int(a.CREATE_DATE)/100=${op_month}
      	              and a.usertype_id <> '3'      
      	              "

        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set ADD_USER_NUM [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "�����û���:$ADD_USER_NUM"
	aidb_commit $conn
	
	#AWY_USER_NUM ���������û���--#
	
	
	set handle [aidb_open $conn]
	set sqlbuf "select
                      count(distinct a.user_id)
                      from session.g_bus_99999_mid1 a,session.g_bus_99999_mid2 b
                      where a.user_id = b.user_id
      	              and int(b.time_id)/100 =${op_month}	
      	              and a.usertype_id <> '3'              
      	              and b.usertype_id in ('2010','2020','2030','9000'); "

        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set AWY_USER_NUM [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "�����û���:$AWY_USER_NUM"

	aidb_commit $conn
	
	#---ZERO_CALL_USER_NUM ���ͨ���û���--#
	set handle [aidb_open $conn]
	set sqlbuf " select
                      count(distinct a.product_no)
                      from
                      (
                      select a.product_no from session.g_bus_99999_mid1 a,session.g_bus_99999_mid2 b
                      where a.user_id = b.user_id
      	              and int(a.time_id)/100=${op_month}		
      	              and a.usertype_id <> '3'
      	              and b.usertype_id not in ('2010','2020','2030','9000')
      	              except select a.product_no from
               	       (select product_no from bass1.g_s_21003_month where time_id = ${op_month} and bigint(base_bill_duration) > 0) a
               	        left join (select product_no from bass1.g_s_21006_month where time_id = ${op_month} and bigint(base_bill_duration) > 0) b on a.product_no = b.product_no
               	        left join (select product_no from bass1.g_s_21009_day where int(time_id)/100 = ${op_month} and bigint(base_bill_duration) > 0) c on a.product_no = c.product_no) a;"

        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set ZERO_CALL_USER_NUM  [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "���ͨ���û���:$ZERO_CALL_USER_NUM"

	aidb_commit $conn
	
	#--NEW_BUS_USE_USER_NUM ��ҵ��ʹ���û���--#
	set handle [aidb_open $conn]
	set sqlbuf " select
        a.cnt+b.cnt
        from (select count(distinct a.user_id) as cnt from bass1.g_s_03004_month a,session.g_bus_99999_mid1 b
      	where a.user_id = b.user_id
      	      and a.time_id = ${op_month}
      	      and b.usertype_id <> '3'
      	      and (a.acct_item_id in ('0405','0407') or int(a.acct_item_id)/100 in (5,6,7))) a,
            (select count(distinct user_id) as cnt from bass1.g_s_03012_month
      	where time_id = ${op_month}
      	      and (acct_item_id in ('0405','0407') or int(acct_item_id)/100 in (5,6,7))) b;"

        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set NEW_BUS_USE_USER_NUM  [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "��ҵ��ʹ���û���:$NEW_BUS_USE_USER_NUM"

	aidb_commit $conn
	
	#---HIGH_VALUE_USER_NUM �߼�ֵ�û���--#
	set handle [aidb_open $conn]
	set sqlbuf " select
                       c.cnt+d.cnt
                    from (select count(*) as cnt from
      	             (select user_id from bass1.g_s_03005_month
      	              where time_id = ${op_month}
      	              group by user_id
      	              having sum(int(should_fee))/700 >= ${thismonthdays}) a) c,
                       (select count(*) as cnt from
      	             (select user_id from bass1.g_s_03012_month
      	              where time_id = ${op_month}
      	              group by user_id
      	              having sum(int(incm_amt))/700 >= ${thismonthdays}) b) d"

        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set HIGH_VALUE_USER_NUM  [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "�߼�ֵ�û���:$HIGH_VALUE_USER_NUM"

	aidb_commit $conn
	
	#--GSM_HIGH_VALUE_USER_NUM ȫ��ͨ�߼�ֵ�û���#
	#--SZX_HIGH_VALUE_USER_NUM �����и߼�ֵ�û���#
	#--M_ZONE_HIGH_VALUE_USER_NUM ���еش��߼�ֵ�û���#
	set handle [aidb_open $conn]

	set sqlbuf "\
                  select
                  case when b.brand_id='1' then count(distinct a.user_id) else 0 end,
                  case when b.brand_id='2' then count(distinct a.user_id) else 0 end,
                  case when b.brand_id='3' then count(distinct a.user_id) else 0 end
                  from (
                   select
                   user_id
                  from
                  (select user_id from bass1.g_s_03005_month
                   where time_id = ${op_month}
                   group by user_id
                   having sum(int(should_fee))/700 >= ${thismonthdays}
                   union
                   select user_id from bass1.g_s_03012_month
                   where time_id = ${op_month}
                   group by user_id
                   having sum(int(incm_amt))/700 >= ${thismonthdays})c)a,session.g_bus_99999_mid1 b
                   where a.user_id = b.user_id
                   and b.usertype_id <> '3'               
                   group by
                   b.brand_id "


        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
        	WriteTrace $errmsg 1013
        	return -1
        }

        while {[set p_success [aidb_fetch $handle]] != "" } {
                set GSM_HIGH_VALUE_USER_NUM     	      [lindex $p_success 0]
	        set SZX_HIGH_VALUE_USER_NUM      	      [lindex $p_success 1]
	        set M_ZONE_HIGH_VALUE_USER_NUM     	      [lindex $p_success 2]
	                    }

	aidb_commit $conn
	puts "ȫ��ͨ�߼�ֵ�û���: ${GSM_HIGH_VALUE_USER_NUM}"
	puts "�����и߼�ֵ�û���: ${SZX_HIGH_VALUE_USER_NUM}"
	puts "���еش��߼�ֵ�û���: ${M_ZONE_HIGH_VALUE_USER_NUM}"
	
	#--RESERV_USER_NUM �������û���--#
	set handle [aidb_open $conn]
	set sqlbuf " select
                      count(distinct a.user_id)
                      from session.g_bus_99999_mid1 a,session.g_bus_99999_mid2 b
                      where a.user_id = b.user_id
      	              and a.usertype_id <> '3'              
      	              and b.usertype_id='1040';"

        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set RESERV_USER_NUM  [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "�������û���:$RESERV_USER_NUM"

	aidb_commit $conn
	
	#--FREEZE_USER_NUM �䶳���û���--#
	set handle [aidb_open $conn]
	set sqlbuf " select
                      count(distinct a.user_id)
                      from session.g_bus_99999_mid1 a,session.g_bus_99999_mid2 b
                      where a.user_id = b.user_id
      	              and a.usertype_id <> '3'              
      	              and b.usertype_id='2030';"

        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set FREEZE_USER_NUM  [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "�䶳���û���:$FREEZE_USER_NUM"
	
	                  
        #CALLING_TIMES  ͨ������ --#                  
        #BILLING_DUR    �Ʒ�ʱ�� --#
        #CALLING_DUR ͨ��ʱ�� --# 
        set handle [aidb_open $conn]

	set sqlbuf "
                   select
                   a.cnt1+b.cnt1+c.cnt1,
                   a.cnt2+b.cnt2+c.cnt2,
                   bigint(double(a.cnt3+b.cnt3+c.cnt3)/60)
                   from (select sum(bigint(call_counts)) as cnt1,sum(bigint(base_bill_duration)) as cnt2,sum(bigint(call_duration)) as cnt3
      	           from bass1.g_s_21003_month
      	           where time_id = ${op_month}
      	                 and roam_type_id not in ('122','202','302','401')) a,
                       (select sum(bigint(call_counts)) as cnt1,sum(bigint(base_bill_duration)) as cnt2,sum(bigint(call_duration)) as cnt3
      	           from bass1.g_s_21006_month
      	           where time_id = ${op_month}
      	                 and roam_type_id not in ('122','202','302','401')) b,
                       (select sum(bigint(call_counts)) as cnt1,sum(bigint(base_bill_duration)) as cnt2,sum(bigint(CALL_DURATION)) as cnt3
      	           from bass1.g_s_21009_day
      	           where int(time_id)/100 = ${op_month}
      	      and roam_type_id not in ('122','202','302','401')) c "


        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
        	WriteTrace $errmsg 1013
        	return -1
        }

        while {[set p_success [aidb_fetch $handle]] != "" } {
                set CALLING_TIMES     	      [lindex $p_success 0]
	        set BILLING_DUR      	      [lindex $p_success 1]
	        set CALLING_DUR     	      [lindex $p_success 2]
	        		             }

	aidb_commit $conn
	puts "ͨ������: ${CALLING_TIMES}"
	puts "�Ʒ�ʱ��: ${BILLING_DUR}"
	puts "ͨ��ʱ��: ${CALLING_DUR}"
	
	#BUSY_TIME_BILLING_DUR æʱ�Ʒ�ʱ��#
	set handle [aidb_open $conn]
	set sqlbuf "select
                    sum(cnt)
                    from
                    (
                    select
                            sum(cnt) as cnt,
                            call_moment_id
                           from (
                    	   select sum(bigint(base_bill_duration)) as cnt,call_moment_id from bass1.g_s_21002_day where int(time_id)/100 = ${op_month} group by call_moment_id
                    	   union
                           select sum(bigint(base_bill_duration)) as cnt,call_moment_id from bass1.g_s_21005_day where int(time_id)/100 = ${op_month} group by call_moment_id
                    	   union
                           select sum(bigint(base_bill_duration)) as cnt,callmoment_id as call_moment_id from bass1.g_s_21016_day where int(time_id)/100 = ${op_month} group by callmoment_id)a
                           group by 
                           call_moment_id
                    	   order by cnt desc
                    	   fetch first 2 rows only
                    	   )aa"

        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set BUSY_TIME_BILLING_DUR [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "æʱ�Ʒ�ʱ��:$BUSY_TIME_BILLING_DUR"

	aidb_commit $conn
	
	#--SMS_DATA_VOL -����ͨ����--#
	set handle [aidb_open $conn]
	set sqlbuf "select
        a.cnt+b.cnt+c.cnt
        from (select count(*) as cnt from bass1.g_s_04005_day
      	where int(time_id)/100 = ${op_month}
      	      and sms_status = '0' and info_type in ('01','02','03','99')) a,
            (select count(*) as cnt from bass1.g_s_04014_day
      	where int(time_id)/100 = ${op_month}
      	      and sms_send_state = '0' and sms_bill_type in ('00','01','10','11')) b,
            (select sum(bigint(sms_counts)) as cnt from bass1.g_s_21008_month
      	where time_id = ${op_month}
      	      and svc_type_id in ('11','12','13','14','20','31','32','40','64','65','66','70') 
      	      and end_status = '0' and cdr_type_id in ('00','01','10','11')) c"

        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set SMS_DATA_VOL  [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "����ͨ����:$SMS_DATA_VOL"

	aidb_commit $conn
	
	
	#--PPP_SMS_DATA_VOL ��Ե����ͨ����#
	set handle [aidb_open $conn]
	set sqlbuf "select
                     sum(bigint(sms_counts))
                     from bass1.g_s_21008_month
                     where time_id = ${op_month}
                     and end_status ='0'
                     and cdr_type_id in ('00','01','10','11','21','28')
                     and svc_type_id in ('11','12','13');"
                                                             
        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set PPP_SMS_DATA_VOL  [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "��������ͨ����:$PPP_SMS_DATA_VOL"

	aidb_commit $conn
		
	
	#--MNTERNET_SMS_DATA_VOL ��������ͨ����--#
	set handle [aidb_open $conn]
	set sqlbuf "select
                    count(*)
                    from bass1.g_s_04005_day
                    where 
                     int(time_id)/100 =${op_month}
      	             and sms_status = '0'
      	             and info_type in ('01','02','03','99')"
                                                             
        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set MNTERNET_SMS_DATA_VOL  [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "��������ͨ����:$MNTERNET_SMS_DATA_VOL"

	aidb_commit $conn
	
	#-- MMS_DATA_VOL����ͨ����--#
	set handle [aidb_open $conn]
	set sqlbuf "select
                     count(*)
                     from bass1.G_S_04004_DAY
                     where int(time_id)/100 =${op_month}"
                                                             
        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set MMS_DATA_VOL  [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "����ͨ����:$MMS_DATA_VOL"

	aidb_commit $conn
	
	#--PPP_MMS_DATA_VOL ��Ե����ͨ����--#
	set handle [aidb_open $conn]
	set sqlbuf " select
                      count(*)
                      from bass1.g_s_04004_day
                      where int(time_id)/100 =${op_month}
                      and bus_srv_id ='1'"
                                                             
        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set PPP_MMS_DATA_VOL  [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "��Ե����ͨ����:$PPP_MMS_DATA_VOL"

	aidb_commit $conn
	
	#--GPRS_UP_DATA_VOL GPRS ͨ����(����)--#
	#--GPRS_DOWN_DATA_VOL GPRS ͨ����(����)--#
	set handle [aidb_open $conn]

	set sqlbuf "
                   select
                   sum(bigint(uplink_flow_on_tariff1))/1024,sum(bigint(downlink_flow_on_tariff1))/1024
                   from bass1.g_s_04002_day
                   where int(time_id)/100 =${op_month} "


        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
        	WriteTrace $errmsg 1013
        	return -1
        }

        while {[set p_success [aidb_fetch $handle]] != "" } {
                set GPRS_UP_DATA_VOL    	      [lindex $p_success 0]
	        set GPRS_DOWN_DATA_VOL      	      [lindex $p_success 1]

	        		             }

	aidb_commit $conn
	puts "ͨ����(����): ${GPRS_UP_DATA_VOL}"
	puts "ͨ����(����): ${GPRS_DOWN_DATA_VOL}"
	
	#--BUS_INCOME ҵ������--#
	set handle [aidb_open $conn]
	set sqlbuf " select
        sum(cnt)
        from (
		select sum(bigint(should_fee)) as cnt from bass1.g_s_03005_month
      	where time_id = ${op_month} and item_id in ('0100','0200','0300','0400','0500','0600','0700','0900')
		union
	    select sum(bigint(incm_amt)) as cnt from bass1.g_s_03012_month
        where time_id = ${op_month}
		)a"
                                                             
        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set BUS_INCOME  [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "ҵ������:$BUS_INCOME"
#        
	aidb_commit $conn
	#set BUS_INCOME 1000000
	
	#--NEW_BUS_INCOME ��ҵ������--#
	set handle [aidb_open $conn]
	set sqlbuf " select
                     a.cnt+b.cnt
                     from (select coalesce(sum(bigint(fee_receivable)),0) as cnt from bass1.g_s_03004_month
      	            where time_id =${op_month}
      	                  and (acct_item_id in ('0405','0407')
      	                  or int (acct_item_id)/100 in (5,6,7))) a,
                  (select coalesce(sum(bigint(incm_amt)),0) as cnt from g_s_03012_month
      	            where time_id =${op_month}
      	                  and (acct_item_id in ('0405','0407')
      	                   or int (acct_item_id)/100 in (5,6,7))) b"
                                                             
        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set NEW_BUS_INCOME  [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "��ҵ������:$NEW_BUS_INCOME"

	aidb_commit $conn
	
	#--��arpu--#
	#����ARPU=����ҵ������/������������12/����������/����ƽ���ͻ���
	#����ƽ���ͻ���=������ĩ�û�������������ĩ�û���������/2 
	#--this_user_num ����ĩ�û�������--#
	set handle [aidb_open $conn]
	set sqlbuf " select                                                                   
                      count(distinct a.user_id)                                              
                      from session.g_bus_99999_mid1 a,session.g_bus_99999_mid2 b             
                      where a.user_id = b.user_id                                            
                      and a.usertype_id <> '3'                   
                      and b.usertype_id not in ('2010','2020','2030','9000'); "
                                                             
        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set this_user_num  [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "����ĩ�û�������:$this_user_num"

	aidb_commit $conn
	#--last_user_num ����ĩ�û�������--#
	set handle [aidb_open $conn]
	set sqlbuf " select                                                                   
                      count(distinct a.user_id)                                              
                      from session.g_bus_99999_mid3 a,session.g_bus_99999_mid4 b             
                      where a.user_id = b.user_id                                            
                      and a.usertype_id <> '3'                    
                      and b.usertype_id not in ('2010','2020','2030','9000'); "
                                                             
        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set last_user_num  [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	puts "����ĩ�û�������:$last_user_num"

	set aidb_commit $conn

	puts ${thismonthdays}
	puts ${thisyeardays}
  puts $this_user_num
  puts $last_user_num
	

	set ARPU [format "%.0f" [expr (${BUS_INCOME} /1.00000/ (${thismonthdays} * 12.0000/${thisyeardays})/(($this_user_num+$last_user_num)/2))]]
	puts "ARPU:$ARPU"
	
	#��$db_user.G_BUS_CHECK_BILL_MONTH�в���һ������#
        set handle [aidb_open $conn]
	set sql_buff "insert into $db_user.G_BUS_CHECK_BILL_MONTH values($op_month,'${VIP_CUST_NUM}','${ADD_VIP_CUST_NUM}','${AWY_VIP_CUST_NUM}','${ADD_USER_NUM}','${AWY_USER_NUM}','${ZERO_CALL_USER_NUM}','${NEW_BUS_USE_USER_NUM}','${HIGH_VALUE_USER_NUM}','${GSM_HIGH_VALUE_USER_NUM}','${SZX_HIGH_VALUE_USER_NUM}','${M_ZONE_HIGH_VALUE_USER_NUM}','${RESERV_USER_NUM}','${FREEZE_USER_NUM}','${CALLING_TIMES}','${BILLING_DUR}','${BUSY_TIME_BILLING_DUR}','${CALLING_DUR}','${SMS_DATA_VOL}','${PPP_SMS_DATA_VOL}','${MNTERNET_SMS_DATA_VOL}','${MMS_DATA_VOL}','${PPP_MMS_DATA_VOL}','${GPRS_UP_DATA_VOL}','${GPRS_DOWN_DATA_VOL}','${BUS_INCOME}','${NEW_BUS_INCOME}','${ARPU}')"

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}

	aidb_commit $conn
	      
	aidb_close $handle

	return 0
}
#############################����
#######################################################################################################
##�ӿ����ƣ�ÿ���ṩ��ҵ��ָ��У���ļ�
##�ӿڱ��룺99999
##�ӿ�˵����
##��������: G_BUS_99999_MONTH.tcl
##��������: ����99999����
##��������: ��
##Դ    ��
##          
##�������: 
##�������: ����ֵ:0 �ɹ�;-1 ʧ��
##�� д �ˣ�����
##��дʱ�䣺2007-03-22
##�����¼��
##�޸���ʷ: 
########################################################################################################
#
#
#proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#
#	global env
#
#	append op_time_month ${optime_month}-01
#	set db_user $env(DB_USER)
#        #�½ӿ�ʹ��
#        set op_month [string range $op_time_month 0 3][string range $op_time_month 5 6]
#        #���ϸ��� ��ʽ yyyymm
#        set last_month [GetLastMonth $op_month]
#        #----���������һ��---#,��ʽ yyyymmdd
#        set last_month_day [GetLastDay [string range $op_month 0 5]01]
#        #���¸��� ��ʽ yyyymm
#        set next_month [GetNextMonth $op_month]
#        #���¸��µ�һ��
#        set next_month_firstday "${next_month}01"
#        puts ${next_month_firstday}
#        #���µ�һ��#
#        set this_month_firstday "${op_month}01"
#        
#        #----��������-----#
#        set thismonthdays [GetThisMonthDays ${op_month}01]
#        #puts $thismonthdays
#        #----��������-----#
#        set thisyeardays [GetThisYearDays ${op_month}01]
#        #puts $thisyeardays
#        set day [string range $op_time 8 9]
##        #--------------------------------------
#        
#        
#        
#        set handle [aidb_open $conn]
#	set sql_buff "\
#		DELETE FROM $db_user.G_BUS_CHECK_BILL_MONTH where time_id=${op_month}"
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#       
#       
#       
#       
#       
#        #------�����û�ȫ����ʱ��-------#
#        set handle [aidb_open $conn]
#
#	set sql_buff "declare global temporary table session.G_BUS_99999_MID1
#                     (
#                       TIME_ID		INTEGER,		--״̬�ı�����ʱ��--
#                       USER_ID		VARCHAR(20),		--�û���־--
#                       CREATE_DATE      VARCHAR(10),		--��������--
#                       PRODUCT_NO	VARCHAR(15),            --PRODUCT_NO--
#                       USERTYPE_ID	VARCHAR(1),		--�û�״̬��־--
#                       SIM_CODE		VARCHAR(1),		--����SIM����־--
#                       BRAND_ID		VARCHAR(1)		--�û�Ʒ�Ʊ�־--		
#                      )
#        with replace on commit preserve rows not logged in tbs_user_temp"
#        
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#        
#	aidb_commit $conn
#	puts "�����û�ȫ����ʱ��G_BUS_99999_MID1"
#	
#	#----�����û�״̬ȫ����ʱ��G_BUS_99999_MID2-------#
#	set handle [aidb_open $conn]
#
#	set sql_buff "declare global temporary table session.G_BUS_99999_MID2
#                     (
#                       TIME_ID		        INTEGER,		--״̬�ı�����ʱ��--
#                       USER_ID			VARCHAR(20),            --�û���־--
#                       USERTYPE_ID		VARCHAR(4)		--�û�״̬��־--		
#                      )
#        with replace on commit preserve rows not logged in tbs_user_temp"
#
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#
#	aidb_commit $conn
#	puts "�����û�״̬ȫ����ʱ��G_BUS_99999_MID2"
#
#	
#	
#	#---�����û�ȫ����ʱ��---#
#       set handle [aidb_open $conn]
#
#	set sql_buff "insert into session.G_BUS_99999_MID1
#                     select
#                       a.time_id		
#                      ,a.user_id		
#                      ,create_date		
#                      ,product_no		
#                      ,usertype_id	        
#                      ,sim_code		
#                      ,brand_id	
#	              from bass1.g_a_02004_day a,
#                      (select max(time_id) as time_id,user_id from bass1.g_a_02004_day where time_id<${next_month_firstday} group by user_id)b
#	              where a.user_id=b.user_id and a.time_id=b.time_id"
#
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#
#	aidb_commit $conn
#	puts "�����û�ȫ����ʱ��G_BUS_99999_MID1"
#	
#	#-----�����û�״̬ȫ����ʱ��-----#
#	set handle [aidb_open $conn]
#
#	set sql_buff "insert into session.G_BUS_99999_MID2
#                     select 
#                       a.time_id
#                      ,a.user_id
#                      ,a.USERTYPE_ID 
#                      from bass1.G_A_02008_DAY a,
#                      (select max(time_id) as time_id,user_id from bass1.G_A_02008_DAY where time_id<${next_month_firstday} group by user_id)b 
#                       where a.user_id = b.user_id and a.time_id=b.time_id"
#
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#
#	aidb_commit $conn
#	puts "�����û�״̬ȫ����ʱ��G_BUS_99999_MID2"
#	
#	#------�����û�ȫ����ʱ��-------#
#        set handle [aidb_open $conn]
#
#	set sql_buff "declare global temporary table session.G_BUS_99999_MID3
#                     (
#                       TIME_ID		INTEGER,		--״̬�ı�����ʱ��--
#                       USER_ID		VARCHAR(20),		--�û���־--
#                       CREATE_DATE      VARCHAR(10),		--��������--
#                       PRODUCT_NO	VARCHAR(15),            --PRODUCT_NO--
#                       USERTYPE_ID	VARCHAR(1),		--�û�״̬��־--
#                       SIM_CODE		VARCHAR(1),		--����SIM����־--
#                       BRAND_ID		VARCHAR(1)		--�û�Ʒ�Ʊ�־--		
#                      )
#        with replace on commit preserve rows not logged in tbs_user_temp"
#        
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#        
#	aidb_commit $conn
#	puts "�����û�ȫ����ʱ��G_BUS_99999_MID3"
#	
#	#----�����û�״̬ȫ����ʱ��-------#
#	set handle [aidb_open $conn]
#
#	set sql_buff "declare global temporary table session.G_BUS_99999_MID4
#                     (
#                       TIME_ID		        INTEGER,		--״̬�ı�����ʱ��--
#                       USER_ID			VARCHAR(20),            --�û���־--
#                       USERTYPE_ID		VARCHAR(4)		--�û�״̬��־--		
#                      )
#        with replace on commit preserve rows not logged in tbs_user_temp"
#
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#
#	aidb_commit $conn
#	puts "�����û�ȫ����ʱ��G_BUS_99999_MID4"
#
#	
#	
#	#---�����û�ȫ����ʱ��---#
#       set handle [aidb_open $conn]
#
#	set sql_buff "insert into session.G_BUS_99999_MID3
#                     select
#                       a.time_id		
#                      ,a.user_id		
#                      ,create_date		
#                      ,product_no		
#                      ,usertype_id	        
#                      ,sim_code		
#                      ,brand_id	
#	              from bass1.g_a_02004_day a,
#                      (select max(time_id) as time_id,user_id from bass1.g_a_02004_day where time_id<${this_month_firstday} group by user_id)b
#	              where a.user_id=b.user_id and a.time_id=b.time_id"
#
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#
#	aidb_commit $conn
#	puts "�����û�ȫ����ʱ��G_BUS_99999_MID3"
#	
#	#-----�����û�״̬ȫ����ʱ��-----#
#	set handle [aidb_open $conn]
#
#	set sql_buff "insert into session.G_BUS_99999_MID4
#                     select 
#                       a.time_id
#                      ,a.user_id
#                      ,a.USERTYPE_ID 
#                      from bass1.G_A_02008_DAY a,
#                      (select max(time_id) as time_id,user_id from bass1.G_A_02008_DAY where time_id<${this_month_firstday} group by user_id)b 
#                       where a.user_id = b.user_id and a.time_id=b.time_id"
#
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#
#	aidb_commit $conn
#	puts "�����û�״̬ȫ����ʱ��G_BUS_99999_MID4"
#	
#	#VIP_CUST_NUM --���˴�ͻ���--#
#	
#	set handle [aidb_open $conn]
#	set sqlbuf "\
#            select
#              count(distinct a.user_id)
#              from bass1.g_i_02005_month a,session.G_BUS_99999_MID2 b
#              where a.user_id = b.user_id
#      	      and a.time_id = $op_month
#     	      and b.time_id < ${next_month_firstday}
#      	      and b.usertype_id not in ('2010','2020','2030','1040','1021','9000'); --�û�״̬���ͱ���--"
#
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set VIP_CUST_NUM [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "���˴�ͻ���: $VIP_CUST_NUM"
#
#	aidb_commit $conn
#	
#	#ADD_VIP_CUST_NUM �������˴�ͻ���#
#	set handle [aidb_open $conn]
#	set sqlbuf "\
#            select
#        count(distinct a.user_id)
#        from 
#        (
#        select user_id from bass1.g_i_02005_month where time_id=$op_month
#        except select user_id from bass1.g_i_02005_month where time_id=${last_month})a, session.g_bus_99999_mid2 b
#        where a.user_id = b.user_id
#      	and b.usertype_id not in ('2010','2020','2030','1040','1021','9000');	--�û�״̬���ͱ���--"
#
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set ADD_VIP_CUST_NUM [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "�������˴�ͻ���: $ADD_VIP_CUST_NUM"
#
#	aidb_commit $conn
#	
#	# AWY_VIP_CUST_NUM �������˴�ͻ���--#
#	set handle [aidb_open $conn]
#	set sqlbuf "\
#            select
#        count(distinct a.user_id)
#        from 
#        (
#        select user_id from bass1.g_i_02005_month where time_id=${last_month}
#        except select user_id from bass1.g_i_02005_month where time_id=${op_month})a, session.g_bus_99999_mid2 b
#        where a.user_id = b.user_id
#      	and b.usertype_id  in ('2010','2020','2030');	--�û�״̬���ͱ���--"
#
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set AWY_VIP_CUST_NUM [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "�������˴�ͻ���: $AWY_VIP_CUST_NUM"
#
#	aidb_commit $conn
#	
#	
#	#ADD_USER_NUM ���������û���--#
#	set handle [aidb_open $conn]
#	set sqlbuf "select
#                      count(distinct a.user_id)
#                      from session.g_bus_99999_mid1 a
#                      where 
#      	              int(a.CREATE_DATE)/100=${op_month}	--ͳ����
#      	              and a.usertype_id <> '3'		--�û����ͱ���
#      	              and a.sim_code <> '1'	        --����SIM���û���־--
#      	              "
#
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set ADD_USER_NUM [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "�����û���:$ADD_USER_NUM"
#	aidb_commit $conn
#	
#	#AWY_USER_NUM ���������û���--#
#	
#	
#	set handle [aidb_open $conn]
#	set sqlbuf "select
#                      count(distinct a.user_id)
#                      from session.g_bus_99999_mid1 a,session.g_bus_99999_mid2 b
#                      where a.user_id = b.user_id
#      	              and int(b.time_id)/100 =${op_month}		        --ͳ����
#      	              and a.usertype_id <> '3'		        --�û����ͱ���--
#      	              and a.sim_code <> '1'	                --����SIM���û���־--
#      	              and b.usertype_id in ('2010','2020','2030'); --�û�״̬���ͱ���--"
#
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set AWY_USER_NUM [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "�����û���:$AWY_USER_NUM"
#
#	aidb_commit $conn
#	
#	#---ZERO_CALL_USER_NUM ���ͨ���û���--#
#	set handle [aidb_open $conn]
#	set sqlbuf " select
#                      count(distinct a.product_no)
#                      from
#                      (
#                      select a.product_no from session.g_bus_99999_mid1 a,session.g_bus_99999_mid2 b
#                      where a.user_id = b.user_id
#      	              and int(a.time_id)/100=${op_month}		--ͳ����
#      	              and a.usertype_id <> '3'		--�û����ͱ���
#      	              and a.sim_code <> '1'	                 --����SIM���û���־
#      	              and b.usertype_id not in ('2010','2020','2030','1040','1021','9000')	--�û�״̬���ͱ���--
#      	              except select a.product_no from
#               	       (select product_no from bass1.g_s_21003_month where time_id = ${op_month} and bigint(base_bill_duration) > 0) a
#               	        left join (select product_no from bass1.g_s_21006_month where time_id = ${op_month} and bigint(base_bill_duration) > 0) b on a.product_no = b.product_no
#               	        left join (select product_no from bass1.g_s_21009_day where int(time_id)/100 = ${op_month} and bigint(base_bill_duration) > 0) c on a.product_no = c.product_no) a;"
#
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set ZERO_CALL_USER_NUM  [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "���ͨ���û���:$ZERO_CALL_USER_NUM"
#
#	aidb_commit $conn
#	
#	#--NEW_BUS_USE_USER_NUM ��ҵ��ʹ���û���--#
#	set handle [aidb_open $conn]
#	set sqlbuf " select
#        a.cnt+b.cnt
#        from (select count(distinct a.user_id) as cnt from bass1.g_s_03004_month a,session.g_bus_99999_mid1 b
#      	where a.user_id = b.user_id
#      	      and a.time_id = ${op_month}
#      	      and b.usertype_id <> '3'
#      	      and b.sim_code <> '1'
#      	      and (a.acct_item_id in ('0405','0407') or int(a.acct_item_id)/100 in (5,6,7))) a,
#            (select count(distinct user_id) as cnt from bass1.g_s_03012_month
#      	where time_id = ${op_month}
#      	      and (acct_item_id in ('0405','0407') or int(acct_item_id)/100 in (5,6,7))) b;"
#
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set NEW_BUS_USE_USER_NUM  [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "��ҵ��ʹ���û���:$NEW_BUS_USE_USER_NUM"
#
#	aidb_commit $conn
#	
#	#---HIGH_VALUE_USER_NUM �߼�ֵ�û���--#
#	set handle [aidb_open $conn]
#	set sqlbuf " select
#                       c.cnt+d.cnt
#                    from (select count(*) as cnt from
#      	             (select user_id from bass1.g_s_03005_month
#      	              where time_id = ${op_month}
#      	              group by user_id
#      	              having sum(int(should_fee))/700 >= ${thismonthdays}) a) c,
#                       (select count(*) as cnt from
#      	             (select user_id from bass1.g_s_03012_month
#      	              where time_id = ${op_month}
#      	              group by user_id
#      	              having sum(int(incm_amt))/700 >= ${thismonthdays}) b) d"
#
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set HIGH_VALUE_USER_NUM  [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "�߼�ֵ�û���:$HIGH_VALUE_USER_NUM"
#
#	aidb_commit $conn
#	
#	#--GSM_HIGH_VALUE_USER_NUM ȫ��ͨ�߼�ֵ�û���#
#	#--SZX_HIGH_VALUE_USER_NUM �����и߼�ֵ�û���#
#	#--M_ZONE_HIGH_VALUE_USER_NUM ���еش��߼�ֵ�û���#
#	set handle [aidb_open $conn]
#
#	set sqlbuf "\
#                  select
#                  case when b.brand_id='1' then count(distinct a.user_id) else 0 end,
#                  case when b.brand_id='2' then count(distinct a.user_id) else 0 end,
#                  case when b.brand_id='3' then count(distinct a.user_id) else 0 end
#                  from (
#                   select
#                   user_id
#                  from
#                  (select user_id from bass1.g_s_03005_month
#                   where time_id = ${op_month}
#                   group by user_id
#                   having sum(int(should_fee))/700 >= ${thismonthdays}
#                   union
#                   select user_id from bass1.g_s_03012_month
#                   where time_id = ${op_month}
#                   group by user_id
#                   having sum(int(incm_amt))/700 >= ${thismonthdays})c)a,session.g_bus_99999_mid1 b
#                   where a.user_id = b.user_id
#                   and b.usertype_id <> '3'		         --�û����ͱ���--
#                   and b.sim_code <> '1'	                 --����SIM���û���־--
#                   group by
#                   b.brand_id "
#
#
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#        	WriteTrace $errmsg 1013
#        	return -1
#        }
#
#        while {[set p_success [aidb_fetch $handle]] != "" } {
#                set GSM_HIGH_VALUE_USER_NUM     	      [lindex $p_success 0]
#	        set SZX_HIGH_VALUE_USER_NUM      	      [lindex $p_success 1]
#	        set M_ZONE_HIGH_VALUE_USER_NUM     	      [lindex $p_success 2]
#	                    }
#
#	aidb_commit $conn
#	puts "ȫ��ͨ�߼�ֵ�û���: ${GSM_HIGH_VALUE_USER_NUM}"
#	puts "�����и߼�ֵ�û���: ${SZX_HIGH_VALUE_USER_NUM}"
#	puts "���еش��߼�ֵ�û���: ${M_ZONE_HIGH_VALUE_USER_NUM}"
#	
#	#--RESERV_USER_NUM �������û���--#
#	set handle [aidb_open $conn]
#	set sqlbuf " select
#                      count(distinct a.user_id)
#                      from session.g_bus_99999_mid1 a,session.g_bus_99999_mid2 b
#                      where a.user_id = b.user_id
#      	              and a.usertype_id <> '3'		
#      	              and a.sim_code <> '1'	                 
#      	              and b.usertype_id='1040';"
#
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set RESERV_USER_NUM  [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "�������û���:$RESERV_USER_NUM"
#
#	aidb_commit $conn
#	
#	#--FREEZE_USER_NUM �䶳���û���--#
#	set handle [aidb_open $conn]
#	set sqlbuf " select
#                      count(distinct a.user_id)
#                      from session.g_bus_99999_mid1 a,session.g_bus_99999_mid2 b
#                      where a.user_id = b.user_id
#      	              and a.usertype_id <> '3'		
#      	              and a.sim_code <> '1'	                 
#      	              and b.usertype_id='2030';"
#
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set FREEZE_USER_NUM  [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "�䶳���û���:$FREEZE_USER_NUM"
#	
#	                  
#        #CALLING_TIMES  ͨ������ --#                  
#        #BILLING_DUR    �Ʒ�ʱ�� --#
#        #CALLING_DUR ͨ��ʱ�� --# 
#        set handle [aidb_open $conn]
#
#	set sqlbuf "
#                   select
#                   a.cnt1+b.cnt1+c.cnt1,
#                   a.cnt2+b.cnt2+c.cnt2,
#                   bigint(double(a.cnt3+b.cnt3+c.cnt3)/60)
#                   from (select sum(bigint(call_counts)) as cnt1,sum(bigint(base_bill_duration)) as cnt2,sum(bigint(call_duration)) as cnt3
#      	           from bass1.g_s_21003_month
#      	           where time_id = ${op_month}
#      	                 and roam_type_id not in ('122','202','302','401')) a,
#                       (select sum(bigint(call_counts)) as cnt1,sum(bigint(base_bill_duration)) as cnt2,sum(bigint(call_duration)) as cnt3
#      	           from bass1.g_s_21006_month
#      	           where time_id = ${op_month}
#      	                 and roam_type_id not in ('122','202','302','401')) b,
#                       (select sum(bigint(call_counts)) as cnt1,sum(bigint(base_bill_duration)) as cnt2,sum(bigint(CALL_DURATION)) as cnt3
#      	           from bass1.g_s_21009_day
#      	           where int(time_id)/100 = ${op_month}
#      	      and roam_type_id not in ('122','202','302','401')) c "
#
#
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#        	WriteTrace $errmsg 1013
#        	return -1
#        }
#
#        while {[set p_success [aidb_fetch $handle]] != "" } {
#                set CALLING_TIMES     	      [lindex $p_success 0]
#	        set BILLING_DUR      	      [lindex $p_success 1]
#	        set CALLING_DUR     	      [lindex $p_success 2]
#	        		             }
#
#	aidb_commit $conn
#	puts "ͨ������: ${CALLING_TIMES}"
#	puts "�Ʒ�ʱ��: ${BILLING_DUR}"
#	puts "ͨ��ʱ��: ${CALLING_DUR}"
#	
#	#BUSY_TIME_BILLING_DUR æʱ�Ʒ�ʱ��#
#	set handle [aidb_open $conn]
#	set sqlbuf "select
#                    sum(cnt)
#                    from
#                    (
#                    select
#                            sum(cnt) as cnt,
#                            call_moment_id
#                           from (
#                    	   select sum(bigint(base_bill_duration)) as cnt,call_moment_id from bass1.g_s_21002_day where int(time_id)/100 = ${op_month} group by call_moment_id
#                    	   union
#                           select sum(bigint(base_bill_duration)) as cnt,call_moment_id from bass1.g_s_21005_day where int(time_id)/100 = ${op_month} group by call_moment_id
#                    	   union
#                           select sum(bigint(base_bill_duration)) as cnt,callmoment_id as call_moment_id from bass1.g_s_21016_day where int(time_id)/100 = ${op_month} group by callmoment_id)a
#                           group by 
#                           call_moment_id
#                    	   order by cnt desc
#                    	   fetch first 2 rows only
#                    	   )aa"
#
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set BUSY_TIME_BILLING_DUR [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "æʱ�Ʒ�ʱ��:$BUSY_TIME_BILLING_DUR"
#
#	aidb_commit $conn
#	
#	#--SMS_DATA_VOL -����ͨ����--#
#	set handle [aidb_open $conn]
#	set sqlbuf "select
#        a.cnt+b.cnt+c.cnt
#        from (select count(*) as cnt from bass1.g_s_04005_day
#      	where int(time_id)/100 = ${op_month}
#      	      and sms_status = '0' and info_type in ('01','02','03','99')) a,
#            (select count(*) as cnt from bass1.g_s_04014_day
#      	where int(time_id)/100 = ${op_month}
#      	      and sms_send_state = '0' and sms_bill_type in ('00','01','10','11')) b,
#            (select sum(bigint(sms_counts)) as cnt from bass1.g_s_21008_month
#      	where time_id = ${op_month}
#      	      and svc_type_id in ('11','12','13','14','20','31','32','40','64','65','66','70') 
#      	      and end_status = '0' and cdr_type_id in ('00','01','10','11')) c"
#
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set SMS_DATA_VOL  [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "����ͨ����:$SMS_DATA_VOL"
#
#	aidb_commit $conn
#	
#	
#	#--PPP_SMS_DATA_VOL ��Ե����ͨ����#
#	set handle [aidb_open $conn]
#	set sqlbuf "select
#                     sum(bigint(sms_counts))
#                     from bass1.g_s_21008_month
#                     where time_id = ${op_month}
#                     and end_status ='0'
#                     and cdr_type_id in ('00','01','10','11','21','28')
#                     and svc_type_id in ('11','12','13');"
#                                                             
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set PPP_SMS_DATA_VOL  [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "��������ͨ����:$PPP_SMS_DATA_VOL"
#
#	aidb_commit $conn
#		
#	
#	#--MNTERNET_SMS_DATA_VOL ��������ͨ����--#
#	set handle [aidb_open $conn]
#	set sqlbuf "select
#                    count(*)
#                    from bass1.g_s_04005_day
#                    where 
#                     int(time_id)/100 =${op_month}
#      	             and sms_status = '0'
#      	             and info_type in ('01','02','03','99')"
#                                                             
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set MNTERNET_SMS_DATA_VOL  [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "��������ͨ����:$MNTERNET_SMS_DATA_VOL"
#
#	aidb_commit $conn
#	
#	#-- MMS_DATA_VOL����ͨ����--#
#	set handle [aidb_open $conn]
#	set sqlbuf "select
#                     count(*)
#                     from bass1.G_S_04004_DAY
#                     where int(time_id)/100 =${op_month}"
#                                                             
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set MMS_DATA_VOL  [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "����ͨ����:$MMS_DATA_VOL"
#
#	aidb_commit $conn
#	
#	#--PPP_MMS_DATA_VOL ��Ե����ͨ����--#
#	set handle [aidb_open $conn]
#	set sqlbuf " select
#                      count(*)
#                      from bass1.g_s_04004_day
#                      where int(time_id)/100 =${op_month}
#                      and bus_srv_id ='1'"
#                                                             
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set PPP_MMS_DATA_VOL  [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "��Ե����ͨ����:$PPP_MMS_DATA_VOL"
#
#	aidb_commit $conn
#	
#	#--GPRS_UP_DATA_VOL GPRS ͨ����(����)--#
#	#--GPRS_DOWN_DATA_VOL GPRS ͨ����(����)--#
#	set handle [aidb_open $conn]
#
#	set sqlbuf "
#                   select
#                   sum(bigint(uplink_flow_on_tariff1))/1024,sum(bigint(downlink_flow_on_tariff1))/1024
#                   from bass1.g_s_04002_day
#                   where int(time_id)/100 =${op_month} "
#
#
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#        	WriteTrace $errmsg 1013
#        	return -1
#        }
#
#        while {[set p_success [aidb_fetch $handle]] != "" } {
#                set GPRS_UP_DATA_VOL    	      [lindex $p_success 0]
#	        set GPRS_DOWN_DATA_VOL      	      [lindex $p_success 1]
#
#	        		             }
#
#	aidb_commit $conn
#	puts "ͨ����(����): ${GPRS_UP_DATA_VOL}"
#	puts "ͨ����(����): ${GPRS_DOWN_DATA_VOL}"
#	
#	#--BUS_INCOME ҵ������--#
#	set handle [aidb_open $conn]
#	set sqlbuf " select
#        sum(cnt)
#        from (
#		select sum(bigint(should_fee)) as cnt from bass1.g_s_03005_month
#      	where time_id = ${op_month} and item_id in ('0100','0200','0300','0400','0500','0600','0700','0900')
#		union
#	    select sum(bigint(incm_amt)) as cnt from bass1.g_s_03012_month
#        where time_id = ${op_month}
#		)a"
#                                                             
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set BUS_INCOME  [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "ҵ������:$BUS_INCOME"
##        
#	aidb_commit $conn
#	#set BUS_INCOME 1000000
#	
#	#--NEW_BUS_INCOME ��ҵ������--#
#	set handle [aidb_open $conn]
#	set sqlbuf " select
#                     a.cnt+b.cnt
#                     from (select coalesce(sum(bigint(fee_receivable)),0) as cnt from bass1.g_s_03004_month
#      	            where time_id =${op_month}
#      	                  and (acct_item_id in ('0405','0407')
#      	                  or int (acct_item_id)/100 in (5,6,7))) a,
#                  (select coalesce(sum(bigint(incm_amt)),0) as cnt from g_s_03012_month
#      	            where time_id =${op_month}
#      	                  and (acct_item_id in ('0405','0407')
#      	                   or int (acct_item_id)/100 in (5,6,7))) b"
#                                                             
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set NEW_BUS_INCOME  [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "��ҵ������:$NEW_BUS_INCOME"
#
#	aidb_commit $conn
#	
#	#--��arpu--#
#	#����ARPU=����ҵ������/������������12/����������/����ƽ���ͻ���
#	#����ƽ���ͻ���=������ĩ�û�������������ĩ�û���������/2 
#	#--this_user_num ����ĩ�û�������--#
#	set handle [aidb_open $conn]
#	set sqlbuf " select                                                                   
#                      count(distinct a.user_id)                                              
#                      from session.g_bus_99999_mid1 a,session.g_bus_99999_mid2 b             
#                      where a.user_id = b.user_id                                            
#                      and a.usertype_id <> '3'		         --�û����ͱ���--                 
#                      and a.sim_code <> '1'	                 --����SIM���û���־--           
#                      and b.usertype_id not in ('2010','2020','2030','1040','1021','9000'); "
#                                                             
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set this_user_num  [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "����ĩ�û�������:$this_user_num"
#
#	aidb_commit $conn
#	#--last_user_num ����ĩ�û�������--#
#	set handle [aidb_open $conn]
#	set sqlbuf " select                                                                   
#                      count(distinct a.user_id)                                              
#                      from session.g_bus_99999_mid3 a,session.g_bus_99999_mid4 b             
#                      where a.user_id = b.user_id                                            
#                      and a.usertype_id <> '3'		         --�û����ͱ���--                 
#                      and a.sim_code <> '1'	                 --����SIM���û���־--           
#                      and b.usertype_id not in ('2010','2020','2030','1040','1021','9000'); "
#                                                             
#        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#
#	if [catch {set last_user_num  [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	puts "����ĩ�û�������:$last_user_num"
#
#	set aidb_commit $conn
#	
#
#	set ARPU [format "%.2f" [expr (${BUS_INCOME} /1.00000/ (${thismonthdays} * 12/${thisyeardays})/(($this_user_num+$last_user_num)/2))]]
#	puts "ARPU:$ARPU"
#	
#	#��$db_user.G_BUS_CHECK_BILL_MONTH�в���һ������#
#        set handle [aidb_open $conn]
#	set sql_buff "insert into $db_user.G_BUS_CHECK_BILL_MONTH values($op_month,'${VIP_CUST_NUM}','${ADD_VIP_CUST_NUM}','${AWY_VIP_CUST_NUM}','${ADD_USER_NUM}','${AWY_USER_NUM}','${ZERO_CALL_USER_NUM}','${NEW_BUS_USE_USER_NUM}','${HIGH_VALUE_USER_NUM}','${GSM_HIGH_VALUE_USER_NUM}','${SZX_HIGH_VALUE_USER_NUM}','${M_ZONE_HIGH_VALUE_USER_NUM}','${RESERV_USER_NUM}','${FREEZE_USER_NUM}','${CALLING_TIMES}','${BILLING_DUR}','${BUSY_TIME_BILLING_DUR}','${CALLING_DUR}','${SMS_DATA_VOL}','${PPP_SMS_DATA_VOL}','${MNTERNET_SMS_DATA_VOL}','${MMS_DATA_VOL}','${PPP_MMS_DATA_VOL}','${GPRS_UP_DATA_VOL}','${GPRS_DOWN_DATA_VOL}','${BUS_INCOME}','${NEW_BUS_INCOME}','${ARPU}')"
#
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "Error: SQL query failed! The error message is:\n	$errmsg" 2020
#		puts $errmsg
#		aidb_close $handle
#		return -1
#	}
#
#	aidb_commit $conn
#	      
#	aidb_close $handle
#
#	return 0
#}

#############################