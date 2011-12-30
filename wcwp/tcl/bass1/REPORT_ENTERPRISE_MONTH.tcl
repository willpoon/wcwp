######################################################################################################
#±¨±íÃû³Æ£º2007¼¯ÍÅ¿Í»§ÔÂ±¨±í
#³ÌĞòÃû³Æ: REPORT_ENTERPRISE_MONTH.tcl
#¹¦ÄÜÃèÊö: ¸ù¾İ¼¯ÍÅÍ³¼Æ¿Ú¾¶ÔÚ±¾µØÉú³ÉÕâĞ©Êı¾İÖµ
#ÔËĞĞÁ£¶È: ÔÂ
#Ô´    ±í£º1.bass2.dwd_enterprise_msg_yyyymmdd
#          2.bass1.dwd_enterprise_manager_rela_yyyymmdd
#          3.bass2.dw_enterprise_member_ds
#          4.bass1.g_a_01001_day
#ÊäÈë²ÎÊı: 
#Êä³ö²ÎÊı: ·µ»ØÖµ:0 ³É¹¦;-1 Ê§°Ü
#±à Ğ´ ÈË£ºÍõçù
#±àĞ´Ê±¼ä£º2007-07-05
#ÎÊÌâ¼ÇÂ¼£º1.
#ĞŞ¸ÄÀúÊ·: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        
        #±¾ÔÂ yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        #±¾ÔÂ×îºóÒ»Ìì yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        

	 #±¾ÄêÌìÊı this_year_days	
        set this_year_days [GetThisYearDays ${op_month}01]

	      #±¾ÔÂÌìÊı	this_month_days
        set this_month_days [GetThisMonthDays ${op_month}01]


        #ÉÏÔÂ	$last_month	
       set last_month [GetLastMonth [string range $op_month 0 5]]
       
       #ÉÏÄê×îºóÒ»¸öÔÂ
       set LastYearMonth [GetLastMonth [string range $op_month 0 3]01]
   


        #É¾³ı±¾ÆÚÊı¾İ
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.report_enterprise_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
            
        #GP0030001:¼¯ÍÅ¿Í»§µ±ÔÂµ½´ïÊı
        #GP0030002:    ÆäÖĞAÀà¼¯ÍÅ¿Í»§µ±ÔÂµ½´ïÊı
        #GP0030003:    ÆäÖĞBÀà¼¯ÍÅ¿Í»§µ±ÔÂµ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month
                     ,'GP0030001'
                     ,count(a.enterprise_id)
		   from 
		     bass1.g_a_01004_day a,
		     (select max(time_id) as time_id,enterprise_id from bass1.g_a_01004_day
		      where time_id<=$this_month_last_day
		      group by enterprise_id
		     )b
		   where 
		     a.cust_statu_typ_id='20' 
		     and a.time_id=b.time_id 
		     and a.enterprise_id=b.enterprise_id "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

        #GP0030002:    ÆäÖĞAÀà¼¯ÍÅ¿Í»§µ±ÔÂµ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month
                     ,'GP0030002'
                     ,count(a.enterprise_id)
		   from 
		     bass1.g_a_01004_day a,
		     (select max(time_id) as time_id,enterprise_id from bass1.g_a_01004_day
		      where time_id<=$this_month_last_day
		      group by enterprise_id
		     )b
		   where 
		     a.cust_statu_typ_id='20' 
		     and a.ent_def_mode='1'
		     and a.time_id=b.time_id 
		     and a.enterprise_id=b.enterprise_id "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	

        #GP0030003:    ÆäÖĞBÀà¼¯ÍÅ¿Í»§µ±ÔÂµ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month
                     ,'GP0030003'
                     ,count(a.enterprise_id)
		   from 
		     bass1.g_a_01004_day a,
		     (select max(time_id) as time_id,enterprise_id from bass1.g_a_01004_day
		      where time_id<=$this_month_last_day
		      group by enterprise_id
		     )b
		   where 
		     a.cust_statu_typ_id='20' 
		     and a.ent_def_mode='2'
		     and a.time_id=b.time_id 
		     and a.enterprise_id=b.enterprise_id "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
        
        #GP0030004:µ±ÔÂ¼¯ÍÅ¿Í»§ÕûÌåÊÕÈë
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month
                     ,'GP0030004'
                     ,sum(t.fee)
                   from
                     (
                      select
                        sum(bigint(income)) as fee
                      from
                        bass1.g_s_03013_month
                      where 
                        time_id=$op_month
                      union all
                      select
                       sum(CONSUME_FEE) as fee 
                     from
                       bass1.report_user_month 
                     where
                       time_id=$op_month 
                    )t"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	

        #GP0030005:Í³Ò»¸¶·ÑµÄ¼¯ÍÅ¿Í»§µ±ÔÂµ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030005',
                     count(a.enterprise_id)
		   from 
		     bass1.g_a_01004_day a,
		     (select max(time_id) as time_id,enterprise_id from bass1.g_a_01004_day
		      where time_id<=$this_month_last_day
		      group by enterprise_id
		     )b
		   where 
		     a.cust_statu_typ_id='20' 
		     and a.UNITE_PAY_FLAG='1'
		     and a.time_id=b.time_id 
		     and a.enterprise_id=b.enterprise_id "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
	
        #GP0030034:»îÔ¾µÄÍ³Ò»¸¶·ÑµÄ¼¯ÍÅ¿Í»§µ±ÔÂµ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030034',
                     count(ENTERPRISE_ID)
                   from bass1.g_s_03013_month
                   where time_id=$op_month "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
	
        #GP0030007:¼¯ÍÅ¸öÈË¿Í»§µ±ÔÂµ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030007',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               and
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') and
                     DATE_SIM_FLAG = 0                                                 and 
                     ENTERPRISE_FLAG = 1"
                     
                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
	
        #GP0030008:ÆäÖĞAÀà¼¯ÍÅ¸öÈË¿Í»§µ±ÔÂµ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030008',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     ENTERPRISE_FLAG = 1 "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	

        #GP0030009:ÆäÖĞBÀà¼¯ÍÅ¸öÈË¿Í»§µ±ÔÂµ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030009',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     ENTERPRISE_FLAG = 2 "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
        
        
	
	
        #GP0030010:¼¯ÍÅÖĞ¸ß¶Ë¸öÈË¿Í»§µ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030010',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     HIGH_PHOTO_FLAG = 1 "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
	
        #GP0030011:AÀà¼¯ÍÅÖĞ¸ß¶Ë¸öÈË¿Í»§µ±ÔÂµ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030011',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     ENTERPRISE_FLAG = 1                                               AND 
                     HIGH_PHOTO_FLAG = 1 "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	

        #GP0030012:BÀà¼¯ÍÅÖĞ¸ß¶Ë¸öÈË¿Í»§µ±ÔÂµ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030012',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     ENTERPRISE_FLAG = 2                                               AND 
                     HIGH_PHOTO_FLAG = 1 "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
	
        #GP0030013:¼¯ÍÅÅÄÕÕÖĞ¸ß¶Ë¸öÈË¿Í»§±£ÓĞÂÊ
        set handle [aidb_open $conn]
        set sql_buff "\
                    select
                      count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $LastYearMonth                                          AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     ENTERPRISE_FLAG = 2                                               AND 
                     HIGH_PHOTO_FLAG = 1"               
        puts $sql_buff
        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set Tmp_GP0030013_LastYearMonth [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
###################Ğ´Ò»¸öSELECT ´ÓÀÏÏµÍ³ÖĞÕÒ³öÈ¥ÄêµÄ¼¯ÍÅÅÄÕÕÖĞ¸ß¶Ë¸öÈË¿Í»§
############################µ÷ÊÔ
set  Tmp_GP0030013_LastYearMonth 1
#####################	
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030013',
                     count(*)/$Tmp_GP0030013_LastYearMonth
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     ENTERPRISE_FLAG = 2                                               AND 
                     HIGH_PHOTO_FLAG = 1 "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	 
	 

        #GP0030014:¼¯ÍÅ¸öÈË´ó¿Í»§µ±ÔÂµ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030014',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     VIP_FLAG = 1 "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	


        #GP0030015:ÆäÖĞ×ê¿¨»áÔ±µ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030015',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     VIP_LVL = '1'                                                     AND
                     VIP_FLAG = 1 "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	

        #GP0030016:ÆäÖĞ½ğ¿¨»áÔ±µ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030016',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     VIP_LVL = '2'                                                     AND
                     VIP_FLAG = 1 "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	



        #GP0030017:ÆäÖĞÒø¿¨»áÔ±µ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030017',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     VIP_LVL = '3'                                                     AND
                     VIP_FLAG = 1 "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	


        #GP0030018:µ±ÔÂĞÂÔö¼¯ÍÅ¸öÈË¿Í»§µ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030018',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     CREATE_DATE/100 = $op_month "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	

        #GP0030019:µ±ÔÂĞÂÈëÍø¼¯ÍÅ¸öÈË¿Í»§µ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030019',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     NEW_MONTH_FLAG = 1                                                AND
                     CREATE_DATE/100 = $op_month "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	

        #GP0030020:AÀà¼¯ÍÅÖĞĞÂÈëÍøµÄ¼¯ÍÅ¸öÈË¿Í»§µ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030020',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     NEW_MONTH_FLAG = 1                                                AND
                     ENTERPRISE_FLAG = 1                                               AND
                     CREATE_DATE/100 = $op_month "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	

        #GP0030021:BÀà¼¯ÍÅÖĞĞÂÈëÍøµÄ¼¯ÍÅ¸öÈË¿Í»§µ½´ïÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030021',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     NEW_MONTH_FLAG = 1                                                AND
                     ENTERPRISE_FLAG = 2                                               AND
                     CREATE_DATE/100 = $op_month "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
	
        #GP0030022:¼¯ÍÅ¸öÈË¿Í»§µ±ÔÂÀëÍøÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030022',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID  in ('2010','2020','2030')                         AND
                     DATE_SIM_FLAG = 0 "                                                 
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
        #GP0030023:AÀà¼¯ÍÅ¸öÈË¿Í»§µ±ÔÂÀëÍøÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030023',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID  in ('2010','2020','2030')                         AND
                     ENTERPRISE_FLAG = 1                                               AND
                     DATE_SIM_FLAG = 0 "                                                 
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
        #GP0030024:BÀà¼¯ÍÅ¸öÈË¿Í»§µ±ÔÂÀëÍøÊı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030024',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID  in ('2010','2020','2030')                         AND
                     ENTERPRISE_FLAG = 2                                               AND
                     DATE_SIM_FLAG = 0 "                                                 
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
	
        #GP0030025:ÔÂÄ©Á÷Ê§µÄ¼¯ÍÅ¸öÈË¿Í»§Êı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030025',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     OFF_MONTH_FLAG = 1                                                AND
                     DATE_SIM_FLAG = 0 "                                                 
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
        #GP0030026:AÀà¼¯ÍÅ¸öÈË¿Í»§µ±ÔÂÁ÷Ê§Êı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030026',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     OFF_MONTH_FLAG = 1                                                AND
                     ENTERPRISE_FLAG = 1                                               AND
                     DATE_SIM_FLAG = 0 "                                                 
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
        #GP0030027:BÀà¼¯ÍÅ¸öÈË¿Í»§µ±ÔÂÁ÷Ê§Êı
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030027',
                     count(*)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     USER_STATUS_ID not in ('2010','2020','2030','1040','9000','1021') AND
                     OFF_MONTH_FLAG = 1                                                AND
                     ENTERPRISE_FLAG = 2                                               AND
                     DATE_SIM_FLAG = 0 "                                                 
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
	
        #GP0030028:µ±ÔÂ¼¯ÍÅ¸öÈË¿Í»§ÀëÍøÂÊ
        set handle [aidb_open $conn]
        set sql_buff "\
                    select
                      result
                    from
                      bass1.report_enterprise_month
                    where 
                      TIME_ID = $last_month
                      and s_index_id='GP0030007' "     
        puts $sql_buff
        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set Tmp_GP0030007_LastMonth [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
####################µ÷ÊÔ
set Tmp_GP0030007_LastMonth 234
######################	        
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030028',
                      a.result*$this_year_days*1.000/($this_month_days*12.00)/(b.result+$Tmp_GP0030007_LastMonth)*2.0
                   from
                     (
                     select result from bass1.report_enterprise_month
                     where TIME_ID = $op_month  and s_index_id='GP0030022'
                     )a,
                     (
                     select result from bass1.report_enterprise_month
                     where TIME_ID = $op_month  and s_index_id='GP0030007'
                     )b                  "                                              
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle		
	

        #GP0030029:µ±ÔÂ¼¯ÍÅ¸öÈË¿Í»§ÀëÍøÂÊ	        
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030029',
                      a.result*$this_year_days*1.00/($this_month_days*12.00)/(b.result+$Tmp_GP0030007_LastMonth)*2.0
                   from
                     (
                      select result from bass1.report_enterprise_month 
                      where TIME_ID = $op_month  and  s_index_id='GP0030025' 
                     )a,
                     (
                      select result from bass1.report_enterprise_month 
                      where TIME_ID = $op_month  and s_index_id='GP0030007' 
                     )b"                                                 
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle		
	
	
        #GP0030030:¼¯ÍÅ¸öÈË¿Í»§ARPU(Ôª/»§.ÔÂ)	        
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030030',
                      sum(CONSUME_FEE)/count(*)
                   from
                     bass1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month "                                                 
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle		

	
        #GP0030031:¼¯ÍÅVÍø³ÉÔ±ARPU(Ôª/»§.ÔÂ)   
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030031',
                      sum(t.CONSUME_FEE)/sum(t.UserCount)
                   from
                     (
                      select
                        sum(CONSUME_FEE) as CONSUME_FEE,Count(*) as UserCount
                      from
                        bass1.g_i_02016_month left outer join bass1.REPORT_USER_MONTH
                      on 
                        bass1.REPORT_USER_MONTH.TIME_ID=$op_month  AND 
                        bass1.g_i_02016_month.User_ID = bass1.REPORT_USER_MONTH.User_ID
                        
                      union all
                      
                      select
                        sum(CONSUME_FEE) as CONSUME_FEE,Count(*) as UserCount
                      from
                        bass1.g_i_02019_month left outer join bass1.REPORT_USER_MONTH
                      on 
                        bass1.REPORT_USER_MONTH.TIME_ID=$op_month  AND 
                        bass1.g_i_02019_month.User_ID = bass1.REPORT_USER_MONTH.User_ID
                     
                     ) t"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle		




        #GP0030032:¼¯ÍÅ¸öÈË¿Í»§MOU(·ÖÖÓ/»§.ÔÂ)        
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030032',
                      sum(CALL_DURN)/count(*)
                   from
                     bass1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month "                                                 
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle		


        #GP0030033:¼¯ÍÅVÍø³ÉÔ±MOU(·ÖÖÓ/»§.ÔÂ)  
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030033',
                      sum(t.CALL_DURN)/sum(t.UserCount)
                   from
                     (
                      select
                        sum(CALL_DURN) as CALL_DURN,Count(*) as UserCount
                      from
                        bass1.g_i_02016_month left outer join bass1.REPORT_USER_MONTH
                      on 
                        bass1.REPORT_USER_MONTH.TIME_ID=$op_month  AND 
                        bass1.g_i_02016_month.User_ID = bass1.REPORT_USER_MONTH.User_ID
                        
                      union all
                      
                      select
                        sum(CALL_DURN) as CALL_DURN,Count(*) as UserCount
                      from
                        bass1.g_i_02019_month left outer join bass1.REPORT_USER_MONTH
                      on 
                        bass1.REPORT_USER_MONTH.TIME_ID=$op_month  AND 
                        bass1.g_i_02019_month.User_ID = bass1.REPORT_USER_MONTH.User_ID
                     
                     ) t"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle		
	
	
	

        #GP0030067:ÆäÖĞµ±ÔÂMASÀàĞĞÒµÓ¦ÓÃÊÕÈë
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030067',
                     sum(CONSUME_FEE)
                   from
                     BASS1.g_s_03013_month
                   where
                     TIME_ID = $op_month                                               AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     NEW_MONTH_FLAG = 1  "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
	

        #GP0030088:µ±ÔÂ¼¯ÍÅ¸öÈË¿Í»§ÊÕÈë
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030088',
                     sum(CONSUME_FEE)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     NEW_MONTH_FLAG = 1  "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
        #GP0030089:µ±ÔÂ¼¯ÍÅ¸öÈË¿Í»§¼Æ·ÑÊ±³¤
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030089',
                     sum(CALL_DURN)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     NEW_MONTH_FLAG = 1  "                                                     
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
	

        #GP0030064:µ±ÔÂ¼¯ÍÅ¿Í»§ÕûÌåÊÕÈë
        #¼ÆËãß
        
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.report_enterprise_month
                   select
                     $op_month,
                     'GP0030064',
                     sum(CALL_DURN)
                   from
                     BASS1.REPORT_USER_MONTH
                   where
                     TIME_ID = $op_month                                               AND
                     DATE_SIM_FLAG = 0                                                 AND 
                     NEW_MONTH_FLAG = 1  "                                                     
        puts $sql_buff
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

















	
#ÄÚ²¿º¯Êı²¿·Ö	
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
