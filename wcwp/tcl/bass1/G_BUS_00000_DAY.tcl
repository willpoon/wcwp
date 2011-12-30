######################################################################################################
#接口名称：每日提供的业务指标校验文件
#接口编码：00000
#接口说明：
#程序名称: G_BUS_00000_DAY.tcl
#功能描述: 生成00000数据
#运行粒度: 日
#源    表：1.bass1.g_a_02004_day
#          2.bass1.G_A_02008_DAY
#          3.bass1.G_A_01004_DAY
#          4.bass1.G_A_01001_DAY
#          5.bass1.int_210012916_yyyymm
#          6.bass1.g_s_21002_day
#          7.bass1.g_s_21005_day 
#          8.bass1.g_s_21016_day 
#          9.bass1.G_S_04005_DAY
#         10.bass1.G_S_21007_DAY
#         11.bass1.G_S_04014_DAY
#         12.bass1.G_S_04004_DAY
#         13.bass1.G_S_04002_DAY  
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: 20090921 取消其主体代码程序，提升9点之前接口生成速度
#######################################################################################################



proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env

	set Optime $op_time
	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set op_month [string range $op_time 0 3][string range $op_time 5 6]
        append op_time_month ${optime_month}-01
        set db_user $env(DB_USER)

######        set handle [aidb_open $conn]
######	set sql_buff "\
######		DELETE FROM $db_user.G_BUS_CHECK_ALL_DAY where time_id=${Timestamp} "
######	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######		WriteTrace "$errmsg" 2005
######		aidb_close $handle
######		return -1
######	}
######	aidb_commit $conn
######	aidb_close $handle
######
######        #------生成用户全量临时表-------#
######        set handle [aidb_open $conn]
######	set sql_buff "declare global temporary table session.G_BUS_00000_MID1
######                     (
######                       TIME_ID		INTEGER,
######                       USER_ID		VARCHAR(20),
######                       CREATE_DATE      VARCHAR(10),
######                       PRODUCT_NO	VARCHAR(15),
######                       USERTYPE_ID	VARCHAR(1),
######                       SIM_CODE		VARCHAR(1),
######                       BRAND_ID		VARCHAR(1)
######                      )
######                      partitioning key
######	              (USER_ID)
######	              using hashing
######                      with replace on commit preserve rows not logged in tbs_user_temp"
######        puts $sql_buff
######	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######		WriteTrace "$errmsg" 2020
######		puts $errmsg
######		aidb_close $handle
######		return -1
######	}
######	aidb_commit $conn
######	aidb_close $handle
###### 
######	puts "生成用户全量临时表"
######
######	#----生成用户状态全量临时表-------#
######	set handle [aidb_open $conn]
######	set sql_buff "declare global temporary table session.G_BUS_00000_MID2
######                     (
######                       TIME_ID		        INTEGER,
######                       USER_ID			VARCHAR(20),
######                       USERTYPE_ID		VARCHAR(4)
######                      )
######                      partitioning key
######	              (USER_ID)
######	              using hashing
######                     with replace on commit preserve rows not logged in tbs_user_temp"
######         puts $sql_buff
######	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######		WriteTrace "$errmsg" 2020
######		puts $errmsg
######		aidb_close $handle
######		return -1
######	}
######
######	aidb_commit $conn
######        aidb_close $handle
######        
######	puts "生成用户状态全量临时表"
######
######	#---插入用户全量临时表---#
######       set handle [aidb_open $conn]
######	set sql_buff "insert into session.G_BUS_00000_MID1
######                     select
######                       a.time_id
######                      ,a.user_id
######                      ,create_date
######                      ,product_no
######                      ,usertype_id
######                      ,sim_code
######                      ,brand_id
######	            from 
######	               bass1.g_a_02004_day a,
######                      (select max(time_id) as time_id,user_id 
######                       from bass1.g_a_02004_day 
######                       where time_id<=${Timestamp}
######                       group by user_id
######                      )b
######	            where 
######	               a.time_id=b.time_id
######	               and a.user_id=b.user_id"
######        puts $sql_buff
######	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######		WriteTrace "$errmsg" 2020
######		puts $errmsg
######		aidb_close $handle
######		return -1
######	}
######
######	aidb_commit $conn
######	aidb_close $handle
######	puts "插入用户全量临时表"
######        
######        #在用户全量临时表建索引
######        set handle [aidb_open $conn]
######	set sql_buff "create index session.idx_00000_MID1_2_7
######	             on session.G_BUS_00000_MID1(user_id,brand_id)"
######        puts $sql_buff
######	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######		WriteTrace "$errmsg" 2020
######		puts $errmsg
######		aidb_close $handle
######		return -1
######	}
######	aidb_commit $conn
######	aidb_close $handle
######	puts "用户全量临时表建索引结束"
######	
######	#-----插入用户状态全量临时表-----#
######	set handle [aidb_open $conn]
######	set sql_buff "insert into session.G_BUS_00000_MID2
######                     select
######                       a.time_id
######                      ,a.user_id
######                      ,a.USERTYPE_ID
######                    from 
######                      bass1.G_A_02008_DAY a,
######                      (select max(time_id) as time_id,user_id 
######                       from bass1.G_A_02008_DAY
######                       where time_id<=${Timestamp} 
######                       group by user_id
######                       )b
######                   where 	               
######                     a.time_id=b.time_id
######	             and a.user_id=b.user_id"
######        puts $sql_buff
######	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######		WriteTrace "$errmsg" 2020
######		puts $errmsg
######		aidb_close $handle
######		return -1
######	}
######
######	aidb_commit $conn
######	aidb_close $handle
######	puts "插入用户状态全量临时表"
######	
######	#------生成GRP_CUST_NOOF，集团客户到达数--------#
######	set handle [aidb_open $conn]
######	set sql_buff "\
######             select
######               count(distinct T.ENTERPRISE_ID)
######             from
######               (
######                  select a.ENTERPRISE_ID  , a.CUST_STATU_TYP_ID 
######                  from 
######                    bass1.G_A_01004_DAY a,
######                    (
######                     select max(time_id) as time_id,ENTERPRISE_ID from bass1.G_A_01004_DAY 
######                     where time_id<=${Timestamp} 
######                     group by ENTERPRISE_ID
######                    )b
######                  where 
######                   a.CUST_STATU_TYP_ID='20'
######                   and a.time_id=b.time_id 
######                   and a.ENTERPRISE_ID=b.ENTERPRISE_ID 
######               )t,
######               
######               (
######                 SELECT  A.CUST_ID AS CUST_ID
######                 FROM bass1.G_A_01001_DAY A,
######                      (
######                       SELECT CUST_ID,MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_01001_DAY
######                       WHERE TIME_ID <=${Timestamp} 
######                       GROUP BY CUST_ID 
######                       )B
######                 WHERE 
######                   A.ORG_TYPE_ID = '2'
######                   and A.TIME_ID=B.TIME_ID 
######                   AND A.CUST_ID = B.CUST_ID  
######               )P
######            where T.ENTERPRISE_ID=P.CUST_ID "
######        puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######		WriteTrace $errmsg 1001
######		return -1
######	}
######
######	if [catch {set GRP_CUST_NOOF [lindex [aidb_fetch $handle] 0]} errmsg ] {
######		WriteTrace $errmsg 1002
######		return -1
######	}
######	puts "集团客户到达数: $GRP_CUST_NOOF"
######
######	aidb_commit $conn
######        aidb_close $handle
######
######
######	#-----生成服务主题----------------------------------#
######	#----- ARV_CUST_NOOF  用户到达数-----#
######	set handle [aidb_open $conn]
######	set sql_buff "select
######                      count(distinct a.user_id)
######                    from 
######                      session.g_bus_00000_mid1 a,
######                      session.g_bus_00000_mid2 b
######                    where 
######                      a.user_id = b.user_id
######      	              and a.time_id <=$Timestamp
######      	              and b.time_id <=$Timestamp
######      	              and a.usertype_id <> '3'
######      	              and a.sim_code <> '1'
######      	              and b.usertype_id not in ('2010','2020','2030','1040','1021','9000')"
######        puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######		WriteTrace $errmsg 1001
######		return -1
######	}
######
######	if [catch {set ARV_CUST_NOOF [lindex [aidb_fetch $handle] 0]} errmsg ] {
######		WriteTrace $errmsg 1002
######		return -1
######	}
######	puts "用户到达数: $ARV_CUST_NOOF"
######
######	aidb_commit $conn
######	aidb_close $handle
######
######       #-----ADD_CUST_NOOF 新增用户数----------#
######       set handle [aidb_open $conn]
######	set sql_buff "select
######                      count(distinct a.user_id)
######                    from 
######                      session.g_bus_00000_mid1 a
######                    where
######      	              a.time_id=$Timestamp	
######      	              and a.CREATE_DATE='$Timestamp'
######      	              and a.usertype_id <> '3'	
######      	              and a.sim_code <> '1'
######      	              "
######        puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######		WriteTrace $errmsg 1001
######		return -1
######	}
######
######	if [catch {set ADD_CUST_NOOF [lindex [aidb_fetch $handle] 0]} errmsg ] {
######		WriteTrace $errmsg 1002
######		return -1
######	}
######	puts "新增用户数:$ADD_CUST_NOOF"
######
######	aidb_commit $conn
######	aidb_close $handle
######
######	#---AWY_CUST_NOOF 离网用户数----------#
######	set handle [aidb_open $conn]
######	set sql_buff "select
######                      count(distinct a.user_id)
######                    from 
######                      session.g_bus_00000_mid1 a,
######                      bass1.G_A_02008_DAY b
######                    where 
######                      a.user_id = b.user_id
######      	              and b.time_id =$Timestamp	
######      	              and a.usertype_id <> '3'
######      	              and a.sim_code <> '1'
######      	              and b.usertype_id in ('2010','2020','2030')"
######        puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######		WriteTrace $errmsg 1001
######		return -1
######	}
######
######	if [catch {set AWY_CUST_NOOF [lindex [aidb_fetch $handle] 0]} errmsg ] {
######		WriteTrace $errmsg 1002
######		return -1
######	}
######	puts "离网用户数:$AWY_CUST_NOOF"
######
######	aidb_commit $conn
######	aidb_close $handle
######
######	#--GSM_CUST_NOOF 全球通用户到达数---#
######	#--SCP_CUST_NOOF 神州行用户到达数---#
######	#--MZ_CUST_NOOF 动感地带用户到达数---#
######	
######
######
######	set handle [aidb_open $conn]
######	set sql_buff "\
######	       select 
######	        sum(GSM_CUST_NOOF),sum(SCP_CUST_NOOF),sum(MZ_CUST_NOOF)
######	       from
######	        (
######                    select
######                      case 
######                        when a.brand_id='1' then count(distinct a.user_id) 
######                         else 0 
######                       end as GSM_CUST_NOOF,
######                      case 
######                        when a.brand_id='2' then count(distinct a.user_id) 
######                        else 0 
######                       end as SCP_CUST_NOOF,
######                      case 
######                        when a.brand_id='3' then count(distinct a.user_id) 
######                        else 0 
######                       end as MZ_CUST_NOOF
######                    from 
######                      session.g_bus_00000_mid1 a,
######                      session.g_bus_00000_mid2 b
######                    where 
######                      a.user_id = b.user_id
######      	              and a.time_id <=$Timestamp
######      	              and b.time_id <=$Timestamp
######      	              and a.usertype_id <> '3'	
######      	              and a.sim_code <> '1'	
######      	              and b.usertype_id not in ('2010','2020','2030','1040','1021','9000') 
######      	            group by 
######      	              a.brand_id 
######      	           )t "
######
######        puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######        	WriteTrace $errmsg 1013
######        	return -1
######        }
######
######        while {[set p_success [aidb_fetch $handle]] != "" } {
######                set GSM_CUST_NOOF     	      [lindex $p_success 0]
######	        set SCP_CUST_NOOF	      [lindex $p_success 1]
######	        set MZ_CUST_NOOF    	      [lindex $p_success 2]
######	        		             }
######
######	aidb_commit $conn
######	aidb_close $handle
######
######
######	puts "全球通用户到达数:$GSM_CUST_NOOF"
######	puts "神州行用户到达数:$SCP_CUST_NOOF"
######	puts "动感地带用户到达数:$MZ_CUST_NOOF"
######	
######
######	#--ADD_GSM_CUST_NOOF 全球通新增用户数--#
######	#--ADD_MZ_CUST_NOOF 动感地带新增用户数--#
######	#--ADD_SCP_CUST_NOOF 神州行新增用户数--#
######	set handle [aidb_open $conn]
######	set sql_buff "\
######	       select 
######	        sum(ADD_GSM_CUST_NOOF),sum(ADD_SCP_CUST_NOOF),sum(ADD_MZ_CUST_NOOF)
######	       from
######	        (
######                    select
######                      case 
######                        when a.brand_id='1' then count(distinct a.user_id) 
######                         else 0 
######                       end as ADD_GSM_CUST_NOOF,
######                      case 
######                        when a.brand_id='2' then count(distinct a.user_id) 
######                        else 0 
######                       end as ADD_SCP_CUST_NOOF,
######                      case 
######                        when a.brand_id='3' then count(distinct a.user_id) 
######                        else 0 
######                       end as ADD_MZ_CUST_NOOF
######                    from 
######                      session.g_bus_00000_mid1 a
######                    where
######      	              a.time_id=$Timestamp		
######      	              and a.CREATE_DATE='$Timestamp'
######      	              and a.usertype_id <> '3'	
######      	              and a.sim_code <> '1'
######      	            group by 
######      	              a.brand_id 
######      	          )t "
######
######        puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######        	WriteTrace $errmsg 1013
######        	return -1
######        }
######
######        while {[set p_success [aidb_fetch $handle]] != "" } {
######                set ADD_GSM_CUST_NOOF     	      [lindex $p_success 0]
######	        set ADD_SCP_CUST_NOOF     	      [lindex $p_success 1]
######	        set ADD_MZ_CUST_NOOF     	      [lindex $p_success 2]
######	        		             }
######
######	aidb_commit $conn
######	aidb_close $handle
######
######
######	puts "全球通新增用户数:$ADD_GSM_CUST_NOOF"	
######	puts "神州行新增用户数:$ADD_SCP_CUST_NOOF"
######	puts "动感地带新增用户数:$ADD_MZ_CUST_NOOF"
######
######	#------AWY_GSM_CUST_NOOF            全球通离网用户数      
######        #------AWY_SCP_CUST_NOOF            神州行离网用户数
######        #------AWY_MZ_CUST_NOOF             动感地带离网用户数
######        set handle [aidb_open $conn]
######
######	set sql_buff "\
######	       select 
######	        sum(AWY_GSM_CUST_NOOF),sum(AWY_SCP_CUST_NOOF),sum(AWY_MZ_CUST_NOOF)
######	       from
######	        (
######                    select
######                      case 
######                        when a.brand_id='1' then count(distinct a.user_id) 
######                         else 0 
######                       end as AWY_GSM_CUST_NOOF,
######                      case 
######                        when a.brand_id='2' then count(distinct a.user_id) 
######                        else 0 
######                       end as AWY_SCP_CUST_NOOF,
######                      case 
######                        when a.brand_id='3' then count(distinct a.user_id) 
######                        else 0 
######                       end as AWY_MZ_CUST_NOOF
######                    from 
######                      session.g_bus_00000_mid1 a,
######                      bass1.G_A_02008_DAY b
######                    where 
######                      a.user_id = b.user_id
######      	              and b.time_id =$Timestamp		         
######      	              and a.usertype_id <> '3'	
######      	              and a.sim_code <> '1'	
######      	              and b.usertype_id in ('2010','2020','2030')  
######      	            group by 
######      	              a.brand_id 
######      	           )t"
######
######        puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######        	WriteTrace $errmsg 1013
######        	return -1
######        }
######
######        while {[set p_success [aidb_fetch $handle]] != "" } {
######                set AWY_GSM_CUST_NOOF     	      [lindex $p_success 0]
######	        set AWY_SCP_CUST_NOOF      	      [lindex $p_success 1]
######	        set AWY_MZ_CUST_NOOF    	      [lindex $p_success 2]
######	        		             }
######
######	aidb_commit $conn
######        aidb_close $handle
######
######	puts "全球通离网用户数:$AWY_GSM_CUST_NOOF"	
######	puts "神州行离网用户数:$AWY_SCP_CUST_NOOF"
######	puts "动感地带离网用户数:$AWY_MZ_CUST_NOOF"
######
######
######	#------生成详单汇总主题-------------------#
######	#----VOICE_CALL_TIMES       语音业务通话次数
######        #----VOICE_CALL_DUR         语音业务通话时长
######        #----VOICE_CHRG_DUR         语音业务计费时长
######        set handle [aidb_open $conn]
######	set sql_buff "\
######	 select
######	   sum(t.VOICE_CALL_TIMES),
######	   BIGINT(ROUND(SUM(t.VOICE_CALL_DUR)/60.0,0)),
######	   sum(t.VOICE_CHRG_DUR)
######	 from 
######	   (
######             select
######              sum(bigint(call_counts)) as VOICE_CALL_TIMES,
######              sum(bigint(call_duration)) as VOICE_CALL_DUR,
######              sum(bigint(base_bill_duration)) as VOICE_CHRG_DUR
######             from 
######               bass1.g_s_21002_day
######             where
######               time_id=$Timestamp
######             union all
######             select
######              sum(bigint(call_counts)) as VOICE_CALL_TIMES,
######              sum(bigint(call_duration)) as VOICE_CALL_DUR,
######              sum(bigint(base_bill_duration)) as VOICE_CHRG_DUR
######             from 
######               bass1.g_s_21005_day
######             where
######               time_id=$Timestamp
######             union all
######             select
######              sum(bigint(call_counts)) as VOICE_CALL_TIMES,
######              sum(bigint(call_duration)) as VOICE_CALL_DUR,
######              sum(bigint(base_bill_duration)) as VOICE_CHRG_DUR
######             from 
######               bass1.g_s_21016_day
######             where
######               time_id=$Timestamp
######             )t "         
######        puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######        	WriteTrace $errmsg 1014
######        	return -1
######        }
######
######        while {[set p_success [aidb_fetch $handle]] != "" } {
######                set VOICE_CALL_TIMES 	      [lindex $p_success 0]
######                set VOICE_CALL_DUR   	      [lindex $p_success 1]
######	        set VOICE_CHRG_DUR   	      [lindex $p_success 2]
######        }
######	  puts "语音业务通话次数:$VOICE_CALL_TIMES"
######	  puts "语音业务通话时长:$VOICE_CALL_DUR"
######	  puts "语音业务计费时长:$VOICE_CHRG_DUR"
######	aidb_commit $conn
######	aidb_close $handle
######        
######        #----GSM_CHRG_DUR_GSM           全球通语音业务计费时长(GSM部分)
######        #----SCP_CHRG_DUR_GSM           神州行语音业务计费时长(GSM部分)
######        #----MZ_CHRG_DUR_GSM            动感地带语音业务计费时长(GSM部分)
######        set handle [aidb_open $conn]
######	set sql_buff "\
######	 select
######	   sum(t.GSM_CHRG_DUR_GSM),
######	   SUM(t.SCP_CHRG_DUR_GSM),
######	   sum(t.MZ_CHRG_DUR_GSM)
######	 from 
######	   (
######	     select 
######	       case
######	         when brand_id='1' then sum(bigint(base_bill_duration)) 
######	         else 0 
######	       end as GSM_CHRG_DUR_GSM,
######	       case
######	         when brand_id='2' then sum(bigint(base_bill_duration)) 
######	         else 0 
######	       end as SCP_CHRG_DUR_GSM,
######	       case
######	         when brand_id='3' then sum(bigint(base_bill_duration)) 
######	         else 0 
######	       end as MZ_CHRG_DUR_GSM
######	     from 
######	       bass1.g_s_21001_day
######	     where 
######	       time_id=$Timestamp
######	     group by 
######	       brand_id
######             )t "         
######        puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######        	WriteTrace $errmsg 1014
######        	return -1
######        }
######
######        while {[set p_success [aidb_fetch $handle]] != "" } {
######	        set GSM_CHRG_DUR_GSM     	      [lindex $p_success 0]
######	        set SCP_CHRG_DUR_GSM     	      [lindex $p_success 1]
######	        set MZ_CHRG_DUR_GSM     	      [lindex $p_success 2]
######        }
######	  puts "全球通语音业务计费时长(GSM部分):$GSM_CHRG_DUR_GSM"
######	  puts "神州行语音业务计费时长(GSM部分):$SCP_CHRG_DUR_GSM"
######	  puts "动感地带语音业务计费时长(GSM部分):$MZ_CHRG_DUR_GSM"
######	aidb_commit $conn
######	aidb_close $handle
######
######        #----GSM_CHRG_DUR_VPMN           全球通语音业务计费时长(VPMN部分)
######        set handle [aidb_open $conn]
######	set sql_buff "\
######	     select 
######                sum(base_bill_duration)  as GSM_CHRG_DUR_VPMN
######	     from 
######	        bass1.int_210012916_$op_month a,
######               session.G_BUS_00000_MID1 b
######	     where 
######	       a.op_time=$Timestamp
######	       and a.svcitem_id in ('9901','9902','9903')
######	       and b.brand_id='1'
######	       and a.user_id=b.user_id "       
######        puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######        	WriteTrace $errmsg 1014
######        	return -1
######        }
######	if [catch {set GSM_CHRG_DUR_VPMN [lindex [aidb_fetch $handle] 0]} errmsg ] {
######		WriteTrace $errmsg 1002
######		return -1
######	}
######        puts "全球通语音业务计费时长(VPMN部分):$GSM_CHRG_DUR_VPMN"
######	aidb_commit $conn
######	aidb_close $handle
######	  
######        #----SCP_CHRG_DUR_VPMN          神州行语音业务计费时长(VPMN部分)
######        set handle [aidb_open $conn]
######	set sql_buff "\
######	     select 
######               sum(bigint(a.base_bill_duration))  as SCP_CHRG_DUR_VPMN
######	     from 
######	       bass1.int_210012916_$op_month a,
######               session.G_BUS_00000_MID1 b
######	     where 
######	       a.op_time=$Timestamp
######	       and a.svcitem_id in ('9901','9902','9903')
######	       and b.brand_id='2'
######	       and a.user_id=b.user_id "       
######        puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######        	WriteTrace $errmsg 1014
######        	return -1
######        }
######	if [catch {set SCP_CHRG_DUR_VPMN [lindex [aidb_fetch $handle] 0]} errmsg ] {
######		WriteTrace $errmsg 1002
######		return -1
######	}
######	puts "神州行语音业务计费时长(VPMN部分):$SCP_CHRG_DUR_VPMN"
######	aidb_commit $conn
######	aidb_close $handle
######	 
######        #----MZ_CHRG_DUR_VPMN          动感地带语音业务计费时长(VPMN部分)
######        set handle [aidb_open $conn]
######	set sql_buff "\
######	     select 
######               sum(bigint(a.base_bill_duration))  as MZ_CHRG_DUR_VPMN
######	     from 
######	       bass1.int_210012916_$op_month a,
######               session.G_BUS_00000_MID1 b
######	     where 
######	       a.op_time=$Timestamp
######	       and a.svcitem_id in ('9901','9902','9903')
######	       and b.brand_id='3'
######	       and a.user_id=b.user_id"       
######        puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######        	WriteTrace $errmsg 1014
######        	return -1
######        }
######	if [catch {set MZ_CHRG_DUR_VPMN [lindex [aidb_fetch $handle] 0]} errmsg ] {
######		WriteTrace $errmsg 1002
######		return -1
######	}
######	puts "动感地带语音业务计费时长(VPMN部分):$MZ_CHRG_DUR_VPMN"
######	aidb_commit $conn
######	aidb_close $handle
######	
######	set GSM_CHRG_DUR [format "%.0f" [expr $GSM_CHRG_DUR_GSM + $GSM_CHRG_DUR_VPMN]]
######	set SCP_CHRG_DUR [format "%.0f" [expr $SCP_CHRG_DUR_GSM + $SCP_CHRG_DUR_VPMN]]
######	set MZ_CHRG_DUR [format "%.0f" [expr $MZ_CHRG_DUR_GSM + $MZ_CHRG_DUR_VPMN]]
######
######        puts "全球通语音业务计费时长:$GSM_CHRG_DUR"
######        puts "神州行语音业务计费时长:$SCP_CHRG_DUR"
######        puts "动感地带语音业务计费时长:$MZ_CHRG_DUR"
######	  		                
######        ##----BUSY_CHRG_DUR          忙时计费时长########
######        set handle [aidb_open $conn]
######
######	set sql_buff "\
######		select sum(p.fee)
######		from 
######		(
######		        select t.call_moment_id,sum(t.sc) as fee
######			from 
######			(
######			   select call_moment_id ,sum(bigint(base_bill_duration)) as sc
######			   from bass1.g_s_21002_day 
######		           where time_id =$Timestamp
######			   group by call_moment_id
######			   union all 
######			   select call_moment_id ,sum(bigint(base_bill_duration)) as sc
######		           from bass1.g_s_21005_day 
######		           where time_id =$Timestamp
######		           group by call_moment_id
######		           union all
######		           select callmoment_id  as call_moment_id  ,sum(bigint(base_bill_duration)) as sc
######		           from bass1.g_s_21016_day 
######		           where time_id =$Timestamp
######		           group by callmoment_id
######			)t
######			group by t.call_moment_id
######			order by 2 desc
######			fetch first 2 rows only
######		) p"
######
######        puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######        	WriteTrace $errmsg 1014
######        	return -1
######        }
######
######        while {[set p_success [aidb_fetch $handle]] != "" } {
######                set BUSY_CHRG_DUR 	      [lindex $p_success 0]
######        }
######	puts "忙时计费时长:$BUSY_CHRG_DUR"
######	aidb_commit $conn
######	aidb_close $handle
######	        
######	#------生成详单汇总主题-------------------#
######	#--SMS_TIMES        短信通信量
######	set handle [aidb_open $conn]
######	set sql_buff "\
######            SELECT
######                value((A.CNT + B.CNT + C.CNT),0)
######               FROM (
######                     SELECT 
######                       COUNT(*) AS CNT
######      	             FROM 
######      	               bass1.G_S_04005_DAY
######      	             WHERE 
######      	               TIME_ID = $Timestamp
######      	               AND INFO_TYPE IN ('01','02','03','99')
######      	               AND SMS_STATUS = '0'
######      	            ) A,
######                   (
######                    SELECT 
######                      SUM(bigint(SMS_COUNTS)) AS CNT
######      	            FROM 
######      	              bass1.G_S_21007_DAY
######      	            WHERE 
######      	              TIME_ID = $Timestamp
######      	              AND SVC_TYPE_ID IN ('11','12','13','14','20','31','32','40','64','65','66','70')
######      	              AND END_STATUS = '0'
######      	              AND CDR_TYPE_ID IN ('00','01','10','11')
######      	           )B,
######                   (
######                    SELECT 
######                      COUNT(*) AS CNT
######      	            FROM 
######      	              bass1.G_S_04014_DAY
######      	            WHERE TIME_ID = $Timestamp
######      	              AND SMS_SEND_STATE = '0'
######      	              AND SMS_BILL_TYPE IN ('00','01','10','11')
######      	            )C"
######      	puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######		WriteTrace $errmsg 1001
######		return -1
######	}
######
######	if [catch {set SMS_TIMES [lindex [aidb_fetch $handle] 0]} errmsg ] {
######		WriteTrace $errmsg 1002
######		return -1
######	}
######	aidb_commit $conn
######	aidb_close $handle
######	puts "短信通信量: ${SMS_TIMES}"
######
######
######
######       #--PPP_SMS_TIMES    点对点短信通信量
######
######        set handle [aidb_open $conn]
######	set sql_buff "\
######            SELECT
######              SUM(BIGINT(B.SMS_COUNTS))
######            FROM 
######              bass1.G_S_21007_DAY B
######            WHERE 
######              B.TIME_ID = $Timestamp
######      	      AND B.SVC_TYPE_ID IN ('11','12','13')
######      	      AND B.END_STATUS = '0'
######      	      AND B.CDR_TYPE_ID IN ('00','01','10','11') "
######         puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######		WriteTrace $errmsg 1001
######		return -1
######	}
######
######	if [catch {set PPP_SMS_TIMES [lindex [aidb_fetch $handle] 0]} errmsg ] {
######		WriteTrace $errmsg 1002
######		return -1
######	}
######	puts "点对点短信通信量:$PPP_SMS_TIMES"
######
######	aidb_commit $conn
######	aidb_close $handle
######
######         #------生成服务使用主题-----------#
######         #--MWAP_SMS_VOL 梦网短信通信量 #
######         set handle [aidb_open $conn]
######
######         set sql_buff "\
######                 select 
######                   count(*) 
######                 from 
######                   G_S_04005_DAY 
######                where
######                  time_id=$Timestamp 
######                  and sms_status='0'"
######         puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######		WriteTrace $errmsg 1016
######		return -1
######	}
######
######	if [catch {set MWAP_SMS_VOL [lindex [aidb_fetch $handle] 0]} errmsg ] {
######		WriteTrace $errmsg 1017
######		return -1
######	}
######
######	puts "梦网短信通信量:$MWAP_SMS_VOL"
######	aidb_commit $conn
######	aidb_close $handle
######
######	#---MMS_VOL 彩信通信量#
######	set handle [aidb_open $conn]
######         set sql_buff "\
######                select 
######                  count(*) 
######                from 
######                  bass1.G_S_04004_DAY 
######                where 
######                  time_id=$Timestamp"
######        puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######		WriteTrace $errmsg 1016
######		return -1
######	}
######
######	if [catch {set MMS_VOL [lindex [aidb_fetch $handle] 0]} errmsg ] {
######		WriteTrace $errmsg 1017
######		return -1
######	}
######
######	puts "彩信通信量:$MMS_VOL"
######	aidb_commit $conn
######	aidb_close $handle
######
######	#----GPRS_COMM_VOL GPRS业务通信量#
######
######	set handle [aidb_open $conn]
######         set sql_buff "\
######                 select 
######                   BIGINT(ROUND(sum(bigint(UPLINK_FLOW_ON_TARIFF1) + bigint(DOWNLINK_FLOW_ON_TARIFF1) + bigint(UPLINK_FLOW_ON_TARIFF2) + bigint(DOWNLINK_FLOW_ON_TARIFF2) + bigint(UPLINK_FLOW_ON_OTHER) + bigint(DOWNLINK_FLOW_ON_OTHER))/1024.0,0))
######                 from 
######                   bass1.G_S_04002_DAY 
######                 where 
######                   time_id=$Timestamp"
######        puts $sql_buff
######        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######		WriteTrace $errmsg 1018
######		return -1
######	}
######
######	if [catch {set GPRS_COMM_VOL [lindex [aidb_fetch $handle] 0]} errmsg ] {
######		WriteTrace $errmsg 1019
######		return -1
######	}
######
######	puts "业务通信量:$GPRS_COMM_VOL"
######	aidb_commit $conn
######	aidb_close $handle
######
######
######
######
######
######
######
######
######
######        #向G_BUS_CHECK_ALL_DAY中插入一行数据#
######        set handle [aidb_open $conn]
######	set sql_buff "insert into G_BUS_CHECK_ALL_DAY values($Timestamp,'$GRP_CUST_NOOF','$ARV_CUST_NOOF','$ADD_CUST_NOOF','$AWY_CUST_NOOF','$GSM_CUST_NOOF','$SCP_CUST_NOOF','$MZ_CUST_NOOF','$ADD_GSM_CUST_NOOF','$ADD_SCP_CUST_NOOF','$ADD_MZ_CUST_NOOF','$AWY_GSM_CUST_NOOF','$AWY_SCP_CUST_NOOF','$AWY_MZ_CUST_NOOF','$VOICE_CALL_TIMES','$VOICE_CALL_DUR','$VOICE_CHRG_DUR','$GSM_CHRG_DUR','$SCP_CHRG_DUR','$MZ_CHRG_DUR','$BUSY_CHRG_DUR','$SMS_TIMES','$PPP_SMS_TIMES','$MWAP_SMS_VOL','$MMS_VOL','$GPRS_COMM_VOL')"
######        puts $sql_buff
######	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
######		WriteTrace "$errmsg" 2020
######		puts $errmsg
######		aidb_close $handle
######		return -1
######	}
######	aidb_commit $conn
######	aidb_close $handle

	return 0
}
########################参考###########################
#	set sql_buff "declare global temporary table session.G_BUS_00000_MID1
#                     (
#                       TIME_ID		INTEGER,		--状态改变的最大时间--
#                       USER_ID		VARCHAR(20),		--用户标志--
#                       CREATE_DATE      VARCHAR(10),		--入网日期--
#                       PRODUCT_NO	VARCHAR(15),            --PRODUCT_NO--
#                       USERTYPE_ID	VARCHAR(1),		--用户状态标志--
#                       SIM_CODE		VARCHAR(1),		--数据SIM卡标志--
#                       BRAND_ID		VARCHAR(1)		--用户品牌标志--
#                      )
#        with replace on commit preserve rows not logged in tbs_user_temp"
#	set sql_buff "declare global temporary table session.G_BUS_00000_MID2
#                     (
#                       TIME_ID		        INTEGER,		--状态改变的最大时间--
#                       USER_ID			VARCHAR(20),            --用户标志--
#                       USERTYPE_ID		VARCHAR(4)		--用户状态标志--
#                      )
#        with replace on commit preserve rows not logged in tbs_user_temp"
########################################################