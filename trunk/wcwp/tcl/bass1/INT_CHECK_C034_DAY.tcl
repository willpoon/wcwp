#################################################################
#程序名称: INT_CHECK_C034_DAY.tcl
#功能描述: 
#规则编号：C0,C3,C4,V6016
#规则属性：C0:业务逻辑;C3,C4:交叉验证
#规则类型：日
#指标摘要：C0：忙时计费时长占比
#          C3：神州行日时段计费时长/神州行日汇总计费时长
#          C4：动感地带日时段计费时长/动感地带日汇总计费时长
#规则描述：C0：置信域下界≤忙时计费时长占比≤置信域上界，
#              其中置信域边界由当日各省忙时计费时长占比的统计均值和标准差共同确定，
#              置信上界=均值+3*标准差，置信下界=均值-3*标准差
#          C3：|神州行日时段计费时长/神州行日汇总计费时长 - 1| ≤ 0.1%
#          C4：|动感地带日时段计费时长/动感地带日汇总计费时长 - 1| ≤ 0.1%
#校验对象：
#          1.GBAS.G_S_21001_DAY   GSM普通语音业务日使用           
#          2.GBAS.G_S_21002_DAY   GSM普通语音业务日时段使用
#          3.GBAS.G_S_21004_DAY   智能网语音业务日使用            (目前送空)
#          4.GBAS.G_S_21005_DAY   智能网语音业务日时段使用        (目前送空)
#          5.GBAS.G_S_21009_DAY   智能网VPMN语音业务日使用      
#          6.GBAS.G_S_21016_DAY   智能网VPMN语音业务日时段使用  
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1. V6016 增加GSM普通语音业务日使用的校验 zhanght 20090616
#修改历史: 1.判断C3、C4在做2值对比的时候保留小数位少，应该保留3位小数以上 王琦 2008-01-17
##################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #程序名称
        set app_name "INT_CHECK_C034_DAY.tcl"

        #--删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$timestamp
		and rule_code in('C0','C3','C4') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn


###################################C0：忙时计费时长占比#############################
#        #--忙时计费时长
#        set handle [aidb_open $conn]
#	set sqlbuf "\
#	       SELECT 
#	         SUM(P.FEE)
#	       FROM
#	       (
#	    	 SELECT 
#	    	   T.CALL_MOMENT_ID,
#	    	   SUM(T.SC) AS FEE
#	    	 FROM
#	    	 (
#	    	   SELECT CALL_MOMENT_ID ,SUM(BIGINT(BASE_BILL_DURATION)) AS SC
#	    	   FROM BASS1.G_S_21002_DAY
#	    	   WHERE TIME_ID =$timestamp
#	    	   GROUP BY CALL_MOMENT_ID
#	    	   UNION ALL
#	    	   SELECT CALL_MOMENT_ID ,SUM(BIGINT(BASE_BILL_DURATION)) AS SC
#	    	   FROM BASS1.G_S_21005_DAY
#	    	   WHERE TIME_ID =$timestamp
#	    	   GROUP BY CALL_MOMENT_ID
#	    	   UNION ALL
#	    	   SELECT CALLMOMENT_ID as CALL_MOMENT_ID,SUM(BIGINT(BASE_BILL_DURATION)) AS SC
#	    	   FROM BASS1.G_S_21016_DAY
#	    	   WHERE TIME_ID =$timestamp
#	    	   GROUP BY CALLMOMENT_ID
#	    	  )T
#	    	  GROUP BY T.CALL_MOMENT_ID
#	    	  ORDER BY 2 DESC
#	    	  FETCH FIRST 2 ROWS ONLY
#	    	) P"
#
#         if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#	   	WriteTrace $errmsg 1001
#	   	return -1
#	 }      
#	 if [catch {set BUSI_DURATION [lindex [aidb_fetch $handle] 0]} errmsg ] {
#	   	WriteTrace $errmsg 1002
#	   	return -1
#	 }        
#	aidb_commit $conn
#	set BUSI_DURATION [format "%.2f" [expr ${BUSI_DURATION} /1.00]]
#	#puts $BUSI_DURATION
#	
#	#--基本计费时长
#	set handle [aidb_open $conn]
#	set sqlbuf "\
#	    SELECT SUM(T.SC)
#	   	FROM
#	   	(
#	   		SELECT SUM(BIGINT(BASE_BILL_DURATION))AS SC
#	   		FROM BASS1.G_S_21001_DAY
#	   		WHERE TIME_ID=$timestamp
#	   			AND ROAM_TYPE_ID NOT IN ('122', '202', '302', '401')
#	   		UNION ALL
#	   		SELECT SUM(BIGINT(BASE_BILL_DURATION))AS SC
#	   		FROM BASS1.G_S_21004_DAY
#	   		WHERE TIME_ID=$timestamp
#	   			AND ROAM_TYPE_ID NOT IN ('122', '202', '302', '401')
#	   		UNION ALL
#	   		SELECT SUM(BIGINT(BASE_BILL_DURATION))AS SC
#	   		FROM BASS1.G_S_21009_DAY
#	   		WHERE TIME_ID=$timestamp
#	   			AND ROAM_TYPE_ID NOT IN ('122', '202', '302', '401')
#	   	)T"
#        
#          if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#	   	WriteTrace $errmsg 1001
#	   	return -1
#	    }
#        
#	    if [catch {set BASE_DURATION [lindex [aidb_fetch $handle] 0]} errmsg ] {
#	   	WriteTrace $errmsg 1002
#	   	return -1
#	   }        
#	   aidb_commit $conn
#	   set BASE_DURATION [format "%.2f" [expr ${BASE_DURATION}/1.00]]
#	   #puts $BASE_DURATION
#	
#	   #--将校验值插入校验结果表
#	   set handle [aidb_open $conn]
#	   set sql_buff "\
#	   	INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'C0',$BUSI_DURATION,$BASE_DURATION,0,0) "
#	   if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#	   	WriteTrace "$errmsg" 2005
#	   	aidb_close $handle
#	   	return -1
#	   }
#	   aidb_commit $conn
#
#	    #--判断
#        #--异常
#        #--1：置信域下界≤忙时计费时长占比≤置信域上界超标
#        #--根据经验,我们暂定置信域为17%(2006/12/30讨论将经验值改为16%)
#         if {[format "%.2f" [expr ${BUSI_DURATION} / ${BASE_DURATION}]]>0.16 || [format "%.2f" [expr ${BUSI_DURATION} / ${BASE_DURATION} ]]<-0.16} {
#	  	set grade 2
#	        set alarmcontent "扣分性指标C0超出集团考核范围"
#	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
#	 }
#
#         puts "C0结束"


################################### V6016 GSM普通语音业务日使用  #############################

  set handle [aidb_open $conn]
	set sql_buff "\
	           select  value(count(*),0)
             from BASS1.G_S_21001_DAY
             where time_id=$timestamp
             and ROAM_TYPE_ID='500'
             and CALL_TYPE_ID='02'
             and 
             (int(TOLL_BILL_DURATION)<>0
             or int(TOLL_CALL_FEE)<>0
             or int(FAVOURED_TOLLCALL_FEE)<>0)
             with ur "


     puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	
	puts $DEC_RESULT_VAL1
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	puts $DEC_RESULT_VAL1

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'V6016',$DEC_RESULT_VAL1,0,0,0) "
    puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]] > 0 } {
		set grade 2
	        set alarmcontent "  V6016 校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
##################################################################################################################################################    



#################################C3：神州行日时段计费时长/神州行日汇总计费时长###########
       #--神州行日时段计费时长
       set handle [aidb_open $conn]
       set sqlbuf "SELECT SUM(T.SC) FROM
            (
              SELECT
              	value(SUM(BIGINT(BASE_BILL_DURATION )),0) AS SC
              FROM BASS1.G_S_21004_DAY
              WHERE TIME_ID=$timestamp
                     AND BRAND_ID ='2'
              UNION
              SELECT
              	value(SUM(BIGINT(BASE_BILL_DURATION )),0) AS SC
              FROM BASS1.G_S_21001_DAY
              WHERE TIME_ID=$timestamp
                    AND BRAND_ID ='2'
             )T"

         if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	 	WriteTrace $errmsg 1001
	 	return -1
	 }

	 if [catch {set SZXTIME_DURATION [lindex [aidb_fetch $handle] 0]} errmsg ] {
	 	WriteTrace $errmsg 1002
	 	return -1
	 }

	 aidb_commit $conn

	 set SZXTIME_DURATION [format "%.2f" [expr ${SZXTIME_DURATION} /1.00]]
	 puts $SZXTIME_DURATION

	 #----神州行日汇总计费时长
	 set handle [aidb_open $conn]

	  set sqlbuf "SELECT SUM(T.SC) FROM
            (
               SELECT 
               	value(SUM(BIGINT(BASE_BILL_DURATION)),0) AS SC
               FROM BASS1.G_S_21005_DAY
               WHERE TIME_ID=$timestamp
                     AND BRAND_ID='2'
               UNION
               SELECT 
               	value(SUM(BIGINT(BASE_BILL_DURATION)),0)  AS SC
               FROM BASS1.G_S_21002_DAY
               WHERE TIME_ID=$timestamp
                     AND BRAND_ID='2'
             )T ;"
     
     puts $
     if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	 	WriteTrace $errmsg 1001
	 	return -1
	  }

	  if [catch {set SZXALL_DURATION [lindex [aidb_fetch $handle] 0]} errmsg ] {
	 	WriteTrace $errmsg 1002
	 	return -1
	 }

	 aidb_commit $conn
	 set SZXALL_DURATION [format "%.2f" [expr ${SZXALL_DURATION} /1.00]]
	 #puts $SZXALL_DURATION

	 #--将校验值插入校验结果表
	 set handle [aidb_open $conn]
	 set sql_buff "\
	 	INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'C3',$SZXTIME_DURATION,$SZXALL_DURATION,0,0) "
	 if [catch { aidb_sql $handle $sql_buff } errmsg ] {
	 	WriteTrace "$errmsg" 2005
	 	aidb_close $handle
	 	return -1
	 }
	 aidb_commit $conn

	#--判断
        #--异常
        #--1：|神州行日时段计费时长/神州行日汇总计费时长 - 1|<=0.1%超标
         if {[format "%.3f" [expr ${SZXTIME_DURATION} / ${SZXALL_DURATION} -1 ]]>0.001 || [format "%.3f" [expr ${SZXTIME_DURATION} / ${SZXALL_DURATION} - 1]]<-0.001} {
		set grade 2
	        set alarmcontent "扣分性指标C3超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
			  	}
	  	
	puts "C3结束"
	#--------------------------------------------------------------  	
	  	
	   #--C4：动感地带日时段计费时长/动感地带日汇总计费时长
           #--动感地带日时段计费时长
          set handle [aidb_open $conn]

	  set sqlbuf "SELECT SUM(T.SC) FROM
            (
              SELECT 
              	value(SUM(BIGINT(BASE_BILL_DURATION)),0) AS SC
              FROM BASS1.G_S_21004_DAY
              WHERE TIME_ID=$timestamp 
                     AND BRAND_ID='3'
              UNION
              SELECT 
              	value(SUM(BIGINT(BASE_BILL_DURATION)),0) AS SC
              FROM BASS1.G_S_21001_DAY
              WHERE TIME_ID=$timestamp 
                    AND BRAND_ID='3'
            ) T"
     
     puts $sqlbuf
     if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	 	WriteTrace $errmsg 1001
	 	return -1
	 }

	 if [catch {set MZONETIME_DURATION [lindex [aidb_fetch $handle] 0]} errmsg ] {
	 	WriteTrace $errmsg 1002
	 	return -1
	 }

	 aidb_commit $conn
	 set MZONETIME_DURATION [format "%.2f" [expr ${MZONETIME_DURATION} /1.00]]
	 #puts $MZONETIME_DURATION

	 #----动感地带日汇总计费时长
	 set handle [aidb_open $conn]

	  set sqlbuf "SELECT SUM(T.SC) FROM
            (
              SELECT 
              	value(SUM(BIGINT(BASE_BILL_DURATION)),0) AS SC
              FROM BASS1.G_S_21005_DAY
              WHERE TIME_ID=$timestamp
                    AND BRAND_ID='3'
              UNION
              SELECT 
              	value(SUM(BIGINT(BASE_BILL_DURATION)),0) AS SC
              FROM BASS1.G_S_21002_DAY
              WHERE TIME_ID=$timestamp
                    AND BRAND_ID='3'
             )T"

         if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	 	WriteTrace $errmsg 1001
	 	return -1
	  }

	  if [catch {set MZONEALL_DURATION [lindex [aidb_fetch $handle] 0]} errmsg ] {
	 	WriteTrace $errmsg 1002
	 	return -1
	 }

	 aidb_commit $conn
	 set MZONEALL_DURATION [format "%.2f" [expr ${MZONEALL_DURATION} /1.00]]
	 #puts $MZONEALL_DURATION

	 #--将校验值插入校验结果表
	 set handle [aidb_open $conn]
	 set sql_buff "\
	 	INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'C4',$MZONETIME_DURATION,$MZONEALL_DURATION,0,0) "
	 if [catch { aidb_sql $handle $sql_buff } errmsg ] {
	 	WriteTrace "$errmsg" 2005
	 	aidb_close $handle
	 	return -1
	 }
	 aidb_commit $conn

	#--判断
        #--异常
        #--1：|动感地带日时段计费时长/动感地带日汇总计费时长 - 1|<=0.1%超标
         if {[format "%.3f" [expr ${MZONETIME_DURATION} / ${MZONEALL_DURATION} -1]]>0.001 || [format "%.3f" [expr ${MZONETIME_DURATION} / ${MZONEALL_DURATION} - 1]]<-0.001} {
		set grade 2
	        set alarmcontent "扣分性指标C4超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
		 	}
	  	puts "C4结束"	

	aidb_close $handle
	return 0
}

