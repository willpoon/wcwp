#################################################################
#程序名称: INT_CHECK_SAMPLE_TO_DAY.tcl
#功能描述: 抽样数据校验
#输入参数:
#	    op_time		数据时间（批次）,“yyyy-mm-dd”
#	    province_id	        省份编码
#	    redo_number	        重传序号。只有最终的一经编码定长数据文件需要重传序号
#	    trace_fd	        trace文件句柄
#	    temp_data_dir	临时数据文件的存放位置，$BASS1FILEDIR/tempdata/，用于保存e_transform生成的数据文件
#	    semi_data_dir	中间数据文件的存放位置，$BASS1FILEDIR/semidata/，用于保存.bass1、.bass2、.sample等文件
#	    final_data_dir	最终数据文件的存放位置，$BASS1FILEDIR/finaldata/，用于保存.dat，即一经编码定长数据文件
#	    conn		数据库连接
#	    src_data	        数据源
#	    obj_data	        数据目的
#
#规则编号：R138、R139、R140、R141
#规则属性：交叉验证
#规则类型：月
#指标摘要：
#        R138->R107	月	人均通话费(元)	
#        已去除R141	月	人均计费时长	
#        R139->R108	月	人均长途计费时长	
#        已去除R140	月	人均漫游通话次数	

#规则描述：R138->R107: |抽样详单人均通话费/汇总数据人均通话费 - 1| ≤ 5%，且人均通话费＞0
#          已去除R141：|抽样详单人均漫游通话次数/汇总数据人均漫游通话次数 - 1| ≤ 5％，且人均漫游通话次数＞0
#          R139->R108：|抽样详单人均计费时长/汇总数据人均计费时长 - 1| ≤ 5%，且人均计费时长＞0
#          已去除R140：|抽样详单人均长途计费时长/汇总数据人均长途计费时长 - 1| ≤ 5％，且人均长途计费时长＞0
#校验对象：
#          BASS1.G_S_04008_DAY     --抽样GSM普通语音话单             
#          BASS1.G_S_04009_DAY     --抽样智能网语音话单              
#          BASS1.G_S_21003_TO_DAY  --GSM普通语音业务月使用(日使用)
#          BASS1.G_S_21006_TO_DAY  --智能网语音业务月使用(日使用) 
#输出参数:
# 返回值:   0 成功; -1 失败
# liuzhilong 20090910 修改校验规则编码 R138->R107 R139->R108 去掉R140 R141校验，修改时间25日再告警
##~   不能用 int 表代替 G_S_04008_DAY
#################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env
	
  ##~   set op_time  2012-05-31
	set p_optime $op_time
	set p_timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set this_month [string range $op_time 0 3][string range $op_time 5 6]
	set db_user $env(DB_USER)
        
        ##今天的日期，格式dd(例：输入20070411 返回11)
        set today_dd [format "%.0f" [string range $p_timestamp 6 7]]
        #puts $today_dd

        #程序名称
        set app_name "INT_CHECK_SAMPLE_TO_DAY.tcl"


#删除本期数据
	set sql_buff "
		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$p_timestamp
    	              AND RULE_CODE IN ('R107','R108') 
				"
exec_sql $sql_buff
###################################R107：人均通话费##########################
        if { $today_dd<25 } {
            puts "今天 $today_dd 号，未到25号，暂不处理"
            return 0
        }
        #抽样详单人均通话费
	set sql_buff "\
                      SELECT 
            	        SUM(T.JB+T.CT+T.CF) AS FY,
            	        COUNT(DISTINCT T.PRODUCT_NO) AS RS
                      FROM (
                             SELECT 
                               PRODUCT_NO,
                               SUM(BIGINT(BASE_CALL_FEE)) AS JB,
                               SUM(BIGINT(TOLL_CALL_FEE)) AS CT,  
                               SUM(BIGINT(callfw_toll_fee)) AS CF
                             FROM 
                               BASS1.G_S_04008_DAY
                             WHERE TIME_ID/100=$this_month and roam_type_id not in ('122','202','302','401')
                             GROUP BY PRODUCT_NO
                             UNION ALL
                             SELECT 
                               PRODUCT_NO,
                               SUM(BIGINT(BASE_CALL_FEE)) AS JB,
                               SUM(BIGINT(TOLL_CALL_FEE)) AS CT,  
                               SUM(BIGINT(0)) AS CF
                             FROM 
                               BASS1.G_S_04009_DAY
                             WHERE TIME_ID/100=$this_month
                             GROUP BY PRODUCT_NO
                           )T
						with ur"
						
   set p_row [get_row $sql_buff]
   set CHECK_VAL1 [lindex $p_row 0]
   set CHECK_VAL2 [lindex $p_row 1]

   set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/100.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #数据清零
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	    
             #汇总数据
	     set sql_buff "\	
                   SELECT 
                     SUM(T.JB+T.CT) AS FY,
                     COUNT(DISTINCT T.PRODUCT_NO) AS RS
                   FROM (
                         SELECT PRODUCT_NO,SUM(BIGINT(FAVOURED_BASECALL_FEE)) AS JB,SUM(BIGINT(FAVOURED_TOLLCALL_FEE)) AS CT  
                         FROM BASS1.G_S_21003_TO_DAY
                         WHERE TIME_ID/100=$this_month
                         GROUP BY PRODUCT_NO
                         UNION 
                         SELECT PRODUCT_NO,SUM(BIGINT(FAVOURED_BASECALL_FEE)) AS JB,SUM(BIGINT(FAVOURED_TOLLCALL_FEE))AS CT
                         FROM BASS1.G_S_21006_TO_DAY
                         WHERE TIME_ID/100=$this_month
                         GROUP BY PRODUCT_NO
                       ) T"

   set p_row [get_row $sql_buff]
   set CHECK_VAL1 [lindex $p_row 0]
   set CHECK_VAL2 [lindex $p_row 1]
	      
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/100.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

             #(抽样值/汇总值-1)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1)]]
	     puts $RESULT
	     
             #将校验值插入校验结果表
             set sql_buff "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($p_timestamp ,
             		'R107',
             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
             		cast ($RESULT as  DECIMAL(18, 5) ),
             		0.05)"

             
			exec_sql $sql_buff
             
             
             #判断R107：|抽样详单人均通话费/汇总数据人均通话费 - 1| ≤ 5%，且人均通话费＞0超标
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "扣分性指标R107(原R138)超出集团考核范围"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "扣分性指标R107(原R138)接近集团考核范围5%,达到${RESULT}"
	           WriteAlarm $app_name $p_optime $grade ${alarmcontent}
	          
	    }                 
    
#############################R108：人均计费时长#################
        #抽样详单人均计费时长
	set sql_buff "\
	            SELECT 
	            	SUM(T.SC),
	            	COUNT(DISTINCT T.PRODUCT_NO)AS RS
	            FROM (
	                  SELECT PRODUCT_NO,SUM(BIGINT(BASE_BILL_DURATION)) AS SC		  
	                  FROM BASS1.G_S_04008_DAY
	                  WHERE TIME_ID/100=$this_month
	                  GROUP BY PRODUCT_NO
	                  UNION 
	                  SELECT PRODUCT_NO,SUM(CEIL(BIGINT(CALL_DURATION)/60.0)) AS SC
	                  FROM BASS1.G_S_04009_DAY
	                  WHERE TIME_ID/100=$this_month
	                  GROUP BY PRODUCT_NO
	                  )T"

   set p_row [get_row $sql_buff]
   set CHECK_VAL1 [lindex $p_row 0]
   set CHECK_VAL2 [lindex $p_row 1]
	      
	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #数据清零
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	    
             #汇总数据人均计费时长
	     set sql_buff "\	
	            SELECT 
	            	SUM(T.SC),
	            	COUNT(DISTINCT T.PRODUCT_NO) 
	            FROM (
	                  SELECT PRODUCT_NO,SUM(BIGINT(BASE_BILL_DURATION)) AS SC	  
	                  FROM BASS1.G_S_21003_TO_DAY
	                  WHERE TIME_ID/100=$this_month
	                  GROUP BY PRODUCT_NO
	                  UNION 
	                  SELECT PRODUCT_NO,SUM(BIGINT(BASE_BILL_DURATION)) AS SC  
	                  FROM BASS1.G_S_21006_TO_DAY
	                  WHERE TIME_ID/100=$this_month
	                  GROUP BY PRODUCT_NO
	                  )T"
   set p_row [get_row $sql_buff]
   set CHECK_VAL1 [lindex $p_row 0]
   set CHECK_VAL2 [lindex $p_row 1]
	      
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

             #(抽样值/汇总值-1)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1)]]
	     puts $RESULT
	     
             #将校验值插入校验结果表
             set sql_buff "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($p_timestamp ,
             		'R108',
             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
             		cast ($RESULT as  DECIMAL(18, 5) ),
             		0.05)"
			 exec_sql $sql_buff
             
             
             
             #判断R108：|抽样详单人均计费时长/汇总数据人均计费时长 - 1| ≤ 5%，且人均计费时长＞0超标
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "扣分性指标R108(原R139)超出集团考核范围"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "扣分性指标R108(原R139)接近集团考核范围5%,达到${RESULT}"
	           WriteAlarm $app_name $p_optime $grade ${alarmcontent}
	          
	    } 	  
	return 0
}	    	    