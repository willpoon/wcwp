######################################################################################################
#接口名称： 上网本GPRS话单
#接口编码：04018
#接口说明：上网本用户的GPRS话单 每日抽取新增数据
#程序名称: G_S_04018_DAY.tcl
#功能描述:
#运行粒度: 日
#源    表：1.
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuzhilong
#编写时间：20090828
#问题记录：1.
#修改历史: 1.20091123 and a.apn_ni in ('CMTDS') 口径修改为：a.drtype_id in (8307) 
#          2.20091201 根据集团要求过滤 a.apn_ni<>'CMLAP'
#          1.6.5规范修改，增减字段  20100520 此号码13989088567 映射为空，进行剔除ext_holds5 is not null
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {


	     #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]   
        
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
			 #上月  yyyymm
			 set last_month [GetLastMonth [string range $op_month 0 5]]
			 #上上月 yyyymm
			 set last_last_month [GetLastMonth [string range $last_month 0 5]]


  #删除本期数据
	set sql_buff "delete from bass1.G_S_04018_DAY where time_id=$timestamp "
	puts $sql_buff
	exec_sql $sql_buff


	#============向正式表中插入 数据============================
	#   
	set sql_buff "
	 insert into bass1.G_S_04018_DAY (
					 TIME_ID
					,INTRA_PRODUCT_NO
					,PRODUCT_NO
					,APNNI
					,UP_FLOWS
					,DOWN_FLOWS
					,START_TIME
					,DURATION
					,MNS_TYPE
					,IMEI
					)
          select $timestamp
    			 ,b.ext_holds5
    			 ,a.product_no
    			 ,a.apn_ni
    			 ,char(sum(a.upflow))
    			 ,char(sum(a.downflow))
    			 ,replace(char(date(a.start_time)),'-','')||replace(char(time(a.start_time)),'.','')
    			 ,char(sum(a.duration))
    			 ,value(char(a.mns_type),'0')
    			 ,value(a.imei,'0')
		    from bass2.CDR_GPRS_LOCAL_$timestamp a ,
		         (select ext_holds5
		            	,id_value
		            	,valid_date
		            	,sts  
		            	,row_number() over(partition by ext_holds5 order by valid_date desc ) row_id
					from bass2.dwd_product_regsp_$timestamp
				   where busi_type='737' and ext_holds5 is not null ) b 
		    where a.product_no=b.id_value
		      and a.drtype_id in (8307)
		      and a.apn_ni<>'CMLAP'
		      and b.row_id=1     
		    group by b.ext_holds5
		    		,a.product_no
		    		,a.apn_ni
		    		,a.start_time
		    		,value(char(a.mns_type),'0')
		    		,value(a.imei,'0')
		"
   puts $sql_buff
   exec_sql $sql_buff
aidb_runstats bass1.G_S_04018_DAY 3
	return 0
}


