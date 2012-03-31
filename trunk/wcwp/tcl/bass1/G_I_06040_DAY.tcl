
######################################################################################################		
#接口名称: 空中充值点基础信息                                                               
#接口编码：06040                                                                                          
#接口说明：记录具备空中充值功能的网点情况信息
#程序名称: G_I_06040_DAY.tcl                                                                            
#功能描述: 生成06040的数据
#运行粒度: DAY
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20120328
#问题记录：
#修改历史: 1. panzw 20120328	中国移动一级经营分析系统省级数据接口规范 (V1.7.9) 
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
		global app_name
		set app_name "G_I_06040_DAY.tcl"        


  #删除本期数据
	set sql_buff "delete from bass1.g_i_06040_day where time_id=$timestamp"
	exec_sql $sql_buff

	set sql_buff "
	insert into bass1.g_i_06040_day
	select  distinct
			$timestamp time_id
			,a.OTHER_INFO  CHRG_NBR
			,b.REGION_CODE CMCC_ID
			,char(a.CHANNEL_ID)   CHNL_ID
			,case when PARTNER_CODE in ('00','01','02','03') then '2' else '1' end CHNL_TYPE
	from bass2.Dw_channel_dealer_$timestamp a
		,bass2.dim_channel_info b 
	where a.CHANNEL_ID = b.CHANNEL_ID
			and a.DEALER_STATE = 1
	with ur 
"
	exec_sql $sql_buff
	
  #进行结果数据检查
  #检查chkpkunique
	set tabname "g_i_06040_day"
	set pk 			"CHRG_NBR"
	chkpkunique ${tabname} ${pk} ${timestamp}

	return 0
}


##~   00		记录行号
##~   01		空中充值专用手机号
##~   02		所属CMCC运营公司标识
##~   03		实体渠道标识
##~   04		空中充值点类型

##~   合作单位编码：如果渠道经销商级别为“全国”，编号范围为00-20，分别为“00：苏宁、01：国美、02：迪信通、03青年中心；如果为“省级”，
##~   编号范围“21－50”，分公司自定，默认为21：无；如果为“地市级”，编号范围为“51－99”，默认为51:无。


##~   CREATE TABLE G_I_06040_DAY (
        ##~   TIME_ID                INTEGER             --  记录行号        
        ##~   ,CHRG_NBR               CHAR(15)            --  空中充值专用手机号
        ##~   ,CMCC_ID                CHAR(5)             --  所属CMCC运营公司标识
        ##~   ,CHNL_ID                CHAR(40)            --  实体渠道标识    
        ##~   ,CHNL_TYPE              CHAR(1)             --  空中充值点类型  
##~   ) DATA CAPTURE NONE IN TBS_APP_BASS1 INDEX IN TBS_INDEX PARTITIONING KEY( TIME_ID,CHRG_NBR ) USING HASHING;



