
######################################################################################################		
#接口名称: "已确认养卡渠道名单"                                                               
#接口编码：22058                                                                                          
#接口说明："记录上月疑似养卡渠道名单中的疑似养卡渠道，本月确认为养卡渠道的相关数据。"
#程序名称: G_S_22058_MONTH.tcl                                                                            
#功能描述: 生成22058的数据
#运行粒度: MONTH
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20110727
#问题记录：
#修改历史: 
##~   1. panzw 20110727	1.7.4 newly added
##~   2. panzw 20120328	1.7.9 add column : YK_MODE
##~   修改接口22058（已确认养卡渠道名单）：
##~   1、	增加字段“养卡模式”；
##~   2、	定义联合主键“月份”、“实体渠道标识”、“养卡模式”；
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      

  #1.定义联合主键“月份”、“实体渠道标识”；
	set tabname "G_S_22058_MONTH"
	set pk 			"OP_TIME||CHANNEL_ID||YK_MODE"
	chkpkunique ${tabname} ${pk} ${op_month}
	
return 0
}


##~   rename bass1.G_S_22058_MONTH to G_S_22058_MONTH_old;
##~   CREATE TABLE "BASS1   "."G_S_22058_MONTH"  (
                  ##~   "TIME_ID" INTEGER , 
                  ##~   "OP_TIME" CHAR(6) , 
                  ##~   "CHANNEL_ID" CHAR(40) , 
                  ##~   "YK_MODE" CHAR(2) , 
                  ##~   "YK_CNT" CHAR(12) , 
                  ##~   "YK_AMT" CHAR(12) )   
                 ##~   DISTRIBUTE BY HASH("OP_TIME","CHANNEL_ID","YK_MODE")   
                   ##~   IN "TBS_APP_BASS1" INDEX IN "TBS_INDEX" ; 
				   
##~   insert into G_S_22058_MONTH
##~   (
         ##~   TIME_ID
        ##~   ,OP_TIME
        ##~   ,CHANNEL_ID
		##~   ,YK_MODE
        ##~   ,YK_CNT
        ##~   ,YK_AMT
##~   )
##~   select 
         ##~   TIME_ID
        ##~   ,OP_TIME
        ##~   ,CHANNEL_ID
		##~   ,'' YK_MODE
        ##~   ,YK_CNT
        ##~   ,YK_AMT
##~   from G_S_22058_MONTH_old
