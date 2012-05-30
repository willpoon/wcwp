
######################################################################################################		
#接口名称: 电子渠道重点增值业务办理日汇总                                                               
#接口编码：22096                                                                                          
#接口说明：记录电子渠道29项重点增值业办理日汇总信息。
#程序名称: G_S_22096_DAY.tcl                                                                            
#功能描述: 生成22096的数据
#运行粒度: DAY
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20120529
#问题记录：
#修改历史: 1. panzw 20120529	中国移动一级经营分析系统省级数据接口规范 (V1.8.1) 
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
    #本月 yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
      
    #上个月 yyyymm
    set last_month [GetLastMonth [string range $op_month 0 5]]
    puts $last_month

        #程序名
        global app_name
        set app_name "G_S_22096_DAY.tcl"
	
  #删除本期数据
	set sql_buff "delete from bass1.G_S_22096_DAY where time_id=$timestamp"
	exec_sql $sql_buff

	return 0
}

此字段仅取以下分类：
01:门户网站
02:10086电话营业厅
03: 短信营业厅
04:WAP网站
05:自助终端（包括所有的自助终端，即包括实体渠道和24小时营业厅内布放的自助终端，还包括商场等场所独立摆放的自助终端。）



select 

网站：

					 select '01' ECHNL_TYPE
							,
							,count(0)
                       from bass2.dw_product_ord_cust_dm_$curr_month a
                       , bass2.dw_product_$timestamp b
					   , bass2.dw_product_ord_offer_dm_201204 c
                    where a.product_instance_id = b.user_id 
					and a.ORDER_STATE = 11
					and a.CUSTOMER_ORDER_ID = c.CUSTOMER_ORDER_ID
					and upper(a.channel_type) = 'B' and    a.op_time = '$op_time' 
					
					
					


01:来电提醒
02:语音信箱
03:号簿管家
04:短信回执
05:信息管家
06:飞信会员
07:139邮箱收费版
08:手机证券
09:手机商界
10:blackberry
11:无线音乐俱乐部
12:手机动漫
13:彩铃
14:多媒体彩铃
15:音乐随身听
16:歌曲下载
17:手机电视(数字广播方式)
18:手机医疗
19:手机地图
20:手机导航
21:快讯
22:手机阅读
23:手机报
24:无线体育俱乐部
25:手机视频
26:手机游戏
27:彩像
28:移动应用商城
29:12580生活播报


