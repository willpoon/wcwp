######################################################################################################
#接口名称：实体渠道资源配置信息
#接口编码：06023
#接口说明：记录实体渠道资源配置信息, 涉及自营厅、委托经营厅或社会代理网点
#程序名称: G_I_06023_MONTH.tcl
#功能描述: 生成06023的数据
#运行粒度: 月
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuzhilong
#编写时间：2010-11-9
#问题记录：
#修改历史:1.6.9规范删除'次要联网方式'字段

##~   20120419 : 渠道相关数据核查：201203：对于核查中STORE_AREA为空或零的渠道，STORE_AREA置为20
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

   #当天 yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

    #当天 yyyy-mm-dd
    set optime $op_time

    #本月 yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month

    #本月第一天 yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day

    #本月最后一天 yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_month 4 5][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day

    #删除正式表本月数据
    set sql_buff "delete from bass1.G_I_06023_MONTH where time_id=$op_month"
    puts $sql_buff
    exec_sql $sql_buff




    #往正式表插入本月数据
    set sql_buff "
    insert into BASS1.G_I_06023_MONTH
		(	 	 TIME_ID          		                               
			 , CHANNEL_ID       		 /*  实体渠道标识                   */
			 , BUILD_AREA       		 /*  建筑面积                       */
			 , USE_AREA         		 /*  使用面积                       */
			 , STORE_AREA       		 /*  前台营业面积                   */
			 , SEAT_NUM         		 /*  台席数量                       */
			 , STORE_EMPLOYE    		 /*  营业人员数量                   */
			 , GUARD_EMPLOYE    		 /*  保安人数                       */
			 , CLEAR_EMPLOYE    		 /*  保洁人数                       */
			 , IF_WAIT_MARK     		 /*  有无排队叫号机                 */
			 , IF_POS_MARK      		 /*  有无POS机                      */
			 , IF_VIP_SEAT      		 /*  有无VIP专席                    */
			 , IF_VIP_ROOM      		 /*  有无VIP室                      */
			 , PRINT_NUM        		 /*  帐详单打印机台数               */
			 , TERM_NUM         		 /*  综合性自助终端台数             */
			 , G3_AREA          		 /*  G3体验区面积                   */
			 , TV_NUM           		 /*  电视屏个数                     */
			 , NEW_BUSITERM_NUM 		 /*  新业务体验营销平台终端个数     */
			 , HEART_TERM_NUM   		 /*  心机体验平台终端个数           */
			 , NET_TERM_NUM     		 /*  网上营业厅接入终端个数         */
			 , AREA             		 /*  店面面积                       */
			 , ACCEPT_AREA      		 /*  移动受理区面积                 */
			 , MAIN_NET_TYPE    		 /*  主要联网方式                   */
			 , IF_CZ            		 /*  能否办理空中充值业务           */
		  )
	SELECT
	   $op_month
		,trim(char(a.channel_id))
		,value(char(b.BUILD_AREA/100),'') BUILD_AREA
		,'' USE_AREA
		,value(char(b.STORE_AREA/100),'20') STORE_AREA
		,value(char(b.SEAT_NUM),'1') SEAT_NUM
		,value(char(b.EMPLOYEE_NUM),'1') STORE_EMPLOYE
		,value(char(b.ENSURE_NUM),'') GUARD_EMPLOYE
		,value(char(b.CLEAN_NUM),'') CLEAR_EMPLOYE
		,value(char(b.HAVE_QUEUE),'') IF_WAIT_MARK
		,value(char(b.HAVE_POS),'') IF_POS_MARK
		,value(char(b.HAVE_VIPLINE),'') IF_VIP_SEAT
		,value(char(b.HAVE_VIPROOM),'') IF_VIP_ROOM
		,value(char(b.PRINTER_NUM),'') PRINT_NUM
		,value(char(b.GENERALATM_NUM),'') TERM_NUM
		,value(char(b.TASTE_AREA),'') G3_AREA
		,value(char(b.TVSCREEN_NUM),'') TV_NUM
		,value(char(b.NEWBUSI_PLATFORM_NUM),'') NEW_BUSITERM_NUM
		,value(char(b.HEART_PLATFORM_NUM),'') HEART_TERM_NUM
		,value(char(b.ONLINE_NUM),'') NET_TERM_NUM
		,'' AREA
		,'' ACCEPT_AREA
		,'' MAIN_NET_TYPE
		,'' IF_CZ
	FROM BASS2.DW_CHANNEL_INFO_$op_month A 
	left join bass2.Dwd_channel_selfsite_info_$this_month_last_day b on  a.channel_id = b.channel_id
	where A.CHANNEL_TYPE_CLASS IN (90105,90102)
	"
    exec_sql $sql_buff

#STORE_AREA

    set sql_buff "
update (         
select * from G_I_06023_MONTH
where time_id = $op_month
and 
(
STORE_AREA = '' or STORE_AREA = '0'
)
and bigint(BUILD_AREA) > 2
and BUILD_AREA <> ''
) t 
set STORE_AREA = char(bigint(BUILD_AREA)-2)
	"
    exec_sql $sql_buff


#SEAT_NUM
    set sql_buff "
	update (         
	select * from G_I_06023_MONTH
	where time_id = $op_month
	and 
	(
	SEAT_NUM = '' or SEAT_NUM = '0'
	)
	) t 
	set SEAT_NUM = '1'
"
    exec_sql $sql_buff

#STORE_EMPLOYE
    set sql_buff "
	update (
	select * from G_I_06023_MONTH
	where time_id = $op_month
	and SEAT_NUM > '0'
	) t 
	set STORE_EMPLOYE = char(2*bigint(SEAT_NUM))
	with ur
"
    exec_sql $sql_buff


##~   质量管理平台：渠道相关数据核查：
##~   为了防止有06021中的有效渠道在06023中各项属性为空的情况，特设立此校验：

    set sql_buff "
		select count(0)
		from 
		(
			select * from G_I_06021_MONTH
			where time_id = $op_month
			and CHANNEL_TYPE = '1'
			and CHANNEL_STATUS = '1'
		) a 
		left join (select * from G_I_06023_MONTH where time_id = $op_month ) b on a.channel_id = b.channel_id
		where (b.BUILD_AREA  <= '0' or b.BUILD_AREA  = ''
				 or SEAT_NUM = '' or SEAT_NUM = '0'
			   )
		with ur
"

chkzero2 $sql_buff "《渠道相关核查》校验：06023渠道属性有问题，不合核查要求，请调整!(若无正式考核，可忽略！)"




  set tabname "G_I_06023_MONTH"
        set pk                  "CHANNEL_ID"
        chkpkunique ${tabname} ${pk} ${op_month}
        #
	
	return 0
}
