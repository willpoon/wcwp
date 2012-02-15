######################################################################################################
#�ӿ����ƣ�ʵ��������Դ������Ϣ
#�ӿڱ��룺06023
#�ӿ�˵������¼ʵ��������Դ������Ϣ, �漰��Ӫ����ί�о�Ӫ��������������
#��������: G_I_06023_MONTH.tcl
#��������: ����06023������
#��������: ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuzhilong
#��дʱ�䣺2010-11-9
#�����¼��
#�޸���ʷ:1.6.9�淶ɾ��'��Ҫ������ʽ'�ֶ�
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

   #���� yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

    #���� yyyy-mm-dd
    set optime $op_time

    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month

    #���µ�һ�� yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day

    #�������һ�� yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_month 4 5][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day

    #ɾ����ʽ��������
    set sql_buff "delete from bass1.G_I_06023_MONTH where time_id=$op_month"
    puts $sql_buff
    exec_sql $sql_buff




    #����ʽ����뱾������
    set sql_buff "
    insert into BASS1.G_I_06023_MONTH
		(	 	 TIME_ID          		                               
			 , CHANNEL_ID       		 /*  ʵ��������ʶ                   */
			 , BUILD_AREA       		 /*  �������                       */
			 , USE_AREA         		 /*  ʹ�����                       */
			 , STORE_AREA       		 /*  ǰ̨Ӫҵ���                   */
			 , SEAT_NUM         		 /*  ̨ϯ����                       */
			 , STORE_EMPLOYE    		 /*  Ӫҵ��Ա����                   */
			 , GUARD_EMPLOYE    		 /*  ��������                       */
			 , CLEAR_EMPLOYE    		 /*  ��������                       */
			 , IF_WAIT_MARK     		 /*  �����ŶӽкŻ�                 */
			 , IF_POS_MARK      		 /*  ����POS��                      */
			 , IF_VIP_SEAT      		 /*  ����VIPרϯ                    */
			 , IF_VIP_ROOM      		 /*  ����VIP��                      */
			 , PRINT_NUM        		 /*  ���굥��ӡ��̨��               */
			 , TERM_NUM         		 /*  �ۺ��������ն�̨��             */
			 , G3_AREA          		 /*  G3���������                   */
			 , TV_NUM           		 /*  ����������                     */
			 , NEW_BUSITERM_NUM 		 /*  ��ҵ������Ӫ��ƽ̨�ն˸���     */
			 , HEART_TERM_NUM   		 /*  �Ļ�����ƽ̨�ն˸���           */
			 , NET_TERM_NUM     		 /*  ����Ӫҵ�������ն˸���         */
			 , AREA             		 /*  �������                       */
			 , ACCEPT_AREA      		 /*  �ƶ����������                 */
			 , MAIN_NET_TYPE    		 /*  ��Ҫ������ʽ                   */
			 , IF_CZ            		 /*  �ܷ������г�ֵҵ��           */
		  )
	SELECT
	   $op_month
		,trim(char(a.channel_id))
		,value(char(b.BUILD_AREA/100),'') BUILD_AREA
		,'' USE_AREA
		,value(char(b.STORE_AREA/100),'') STORE_AREA
		,value(char(b.SEAT_NUM),'') SEAT_NUM
		,value(char(b.EMPLOYEE_NUM),'') STORE_EMPLOYE
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






  set tabname "G_I_06023_MONTH"
        set pk                  "CHANNEL_ID"
        chkpkunique ${tabname} ${pk} ${op_month}
        #
	
	return 0
}
