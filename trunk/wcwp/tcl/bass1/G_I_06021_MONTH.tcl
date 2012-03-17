######################################################################################################
#�ӿ����ƣ�ʵ������������Ϣ
#�ӿڱ��룺06021
#�ӿ�˵������¼��Ӫ����ί�о�Ӫ���������������ʵ�������Ļ�����Ϣ
#��������: G_I_06021_MONTH.tcl
#��������: ����06021������
#��������: ��
#�������:
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuzhilong
#��дʱ�䣺2010-11-9
#�����¼��
#�޸���ʷ: 	liuzhilong 2010-12-6 15:11:42 ����1.6.9�淶�޸Ľӿ�
#           liuqf 2011-2-23 17:07:39 �޸��������������Լ�״̬�ھ�
#           2011-04-15  �������ݽ�Ҫ�º˲�淶��У�飬���޸���γ������.
#           2011-11-25   ���� 06035 �� 06021 channel_id һ����У��
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#set optime_month 2011-06
   #���� yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

    #���� yyyy-mm-dd
    set optime $op_time

    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
		set last_month [GetLastMonth [string range $op_month 0 5]]
    #���µ�һ�� yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day

    #�������һ�� yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day

    #ɾ����ʽ��������
    set sql_buff "delete from bass1.G_I_06021_MONTH where time_id=$op_month"
    puts $sql_buff
    exec_sql $sql_buff

##		, CASE WHEN A.CHANNEL_TYPE_CLASS=90105 THEN '1'
##			 		WHEN A.CHANNEL_TYPE_CLASS=90102 AND A.CHANNEL_TYPE IN (90175,90186,90740,90741,90881) THEN '2'
##			 		ELSE '3'
##		  END  CHANNEL_TYPE

#2011-04-15  �޸���γ����У��ͨ����������ͨ������������ɵġ�
    #����ʽ����뱾������
    set sql_buff "
    insert into BASS1.G_I_06021_MONTH
		(	 	time_id
			 ,channel_id       	/* ʵ��������ʶ                                          */
			 ,channel_type     	/* ʵ����������                                          */
			 ,self_channel_id	 	/* 24Сʱ����Ӫҵ���Ͻ�Ӫҵ��������ʶ 20101206 �����ֶ�  */
			 ,cmcc_id          	/* ����CMCC��Ӫ��˾��ʶ                                  */
			 ,country_name     	/* ��������                                              */
			 ,thorpe_name      	/* ����/Ƭ������                                         */
			 ,channel_name     	/* ��������                                              */
			 ,channel_addr     	/* ������ַ                                              */
			 ,position         	/* ����λ������                                          */
			 ,region_info      	/* ������̬                                              */
			 ,channel_b_type   	/* ������������                                          */
			 ,is_exclude 			 	/* �Ƿ�����  20101206 �����ֶ�                           */
			 ,is_phone_shop 	 	/* �Ƿ�Ϊ�ֻ�����  20101206 �����ֶ�                     */
			 ,channel_star     	/* �����Ǽ�                                              */
			 ,channel_status   	/* ����״̬                                              */
			 ,business_begin   	/* Ӫҵ��ʼʱ��                                          */
			 ,business_end     	/* Ӫҵ����ʱ��                                          */
			 ,valid_date       	/* Э��ǩ����Ч���� 20101206����not nullԼ��             */
			 ,expire_date      	/* Э���ֹ���� 20101206����not nullԼ��                 */
			 ,times            	/* �Ѻ�������      20101206 �޸��ֶγ��� ԭΪ9           */
			 ,longitude        	/* ����     20101206 �޸��ֶγ��� ԭΪ18                 */
			 ,latitude         	/* γ��     20101206 �޸��ֶγ��� ԭΪ18                 */
			 ,fitment_price    	/* װ���ۼ�Ͷ���ܶ�                                      */
			 ,equip_price      	/* �豸�ۼ�Ͷ���ܶ�                                      */
			 ,prices           	/* �칫��Ӫҵ�Ҿ��ۼ�Ͷ���ܶ�                            */
			 ,charge           	/* һ������ͷ����                                        */
		  )
	SELECT
	   $op_month
		,trim(char(a.channel_id))
		,case when a.channel_type_class=90105 and a.channel_type in (90196,90153,90154,90155,90156,90157,90158,90940,90941,90942,90943) then '1'
          else '3'
     end channel_type
		,'' self_channel_id
		,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD1_0054',SUBSTR(A.REGION_CODE,2,3)),'13101')
		,value(b.county_name,'δ֪')
		,value(c.thorpe_name,'δ֪')
		,value(a.channel_name,'δ֪')
		,value(a.channel_address,'δ֪')
		,case when a.geography_type in (1,2,3) then '1'
			 		when a.geography_type in (4) then '2'
			 		when a.geography_type in (5) then '3'
			 		else '4'
		 end  position
		,case when a.geography_property=1 then '4'
			 		when a.geography_property=2 then '2'
			 		when a.geography_property=3 then '1'
			 		when a.geography_property=4 then '3'
			 		when a.geography_property=5 then '6'
			 		when a.geography_property=6 then '5'
			 		when a.geography_property=7 then '7'
			 		when a.geography_property=8 then '3'
			 		when a.geography_property=9 then '3'
			 		else '3'
		 end region_info
		,case when a.channel_type_class=90105 and a.channel_type in (90153,90155,90157,90158,90196,90940,90942,90943) then '1'
			 		when a.channel_type_class=90105 and a.channel_type in (90154,90941) then '2'
			 		when a.channel_type_class=90105 and a.channel_type in (90156) then '3'
			 		when a.channel_type_class=90102 and a.channel_type in (90881) then '5'
			 		when a.channel_type_class=90102 and a.channel_type in (90885) then '6'
			 		else '6'
		 end channel_b_type
		,case when a.is_exclude=1 then '1' else '0' end
		,'0' is_phone_shop
		,case when a.channel_type_class=90105 and a.channel_type in (90196,90153,90154,90155,90156,90157,90158,90940,90941,90942,90943) then ''
			    else case when value(trim(char(channel_level)),'6') > '6' then '6' else value(trim(char(channel_level)),'6')
		 end end channel_star
		,case when a.user_state=1 and a.state=0 then '1'
			 		when a.user_state=2 and a.state=0 then '2'
			 		when a.user_state=3 and a.state=0 then '3'
			 		else '3'
		 end channel_status
		,'0930'
		,'1830'
		,'00010101' valid_date
		,'00010101' expire_date
		,'' times
		,value (a1.longitude,
			case when a.longitude is null or a.longitude*1.00/100 < 80  or a.longitude*1.00/100 > 99 then 
								char(cast((case when 80.00000+rand(1)*8 < 80.337524 then 80.337524
								when 80.00000+rand(1)*8 > 98.311157 then 98.311157
        				else 80.00000+rand(1)*8  end) as decimal(7,5)))
          else  char(cast(a.LONGITUDE*1.00/100 as decimal(7,5))) end  
       ) longitude
		,value (a1.latitude,
		case when a.latitude is null or a.latitude*1.00/100 < 26 or a.latitude*1.00/100 > 36 then 
							char(cast((case when 26.00000+rand(1)*10 < 26.425222 then 26.425222 
								when 26.00000+rand(1)*10 > 36.398516 then 36.398516 
					        else 26.00000+rand(1)*10  end) as decimal(7,5)))
					else char(cast(a.latitude*1.00/100 as decimal(7,5))) end  
			) latitude
		,'' fitment_price
		,'' equip_price
		,'' prices
		,'' charge
	from bass2.dw_channel_info_$op_month a
	left join (select * from  G_I_06021_MONTH
							where time_id = $last_month) a1 on trim(char(a.channel_id)) = a1.channel_id
	left join bass2.dim_pub_county b on a.county_code=b.county_id
	left join bass2.dim_thorpe c on a.thorpe_code=c.thorpe_code
 where a.channel_type_class in (90105,90102)
 "


  exec_sql $sql_buff


# ��ԡ������غ˲�%������������ݡ� �޸���������
          

set  sql_buff "                
update (                         
select * from G_I_06021_MONTH
where time_id = $op_month
and channel_addr  = '��'
) t 
set channel_addr = trim(COUNTRY_NAME)||trim(CHANNEL_NAME)
 "
  exec_sql $sql_buff

       
	   
	   

#2011.11.25 ���� 06035 �� 06021 channel_id һ����У��

set  sql_buff "
select count(0)
from (
select channel_id
from
(
select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
from G_A_06035_DAY a
where time_id / 100 = $op_month
) t where t.rn =1 and CHNL_STATE = '1'
except
select channel_id
from G_I_06021_MONTH 
where time_id = $op_month
and CHANNEL_STATUS = '1'
) t
with ur
 "

chkzero2 $sql_buff "06035��06021channel_id��һ��A"

set  sql_buff "
select count(0)
from (
select channel_id
from G_I_06021_MONTH 
where time_id = $op_month
and CHANNEL_STATUS = '1'
except
select channel_id
from
(
select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
from G_A_06035_DAY a
where time_id / 100 = $op_month
) t where t.rn =1 and CHNL_STATE = '1'
) t
with ur
 "

chkzero2 $sql_buff "06035��06021channel_id��һ��B"


##~   ��������ƽ̨������������ݺ˲飺
##~   Ϊ����Ƿ����ǰ̨����Ϊ�յ��������������У�飺
##~   ��Щ����������ҵ������Ƿ��������������⣿��ô�BOSS�����ɾ���ݵĻ������׵���Υ��У��
    set sql_buff "
	select count(0) from (
		select distinct CHANNEL_ID 
		from G_I_06021_MONTH 
		where time_id = $op_month
		and CHANNEL_TYPE = '1'
		and CHANNEL_STATUS = '1'
		except
		select distinct CHANNEL_ID from 
		G_S_22091_DAY where time_id / 100 = $op_month
		) t
		with ur
"

chkzero2 $sql_buff "��������غ˲顷У�飺����������Ч����ҵ��������Ϻ˲�Ҫ�������!(������ʽ���ˣ��ɺ��ԣ�)"



	return 0
}



#
#		CHANNEL_TYPE
#		90196
#		90153
#		90154
#		90155
#		90156
#		90157
#		90158
#		90940
#		90941
#		90942
#		90943
#
#		select * from   bass2.DIM_BBOSS_BASE_TYPE
#		where code_type in (
#		90105
#		,90102
#		)
#
#		90105	90196	������������	������������	1	1	1
#		90105	90153	��Ӫ������	��Ӫ������	1	1	1
#		90105	90154	Ʒ�Ƶ�	Ʒ�Ƶ�	1	1	1
#		90105	90155	ȫ��ͨVIP���ֲ�	ȫ��ͨVIP���ֲ�	1	1	1
#		90105	90156	�콢��	�콢��	1	1	1
#		90105	90157	���ſͻ������	���ſͻ������	1	1	1
#		90105	90158	���˿ͻ������	����?
