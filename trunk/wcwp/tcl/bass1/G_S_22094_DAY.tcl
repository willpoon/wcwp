
######################################################################################################		
#�ӿ�����: �����ɷѼ�¼                                                               
#�ӿڱ��룺22094                                                                                          
#�ӿ�˵������¼�û���ʵ�����������������ɷ��굥��Ϣ��
#��������: G_S_22094_DAY.tcl                                                                            
#��������: ����22094������
#��������: DAY
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20120328
#�����¼��
#�޸���ʷ: 1. panzw 20120328	�й��ƶ�һ����Ӫ����ϵͳʡ�����ݽӿڹ淶 (V1.7.9) 
#######################################################################################################   


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#set op_time 2011-06-07

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
		#��Ȼ��
		set curr_month [string range $op_time 0 3][string range $op_time 5 6]
    #�������һ�� yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day
		global app_name
		set app_name "G_S_22094_DAY.tcl"        


    #ɾ����ʽ��������
    set sql_buff "delete from bass1.G_S_22094_DAY where time_id=$timestamp"
    exec_sql $sql_buff

		set sql_buff "
		insert into G_S_22094_DAY
		select 
			$timestamp time_id
			,'$timestamp'  CHRG_DT
			,replace(substr(char(PEER_DATE),12,8),'.','')  CHRG_TM
			,case when opt_code = '4464' then 'BASS1_ST' else char(staff_org_id) end channel_id
			,key_num MSISDN
			,case 
				when CERTIFICATE_TYPE = '0' then '1'
				when CERTIFICATE_TYPE = '1' then '2'
				when opt_code = '4205' then '3' else '1'
			end CHRG_TYPE
			,char(bigint(amount)) CHRG_AMT
		from BASS2.dw_acct_payment_dm_$curr_month a
		where  replace(char(a.OP_TIME),'-','') = '$timestamp' 
		and OPT_CODE in 
					('4205' --�ֻ�֧��-�û��ɷ�
					,'4101'	--Ӫҵ��ǰ̨�ɷ�
					,'4464' --���������ն��ֽ�ɷ�
					)
		with ur
		"
	
exec_sql $sql_buff


  #���н�����ݼ��
  #1.���chkpkunique
  set tabname "G_S_22094_DAY"
  ##~   set pk   "DEAL_DATE||CHANNEL_ID||DEAL_TYPE||IMP_VAL_TYPE"
        ##~   chkpkunique ${tabname} ${pk} ${timestamp}
        ##~   #
  aidb_runstats bass1.$tabname 3
  
	return 0

}




##~   select *
##~   from 
##~   (
##~   select OPT_CODE paytype_id, count(0) cnt 
##~   --,  count(distinct OPT_CODE ) 
##~   from BASS2.dw_acct_payment_dm_201202 
##~   where staff_org_id = 11111144
##~   group by  OPT_CODE 
##~   ) t , bass2.DIM_ACCT_PAYTYPE b 
##~   where t.paytype_id = b.paytype_id 

##~   PAYTYPE_ID	CNT	PAYTYPE_ID	PAYTYPE_NAME	STS	   
##~   4101	968	4101	Ӫҵ��ǰ̨�ɷ�	1	   
##~   4114	19	4114	Ԥ�����˷�	1	   
##~   4118	198	4118	����Ԥ����ת��	1	   
##~   4178	198	4178	����Ԥ����ת�Ƴ�	1	   
##~   4408	19	4408	Ӫҵ�����˷�	1	   
##~   4464	7474	4464	���������ն��ֽ�ɷ�	1	   
##~   4465	6	4465	���������ն��ֽ�ɷѳ���	1	   
##~   4801	69	4801	[����]ǰ̨�ɷ�	1	   
##~   4864	988	4864	[����]���������ն��ֽ�ɷ�	1	   
##~   GJFBG	858	GJFBG	��Ʒ����ɷ�Ԥ��	1	   
##~   GJFF	14	GJFF	�ڲ�������һ���ʻ��ɷ�	1	   
##~   GJFFZ	1	GJFFZ	��װ�ɷ�Ԥ��	1	   
##~   GJFKH	523	GJFKH	�����ɷ�Ԥ��	1	   
##~   GJFL	91	GJFL	ת�˳�	1	   
##~   GJFM	91	GJFM	ת����	1	   
##~   GTFBG	11	GTFBG	��Ʒ����ɷ�Ԥ��ع�	1	   
##~   GTFKH	5	GTFKH	�����ɷ�Ԥ��ع�	1	   
					


##~   CERTIFICATE_TYPE ƾ֤����
##~   0-�ֽ�
##~   1-��¼֧Ʊ��ţ�
##~   2-��¼���п��ţ�
##~   3-��¼����ȯ��ţ�
##~   4-��¼����ƾ֤��ţ�
##~   5-�е���� 
##~   6-��ֵ���� 
##~   7-�ɷѿ�


##~   ���Ա���	��������	��������	��������	��ע
##~   00		��¼�к�	Ψһ��ʶ��¼�ڽӿ������ļ��е��кš�	number(8)	
##~   01		�ɷ�����	��ʽYYYYMMDD	CHAR(8)	
##~   02		�ɷ�ʱ��	��ʽHH24MISSS	CHAR(6)	
##~   03		MSISDN	���ɷѵ��ֻ�����	CHAR(15)	������Ϊ��
##~   04		������ʶ	���ɷ�����Ϊʵ���������μ���ʵ������������Ϣ�������������ӿ��еġ�ʵ��������ʶ������
##~   ���ɷ�����Ϊ���������������¹�����д����վ�����ߡ����š�wap�������ն˵��������ֱ��Ӧ��д'BASS1_WB', 'BASS1_HL', 'BASS1_SM', 'BASS1_WP', 'BASS1_ST' (�ַ����ִ�Сд)	CHAR(40)	������Ϊ��
##~   05		�ɷ�����	���ֶν�ȡ���·��ࣺ
##~   1���ֽ�
##~   2�����п���
##~   3���ֻ�֧����	NUMBER(1)	������Ϊ��
##~   06		�ɷѽ��	��λ��Ԫ
##~   ע�����������г�ֵ�ɷѽ�	NUMBER(8)	
	##~   0x0D0A	�س����з�		



##~   #===================================================================================================
##~   #�ġ����ָ��   ��      ��
##~   #�ͻ��ܽɷѽ��                     Ԫ  CN4000  CN4001  CN4002  CN4003  CN4004
##~   #    ���У�ͨ����ֵ���ɷ��ܽ��     Ԫ  CN4100  CN4101  CN4102  CN4103  CN4104
##~   #          ͨ�����д��սɷ��ܽ��         Ԫ    CN4200  CN4201  CN4202  CN4203  CN4204
##~   #          ͨ���԰�Ӫҵ���ɷ��ܽ��     Ԫ      CN4300  CN4301  CN4302  CN4303  CN4304
##~   #          ���������ͻ��ɷ��ܽ��         Ԫ    CN4400  CN4401  CN4402  CN4403  CN4404

        ##~   set sql_buf01 "select
                             ##~   case when b.locntype_id = 1 then 1
                                  ##~   when b.locntype_id = 2 then 2
                                  ##~   when b.locntype_id = 3 then 3
                                  ##~   else 4 end,
                             ##~   case when a.PAYTYPE_ID in ('4158','4159') then 41
                                  ##~   when a.PAYTYPE_ID in ('4103','4144') then 42
                            ##~   when a.PAYTYPE_ID in ('4101','4801','4104') then 43
                            ##~   else 44 end,
                             ##~   sum(a.recv_cash) as pay_fee
                 ##~   from DW_ACCT_PAYITEM_${year}${month} a left outer join STAT_ZD_VILLAGE_USERS_${year}${month} b
                 ##~   on a.user_id=b.user_id
                 ##~   where a.rec_sts=0
                 ##~   group by
                       ##~   case when b.locntype_id = 1 then 1
                                  ##~   when b.locntype_id = 2 then 2
                                  ##~   when b.locntype_id = 3 then 3
                                  ##~   else 4 end,
                             ##~   case when a.PAYTYPE_ID in ('4158','4159') then 41
                                  ##~   when a.PAYTYPE_ID in ('4103','4144') then 42
                            ##~   when a.PAYTYPE_ID in ('4101','4801','4104') then 43
                            ##~   else 44 end"
							
							
							
##~   4238	���д���	1	   
##~   4239	���д��۳���	1	   



##~   PAYTYPE_ID	CNT	PAYTYPE_ID	PAYTYPE_NAME	STS	   
##~   4101	809877	4101	Ӫҵ��ǰ̨�ɷ�	1	   
##~   4162	617675	4162	�����ɷ�	1	   
##~   4158	410067	4158	������һ	1	   
##~   GJFW	290236	GJFW	��̯Ԥ�������̯	1	   
##~   4187	143526	4187	�������ʽ�ת��	1	   
##~   GJFE	134889	GJFE	��Ԥ��[ǰ̨]	1	   
##~   4864	98921	4864	[����]���������ն��ֽ�ɷ�	1	   
##~   4801	86346	4801	[����]ǰ̨�ɷ�	1	   
##~   GJFBG	54139	GJFBG	��Ʒ����ɷ�Ԥ��	1	   
##~   4132	47610	4132	CBOSS�ɷ�	1	   
##~   GJFKH	36585	GJFKH	�����ɷ�Ԥ��	1	   
##~   FPDJ	28851	FPDJ	˰��ַ�Ʊ�ҽ�	1	   
##~   GJFY1	19186	GJFY1	�����ʻ��ɷ�	1	   
##~   4118	9421	4118	����Ԥ����ת��	1	   
##~   gJFG	9320	gJFG	�������г�ֵ	1	   
##~   4178	9133	4178	����Ԥ����ת�Ƴ�	1	   
##~   4205	7065	4205	�ֻ�֧��-�û��ɷ�	1	   
##~   4104	4071	4104	����[ǰ̨�ɷ�]	1	   
##~   4103	3427	4103	���д��շ�	1	   
##~   1199	2663	1199	���ų�Ա�����ͻ���	1	   
##~   GQT9	2397	GQT9	������Ԥ��	1	   
##~   GJFM	1792	GJFM	ת����	1	   
##~   GJFL	1792	GJFL	ת�˳�	1	   
##~   4188	1519	4188	������ǰ̨��ֵ	1	   
##~   4108	1387	4108	�ɷѳ���	1	   
##~   4465	1119	4465	���������ն��ֽ�ɷѳ���	1	   
##~   4114	729	4114	Ԥ�����˷�	1	   
##~   4468	640	4468	���Ͻɷ�	1	   
##~   GTFBG	310	GTFBG	��Ʒ����ɷ�Ԥ��ع�	1	   
##~   GTFKH	193	GTFKH	�����ɷ�Ԥ��ع�	1	   
##~   4865	175	4865	[����]���������ն��ֽ�ɷѳ���	1	   
##~   4148	124	4148	CBOSS�ɷѳ���	1	   
##~   4208	96	4208	�������ʽ�ת�Ƴ���	1	   
##~   GTFY1	88	GTFY1	�����ʻ��ɷѷ���	1	   
##~   4115	29	4115	���սɷ�	1	   
##~   GJFFZ	18	GJFFZ	��װ�ɷ�Ԥ��	1	   
##~   GTFF	8	GTFF	�ڲ�������һ���ʻ��ɷѷ���	1	   
##~   GJFg	6	GJFg	����������г�ֵ	1	   
##~   gTFG	4	gTFG	�������г�ֵ����	1	   
##~   4429	2	4429	�������ʻ�ȡ��	1	   
					
					