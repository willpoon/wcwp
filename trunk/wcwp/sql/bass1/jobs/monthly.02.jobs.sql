

�����ݣ�
3��ǰ�ӿڣ�
ls -alrth  \
*02049*dat *02053*dat *03001*dat *03002*dat \
*03003*dat *06011*dat *06012*dat *06029*dat \
*02018*dat *02019*dat *02020*dat *02021*dat

|awk '{print $9,$8,$5}'|sort
--12 in total 


--���г��򡢼��顢����

	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��3��ǰ'
	union all
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��3��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)	
	union all 
		select control_code from app.sch_control_runlog 
		where control_code in 
						(
						select distinct control_code from   app.sch_control_before 
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��3��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
						)	
	
--���˳�������
update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��3��ǰ'
)

--����У�����
update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
		select control_code from app.sch_control_runlog 
		where control_code in 
		(
				select distinct control_code from   app.sch_control_before 
				where  before_control_code in 
				   (
						select b.CONTROL_CODE from    
						BASS1.MON_ALL_INTERFACE a
						, app.sch_control_task b 
							where a.INTERFACE_CODE = substr(control_code , 11,5)
							and a.TYPE = 'm'
							and b.control_code like '%MONTH%'
							and upload_time = 'ÿ��3��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
		)
		and flag = 0
		and date(endtime) < current date
		and month(endtime)  = month(current timestamp)
)


--ɾ�� export_yyyymm �µ����ݣ�Ȼ��

--���˵�������
update  app.sch_control_runlog 
set flag = -2 
where control_code in 
(
		select control_code from app.sch_control_runlog 
		where control_code in 
						(
						select distinct control_code from   app.sch_control_before 
						where  before_control_code in 
							 (
							    select b.CONTROL_CODE from    
							    BASS1.MON_ALL_INTERFACE a
							    , app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
							    and a.TYPE = 'm'
							    and b.control_code like '%MONTH%'
							    and upload_time = 'ÿ��3��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
						)
		and flag = 0
		and date(endtime) <= current date 
		and month(endtime)  = month(current timestamp)
)
and control_code not in ('BASS1_G_S_05001_MONTH.tcl','BASS1_G_S_05002_MONTH.tcl')

--������ 05001-05002 , �������������Ѿ��õ���
BASS1_G_S_05001_MONTH.tcl
BASS1_G_S_05002_MONTH.tcl



!!!!!!!���  02053

--ÿ�¶����������������ݡ�����ܹ���boss�������ˡ�
--���ڴ��������

/**
select * from g_i_02053_month
  where time_id = 201106
 and 
 VALID_DATE> EXPIRE_DATE
 
 update  g_i_02053_month 
 set EXPIRE_DATE = VALID_DATE
 where time_id = 201106
 and 
 VALID_DATE> EXPIRE_DATE
 
 ��д�����
 
 **/
 
������������check 
--ÿ�¶����������������ݡ�



  select * from
                (select * from 
                (
                select user_id,chg_vip_time,row_number()over(partition by user_id order by time_id desc) row_id from BASS1.G_I_02005_MONTH
                where time_id=201204
                ) k
                where k.row_id =1) a
                left outer join 
                (
                select * from
                (
                select user_id,create_date,row_number()over(partition by user_id order by time_id desc) row_id 
                from BASS1.G_A_02004_DAY
                where time_id<=20120430
                ) k
                where k.row_id=1) b
                on a.user_id=b.user_id
                where bigint(chg_vip_time)<bigint(create_date)
                with ur
                
update BASS1.G_I_02005_MONTH
set CHG_VIP_TIME = '20110629'
where user_id = '89160001048760'
and time_id = 201112

USER_ID	CHG_VIP_TIME	ROW_ID	USER_ID	CREATE_DATE	ROW_ID
89160001048760      	20110624	1	89160001048760      	20110629	1



  select * from
                (select * from 
                (
                select user_id,chg_vip_time,row_number()over(partition by user_id order by time_id desc) row_id from BASS1.G_I_02005_MONTH
                where time_id=201106
                ) k
                where k.row_id =1) a
                left outer join 
                (
                select * from
                (
                select user_id,create_date,row_number()over(partition by user_id order by time_id desc) row_id 
                from BASS1.G_A_02004_DAY
                where time_id<=20110630
                ) k
                where k.row_id=1) b
                on a.user_id=b.user_id
                where bigint(chg_vip_time)<bigint(create_date)
                with ur
                

USER_ID	CHG_VIP_TIME	ROW_ID	USER_ID	CREATE_DATE	ROW_ID
89460000740915      	20110320	1	89460000740915      	20110321	1
89160000265019      	20100901	1	89160000265019      	20100917	1


update BASS1.G_I_02005_MONTH
set CHG_VIP_TIME = '20100917'
where user_id = '89160000265019'
and time_id = 201106


update BASS1.G_I_02005_MONTH
set CHG_VIP_TIME = '20110321'
where user_id = '89460000740915'
and time_id = 201105

