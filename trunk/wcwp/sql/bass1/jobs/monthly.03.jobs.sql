/**
5��ǰ�ӿڣ�
ls -alrt \
*01005*dat *02005*dat *02014*dat *02015*dat \
*02016*dat *02047*dat *06021*dat *06022*dat \
*06023*dat *22009*dat *22101*dat *22103*dat \
*22105*dat *22106*dat *06002*dat |awk '{print $9,$8,$5}'|sort
**/
8��ǰ�ӿڣ�
ls -alrth *02006*dat *02007*dat *02052*dat *03004*dat *03005*dat *03012*dat \
*03015*dat *03016*dat *03017*dat *03018*dat *21003*dat *21006*dat *21008*dat \
*21011*dat *21012*dat *21020*dat *22204*dat *22036*dat *22040*dat *22072*dat \
*22303*dat *22304*dat *22305*dat *22306*dat *22307*dat *02017*dat *22401*dat 


	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = 'ÿ��5��ǰ'
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
							and upload_time = 'ÿ��5��ǰ'
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
							    and upload_time = 'ÿ��5��ǰ'
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
		and upload_time = 'ÿ��5��ǰ'
)
and 		 date(endtime) < current date
and month(endtime)  = month(current timestamp)

/** 
--�޳�δ���е�
and control_code not in (select control_code 
    from BASS1.MON_ALL_INTERFACE where 
		and date(endtime) < current date
		and month(endtime)  < month(current timestamp)
**/

or 

/** 
--�޳�δ���е�
and 		 date(endtime) < current date
and month(endtime)  = month(current timestamp)
**/


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
							and upload_time = 'ÿ��5��ǰ'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
		)
		and flag = 0
)
		and date(endtime) < current date
		and month(endtime)  = month(current timestamp)

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
							    and upload_time = 'ÿ��5��ǰ'
						    )
						    and control_code like 'BASS1%EXP%'
						)
		and flag = 0
		and date(endtime) <= current date 
		and month(endtime)  = month(current timestamp)
)
and control_code not in ('BASS1_G_S_05001_MONTH.tcl','BASS1_G_S_05002_MONTH.tcl')
	and date(endtime) < current date
	and month(endtime)  = month(current timestamp)
		
--������ 05001-05002 , �������������Ѿ��õ���
BASS1_G_S_05001_MONTH.tcl
BASS1_G_S_05002_MONTH.tcl


update  app.sch_control_task a
set time_value = 310
where control_code in 
('BASS1_G_S_02047_MONTH.tcl'
,'BASS1_G_I_02015_MONTH.tcl'
,'BASS1_G_I_06002_MONTH.tcl'
,'BASS1_G_I_06021_MONTH.tcl'
,'BASS1_G_A_01005_MONTH.tcl'
,'BASS1_G_I_02016_MONTH.tcl'
,'BASS1_G_I_06023_MONTH.tcl'
,'BASS1_G_I_02014_MONTH.tcl'
,'BASS1_G_I_06022_MONTH.tcl')    
and time_value = 312



update  app.sch_control_task a
set time_value = 310
where control_code in 
(
 'BASS1_G_S_22103_MONTH.tcl'
,'BASS1_G_S_22106_MONTH.tcl'
,'BASS1_G_I_02005_MONTH.tcl'
,'BASS1_G_S_22105_MONTH.tcl'
,'BASS1_G_S_22009_MONTH.tcl'
,'BASS1_G_S_22101_MONTH.tcl'
)    
and time_value = 212

                           


