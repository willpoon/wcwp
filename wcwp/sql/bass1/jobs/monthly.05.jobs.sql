特别关注：
05001,05002 入库
22039

出月报

/**
update  app.sch_control_task
set time_value = 510
where  time_value = 112
and control_code = 'BASS1_G_S_05001_MONTH.tcl'

        
update  app.sch_control_task
set time_value = 510
where  time_value = 112
and control_code = 'BASS1_G_S_05002_MONTH.tcl'

update app.sch_control_task a
set time_value = 512
where control_code in (
	select b.control_code
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
    ,app.sch_control_runlog c 
	where a.INTERFACE_CODE = substr(b.control_code , 11,5)
    and b.control_code = c.control_code 
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = '每月10日前'
) 
and time_value < 512
        
        
**/


10号前接口：
ls -alrt  *03007*dat *21010*dat *21013*dat *21014*dat *21015*dat *05001*dat \
*05002*dat *05003*dat *22013*dat *22021*dat *22025*dat *22032*dat *22033*dat \
*22039*dat *22041*dat *22042*dat *22043*dat

15号前接口：
ls -lart *22049*dat *22050*dat *22052*dat *22055*dat *22056*dat *22061*dat \
*22062*dat *22063*dat *22064*dat *22065*dat  

--D:\pzw\prj\wcwp\inc\all_shell_functions.ksh

insert into bass1.g_s_05001_month
select * from  bass1.T_GS05001M where time_id = 201104


insert into bass1.g_s_05002_month
select * from  bass1.T_GS05002M where time_id = 201104

select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05001_month 
group by  time_id 
order by 1 desc 

select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05002_month 
group by time_id 
order by 1 desc 






	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = '每月10日前'
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
							and upload_time = '每月10日前'
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
							    and upload_time = '每月10日前'
						    )
						    and control_code like 'BASS1%EXP%'
						)	
	



--重运出数代码

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
		and upload_time = '每月10日前'
)
and  flag = 0 
and control_code not in ('BASS1_G_S_05001_MONTH.tcl','BASS1_G_S_05002_MONTH.tcl')



--重运校验代码
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
							and upload_time = '每月10日前'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
		)
		and flag = 0
		and date(endtime) < current date
		and month(endtime)  = month(current timestamp)
)


--删除 export_yyyymm 下的数据，然后：

--重运导出程序
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
							    and upload_time = '每月10日前'
						    )
						    and control_code like 'BASS1%EXP%'
						)
		and flag = 0
		and date(endtime) <= current date 
		and month(endtime)  = month(current timestamp)
)
and control_code not in ('BASS1_EXP_G_S_05001_MONTH','BASS1_EXP_G_S_05002_MONTH')


select * from    bass1.MON_ALL_INTERFACE 
where type = 'm'
and deadline = 10
and  interface_code  in 
(select substr(filename,16,5)
from 
(
select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
from APP.G_FILE_REPORT a
where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
and err_code='00'
and length(filename)=length('s_13100_201002_03014_01_001.dat')
) t where rn = 1)



--不导出 05001-05002 , 除非这两个数已经拿到。
BASS1_G_S_05001_MONTH.tcl
BASS1_G_S_05002_MONTH.tcl

--------------------------------------------------------------------


	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = '每月15日前'
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
							and upload_time = '每月15日前'
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
							    and upload_time = '每月15日前'
						    )
						    and control_code like 'BASS1%EXP%'
						)	
	



--重运出数代码

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
		and upload_time = '每月15日前'
)
and  flag = 0 



--重运校验代码
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
							and upload_time = '每月15日前'
						)
				and control_code like '%CHECK%'
				and  control_code in (select control_code from app.sch_control_task where cc_flag = 1)
		)
		and flag = 0
		and date(endtime) < current date
		and month(endtime)  = month(current timestamp)
)


--删除 export_yyyymm 下的数据，然后：

--重运导出程序
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
							    and upload_time = '每月15日前'
						    )
						    and control_code like 'BASS1%EXP%'
						)
		and flag = 0
		and date(endtime) <= current date 
		and month(endtime)  = month(current timestamp)
)
and control_code not in ('BASS1_EXP_G_S_05001_MONTH','BASS1_EXP_G_S_05002_MONTH')

