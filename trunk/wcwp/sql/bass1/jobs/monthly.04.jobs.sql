$ splfile s_13100_201104_21003_00_001.dat 7500000
no such file : s_13100_201104_21003_00_001.dat.dat
$ splfile s_13100_201104_21003_00_001 7500000    
...backup s_13100_201104_21003_00_001.dat to s_13100_201104_21003_00_001.bak...
...spliting...

7号的接口02052依赖报表的手工入库数据bass2.stat_zd_village_users_yyyymm(农村客户统计表用户基础表),春节期间找谁，请问报表相关人，
once : 积分数据修复？？有没有总部类？
ls -lhrt \
 *02006*.dat \
 *02007*.dat \
 *02017*.dat \
 *02052*.dat \
 *03004*.dat \
 *03005*.dat \
 *03012*.dat \
 *03015*.dat \
 *03016*.dat \
 *03017*.dat \
 *03018*.dat \
 *21003*.dat \
 *21006*.dat \
 *21008*.dat \
 *21011*.dat \
 *21012*.dat \
 *21020*.dat \
 *22036*.dat \
 *22040*.dat \
 *22072*.dat \
 *22081*.dat \
 *22083*.dat \
 *22085*.dat \
 *22204*.dat \
 *22303*.dat \
 *22304*.dat \
 *22305*.dat \
 *22306*.dat \
 *22307*.dat \
 *22401*.dat 
 
 
 
	select b.CONTROL_CODE 
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
	where a.INTERFACE_CODE = substr(control_code , 11,5)
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = '每月8日前'
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
							and upload_time = '每月8日前'
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
							    and upload_time = '每月8日前'
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
		and upload_time = '每月8日前'
)

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
							and upload_time = '每月8日前'
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
							    and upload_time = '每月8日前'
						    )
						    and control_code like 'BASS1%EXP%'
						)
		and flag = 0
		and date(endtime) <= current date 
		and month(endtime)  = month(current timestamp)
)
and control_code not in ('BASS1_G_S_05001_MONTH.tcl','BASS1_G_S_05002_MONTH.tcl')



	select *
	from    
	BASS1.MON_ALL_INTERFACE a
	, app.sch_control_task b 
    ,app.sch_control_runlog c 
	where a.INTERFACE_CODE = substr(b.control_code , 11,5)
    and b.control_code = c.control_code 
		and a.TYPE = 'm'
		and b.control_code like '%MONTH%'
		and upload_time = '每月8日前'
        

--已上传的接口
select substr(filename,16,5)
from 
(
select  a.* ,row_number()over(partition by  substr(filename,16,5) order by deal_time desc ) rn 
from APP.G_FILE_REPORT a
where substr(filename,9,6) = substr(replace(char(current date - 1 month),'-',''),1,6)
and err_code='00'
and length(filename)=length('s_13100_201002_03014_01_001.dat')
) t where rn = 1


               
update app.sch_control_task 
set time_value = 510
where CONTROL_CODE in (
select b.CONTROL_CODE from    
BASS1.MON_ALL_INTERFACE a
, app.sch_control_task b where a.INTERFACE_CODE = substr(control_code , 11,5)
and a.INTERFACE_CODE   
in 
(
 '22085'
,'22081'
,'22083'
)
)
and       time_value = -1


update app.sch_control_task  a
set time_value = 510
where a.control_code = 'BASS1_G_A_02052_MONTH.tcl'
and       time_value = 512


检查是否有已上传，避免重复上传
select * from bass1.MON_ALL_INTERFACE
where INTERFACE_CODE in (
select unit_code  from app.g_runlog 
where time_id= int(substr(replace(char(current date - 1 month),'-',''),1,6))
and return_flag=0
)


