
delete from bass1.g_s_05001_month where time_id = 201207;
insert into bass1.g_s_05001_month
select * from  bass1.T_GS05001M where time_id = 201207;

delete from bass1.g_s_05002_month where time_id = 201207;
insert into bass1.g_s_05002_month
select * from  bass1.T_GS05002M where time_id = 201207;

select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05001_month 
group by  time_id 
order by 1 desc 
;
select time_id,sum(bigint(STLMNT_FEE))*1.00/sum(bigint(PAY_STLMNT_FEE)) 
from   bass1.g_s_05002_month 
group by time_id 
order by 1 desc 
;


