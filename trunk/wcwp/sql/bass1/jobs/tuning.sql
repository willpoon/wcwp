db2 RUNSTATS ON table BASS1.G_S_04003_DAY     with distribution and detailed indexes all  
select tabname,card,npages,stats_time from syscat.tables where tabname = 'G_S_04003_DAY'



