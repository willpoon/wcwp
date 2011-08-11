/***
数据空间整理方案：

一、数据空间整理，占用数据空间比较多的接口
    G_S_04002_DAY    保持3+1的数据就行了
    G_S_21003_MONTH  保持3+1的数据就行了
    G_S_21008_MONTH  保持3+1的数据就行了
    
二、定期清理临时中间表
    BASS1_INT_02004_02008_YYYYMM.tcl
    BASS1_INT_22038_YYYYMM.tcl
    BASS1.INT_21007_YYYYMM.tcl
    BASS1_INT_0400810_YYYYMM.tcl
    BASS1_INT_210012916_YYYYMM.tcl
保持当月和上月中间表就行


三、定期清理固定中间表
    G_S_21003_TO_DAY  保持1+1的数据
    G_S_21008_TO_DAY  保持1+1的数据
  
  
  
以上几点能确保已经表空间保持在80%以下。
要注意定期清理，特别是 G_S_21003_TO_DAY 表 一定要注意清理 。
**/


## see  INT_COMMON_ROUTINE_MONTH.tcl