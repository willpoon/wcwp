//---------------------------------------------------------------------------
#pragma hdrstop                                 

#include "cmpublic.h"                                   
#include "c_thread_pool_run.h"                         
//---------------------------------------------------------------------------
void MainFunc();                                    
void ReleaseApplication();

#pragma argsused                                                      
int main(int argc, char* argv[])                             
{                                           
    Application.Initialize("boss_crm_interface", argc,argv);
    Application.SetRunningHandle(MainFunc);
    Application.SetRunningDelay(2000);
    Application.Run();                                         

    return 0;                                               
}                                                           
//---------------------------------------------------------------------------
void MainFunc()
{
    GetThreadPoolRun().Run();
}                                                   

void ReleaseApplication()
{                                                
//
}  