//---------------------------------------------------------------------------
#pragma hdrstop
//---------------------------------------------------------------------------
//#pragma argsused
#include <iostream.h>
#include "c_file_trans.h"
void MainFunc();
TCString GetFtpDirection();

int main(int argc, char* argv[])
{
    //Application.MutiProcess(9);
    Application.Initialize("ftp_crm", argc,argv) ;
    Application.SetRunningHandle(MainFunc) ;
    Application.SetRunningDelay(10000) ;
    Application.Run() ;

    return 0;
}
//---------------------------------------------------------------------------


void MainFunc()
{
    TCFileTrans tcFileTrans;
    TCString sDirection = GetFtpDirection();
    if(sDirection == TCString("Get") )
        tcFileTrans.DoGetFile();
    else if(sDirection == TCString("Put") )
        tcFileTrans.DoPutFile();
    else
        throw TCException("Ftp direction is invalid, must be 'Get' or 'Put'.");
}
//---------------------------------------------------------------------------

TCString GetFtpDirection()
{
    static TCString s_sDirection;
    if(s_sDirection == TCString("") )
    {
        s_sDirection = ProfileAppString(Application.GetAppName(), "ftp", "direction", "");

        if(s_sDirection == TCString("") )
            throw TCException("TCFileTrans::GetFtpDirection() Error:"
                "[ftp] direction not set.");
        if(s_sDirection != TCString("Get") && s_sDirection != TCString("Put") )
            throw TCException("Ftp direction is invalid, must be 'Get' or 'Put'.");
    }
    return s_sDirection;
}

#ifdef __TEST__
void Reconnect(TCFtpClient &tcFtp)
{
    TCSystem::DelayMicroSeconds(10000);
    tcFtp.Open();
    tcFtp.ChangeDir("/home/zjjs/unix");
}

void testFtp()
{
    TCStringList slFileList;
    TCFtpClient tcFtp;
    tcFtp.m_sUserID   = "boss3";
    tcFtp.m_sPassword = "KHTSZBD";
    tcFtp.m_sHost     = "135.20.22.220";
    char sBuf[2048];
    TCString sInput;
    try
    {
        tcFtp.Open();
        tcFtp.ChangeDir("/boss/boss3/process/rawbill/gyd5");
        while(sInput != "quit")
        {
            try
            {
                tcFtp.NameList(slFileList, "");
                for(long i=0; i<slFileList.GetCount(); i++)
                    printf("%s", (char *)slFileList[i]);
                cin >> sBuf;
                sInput = sBuf;
            }

            catch(TCFTPException &e)
            {
                printf("%s\n", (char *)e.GetMessage());
                printf("wait 10s to reconnect...");
                Reconnect(tcFtp);
            }

        }
    }
    catch(TCException &e)
    {
        printf("%s\n", (char *)e.GetMessage());
        getchar();
    }
    tcFtp.Close();
}

#endif
