; =======================================
; CUSTOM WINDOWS SHAPE by mob aka drcmda. 
; this piece of code should be very easy
; to understand. the region-data for the
; bmp is made by 'RGN Generator v1.01' 
; try to download it somewhere...
; i never used any gdi function before
; (there are better things to do in win32)
; so it would be unfair not to mention 
; safcon, his gfx-sources helped me alot.
; write to drcmda@gmx.de 
; =======================================

.386
.model          flat, stdcall  
option          casemap :none 

include         \MASM32\INCLUDE\windows.inc
include         \MASM32\INCLUDE\gdi32.inc
include         \MASM32\INCLUDE\user32.inc
include         \MASM32\INCLUDE\kernel32.inc

includelib      \MASM32\LIB\gdi32.lib
includelib      \MASM32\LIB\user32.lib
includelib      \MASM32\LIB\kernel32.lib
      
WinMain         PROTO :DWORD,:DWORD,:DWORD,:DWORD
WndProc         PROTO :DWORD,:DWORD,:DWORD,:DWORD
     
.DATA

ClassName       db "cws_class",0
DisplayName     db "custom windows shape",0

Text            db "good bye...",0              

ButtonClassName db "button",0
ButtonText      db "Click Me!",0

RsrcName        db "RANGE",0
RsrcType        db "RGN",0  

.DATA?

hWnd            dd ?
hInstance       dd ?
RsrcHand        dd ?
RsrcSize        dd ?
RsrcPoint       dd ?
hwndButton      dd ?

.CONST

ButtonID        equ 1000
PictureW        equ 300
PictureH        equ 300

.CODE

start:
        invoke  GetModuleHandle, NULL
        mov     hInstance, eax

        invoke  WinMain,hInstance,NULL,NULL,SW_SHOWDEFAULT
        invoke  ExitProcess,eax

WinMain         PROC hInst :DWORD,hPrevInst :DWORD,CmdLine :DWORD,CmdShow :DWORD

                LOCAL   wc   :WNDCLASSEX
                LOCAL   msg  :MSG

        mov     wc.cbSize,sizeof WNDCLASSEX
        mov     wc.style,CS_HREDRAW or CS_VREDRAW or CS_BYTEALIGNWINDOW
        mov     wc.lpfnWndProc,offset WndProc
        mov     wc.cbClsExtra,NULL
        mov     wc.cbWndExtra,NULL      
        push    hInst
        pop     wc.hInstance      
        
; -->   LOAD BITMAP FROM EXECUTABLE (RESOURCE)
        invoke  LoadBitmap,hInst,1000                   
        
; -->   USE THAT BITMAP AS WINDOW BACKGROUND            
        invoke  CreatePatternBrush,eax
        mov     wc.hbrBackground,eax
        
        mov     wc.lpszMenuName,NULL
        mov     wc.lpszClassName,offset ClassName             
        mov     wc.hIcon,NULL
        invoke  LoadCursor,NULL,IDC_CROSS
        mov     wc.hCursor,eax      
        mov     wc.hIconSm,NULL
        
        invoke  RegisterClassEx, ADDR wc

; -->   CALCULATE THE MIDDLE OF THE SCREEN FOR OUR WINDOW
;       1. get width of the screen
;       2. divide it
;       3. get the width of our BG-Picture
;       4. divide it
;       5. horizontal middle = value of step 2 - value of step 4
;       6. ... do the same with screen/picture height ...
        invoke  GetSystemMetrics,SM_CXSCREEN
        shr     eax,1
        sub     eax,PictureW/2
        push    eax

        invoke  GetSystemMetrics,SM_CYSCREEN
        shr     eax,1
        sub     eax,PictureH/2
        pop     ebx

; -->   CREATE OUR WINDOW (WITH POPUP STYLE!)
        invoke  CreateWindowEx,WS_EX_LEFT,ADDR ClassName,ADDR DisplayName,
                WS_POPUP,ebx,eax,PictureW,PictureH,NULL,NULL,hInst,NULL
        mov     hWnd,eax
        
        invoke  ShowWindow,eax,SW_SHOWNORMAL
        invoke  UpdateWindow,hWnd

_Start:
        invoke  GetMessage,ADDR msg,NULL,0,0
        test    eax, eax
        jz      _Exit
        invoke  TranslateMessage,ADDR msg
        invoke  DispatchMessage,ADDR msg
        jmp     _Start
_Exit:

        mov     eax,msg.wParam
        ret

WinMain         ENDP

WndProc         PROC hWin :DWORD,uMsg :DWORD,wParam :DWORD,lParam :DWORD

.IF     uMsg == WM_CREATE

; -->   LOAD REGION_DATA (SEE API REF FOR QUESTIONS)
        invoke  FindResource,hInstance,addr RsrcName,addr RsrcType
        mov     RsrcHand, eax

        invoke  LoadResource,hInstance,eax
        mov     RsrcPoint, eax

        invoke  SizeofResource,hInstance,RsrcHand
        mov     RsrcSize, eax

        invoke  LockResource,RsrcPoint
        mov     RsrcPoint, eax          

; -->   CREATE REGION AND PASS IT TO OUR WINDOW 
        invoke  ExtCreateRegion,NULL,RsrcSize,eax
        invoke  SetWindowRgn,hWin,eax,TRUE

; -->   CREATE A SIMPLE BUTTON
        invoke  CreateWindowEx,NULL, ADDR ButtonClassName,ADDR ButtonText,\
                WS_CHILD or WS_VISIBLE or BS_DEFPUSHBUTTON,\
                150,185,100,25,hWin,ButtonID,hInstance,NULL
        mov     hwndButton,eax

.ELSEIF uMsg == WM_COMMAND   
        mov     eax,wParam

.IF     ax ==   ButtonID
        invoke  MessageBox,hWin,addr Text,addr DisplayName,MB_OK
        invoke  SendMessage,hWin,WM_DESTROY,NULL,NULL

.ENDIF
                                          
.ELSEIF uMsg == WM_LBUTTONDOWN

; -->   MAKE OUR WINDOW THINK THAT THE USER MOVES THE CAPTION_BAR
        invoke  SendMessage,hWin,WM_NCLBUTTONDOWN,HTCAPTION,lParam
        
.ELSEIF uMsg == WM_DESTROY
        invoke  PostQuitMessage,NULL
        xor     eax,eax
        ret

.ENDIF

        invoke  DefWindowProc,hWin,uMsg,wParam,lParam
        ret

WndProc         ENDP

END             start