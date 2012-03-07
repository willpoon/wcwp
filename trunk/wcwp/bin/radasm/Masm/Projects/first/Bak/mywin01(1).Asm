
;--------------------------------------------------------------------------------
;程序作者：申志远
;开发日期：2010-5-23 14:47
;功能描述：这是我个人的应用程序模板
;注意事项：
;更新历史：
;--------------------------------------------------------------------------------

.586
.model flat,stdcall
option casemap:none

include 		windows.inc
include 		user32.inc
include 		kernel32.inc

includelib 		user32.lib
includelib 		kernel32.lib


WinMain proto :DWORD,:DWORD,:DWORD,:DWORD


.data
   ClassName 	db "MainWinClass",0		; 窗口类名
   AppName  	db "我的第一个应用程序！",0		; 应用程序标题名

.data?
   hInstance HINSTANCE ?				; 应用程序的句柄
   CommandLine LPSTR ?					; 程序的命令行参数字符串指针
   
.const
App								equ 1011

.code
; ---------------------------------------------------------------------------
; 程序入口点
start:
	; 为程序生成一个实例句柄
	invoke GetModuleHandle, NULL
	mov    hInstance,eax
	
	; 为程序获取一个命令行参数
	invoke GetCommandLine
	mov    CommandLine,eax
	
	; 调用Windows的主函数
	invoke WinMain, hInstance,NULL,CommandLine, SW_SHOWDEFAULT
	invoke ExitProcess,eax


WinMain proc hInst:HINSTANCE,hPrevInst:HINSTANCE,CmdLine:LPSTR,CmdShow:DWORD
	LOCAL wc:WNDCLASSEX
	LOCAL msg:MSG
	LOCAL hwnd:HWND
	
	;--------------------------------------------------------------------------------	
	; 填充windows的窗口结构体
	mov   wc.cbSize,SIZEOF WNDCLASSEX			; 表示窗口结构体的总大小
	mov   wc.style, CS_HREDRAW or CS_VREDRAW	; 窗口类型
	mov   wc.lpfnWndProc, OFFSET WndProc		; 传递消息处理函数的指针
	mov   wc.cbClsExtra,NULL					; 窗口类的附加数据
	mov   wc.cbWndExtra,NULL					; 窗口的附加数据
	
	push  hInstance								; 传递程序的实例句柄给窗口类
	pop   wc.hInstance
	
	mov   wc.hbrBackground,COLOR_BTNFACE+1		; 创建窗口时的默认背景颜色
	mov   wc.lpszMenuName,NULL					; 为此窗口建立一个主菜单
	mov   wc.lpszClassName,OFFSET ClassName		; 窗口类的名称
	
	invoke LoadIcon,NULL,App					; 为此窗口建立一个图标
	mov   wc.hIcon,eax
	mov   wc.hIconSm,eax
	
	invoke LoadCursor,NULL,IDC_ARROW			; 指定窗口上的鼠标指针类型
	mov   wc.hCursor,eax
	;--------------------------------------------------------------------------------
	
	; 向Windows注册一个窗口
	invoke RegisterClassEx, addr wc
	
	; 创建一个窗口，然后把窗口句柄赋值给hwnd变量。
	INVOKE CreateWindowEx,NULL,ADDR ClassName,ADDR AppName,\
           WS_OVERLAPPEDWINDOW,CW_USEDEFAULT,\
           CW_USEDEFAULT,CW_USEDEFAULT,CW_USEDEFAULT,NULL,NULL,\
           hInst,NULL
	mov   hwnd,eax
	
	; 显示一个窗口
	invoke ShowWindow, hwnd, SW_SHOWNORMAL
	; 刷新一个窗口
	invoke UpdateWindow, hwnd
	
	; 进入了处理消息的循环
	.WHILE TRUE
		; 获取一条消息
		invoke GetMessage, ADDR msg,NULL,0,0
		.BREAK .IF (!eax)
		
		; 转换消息，分发消息。
		invoke TranslateMessage, ADDR msg
		invoke DispatchMessage, ADDR msg
	.ENDW
	
	mov     eax,msg.wParam
	ret
WinMain endp

WndProc proc hWnd:HWND, uMsg:UINT, wParam:WPARAM, lParam:LPARAM
	
	.IF uMsg == WM_DESTROY
		invoke PostQuitMessage,NULL
		
	.ELSEIF uMsg == WM_CREATE
		invoke  LoadIcon, hInstance, App
		invoke  SendMessage, hWnd, WM_SETICON, ICON_BIG, eax

		
	.ELSE
		invoke DefWindowProc,hWnd,uMsg,wParam,lParam		
		ret
	.ENDIF
	
	xor eax,eax
	ret
WndProc endp


end start
