;-------------------------------------------------------------------------------
;TDITB v1.2 (04.11.2001) - Copyright (C) 2001 Sami Paju
;
;Transparent Desktop Icon Text Background (TDITB) changes text background
;of desktop icons transparent.
;
;Usage:
;	TDITB	- When run first time, installs hook to monitor color changes
;			  of desktop icon text backgrounds.
;			- When run second time, uninstalls the hook and restores original
;			  color.
;-------------------------------------------------------------------------------

.486
.MODEL		FLAT, STDCALL
option		casemap:none

include         \masm32\include\windows.inc
include         \masm32\include\kernel32.inc
include         \masm32\include\user32.inc
include         dll\TDITBHook.inc
includelib      \masm32\lib\kernel32.lib
includelib      \masm32\lib\user32.lib
includelib      dll\TDITBHook.lib


WinMain                     PROTO :DWORD, :DWORD, :DWORD, :DWORD
WndProc                     PROTO :DWORD, :DWORD, :DWORD, :DWORD
SetDITB                     PROTO
HandleError                 PROTO

.const
lpCopyright				db "TDITB v1.2 (04.11.2001) - Copyright (C) 2001 Sami Paju", 0

.data
lpMsgCaption			db "TDITB v1.1", 0
lpClassName				db "TDITB_CLASS", 0
lpProgmanClassName		db "Progman", 0
lpProgmanWindowName		db "Program Manager", 0
lpDefViewClassName		db "SHELLDLL_DefView", 0
lpSysListClassName		db "SysListView32", 0
MsgLineFeed				db 13, 10, 0
NotWinNT				db "This program only works in NT4 & Win2000 and possibly later operating systems.", 0
MessageStringOn			db "TDITB_On", 0
MessageStringOff		db "TDITB_Off",0

.data?
CommandLine				dd ?
hInstance				dd ?
hSysList				dd ?
nOn						dd ?
nOff					dd ?

.code
Start:
	invoke	FindWindow, ADDR lpClassName, ADDR lpMsgCaption ;Check if TDITB window exist
	.if eax != NULL
		invoke	SendMessage, eax, WM_DESTROY, 0, 0 ;Send DESTROY message to old window
		xor		eax, eax
		jmp		EndStart
	.endif

	invoke	GetModuleHandle, NULL
	mov		hInstance, eax
	invoke	GetCommandLine
	mov		CommandLine, eax
	invoke	WinMain, hInstance, NULL, CommandLine, SW_SHOWDEFAULT

EndStart:
	invoke	ExitProcess, eax
	
WinMain proc hInst:DWORD, hPrevInst:DWORD, CmdLine:DWORD, CmdShow:DWORD
LOCAL wc:WNDCLASSEX
LOCAL msg:MSG
LOCAL hwnd:DWORD
	
	mov		wc.cbSize, SIZEOF WNDCLASSEX
	mov		wc.style, CS_HREDRAW or CS_VREDRAW
	mov		wc.lpfnWndProc, OFFSET WndProc
	mov		wc.cbClsExtra, NULL
	mov		wc.cbWndExtra, NULL
	push	hInstance
	pop		wc.hInstance
	mov		wc.hbrBackground, COLOR_WINDOW + 1
	mov		wc.lpszMenuName, NULL
	mov		wc.lpszClassName, OFFSET lpClassName
	invoke	LoadIcon, NULL, IDI_APPLICATION
	mov		wc.hIcon, eax
	mov		wc.hIconSm, 0
	invoke	LoadCursor, NULL, IDC_ARROW
	mov		wc.hCursor, eax
	invoke	RegisterClassEx, ADDR wc
	
	invoke	CreateWindowEx, NULL, ADDR lpClassName, ADDR lpMsgCaption, WS_OVERLAPPEDWINDOW, 50, 50, 150, 50,\
			NULL, NULL, hInst, NULL
	mov		hwnd, eax
;	invoke	ShowWindow, hwnd, CmdShow ;We don't want to show any window to user
;	invoke	UpdateWindow, hwnd
	
	.while TRUE
		invoke	GetMessage, ADDR msg, NULL, 0, 0
		.break .if (!eax)
		invoke	TranslateMessage, ADDR msg
		invoke	DispatchMessage, ADDR msg
	.endw
	
	mov		eax, msg.wParam
Exitus:
	ret
WinMain endp

WndProc proc uses ebx esi edi hWnd:DWORD, uMsg:DWORD, wParam:DWORD, lParam:DWORD
	.if uMsg==WM_CREATE
		invoke	RegisterWindowMessage, ADDR MessageStringOn ;We register our own messages, so they don't interfere with any other messages
		mov		nOn, eax ;Store our message identifier
		invoke	RegisterWindowMessage, ADDR MessageStringOff
		mov		nOff, eax
		invoke	SetDITB ;Main function to install hook
		xor		eax, eax
	.elseif uMsg==WM_DESTROY
		invoke	SendMessage, hSysList, nOff, 0, 0
		invoke	PostQuitMessage, NULL
		xor	eax, eax
	.else
		invoke	DefWindowProc, hWnd, uMsg, wParam, lParam
	.endif
Exitus:
	ret
WndProc endp

;-------------------------------------------------------------------------------
;SetDITB
;Finds the SysList Window where the icons reside and installs hook.
;-------------------------------------------------------------------------------
SetDITB proc uses ebx esi edi
LOCAL hProgman :DWORD
LOCAL hDefView :DWORD

	invoke	FindWindow, ADDR lpProgmanClassName, ADDR lpProgmanWindowName
	.if eax == NULL
		invoke	HandleError
		jmp		Exitus
 	.endif
	mov		hProgman, eax
	invoke	FindWindowEx, hProgman, NULL, ADDR lpDefViewClassName, NULL
	.if eax == NULL
		invoke	HandleError
		jmp		Exitus
	.endif
	mov		hDefView, eax
	invoke	FindWindowEx, hDefView, NULL, ADDR lpSysListClassName, NULL
	.if eax == NULL
		invoke	HandleError
		jmp		Exitus
	.endif
	mov		hSysList, eax

	invoke	InstallHook, hSysList
	invoke	SendMessage, hSysList, nOn, 0, 0
	invoke	SendMessage, hSysList, LVM_SETTEXTBKCOLOR, 0, CLR_NONE
	invoke	InvalidateRect, hSysList, 0, TRUE
	invoke	UpdateWindow, hSysList

Exitus:
	ret
SetDITB endp

;-------------------------------------------------------------------------------
;HandleError
;Muokataan virhekoodit ihmisen ymm‰rt‰m‰‰n muotoon ja tulostetaan ne.
;-------------------------------------------------------------------------------
HandleError proc uses ebx esi edi
LOCAL lpMsgBuffer :LPVOID
	
	mov cx, SUBLANG_DEFAULT
	shl ecx, 10
	;or  cx, LANG_NEUTRAL		; LANG_NEUTRAL = 0, nothing necessary
	
	push NULL
	push 0
	lea  eax, lpMsgBuffer
	push eax
	push ecx
	invoke GetLastError
	push eax
	push NULL
	mov edx, FORMAT_MESSAGE_ALLOCATE_BUFFER or FORMAT_MESSAGE_FROM_SYSTEM
	push edx
	call FormatMessage
	
;	invoke	StdOut, lpMsgBuffer
	invoke	MessageBox, NULL, lpMsgBuffer, ADDR lpMsgCaption, MB_OK
		
	invoke LocalFree, lpMsgBuffer
	ret
HandleError endp

end Start