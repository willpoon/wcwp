;-------------------------------------------------------------------------------
;TDITBHook v1.2 (04.11.2001) - Copyright (C) 2001 Sami Paju
;
; This is the dll used by TDITB.exe
;-------------------------------------------------------------------------------

.486
.model flat,stdcall
option casemap:none

include			\masm32\include\windows.inc
include			\masm32\include\user32.inc
include			\masm32\include\kernel32.inc
include			\masm32\include\masm32.inc
includelib		\masm32\lib\kernel32.lib
includelib		\masm32\lib\user32.lib
includelib		\masm32\lib\masm32.lib

InstallHook				PROTO :DWORD
UninstallHook			PROTO
MyWndProc				PROTO :DWORD, :DWORD, :DWORD, :DWORD
GetMsgProc				PROTO :DWORD, :DWORD, :DWORD

.const
lpCopyright				db "TDITB v1.2 (04.11.2001) - Copyright (C) 2001 Sami Paju", 0

.data
DllName					db "tditbhook.dll", 0
MessageStringOn			db "TDITB_On", 0
MessageStringOff		db "TDITB_Off",0
MsgDestroy				db "Destroy", 0

.data?
hHook					dd ?
hInstance				dd ?
hSysList				dd ?
OldWndProc				dd ?
OldColor				dd ?
nOn						dd ?
nOff					dd ?
bIsInstalled			dd ?

.code
DllEntry proc hInst:HINSTANCE, reason:DWORD, reserved1:DWORD
	.if reason == DLL_PROCESS_DETACH
		.if bIsInstalled == 1
			invoke	SetWindowLong, hSysList, GWL_WNDPROC, OldWndProc
			invoke	UninstallHook
			invoke	SendMessage, hSysList, LVM_SETTEXTBKCOLOR, 0, OldColor
			invoke	InvalidateRect, hSysList, 0, TRUE
			invoke	UpdateWindow, hSysList
		.endif
	.elseif reason == DLL_PROCESS_ATTACH
		mov		eax, hInst
		mov		hInstance, eax
	.endif
	mov		eax, TRUE
	ret
DllEntry Endp

GetMsgProc proc uses ebx esi edi nCode:DWORD, wParam:WPARAM, lParam:LPARAM
	.if (nCode == HC_ACTION)
		mov		ebx, lParam
		mov		eax, CWPSTRUCT.message[ebx]
		.if	eax == nOn
				invoke	SendMessage, hSysList, LVM_GETTEXTBKCOLOR, 0, 0
				mov		OldColor, eax
				invoke	SetWindowLong, CWPSTRUCT.hwnd[ebx], GWL_WNDPROC, ADDR MyWndProc
				mov		OldWndProc, eax
		.elseif eax == nOff
				.if bIsInstalled == 1
					invoke	SetWindowLong, CWPSTRUCT.hwnd[ebx], GWL_WNDPROC, OldWndProc
					invoke	UninstallHook
				.endif
				invoke	SendMessage, hSysList, LVM_SETTEXTBKCOLOR, 0, OldColor
				invoke	InvalidateRect, hSysList, 0, TRUE
				invoke	UpdateWindow, hSysList
		.endif
		invoke	CallNextHookEx, hHook, nCode, wParam, lParam
	.else
		invoke	CallNextHookEx, hHook, nCode, wParam, lParam
	.endif
ExitProc:
	ret
GetMsgProc endp

MyWndProc proc uses ebx esi edi hwnd:DWORD, uMsg:DWORD, wParam:DWORD, lParam:DWORD
	.if uMsg == LVM_SETTEXTBKCOLOR
		mov		eax, lParam
		.if eax != CLR_NONE
			mov		OldColor, eax
			mov		lParam, CLR_NONE
		.endif
	.elseif uMsg == WM_DESTROY
		.if bIsInstalled == 1
			invoke	SetWindowLong, hwnd, GWL_WNDPROC, OldWndProc
			invoke	UninstallHook
		.endif
	.endif
	invoke	CallWindowProc, OldWndProc, hwnd, uMsg, wParam, lParam
ExitProc:
	ret
MyWndProc endp

InstallHook proc hWnd:DWORD
LOCAL DllInst :DWORD
LOCAL Dummy[1024] :BYTE

	mov		hHook, 0

	mov		eax, hWnd
	mov		hSysList, eax

	invoke	GetAppPath, ADDR Dummy
	invoke	szCatStr, ADDR Dummy, ADDR DllName
	
	invoke	GetModuleHandle, ADDR Dummy
	mov		DllInst, eax
	invoke	GetWindowThreadProcessId, hWnd, NULL
	invoke	SetWindowsHookEx, WH_CALLWNDPROC, ADDR GetMsgProc, DllInst, eax
	mov		hHook, eax
	invoke	RegisterWindowMessage, ADDR MessageStringOn
	mov		nOn, eax
	invoke	RegisterWindowMessage, ADDR MessageStringOff
	mov		nOff, eax
	mov		bIsInstalled, 1
	ret 
InstallHook endp

UninstallHook proc
	mov		bIsInstalled, 0
	invoke	UnhookWindowsHookEx, hHook
	ret
UninstallHook endp

end DllEntry
 