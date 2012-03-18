; Files Stacker. A simple example of MASM32.
; (c) Copyright 2001 by Kefren. kefren@digitalrice.com
; http://oaqm.cjb.net

.386
.model flat,stdcall
option casemap:none

include \masm32\include\windows.inc
include \masm32\include\user32.inc
include \masm32\include\kernel32.inc
includelib \masm32\lib\user32.lib
includelib \masm32\lib\kernel32.lib
include \masm32\include\comdlg32.inc
includelib \masm32\lib\comdlg32.lib
include \masm32\include\shell32.inc
includelib \masm32\lib\shell32.lib

DlgProc PROTO :DWORD,:DWORD,:DWORD,:DWORD

.data
DlgName db "MyDialog",0
AppName db "Files Stacker",0
AppName2 db "Files Stacker # will not own my brain!",0
Copyright db "Files Stacker - © 2001 Andrea 'Kefren' Brunori",10,13,"kefren@digitalrice.com",0
Filtro db "Text Files",0,"*.txt",0,"All Files",0,"*.*",0,0
Nonce db "File cannot be located.",0
HelpFile db "FSHelp.txt",0

.data?
hInstance HINSTANCE ?
CommandLine LPSTR ?

File1 db 512 dup(?)
File2 db 512 dup(?)
File3 db 512 dup(?)

hFile1 dd ?
hFile2 dd ?
hFile3 dd ?

hMFile1 dd ?
hMFile2 dd ?
hMFile3 dd ?

hMVFile1 dd ?
hMVFile2 dd ?
hMVFile3 dd ?

File1Size dd ?
File2Size dd ?
File3Size dd ?

OpenFile1 OPENFILENAME <>
OpenFile2 OPENFILENAME <>
OpenFile3 OPENFILENAME <>

File1Name db 512 dup(?)
File2Name db 512 dup(?)
File3Name db 512 dup(?)

BytesWritten dd ?

.const
IDC_BROWSE1     equ 32000
IDC_BROWSE2     equ 32001
IDC_BROWSE3     equ 32002
IDC_IN1         equ 3000
IDC_IN2         equ 3001
IDC_OUT         equ 3002
IDC_EXIT        equ 3014
IDM_STACK       equ 1000
IDM_ABOUT       equ 30000
IDM_EXIT        equ 30003
IDM_HELP        equ 30001

.code
start:
	invoke GetModuleHandle, NULL
	mov    hInstance,eax
	invoke DialogBoxParam, hInstance, ADDR DlgName,NULL,addr DlgProc,NULL
	invoke ExitProcess,eax

DlgProc proc hWnd:HWND, uMsg:UINT, wParam:WPARAM, lParam:LPARAM
	.IF uMsg==WM_INITDIALOG
		invoke GetDlgItem, hWnd,IDC_IN1
		invoke SetFocus,eax
	.ELSEIF uMsg==WM_CLOSE
		invoke SendMessage,hWnd,WM_COMMAND,IDM_EXIT,0
	.ELSEIF uMsg==WM_COMMAND
		mov eax,wParam
		.IF lParam==0
			.IF ax==IDM_ABOUT
                        invoke ShellAbout, NULL, ADDR AppName2, ADDR Copyright, NULL 

				
			.ELSEIF ax==IDM_HELP
				invoke ShellExecute, NULL, NULL, ADDR HelpFile, NULL, NULL, SW_SHOWNORMAL
                  .ELSEIF ax==IDM_EXIT
				invoke EndDialog, hWnd,NULL

			.ENDIF
		.ELSE
			mov edx,wParam
			shr edx,16
			.if dx==BN_CLICKED
				.IF ax==IDC_BROWSE1
                                mov OpenFile1.lStructSize, SIZEOF OpenFile1
                                mov OpenFile1.hwndOwner, NULL
                                mov OpenFile1.hInstance, NULL
                                mov OpenFile1.lpstrFilter, OFFSET Filtro

                                mov OpenFile1.nFilterIndex, NULL
                                mov OpenFile1.lpstrFile, OFFSET File1Name
                                mov OpenFile1.nMaxFile, 512
                                invoke GetOpenFileName, ADDR OpenFile1
                                invoke SetDlgItemText, hWnd, IDC_IN1, ADDR File1Name
				.ELSEIF ax==IDC_BROWSE2
                                mov OpenFile2.lStructSize, SIZEOF OpenFile2
                                mov OpenFile2.hwndOwner, NULL
                                mov OpenFile2.hInstance, NULL
                                mov OpenFile2.lpstrFilter, OFFSET Filtro

                                mov OpenFile2.nFilterIndex, NULL
                                mov OpenFile2.lpstrFile, OFFSET File2Name
                                mov OpenFile2.nMaxFile, 512
                                invoke GetOpenFileName, ADDR OpenFile2
                                invoke SetDlgItemText, hWnd, IDC_IN2, ADDR File2Name
				.ELSEIF ax==IDC_BROWSE3
                                mov OpenFile3.lStructSize, SIZEOF OpenFile2
                                mov OpenFile3.hwndOwner, NULL
                                mov OpenFile3.hInstance, NULL
                                mov OpenFile3.lpstrFilter, OFFSET Filtro

                                mov OpenFile3.nFilterIndex, NULL
                                mov OpenFile3.lpstrFile, OFFSET File3Name
                                mov OpenFile3.nMaxFile, 512
                                invoke GetSaveFileName, ADDR OpenFile3
                                invoke SetDlgItemText, hWnd, IDC_OUT, ADDR File3Name
                                
        			.ELSEIF ax==IDM_STACK
	           			  invoke GetDlgItemText,hWnd,IDC_IN1,ADDR File1, 512
			              invoke GetDlgItemText,hWnd,IDC_IN2,ADDR File2, 512
        				  invoke GetDlgItemText,hWnd,IDC_OUT,ADDR File3, 512

                                invoke CreateFile, ADDR File1, GENERIC_READ, NULL, NULL, OPEN_EXISTING, NULL, NULL
                                .if eax==INVALID_HANDLE_VALUE
                                invoke MessageBox, NULL, ADDR File1, ADDR Nonce, MB_ICONERROR
                                jmp Errore
                                .endif
                                mov hFile1,eax
                                invoke GetFileSize, eax, NULL
                                mov File1Size,eax
                                invoke CreateFileMapping, hFile1, NULL, PAGE_READONLY,NULL,NULL,NULL
                                mov hMFile1,eax
                                invoke MapViewOfFile, eax, FILE_MAP_READ, NULL, NULL, NULL
                                mov hMVFile1,eax

                                invoke CreateFile, ADDR File2, GENERIC_READ, NULL, NULL, OPEN_EXISTING, NULL, NULL
                                .if eax==INVALID_HANDLE_VALUE
                                invoke MessageBox, NULL, ADDR File2, ADDR Nonce, MB_ICONERROR
                                jmp Errore
                                .endif
                                mov hFile2,eax
                                invoke GetFileSize, eax, NULL
                                mov File2Size,eax
                                mov edx, File1Size
                                add eax,edx
                                mov File3Size,eax
                                
                                invoke CreateFileMapping, hFile2, NULL, PAGE_READONLY,NULL,NULL,NULL
                                mov hMFile2,eax
                                invoke MapViewOfFile, eax, FILE_MAP_READ, NULL, NULL, NULL
                                mov hMVFile2,eax

                                

                                invoke CreateFile, ADDR File3, GENERIC_READ or GENERIC_WRITE, NULL, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL
                                mov hFile3,eax
                                invoke SetFilePointer, eax, File3Size, NULL, FILE_BEGIN
                                invoke SetEndOfFile, hFile3
                                invoke SetFilePointer, hFile3, 0, NULL, FILE_BEGIN
                                invoke WriteFile, hFile3, hMVFile1, File1Size, ADDR BytesWritten, NULL
                                invoke WriteFile, hFile3, hMVFile2, File2Size, ADDR BytesWritten, NULL
                                
                                
                                invoke UnmapViewOfFile, hMVFile1
                                invoke UnmapViewOfFile, hMVFile2
                                invoke CloseHandle, hMFile1 
                                invoke CloseHandle, hMFile2 
                                mov hMFile1,0
                                mov hMFile2,0
                                invoke CloseHandle, hFile1
                                invoke CloseHandle, hFile2
                                invoke CloseHandle, hFile3

                                Errore:
                        .ELSEIF ax==IDC_EXIT
					invoke SendMessage,hWnd,WM_COMMAND,IDM_EXIT,0
				.ENDIF
			.ENDIF
		.ENDIF
	.ELSE
		mov eax,FALSE
		ret
	.ENDIF
	mov eax,TRUE
	ret
DlgProc endp


end start