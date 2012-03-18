; #########################################################################

; "Thomas Vidal" <Thomas-Vidal@wanadoo.fr>

.586
.model flat,stdcall
option casemap:none

; #########################################################################
include \masm32\include\windows.inc
include \masm32\include\user32.inc
include \masm32\include\kernel32.inc
include \masm32\include\masm32.inc
include \masm32\include\comctl32.inc
include \masm32\include\gdi32.inc
include \masm32\include\advapi32.inc

includelib \masm32\lib\user32.lib
includelib \masm32\lib\kernel32.lib
includelib \masm32\lib\masm32.lib
includelib \masm32\lib\gdi32.lib
includelib \masm32\lib\comctl32.lib
includelib \masm32\lib\advapi32.lib

; #########################################################################
include treeview.asm
include macros.asm
; #########################################################################

WinMain proto :DWORD,:DWORD,:DWORD,:DWORD
ERROR proto :DWORD, :BYTE

; #########################################################################

.data
ClassName db "DLGCLASS",0
DlgName db "MyDialog",0
AppName db "Connect",0
; #########################################################################
;Messages d'erreur:
ErreurTitre	db "ERREUR:",0
Erreur1	db " (Accès refusé)",0
; #########################################################################
IndexNum	dd 0
szRegPath	db      256 dup(?),0
szRegValue	db      256 dup(?),0
szBuff		db 256 dup(0)
PathSize	dd 256
ValueSize	dd 256
ft           FILETIME     <>

szKeyName                     db      "\"  ,0
szStringValueNom              db      "RegisteredOwner",0
DejaLance	db 0
; #########################################################################

.data?
hInstance HINSTANCE ?
CommandLine LPSTR ?
buffer db 512 dup(?)
hDlg dd ?
LaGrandeCle	dd ?
; #########################################################################
hEdit1		dd ?
hEdit2		dd ?
; #########################################################################

.const
IDC_EDIT1			equ 3000
IDC_EDIT2			equ 3001
IDC_OK			equ 3002
IDC_EXIT			equ 3003
IDC_LISTBOX			equ 3004
IDC_lbl1			equ 4001
IDC_lbl2			equ 4002
IDC_lbl3			equ 4003
IDC_lbl4			equ 4004

; #########################################################################

.code
start:
	invoke GetModuleHandle, NULL
	mov    hInstance,eax
	invoke GetCommandLine
	mov CommandLine,eax

	invoke WinMain, hInstance,NULL,CommandLine, SW_SHOWDEFAULT
	invoke ExitProcess,eax

; #########################################################################
WinMain proc hInst:HINSTANCE,hPrevInst:HINSTANCE,CmdLine:LPSTR,CmdShow:DWORD
	LOCAL wc:WNDCLASSEX
	LOCAL msg:MSG

	mov   wc.cbSize,SIZEOF WNDCLASSEX
	mov   wc.style, CS_HREDRAW or CS_VREDRAW
	mov   wc.lpfnWndProc, OFFSET WndProc
	mov   wc.cbClsExtra,NULL
	mov   wc.cbWndExtra,DLGWINDOWEXTRA
	push  hInst
	pop   wc.hInstance
	mov   wc.hbrBackground,COLOR_BTNFACE+1
	mov   wc.lpszMenuName, NULL	;OFFSET MenuName
	mov   wc.lpszClassName,OFFSET ClassName
	invoke LoadIcon,hInstance,200
	mov   wc.hIcon,eax
	mov   wc.hIconSm,eax
	invoke LoadCursor,NULL,IDC_ARROW
	mov   wc.hCursor,eax

	invoke RegisterClassEx, addr wc


	invoke CreateDialogParam,hInstance,ADDR DlgName,NULL,NULL,NULL
	mov   hDlg,eax

	invoke GetDlgItem,hDlg,IDC_OK
	invoke SetFocus,eax

	INVOKE ShowWindow, hDlg,SW_SHOWNORMAL
	INVOKE UpdateWindow, hDlg
	.WHILE TRUE
                INVOKE GetMessage, ADDR msg,NULL,0,0
                .BREAK .IF (!eax)
                invoke IsDialogMessage, hDlg, ADDR msg
                .if eax==FALSE
                        INVOKE TranslateMessage, ADDR msg
                        INVOKE DispatchMessage, ADDR msg
                .endif
	.ENDW
	mov     eax,msg.wParam
	ret
WinMain endp
; #########################################################################
WndProc proc hWnd:HWND, uMsg:UINT, wParam:WPARAM, lParam:LPARAM

	.if uMsg==WM_CREATE
; #########################################################################
		invoke CreateWindowEx,NULL,ADDR TreeViewClass,NULL,\
     	      WS_CHILD+WS_VISIBLE+TVS_HASLINES+TVS_HASBUTTONS+TVS_LINESATROOT+WS_BORDER,50,\
          	 30,200,400,hWnd,NULL,\
	           hInstance,NULL
		mov hwndTreeView,eax
		invoke ImageList_Create,16,16,ILC_COLOR16,2,10
		mov hImageList,eax
		invoke LoadBitmap,hInstance,4006
		mov hBitmap,eax
		invoke ImageList_Add,hImageList,hBitmap,NULL
		invoke DeleteObject,hBitmap
		invoke SendMessage,hwndTreeView,TVM_SETIMAGELIST,0,hImageList
		mov tvinsert.hParent,NULL
		mov tvinsert.hInsertAfter,TVI_ROOT
		mov tvinsert.item.imask,TVIF_TEXT+TVIF_IMAGE+TVIF_SELECTEDIMAGE
		mov tvinsert.item.iImage,0 ;N° image
		mov tvinsert.item.iSelectedImage,1;N° image
; #########################################################################

	.elseif uMsg==WM_SHOWWINDOW

	.ELSEIF uMsg==WM_DESTROY || uMsg==WM_CLOSE
		invoke PostQuitMessage,NULL

	.ELSEIF uMsg==WM_COMMAND
		HIWORD wParam
		.if eax==BN_CLICKED
			LOWORD wParam
			.if eax==IDC_EXIT
				invoke DestroyWindow,hWnd
			.elseif eax==IDC_OK
				call RemplirListe
				mov DejaLance, TRUE
			.endif
		.ENDIF

	.ELSEIF uMsg==WM_SIZE

	.ELSE
		invoke DefWindowProc,hWnd,uMsg,wParam,lParam
		ret
	.ENDIF
	xor    eax,eax
	ret
WndProc endp

; #########################################################################

ERROR proc message:DWORD, quitter:BYTE
invoke MessageBox, hDlg, message, addr ErreurTitre, MB_OK+MB_ICONERROR
.if quitter==1
	invoke ExitProcess,90h
.endif
ret
ERROR endp
; #########################################################################
RemplirListe proc
    LOCAL TType:DWORD
    LOCAL pKey  :DWORD

.if DejaLance == TRUE
	ret
.endif

mov LaGrandeCle, HKEY_CLASSES_ROOT
mov tvinsert.item.pszText,offset HKCR

bouclefirst:
    mov TType, REG_NONE
    invoke RegCreateKeyEx, LaGrandeCle,
                             addr szKeyName, NULL, NULL, 
                             REG_OPTION_NON_VOLATILE, 
                             KEY_ALL_ACCESS, NULL,
                             addr pKey, addr TType
.if eax != ERROR_SUCCESS && LaGrandeCle == HKEY_DYN_DATA
	invoke lstrcpy, addr buffer, addr HKDD
	invoke lstrcat, addr buffer, addr Erreur1
	mov tvinsert.item.pszText,offset buffer
	invoke SendMessage,hwndTreeView,TVM_INSERTITEM,0,addr tvinsert
	ret
.endif

        mov eax, REG_DWORD
        mov TType, eax

		invoke SendMessage,hwndTreeView,TVM_INSERTITEM,0,addr tvinsert
		mov hParent,eax
		mov tvinsert.hParent,eax

boucle:
      INVOKE     RegEnumKeyEx, pKey, IndexNum, addr szRegPath, addr PathSize, 0, 0,0, addr ft;addr szRegValue, addr ValueSize, addr ft
      .if eax == ERROR_NO_MORE_ITEMS
            jmp     NoMore
      .endif

 ;         invoke RegQueryValueEx, pKey, lpszValueName,
 ;                              NULL, ADDR TType, 
 ;                              lpszBuffer, 256 

;      INVOKE     SendDlgItemMessage, hDlg, IDC_LISTBOX, LB_ADDSTRING, 0, addr szRegPath
		mov tvinsert.hInsertAfter,TVI_LAST
		mov tvinsert.item.pszText,offset szRegPath
		invoke SendMessage,hwndTreeView,TVM_INSERTITEM,0,addr tvinsert

            mov     PathSize, 256
            mov     ValueSize, 256
	inc IndexNum
	invoke SetDlgItemInt, hDlg, IDC_lbl1, IndexNum, FALSE
	jmp boucle



NoMore:
	invoke SendMessage,hwndTreeView,TVM_SELECTITEM,TVGN_FIRSTVISIBLE,NULL
	invoke SendMessage,hwndTreeView,TVM_SORTCHILDREN,NULL,hParent
	invoke RegCloseKey, pKey
	mov IndexNum, 0
	mov tvinsert.hParent,NULL

	.if LaGrandeCle == HKEY_CLASSES_ROOT
		mov LaGrandeCle, HKEY_CURRENT_USER
		mov tvinsert.item.pszText,offset HKCU
		jmp bouclefirst
	.elseif LaGrandeCle == HKEY_CURRENT_USER
		mov LaGrandeCle, HKEY_LOCAL_MACHINE
		mov tvinsert.item.pszText,offset HKLM
		jmp bouclefirst
	.elseif LaGrandeCle == HKEY_LOCAL_MACHINE
		mov LaGrandeCle, HKEY_USERS
		mov tvinsert.item.pszText,offset HKU
		jmp bouclefirst
	.elseif LaGrandeCle == HKEY_USERS
		mov LaGrandeCle, HKEY_CURRENT_CONFIG
		mov tvinsert.item.pszText,offset HKCC
		jmp bouclefirst
	.elseif LaGrandeCle == HKEY_CURRENT_CONFIG
		mov LaGrandeCle, HKEY_DYN_DATA
		mov tvinsert.item.pszText,offset HKDD
		jmp bouclefirst
	.endif
ret
RemplirListe endp
; #########################################################################
end start
