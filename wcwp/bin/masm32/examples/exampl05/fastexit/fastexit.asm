; #########################################################################

      .386
      .model flat, stdcall
      option casemap :none   ; case sensitive

; #########################################################################

      include \masm32\include\windows.inc
      include \masm32\include\user32.inc
      include \masm32\include\kernel32.inc
      include \masm32\include\gdi32.inc
      include \masm32\include\masm32.inc

      includelib \masm32\lib\user32.lib
      includelib \masm32\lib\kernel32.lib
      includelib \masm32\lib\gdi32.lib
      includelib \masm32\lib\masm32.lib

; #########################################################################

        ;=============
        ; Local macros
        ;=============
  
        szText MACRO Name, Text:VARARG
          LOCAL lbl
            jmp lbl
              Name db Text,0
            lbl:
          ENDM
          
        ;=================
        ; Local prototypes
        ;=================
        WndProc PROTO :DWORD,:DWORD,:DWORD,:DWORD
        
    .data
        hButn1      dd 0
        hButn2      dd 0
        hInstance   dd 0
        hIconImage  dd 0
        hIcon       dd 0
        hIwin       dd 0
        dlgname     db "TESTWIN",0

; #########################################################################

    .code

start:

      invoke GetModuleHandle, NULL
      mov hInstance, eax

      ; -------------------------------------------
      ; Call the dialog box stored in resource file
      ; -------------------------------------------
      invoke DialogBoxParam,hInstance,ADDR dlgname,0,ADDR WndProc,0

      invoke ExitProcess,eax

; #########################################################################

WndProc proc hWin   :DWORD,
             uMsg   :DWORD,
             wParam :DWORD,
             lParam :DWORD

      LOCAL Ps :PAINTSTRUCT

      .if uMsg == WM_INITDIALOG

        szText dlgTitle," Fast Shutdown"
        invoke SendMessage,hWin,WM_SETTEXT,0,ADDR dlgTitle

        invoke LoadIcon,hInstance,200
        mov hIcon, eax

        invoke SendMessage,hWin,WM_SETICON,1,hIcon

        invoke GetDlgItem,hWin,IDOK
        mov hButn1, eax

        invoke GetDlgItem,hWin,IDCANCEL
        mov hButn2, eax

        invoke GetDlgItem,hWin,3000
        mov hIwin, eax


        xor eax, eax
        ret

      .elseif uMsg == WM_COMMAND
        .if wParam == IDOK
          invoke ExitWindowsEx,EWX_SHUTDOWN or EWX_FORCE,0

          ; EWX_LOGOFF    equ 0
          ; EWX_SHUTDOWN  equ 1
          ; EWX_REBOOT    equ 2
          ; EWX_FORCE     equ 4
          ; EWX_POWEROFF  equ 8

        .elseif wParam == IDCANCEL
          jmp outa_here
        .endif

      .elseif uMsg == WM_CLOSE
        outa_here:
        invoke EndDialog,hWin,0

      .elseif uMsg == WM_PAINT
        invoke BeginPaint,hWin,ADDR Ps
      ; ----------------------------------------
      ; The following function are in MASM32.LIB
      ; ----------------------------------------
        invoke FrameGrp,hButn1,hButn2,6,1,0
        invoke FrameGrp,hIwin,hIwin,6,1,0
        invoke FrameGrp,hIwin,hIwin,3,1,1
        invoke FrameWindow,hWin,2,1,1
        invoke EndPaint,hWin,ADDR Ps
        xor eax, eax
        ret

      .endif

    xor eax, eax    ; this must be here in NT4
    ret

WndProc endp

; ########################################################################

end start
