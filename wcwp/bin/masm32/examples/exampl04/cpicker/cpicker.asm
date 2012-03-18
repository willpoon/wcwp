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
    include \masm32\include\shell32.inc
    include \masm32\include\comdlg32.inc

    includelib \masm32\lib\user32.lib
    includelib \masm32\lib\kernel32.lib
    includelib \masm32\lib\gdi32.lib
    includelib \masm32\lib\masm32.lib
    includelib \masm32\lib\shell32.lib
    includelib \masm32\lib\comdlg32.lib

    clearbuffer PROTO :DWORD

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
        hButn3      dd 0
        hButn4      dd 0
        hButn5      dd 0
        hButn6      dd 0

        hEdit1      dd 0
        hEdit2      dd 0
        hEdit3      dd 0

        hInstance   dd 0
        hIconImage  dd 0
        hIcon       dd 0
        dlgname     db "COLORREF Color Picker",0

; #########################################################################

    .code

start:

      invoke GetModuleHandle, NULL
      mov hInstance, eax
      
      ; -------------------------------------------
      ; Call the dialog box stored in resource file
      ; -------------------------------------------
      invoke DialogBoxParam,hInstance,100,0,ADDR WndProc,0

      invoke ExitProcess,eax

; #########################################################################

WndProc proc hWin   :DWORD,
             uMsg   :DWORD,
             wParam :DWORD,
             lParam :DWORD

      LOCAL color :DWORD
      LOCAL Ps :PAINTSTRUCT
      LOCAL buffer1[128]:BYTE
      LOCAL buffer2[128]:BYTE

      .if uMsg == WM_INITDIALOG
      
        szText dlgTitle," COLORREF Color Picker"
        invoke SendMessage,hWin,WM_SETTEXT,0,ADDR dlgTitle

        invoke LoadIcon,hInstance,1
        mov hIcon, eax

        invoke SendMessage,hWin,WM_SETICON,1,hIcon

        invoke GetDlgItem,hWin,104
        mov hEdit1, eax

        invoke GetDlgItem,hWin,105
        mov hEdit2, eax

        invoke GetDlgItem,hWin,106
        mov hEdit3, eax

        invoke GetDlgItem,hWin,101  ; save asm
        mov hButn1, eax

        invoke GetDlgItem,hWin,102  ; save basic
        mov hButn2, eax

        invoke GetDlgItem,hWin,103  ; save C/C++
        mov hButn3, eax

        invoke GetDlgItem,hWin,111  ; pick colour
        mov hButn4, eax

        invoke GetDlgItem,hWin,112  ; about
        mov hButn5, eax

        invoke GetDlgItem,hWin,113  ; exit
        mov hButn6, eax

        xor eax, eax
        ret

      .elseif uMsg == WM_COMMAND
        .if wParam == 111
          invoke ColorDialog,hWin,hInstance,0   ; CC_FULLOPEN
          mov color, eax

          szText szh,"h"
          szText baz,"&H"
          szText cpp,"0x"

          invoke clearbuffer, ADDR buffer1
          invoke clearbuffer, ADDR buffer2

          invoke dw2hex, color, ADDR buffer2
          invoke szCatStr,ADDR buffer1,ADDR buffer2
          invoke szCatStr,ADDR buffer1,ADDR szh
          invoke SetWindowText,hEdit1,ADDR buffer1

          invoke clearbuffer, ADDR buffer1
          invoke szCatStr,ADDR buffer1,ADDR baz
          invoke szCatStr,ADDR buffer1,ADDR buffer2
          invoke SetWindowText,hEdit2,ADDR buffer1

          invoke clearbuffer, ADDR buffer1
          invoke szCatStr,ADDR buffer1,ADDR cpp
          invoke szCatStr,ADDR buffer1,ADDR buffer2
          invoke SetWindowText,hEdit3,ADDR buffer1

        .elseif wParam == 112
          szText AboutMsg,"COLORREF Color Picker",13,10,\
          "Copyright © MASM32 2000"
          invoke ShellAbout,hWin,ADDR dlgTitle,ADDR AboutMsg,hIcon

        .elseif wParam == 113
          jmp Outa_Here

        .elseif wParam == 101
          invoke SendMessage,hEdit1,EM_SETSEL,0,-1
          invoke SendMessage,hEdit1,WM_COPY,0,0
          invoke SendMessage,hEdit1,EM_SETSEL,-1,0

        .elseif wParam == 102
          invoke SendMessage,hEdit2,EM_SETSEL,0,-1
          invoke SendMessage,hEdit2,WM_COPY,0,0
          invoke SendMessage,hEdit2,EM_SETSEL,-1,0

        .elseif wParam == 103
          invoke SendMessage,hEdit3,EM_SETSEL,0,-1
          invoke SendMessage,hEdit3,WM_COPY,0,0
          invoke SendMessage,hEdit3,EM_SETSEL,-1,0

        .endif

      .elseif uMsg == WM_CLOSE
        Outa_Here:
        invoke EndDialog,hWin,0

      .elseif uMsg == WM_PAINT
        invoke BeginPaint,hWin,ADDR Ps
      ; ----------------------------------------
      ; The following function are in MASM32.LIB
      ; ----------------------------------------
        invoke FrameGrp,hButn4,hButn6,2,1,0
        invoke FrameGrp,hButn4,hButn6,4,1,1
        invoke FrameWindow,hWin,2,1,1
        invoke FrameWindow,hWin,4,1,0

        invoke EndPaint,hWin,ADDR Ps
        xor eax, eax
        ret

      .endif

    xor eax, eax    ; this must be here in NT4
    ret

WndProc endp

; ########################################################################

clearbuffer proc lpbuffer:DWORD

    mov ecx, 32
    mov eax, 0
    mov edi, lpbuffer
    rep stosb

    ret

clearbuffer endp

; ########################################################################

end start
