; #########################################################################

    .486                      ; create 32 bit code
    .model flat, stdcall      ; 32 bit memory model
    option casemap :none      ; case sensitive

    include smallwin.inc      ; local includes for this file

    CADD MACRO quoted_text
      LOCAL vname,lbl
        jmp lbl
          vname db quoted_text,0
        lbl:
      EXITM <ADDR vname>
    ENDM

.code

; #########################################################################

start:

      call Main
      invoke ExitProcess,eax

; #########################################################################

Main proc

    LOCAL hWnd     :DWORD
    LOCAL hCursor  :DWORD
    LOCAL hIcon    :DWORD
    LOCAL msg      :MSG
    LOCAL wc       :WNDCLASSEX

    invoke LoadCursor,NULL,IDC_ARROW
    mov hCursor, eax

    invoke LoadIcon,NULL,IDI_ASTERISK
    mov hIcon, eax

    szText szClassName,"smallwin_Class"
    szText szDisplayName,"MASM32 Window"

    mov wc.cbSize,         sizeof WNDCLASSEX
    mov wc.style,          CS_VREDRAW or \
                           CS_HREDRAW
    mov wc.lpfnWndProc,    offset WndProc
    mov wc.cbClsExtra,     NULL
    mov wc.cbWndExtra,     NULL
    mov wc.hInstance,      400000h
    mov wc.hbrBackground,  COLOR_BTNFACE+1
    mov wc.lpszMenuName,   NULL
    mov wc.lpszClassName,  offset szClassName
    m2m wc.hIcon,          hIcon
    m2m wc.hCursor,        hCursor
    m2m wc.hIconSm,        hIcon

    invoke RegisterClassEx, ADDR wc

    invoke CreateWindowEx,WS_EX_LEFT,
                          ADDR szClassName,
                          ADDR szDisplayName,
                          WS_OVERLAPPEDWINDOW,
                          150,150,500,300,
                          NULL,NULL,
                          400000h,NULL
    mov hWnd,eax

  ; ---------------------------
  ; macros for unchanging code
  ; ---------------------------
    DisplayWindow hWnd,SW_SHOWNORMAL

  StartLoop:
    invoke DispatchMessage,  ADDR msg
    invoke GetMessage,ADDR msg,NULL,0,0
    cmp eax, 0
    jne StartLoop

    mov eax, msg.wParam
    ret

Main endp

; #########################################################################

WndProc proc hWin   :DWORD,
             uMsg   :DWORD,
             wParam :DWORD,
             lParam :DWORD

    LOCAL hDC:DWORD
    LOCAL Ps :PAINTSTRUCT
    LOCAL Rct:RECT

    .if uMsg == WM_CLOSE
      invoke MessageBox,hWin,CADD("You wanna quit ?"), ; ADDR dlgmsg,
                        ADDR szDisplayName,
                        MB_YESNO or MB_ICONQUESTION
      cmp eax, IDNO
      jne @F
      ret
    @@:

    .elseif uMsg == WM_DESTROY
      invoke PostQuitMessage,NULL
      return 0

    .elseif uMsg == WM_PAINT
      push esi
      push edi
      push ebx
        lea esi, Rct
        lea ebx, Ps
        invoke BeginPaint,hWin,ebx
        push eax
        pop edi
        invoke GetClientRect,hWin,esi
        invoke DrawText,edi,CADD("MASM32 at 1536 bytes"),-1,esi,
                        DT_VCENTER or DT_CENTER or DT_SINGLELINE
        invoke EndPaint,hWin,ebx
      pop ebx
      pop edi
      pop esi
      return 0
    .endif

    invoke DefWindowProc,hWin,uMsg,wParam,lParam

    ret

WndProc endp

; ########################################################################

end start
