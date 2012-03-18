; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

    .486                        ; create 32 bit code
    .model flat, stdcall        ; 32 bit memory model
    option casemap :none        ; case sensitive

    include skin2.inc           ; local includes for this file

    skin_win PROTO :DWORD
    SkinProc PROTO :DWORD,:DWORD,:DWORD,:DWORD

.const
    tbht equ <25>               ; title bar height
    sbht equ <20>               ; status bar height
    vwid equ <5>                ; side border width

.code

start:

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

  ; ------------------
  ; set global values
  ; ------------------
    mov hInstance,   FUNC(GetModuleHandle, NULL)
    mov CommandLine, FUNC(GetCommandLine)
    mov hIcon,       FUNC(LoadIcon,hInstance,500)
    mov hCursor,     FUNC(LoadCursor,NULL,IDC_ARROW)
    mov sWid,        FUNC(GetSystemMetrics,SM_CXSCREEN)
    mov sHgt,        FUNC(GetSystemMetrics,SM_CYSCREEN)
    mov ttlbmp,      FUNC(LoadImage,hInstance,150,IMAGE_BITMAP,sWid,tbht,LR_DEFAULTCOLOR)
    mov sidbmp,      FUNC(LoadImage,hInstance,155,IMAGE_BITMAP,vwid,sHgt,LR_DEFAULTCOLOR)
    mov stabmp,      FUNC(LoadImage,hInstance,160,IMAGE_BITMAP,sWid,sbht,LR_DEFAULTCOLOR)

    call Main

    invoke ExitProcess,eax

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

Main proc

    LOCAL Wwd:DWORD,Wht:DWORD,Wtx:DWORD,Wty:DWORD

    STRING szClassName,"skin2_Class"

  ; --------------------------------------------
  ; register class name for CreateWindowEx call
  ; --------------------------------------------
    invoke CreateSolidBrush,00FF5555h
    invoke RegisterWinClass,ADDR WndProc,
           ADDR szClassName,hIcon,hCursor,eax

  ; -------------------------------------------------
  ; macro to autoscale window co-ordinates to screen
  ; percentages and centre window at those sizes.
  ; -------------------------------------------------
    AutoScale 60, 45

    invoke CreateWindowEx,WS_EX_LEFT,
                          ADDR szClassName,
                          ADDR szDisplayName,
                          WS_POPUP or WS_SIZEBOX or WS_CLIPCHILDREN,
                          Wtx,Wty,Wwd,Wht,
                          NULL,NULL,
                          hInstance,NULL
    mov hWnd,eax

  ; ---------------------------
  ; macros for unchanging code
  ; ---------------------------
    DisplayWindow hWnd,SW_SHOWNORMAL

    call MsgLoop
    ret

Main endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

RegisterWinClass proc lpWndProc:DWORD, lpClassName:DWORD,
                      Icon:DWORD, Cursor:DWORD, bColor:DWORD

    LOCAL wc:WNDCLASSEX

    mov wc.cbSize,         sizeof WNDCLASSEX
    mov wc.style,          CS_BYTEALIGNCLIENT or \
                           CS_BYTEALIGNWINDOW
    m2m wc.lpfnWndProc,    lpWndProc
    mov wc.cbClsExtra,     NULL
    mov wc.cbWndExtra,     NULL
    m2m wc.hInstance,      hInstance
    m2m wc.hbrBackground,  bColor
    mov wc.lpszMenuName,   NULL
    m2m wc.lpszClassName,  lpClassName
    m2m wc.hIcon,          Icon
    m2m wc.hCursor,        Cursor
    m2m wc.hIconSm,        Icon

    invoke RegisterClassEx, ADDR wc

    ret

RegisterWinClass endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

MsgLoop proc

    LOCAL msg:MSG

    push esi
    push edi
    xor edi, edi                        ; clear EDI
    lea esi, msg                        ; Structure address in ESI
    jmp jumpin

    StartLoop:
      invoke TranslateMessage, esi
    ; --------------------------------------
    ; perform any required key processing here
    ; --------------------------------------
      invoke DispatchMessage,  esi
    jumpin:
      invoke GetMessage,esi,edi,edi,edi
      test eax, eax
      jnz StartLoop

    mov eax, msg.wParam
    pop edi
    pop esi

    ret

MsgLoop endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

WndProc proc hWin:DWORD,uMsg:DWORD,wParam:DWORD,lParam:DWORD

    LOCAL hDC       :DWORD
    LOCAL sDC       :DWORD
    LOCAL xDC       :DWORD
    LOCAL oldobj    :DWORD
    LOCAL wwid      :DWORD
    LOCAL Ps        :PAINTSTRUCT
    LOCAL Rct       :RECT
    LOCAL pt        :POINT
    LOCAL buffer1[260]:BYTE  ; these are two spare buffers
    LOCAL buffer2[260]:BYTE  ; for text manipulation etc..

    Switch uMsg
      case WM_SIZE
        invoke GetClientRect,hWin,ADDR Rct

        mov ecx, Rct.bottom
        sub ecx, tbht
        sub ecx, sbht
        push ecx
        invoke MoveWindow,leftwin,0,tbht,vwid,ecx,TRUE

        pop ecx
        mov edx, Rct.right
        sub edx, vwid
        invoke MoveWindow,rigtwin, edx,tbht,vwid,ecx,TRUE

        mov ecx, Rct.bottom
        sub ecx, sbht
        sub Rct.bottom, 200
        invoke MoveWindow,basewin,0,ecx,Rct.right,sbht,TRUE

      case WM_PAINT
        mov hDC, rv(BeginPaint,hWin,ADDR Ps)

        mov sDC, rv(CreateCompatibleDC,hDC)
        mov oldobj, rv(SelectObject,sDC,ttlbmp)
        invoke BitBlt,hDC,0,0,sWid,tbht,sDC,0,0,SRCCOPY     ; draw titlebar
        invoke SelectObject,sDC,oldobj
        invoke DeleteDC,sDC

        invoke EndPaint,hWin,ADDR Ps

      case WM_COMMAND

      case WM_SYSCOLORCHANGE

      case WM_CLOSE

      case WM_NCHITTEST
        movsx eax, WORD PTR [ebp+20]        ; get X-Y coordinates
        mov pt.x, eax
        movsx eax, WORD PTR [ebp+22]
        mov pt.y, eax

        invoke ScreenToClient,hWin,ADDR pt  ; convert to client coordinates

        .if pt.y < tbht                     ; if location within titlebar
          mov eax, HTCAPTION                ; tell system its a titlebar message
          ret
        .endif

      case WM_DESTROY
        invoke PostQuitMessage,NULL
        return 0

      case WM_CREATE
          mov leftwin, rv(skin_win,hWin)
          mov rigtwin, rv(skin_win,hWin)
          mov basewin, rv(skin_win,hWin)

    Endsw

    invoke DefWindowProc,hWin,uMsg,wParam,lParam

    ret

WndProc endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

OPTION PROLOGUE:NONE 
OPTION EPILOGUE:NONE 

TopXY proc wDim:DWORD, sDim:DWORD

    mov eax, [esp+8]
    sub eax, [esp+4]
    shr eax, 1

    ret 8

TopXY endp

OPTION PROLOGUE:PrologueDef 
OPTION EPILOGUE:EpilogueDef 

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

skin_win proc hParent:DWORD

    LOCAL hndl      :DWORD
    LOCAL pclass    :DWORD
    LOCAL wc        :WNDCLASSEX

    sas pclass, "skin_win"

    mov wc.cbSize,         sizeof WNDCLASSEX
    mov wc.style,          CS_BYTEALIGNCLIENT or CS_BYTEALIGNWINDOW
    mov wc.lpfnWndProc,    OFFSET SkinProc
    mov wc.cbClsExtra,     NULL
    mov wc.cbWndExtra,     NULL
    m2m wc.hInstance,      hInstance
    m2m wc.hbrBackground,  NULL
    mov wc.lpszMenuName,   NULL
    m2m wc.lpszClassName,  pclass
    m2m wc.hIcon,          NULL
    m2m wc.hCursor,        hCursor
    m2m wc.hIconSm,        NULL

    invoke RegisterClassEx, ADDR wc

    invoke CreateWindowEx, WS_EX_LEFT,
                           pclass,
                           NULL,
                           WS_CHILD or WS_CLIPSIBLINGS,
                           -1000,-1000,100,100,
                           hParent,NULL,
                           hInstance,NULL
    mov hndl, eax

    DisplayWindow hndl,SW_SHOWNORMAL

    mov eax, hndl

    ret

skin_win endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

SkinProc proc hWin:DWORD,uMsg:DWORD,wParam:DWORD,lParam:DWORD

    LOCAL hDC   :DWORD
    LOCAL sDC   :DWORD
    LOCAL xDC   :DWORD
    LOCAL old   :DWORD
    LOCAL Ps    :PAINTSTRUCT

    switch uMsg
      case WM_PAINT
        mov hDC, rv(BeginPaint,hWin,ADDR Ps)

        switch hWin
          case leftwin
            mov xDC, rv(GetDC,leftwin)
            mov sDC, rv(CreateCompatibleDC,hDC)
            mov old, rv(SelectObject,sDC,sidbmp)
            invoke BitBlt,xDC,0,0,vwid,sHgt,sDC,0,0,SRCCOPY     ; left
            invoke SelectObject,xDC,old
            invoke DeleteDC,sDC
            invoke ReleaseDC,leftwin,xDC

          case rigtwin
            mov xDC, rv(GetDC,rigtwin)
            mov sDC, rv(CreateCompatibleDC,hDC)
            mov old, rv(SelectObject,sDC,sidbmp)
            invoke BitBlt,xDC,0,0,vwid,sHgt,sDC,0,0,SRCCOPY     ; right
            invoke SelectObject,xDC,old
            invoke DeleteDC,sDC
            invoke ReleaseDC,rigtwin,xDC

          case basewin
            mov xDC, rv(GetDC,basewin)
            mov sDC, rv(CreateCompatibleDC,hDC)
            mov old, rv(SelectObject,sDC,stabmp)
            invoke BitBlt,xDC,0,0,sWid,sbht,sDC,0,0,SRCCOPY     ; base
            invoke SelectObject,xDC,old
            invoke DeleteDC,sDC
            invoke ReleaseDC,basewin,xDC
        endsw

        invoke EndPaint,hWin,ADDR Ps
    endsw

    invoke DefWindowProc,hWin,uMsg,wParam,lParam

    ret

SkinProc endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

end start
