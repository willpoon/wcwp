; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

      .486                      ; create 32 bit code
      .model flat, stdcall      ; 32 bit memory model
      option casemap :none      ; case sensitive

      include \masm32\include\dialogs.inc
      include progress.inc

      dlgproc PROTO :DWORD,:DWORD,:DWORD,:DWORD

      PBS_SMOOTH        equ 1
      PBM_SETBKCOLOR    equ 2000h + 1
      PBM_SETBARCOLOR   equ WM_USER + 9

    .code

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

start:

      GLOBAL hButn1     dd ?
      GLOBAL hButn2     dd ?
      GLOBAL hProgress  dd ?
      GLOBAL hInstance  dd ?
      GLOBAL hIcon      dd ?

      invoke InitCommonControls

      mov hInstance, FUNC(GetModuleHandle,NULL)
      mov hIcon,     FUNC(LoadIcon,hInstance,500)

      call main

      invoke ExitProcess,eax

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

main proc

    LOCAL lpArgs:DWORD

    invoke GlobalAlloc,GMEM_FIXED or GMEM_ZEROINIT, 32
    mov lpArgs, eax

    push hIcon
    pop [eax]

    Dialog "Progress Control","MS Sans Serif",10, \         ; caption,font,pointsize
            WS_OVERLAPPED or WS_SYSMENU or DS_CENTER, \     ; style
            5, \                                            ; control count
            50,50,150,80, \                                 ; x y co-ordinates
            1024                                            ; memory buffer size

    DlgIcon     500,10,10,101
    DlgButton   "Run",WS_TABSTOP,112,5,30,11,IDOK
    DlgButton   "Cancel",WS_TABSTOP,112,17,30,11,IDCANCEL
    DlgStatic   'Click "Run" to activate progress control',SS_CENTER,3,35,140,9,100
    DlgProgress PBS_SMOOTH or WS_BORDER,3,50,141,9,102

    CallModalDialog hInstance,0,dlgproc,ADDR lpArgs

    invoke GlobalFree, lpArgs

    ret

main endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

dlgproc proc hWin:DWORD,uMsg:DWORD,wParam:DWORD,lParam:DWORD

    LOCAL cnt:DWORD

    .if uMsg == WM_INITDIALOG
      mov eax, lParam
      mov eax, [eax]
      invoke SendMessage,hWin,WM_SETICON,1,[eax]

      invoke GetDlgItem,hWin,IDOK
      mov hButn1, eax
      invoke GetDlgItem,hWin,IDCANCEL
      mov hButn2, eax
      invoke GetDlgItem,hWin,102
      mov hProgress, eax

      invoke SendMessage,hProgress,PBM_SETRANGE32,0,100
      invoke SendMessage,hProgress,PBM_SETBKCOLOR,0,00000000h  ; black
      invoke SendMessage,hProgress,PBM_SETBARCOLOR,0,00FF0000h ; blue

    .elseif uMsg == WM_COMMAND
      .if wParam == IDOK
        mov cnt, 0
          push esi
        @@:
          invoke GetTickCount
          mov esi, eax
          add esi, 15
        gtc:
          invoke GetTickCount
          cmp eax, esi
          jl gtc
          inc cnt
          invoke SendMessage,hProgress,PBM_SETPOS,cnt,0
          cmp cnt, 100
          jl @B
          pop esi
      .elseif wParam == IDCANCEL
        jmp quit_dialog
      .endif

    .elseif uMsg == WM_CLOSE
      quit_dialog:
      invoke EndDialog,hWin,0

    .endif

    xor eax, eax
    ret

dlgproc endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

end start