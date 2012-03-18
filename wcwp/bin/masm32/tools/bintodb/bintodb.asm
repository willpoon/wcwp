; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

      .486                      ; create 32 bit code
      .model flat, stdcall      ; 32 bit memory model
      option casemap :none      ; case sensitive

      include bintodb.inc       ; local includes for this file

.code

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

start:

      invoke InitCommonControls

    ; ------------------
    ; set global values
    ; ------------------
      mov hInstance,   FUNC(GetModuleHandle,NULL)
      mov CommandLine, FUNC(GetCommandLine)
      mov hIcon,       FUNC(LoadIcon,hInstance,500)     ; icon ID
      mov hCursor,     FUNC(LoadCursor,NULL,IDC_ARROW)
      mov sWid,        FUNC(GetSystemMetrics,SM_CXSCREEN)
      mov sHgt,        FUNC(GetSystemMetrics,SM_CYSCREEN)

      call Main

      invoke ExitProcess,eax

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

Main proc

    LOCAL Wwd:DWORD,Wht:DWORD,Wtx:DWORD,Wty:DWORD

    STRING szClassName,"Bin_To_DB_Class"

    invoke RegisterWinClass,ADDR WndProc,ADDR szClassName,
                            hIcon,hCursor,0
    AutoScale 75, 70

    invoke CreateWindowEx,WS_EX_LEFT or WS_EX_ACCEPTFILES,
                          ADDR szClassName,
                          ADDR szDisplayName,
                          WS_OVERLAPPEDWINDOW,
                          Wtx,Wty,Wwd,Wht,
                          NULL,NULL,
                          hInstance,NULL
    mov hWnd,eax
    DisplayMenu hWnd,600
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

  @@:
    invoke TranslateMessage, ADDR msg
    invoke DispatchMessage,  ADDR msg
    invoke GetMessage,ADDR msg,NULL,0,0
    cmp eax, 0
    jne @B

    mov eax, msg.wParam
    ret

MsgLoop endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

WndProc proc hWin:DWORD,uMsg:DWORD,wParam:DWORD,lParam:DWORD

    LOCAL var    :DWORD
    LOCAL caW    :DWORD
    LOCAL caH    :DWORD
    LOCAL tbh    :DWORD
    LOCAL sbh    :DWORD
    LOCAL fname  :DWORD
    LOCAL patn   :DWORD
    LOCAL Rct    :RECT

    Switch uMsg
    ;======== toolbar commands ========
      Case WM_COMMAND
        Switch wParam
          Case 50
            jmp clear_edit

          Case 51
            jmp open_file

          Case 52
            jmp save_file

          Case 53
            jmp copy_all

          Case 54
            jmp quit_prog

    ;======== menu commands ========

        Case 1000
          clear_edit:
          invoke SetWindowText,hEdit,0
          fn SendMessage,hStatus,SB_SETTEXT,3,SADD(0)

        Case 1001
          open_file:
          sas patn, "All files",0,"*.*",0
          mov fname, OpenFileDlg(hWin,hInstance,"Open File",patn)
          cmp BYTE PTR [eax], 0
          jne @F
          return 0
          @@:
          invoke loaddump,fname,hEdit

        Case 1002
          save_file:
          sas patn, "asm files",0,"*.asm",0, \
                    "inc files",0,"*.inc",0
          mov fname, SaveFileDlg(hWin,hInstance,"Save File As ....",patn)
          cmp BYTE PTR [eax], 0
          jne @F
          return 0
          @@:
          invoke Write_To_Disk,hEdit,fname

        Case 1010
          quit_prog:
          invoke SendMessage,hWin,WM_SYSCOMMAND,SC_CLOSE,NULL

        Case 1100
          copy_all:
          invoke Select_All,hEdit
          invoke SendMessage,hEdit,WM_COPY,0,0

        Case 1900
        .data
          copyrt db "Copyright й 1998-2003 The MASM32 Project",13,10
                 db "A 32 bit Microsoft Assembler (MASM) Tool",13,10
                 db "All Rights Reserved",0
          align 4
        .code

          fn AboutBox,hWnd,hInstance,hIcon, \
                      "Programming tool for MASM32", \
                      "Binary File To Assembler DB Format Converter", \
                      ADDR copyrt
      Endsw

    ;====== end menu commands ======

    Case WM_SETFOCUS
      invoke SetFocus,hEdit

    Case WM_DROPFILES
      invoke loaddump,DropFileName(wParam),hEdit

    Case WM_NOTIFY
    ; ---------------------------------------------------
    ; The toolbar has the TBSTYLE_TOOLTIPS style enabled
    ; so that a WM_NOTIFY message is sent when the mouse
    ; is over the toolbar buttons.
    ; ---------------------------------------------------
      mov eax, lParam
      mov ecx, [eax+4]    ; get the idFrom member
      mov eax, [eax]      ; get the hwndFrom member

      Switch eax
        Case hToolTips
          Switch ecx
            Case 50
              fn SendMessage,hStatus,SB_SETTEXT,0,"Clear Editor"
            Case 51
              fn SendMessage,hStatus,SB_SETTEXT,0,"Open Binary File"
            Case 52
              fn SendMessage,hStatus,SB_SETTEXT,0,"Save Assembler DB file"
            Case 53
              fn SendMessage,hStatus,SB_SETTEXT,0,"Copy To Clipboard"
            Case 54
              fn SendMessage,hStatus,SB_SETTEXT,0,"Exit program"
          Endsw
      Endsw

    Case WM_CREATE
      fn LoadLibrary,"RICHED32.DLL"
      invoke RichEd1,0,0,100,100,hWin,hInstance,175,0
      mov hEdit, eax
      invoke SendMessage,hEdit,WM_SETFONT,
                         FUNC(GetStockObject,SYSTEM_FIXED_FONT),0
      invoke SendMessage,hEdit,EM_EXLIMITTEXT,0,100000000
      invoke SendMessage,hEdit,EM_SETOPTIONS,ECOOP_XOR,ECO_SELECTIONBAR

      invoke Do_ToolBar,hWin
      invoke Do_Status,hWin

    Case WM_SYSCOLORCHANGE
      invoke Do_ToolBar,hWin

    Case WM_SIZE
      invoke SendMessage,hToolBar,TB_AUTOSIZE,0,0
      invoke MoveWindow,hStatus,0,0,0,0,TRUE

      invoke GetClientRect,hToolBar,ADDR Rct
      mov eax, Rct.bottom
      mov tbh, eax
      add tbh, 2

      invoke GetClientRect,hStatus,ADDR Rct
      mov eax, Rct.bottom
      mov sbh, eax

      invoke GetClientRect,hWin,ADDR Rct
      mov eax, tbh
      sub Rct.bottom, eax
      mov eax, sbh
      sub Rct.bottom, eax

      invoke MoveWindow,hEdit,0,tbh,Rct.right,Rct.bottom,TRUE

      Case WM_CLOSE

      Case WM_DESTROY
        invoke PostQuitMessage,NULL
        return 0
    Endsw

    invoke DefWindowProc,hWin,uMsg,wParam,lParam

    ret

WndProc endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

TopXY proc wDim:DWORD, sDim:DWORD

    shr sDim, 1      ; divide screen dimension by 2
    shr wDim, 1      ; divide window dimension by 2
    mov eax, wDim    ; copy window dimension into eax
    sub sDim, eax    ; sub half win dimension from half screen dimension

    return sDim

TopXY endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

Select_All Proc Edit:DWORD

    LOCAL tl :DWORD
    LOCAL Cr :CHARRANGE

    mov Cr.cpMin,0
    invoke GetWindowTextLength,Edit
    add eax, 1
    mov Cr.cpMax, eax
    invoke SendMessage,Edit,EM_EXSETSEL,0,ADDR Cr

    ret

Select_All endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

loaddump proc fname:DWORD, Edit:DWORD

    LOCAL pMem  :DWORD
    LOCAL flen  :DWORD
    LOCAL hBuf  :DWORD
    LOCAL str1  :DWORD
    LOCAL blen  :DWORD
    LOCAL buffer1[260]:BYTE

    mov pMem, InputFile(fname)        ; read file from disk into buffer
    mov flen, ecx                     ; get its length

    lea ecx, [ecx+ecx*4]              ; mul by 5
    mov hBuf, alloc$(ecx)             ; allocate string memory

    invoke AsciiDump,pMem,hBuf,flen   ; dump its content in DB format
    invoke compact,hBuf               ; compact spaces in buffer

    mov str1, ptr$(buffer1)
    mov str1, cat$(str1,"; ",fname," is ",ustr$(flen)," bytes long",SADD(13,10))

    invoke SetWindowText,Edit,hBuf    ; write buffer to editor
    invoke SendMessage,Edit,EM_REPLACESEL,FALSE,str1 ; write the lead string

  ; -----------------------------
  ; write the status bar message
  ; -----------------------------
    mov blen, len(hBuf)
    mov str1, ptr$(buffer1)
    mov str1, cat$(str1,ustr$(blen)," bytes loaded in editor")
    invoke SendMessage,hStatus,SB_SETTEXT,3,str1

  ; ----------------
  ; free the memory
  ; ----------------
    free$ hBuf                        ; free the string memory
    free pMem                         ; free the source file memory

    ret

loaddump endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

compact proc mem:DWORD

comment * ----------------------------------------
    remove all spaces except after the "db" string
    -------------------------------------------- *

    mov ecx, mem
    sub ecx, 1
    mov edx, ecx

  @@:
    add ecx, 1
    mov al, [ecx]
    cmp BYTE PTR [ecx-1], "b"   ; only allow space after "db"
    je wrt
    cmp al, 32
    je @B
  wrt:
    add edx, 1
    mov [edx], al
    test al, al
    jne @B

    ret

compact endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

end start
