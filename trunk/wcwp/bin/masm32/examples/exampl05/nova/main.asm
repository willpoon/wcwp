; *************************************************************************
;
; The comments are lacking i know...but hey, this aint for -complete- newbies :]
; q's or whines... '/server *efnet*' & '/msg novatrix moo'
; ta for the help, those ppl know who they are.
;
; *************************************************************************

.386
.model flat, stdcall
option casemap:none

; *************************************************************************
;       include files

        include \masm32\include\windows.inc
        include \masm32\include\masm32.inc
        include \masm32\include\gdi32.inc
        include \masm32\include\user32.inc
        include \masm32\include\kernel32.inc
        include \masm32\include\comctl32.inc
        include \masm32\include\comdlg32.inc
        include \masm32\include\shell32.inc

;       libraries

        includelib \masm32\lib\masm32.lib
        includelib \masm32\lib\gdi32.lib
        includelib \masm32\lib\user32.lib
        includelib \masm32\lib\kernel32.lib
        includelib \masm32\lib\comctl32.lib
        includelib \masm32\lib\comdlg32.lib
        includelib \masm32\lib\shell32.lib

; *************************************************************************

        DlgProc     PROTO :DWORD, :DWORD, :DWORD, :DWORD    ;proc for our window
        BtnDlgProc  PROTO :DWORD, :DWORD, :DWORD, :DWORD    ;proc for our "image button"

    .data
        szDlgname   db      "IDD_MAIN",0
        szBtnDlg    db      "IDD_BUTTONDLG",0
        szTest      db      "The Button was clicked!",0

    .data?
        hInstance   dd ?
        hMain       dd ?
        hBtnUp      dd ?
        hBtnDown    dd ?
        bBtnDown    dd ?
        MouseY      dd ?
        MouseX      dd ?
        bClicked    dd ?
        
    .const
        ID_BTNUP    equ 3300
        ID_BTNDOWN  equ 3301
        IMG_CLICKED equ 4400
        
; *************************************************************************

    .code

        start:

    invoke GetModuleHandle, NULL
    mov hInstance, eax
    invoke DialogBoxParam, hInstance, addr szDlgname, NULL, addr DlgProc, NULL
    invoke ExitProcess, eax

; *************************************************************************
    DlgProc proc hWnd:HWND, uMsg:UINT, wParam:WPARAM, lParam:LPARAM

    .if uMsg == WM_INITDIALOG
        invoke CreateDialogParam, hInstance, addr szBtnDlg, hWnd, addr BtnDlgProc, NULL
        mov eax, hWnd       ;<\ 
        mov hMain, eax      ;<- i do this so i can axx the parent handle from outside this proc

    .elseif uMsg == WM_COMMAND
        .if wParam == IMG_CLICKED
            invoke MessageBox, hWnd, addr szTest, addr szTest,MB_OK
            invoke SendMessage, hWnd, WM_CLOSE,0,0
        .endif
        
    .elseif uMsg == WM_CLOSE
        invoke EndDialog, hWnd, NULL

    .else
        mov eax, FALSE
        ret

    .endif

    mov eax, TRUE
    ret

    DlgProc endp

; *************************************************************************
    BtnDlgProc proc hWin:HWND, uMsg:UINT, wParam:WPARAM, lParam:LPARAM

LOCAL PS        :PAINTSTRUCT
LOCAL Rect      :RECT
LOCAL hDC       :DWORD
LOCAL hMemDC    :DWORD

    .if uMsg == WM_INITDIALOG
        invoke LoadBitmap, hInstance, ID_BTNUP
        mov hBtnUp, eax
        invoke LoadBitmap, hInstance, ID_BTNDOWN
        mov hBtnDown, eax
        mov bBtnDown, FALSE

    .elseif uMsg == WM_LBUTTONDOWN
        mov bClicked, TRUE
        invoke SetCapture, hWin         ; we must unpack and store the mouse co-ords here so if the user
        mov bBtnDown, TRUE              ; doesnt move the mouse, and releases, we'll still have valid values
        mov eax,lParam                  ; to perform the little hit test with.
        shr eax,16    
        mov MouseY, eax
        mov eax, lParam
        and eax, 0FFFFh
        mov MouseX, eax
        invoke InvalidateRect, hWin, NULL, FALSE    ; <- look these 2 up in win32.hlp if u dont understand
        invoke UpdateWindow, hWin                   ; <- the reason for their use.

    .elseif uMsg == WM_LBUTTONUP
        mov bClicked, FALSE
        mov bBtnDown, FALSE
        invoke InvalidateRect, hWin, NULL, FALSE
        invoke UpdateWindow, hWin
        invoke GetClientRect, hWin, addr Rect
        mov eax, MouseY                         ; basically all we are doing here is grabbing the window rect
        cmp eax, Rect.top                       ; and testing to see if the mouse is outside it.
        jle outhere                             ; as per normal button action, if the mouse is not over
        mov eax, MouseX                         ; the button when the mouse is released, it doesnt get
        cmp eax, Rect.right                     ; "activated" so to speak.
        jge outhere
        mov eax, MouseY
        cmp eax, Rect.bottom
        jge outhere
        mov eax, MouseX
        cmp eax, Rect.left
        jle outhere
            invoke SendMessage, hMain, WM_COMMAND,IMG_CLICKED,0     ; this tells the main window proc \
        outhere:                                                    ; our button was clicked
        invoke ReleaseCapture

    .elseif uMsg == WM_MOUSEMOVE
        .if bClicked == TRUE
                        ;***    heres a little set of instructions that i scored off TTom ages back
                        ; and i've never had to rack my brain again ;]...
                        ; so for those who want to understand 'em
            mov eax,lParam  ; lParam has mouse XY in packed form
            shr eax,16      ; shift hi word to lo
            mov MouseY, eax ; save Y
            mov eax, lParam ; restore packed XY
            and eax, 0FFFFh ; mask lo word
            mov MouseX, eax ; save X
                        ;***
            invoke GetClientRect, hWin, addr Rect
            mov eax, MouseY
            cmp eax, Rect.top                             ; if we have mouse movement
            jle notinside
            mov eax, MouseX
            cmp eax, Rect.right                           ; and the mouse is still inside our buttons
            jge notinside                                 ; "border" ....
            mov eax, MouseY
            cmp eax, Rect.bottom
            jge notinside
            mov eax, MouseX
            cmp eax, Rect.left
            jle notinside
            mov bBtnDown, TRUE                          ; <- set the "button down" picture
            invoke InvalidateRect, hWin, NULL, FALSE
            invoke UpdateWindow, hWin
            jmp endit
            
            notinside:                                  ; <- if the mouse is outside and moving
            mov bBtnDown, FALSE                         ; <- set the "button up" picture
            invoke InvalidateRect, hWin, NULL, FALSE
            invoke UpdateWindow, hWin
            endit:
        .endif
    
    .elseif uMsg == WM_PAINT
        invoke BeginPaint, hWin, addr PS
        mov hDC, eax
        invoke CreateCompatibleDC, NULL
        mov hMemDC, eax
        .if bBtnDown == TRUE
            invoke SelectObject, hMemDC, hBtnDown
        .else
            invoke SelectObject, hMemDC, hBtnUp
        .endif       
        invoke GetClientRect, hWin, addr Rect    
        invoke BitBlt, hDC,0,0, Rect.right, Rect.bottom, hMemDC,0,0, SRCCOPY
        invoke DeleteDC, hMemDC
        invoke EndPaint, hWin,addr PS

    .elseif uMsg == WM_CLOSE
        invoke EndDialog, hWin, NULL

    .else
        mov eax, FALSE
        ret

    .endif

    mov eax, TRUE
    ret

    BtnDlgProc endp
; *************************************************************************


    end start
