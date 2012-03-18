; #########################################################################

    .486
    .model flat, stdcall  ; 32 bit memory model
    option casemap :none  ; case sensitive

    .code

; #########################################################################

GetPercent proc source:DWORD, percent:DWORD

    LOCAL var1:DWORD

    mov var1, 100   ; to divide by 100

    fild source     ; load source integer
    fild var1       ; load 100
    fdiv            ; divide source by 100
    fild percent    ; load required percentage
    fmul            ; multiply 1% by required percentage
    fistp var1      ; store result in variable
    mov eax, var1

    ret

GetPercent endp

; #########################################################################

    end