; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

    .486                      ; create 32 bit code
    .model flat, stdcall      ; 32 bit memory model
    option casemap :none      ; case sensitive

    .code

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

OPTION PROLOGUE:NONE 
OPTION EPILOGUE:NONE 

align 16

BinSearch proc startpos:DWORD,lpSource:DWORD,sLen:DWORD,
                              lpPattern:DWORD,pLen:DWORD
    push ebx
    push esi
    push edi
    push ebp

  ; ----------------
  ; setup loop code
  ; ----------------
    mov esi, [esp+8+16]     ; lpSource
    mov edi, [esp+16+16]    ; lpPattern
    mov al, [edi]           ; get 1st char in pattern

    mov ecx, [esp+12+16]    ; sLen
    sub ecx, [esp+20+16]    ; pLen           ; sub pattern len to avoid searching past end of src
    add ecx, 1
    add esi, ecx            ; add source length
    neg ecx                 ; invert sign
    add ecx, [esp+4+16]     ; startpos       ; add starting offset
    sub DWORD PTR [esp+20+16], 1
    jmp Scan_Loop

  ; ----------------------------------------

  align 4
  Pre_Match:
    lea ebp, [esi+ecx]      ; put current scan address in EBP
    mov edx, [esp+20+16]    ; put pattern length into EDX

  Test_Match:
  REPEAT 7
    movzx ebx, BYTE PTR [ebp+edx-1] ; load last byte of pattern length in main string
    cmp bl, [edi+edx-1]     ; compare it with last byte in pattern
    jne Pre_Scan            ; exit loop on mismatch
    sub edx, 1
    jz Match
  ENDM

    movzx ebx, BYTE PTR [ebp+edx-1] ; load last byte of pattern length in main string
    cmp bl, [edi+edx-1]     ; compare it with last byte in pattern
    jne Pre_Scan            ; exit loop on mismatch
    sub edx, 1
    jnz Test_Match

    jmp Match

  align 4
  Pre_Scan:
    add ecx, 1              ; start on next byte

  Scan_Loop:
  REPEAT 7
    cmp al, [esi+ecx]       ; scan for 1st byte of pattern
    je Pre_Match            ; test if it matches
    add ecx, 1
    jns No_Match            ; exit on sign inversion
  ENDM

    cmp al, [esi+ecx]       ; scan for 1st byte of pattern
    je Pre_Match            ; test if it matches
    add ecx, 1
    js Scan_Loop            ; exit on sign inversion

  ; ----------------------------------------
    
  No_Match:                 ; fall through here on no match
    mov eax, -1
    jmp isOut

  Match:
    add ecx, [esp+12+16]
    sub ecx, [esp+20+16]
    mov eax, ecx

  isOut:
    pop ebp
    pop edi
    pop esi
    pop ebx

    ret 20

BinSearch endp

OPTION PROLOGUE:PrologueDef 
OPTION EPILOGUE:EpilogueDef 

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

    end