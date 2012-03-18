

.data
    s1 db 32 dup (0)
    s2 db 32 dup (0)
    s3 db 32 dup (0)
    s4 db 32 dup (0)
    s5 db 32 dup (0)
    s6 db 32 dup (0)
    s7 db 32 dup (0)
    s8 db 32 dup (0)
    s9 db 32 dup (0)
    s0 db 32 dup (0)

    d1 db 32 dup (0)
    d2 db 32 dup (0)
    d3 db 32 dup (0)
    d4 db 32 dup (0)
    d5 db 32 dup (0)
    d6 db 32 dup (0)
    d7 db 32 dup (0)
    d8 db 32 dup (0)
    d9 db 32 dup (0)
    d0 db 32 dup (0)

  ; ----------------------------------------
  ; source and destination array of strings
  ; ----------------------------------------
    align 4
    sarr dd s1,s2,s3,s4,s5,s6,s7,s8,s9,s0
    darr dd d1,d2,d3,d4,d5,d6,d7,d8,d9,d0

comment * ------------------------------------------------
    Allowable characters in table are upper and lower case
    This table should be put in the .DATA section.
          ------------------------------------------------ *

    align 4
    ctable \
      db 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
      db 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0     ; 31
      db 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
      db 1,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0     ; 63
      db 0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1
      db 1,1,1,1,1,1,1,1,1,1,1,0,0,0,0,0     ; 95
      db 0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1
      db 1,1,1,1,1,1,1,1,1,1,1,0,0,0,0,0     ; 127
      db 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
      db 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
      db 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
      db 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
      db 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
      db 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
      db 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
      db 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0

    tline  db "   testm eax, 312, 86      ",13,10,0

    tmacro db "   testm    MACRO    reg,var1,var2   ",13,10,
              "mov reg, var1",13,10,
              "add reg, var2",0,13,10,
              "ENDM",13,10,0
.code

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

get_args proc wcnt:DWORD,src:DWORD,arr:DWORD,table:DWORD

comment * ---------------------------------
    wcnt  = count of leading words to skip
    src   = address of line with args
    arr   = array to write the args to
    table = allowable character table
          --------------------------------- *

    LOCAL pcnt:DWORD

    push ebx
    push esi
    push edi

    mov pcnt, 0
    mov esi, src

  ; ---------------------------------------
  ; skip the count of leading spaced words
  ; ---------------------------------------
  gast:
    inc esi
    cmp BYTE PTR [esi], 32  ; strip leading spaces
    je gast
  @@:
    inc esi
    cmp BYTE PTR [esi], 13  ; exit on end of line
    je gaend
    cmp BYTE PTR [esi], 32  ; handle legal characters
    jne @B
    dec wcnt
    jnz gast
  ; -----------------------------------------
  @@:
    inc esi
    cmp BYTE PTR [esi], 32  ; handle leading spaces
    je @B
  ; -----------------------------------------

  ; --------------------------
  ; read arguments into array
  ; --------------------------
    mov ebx, table
    mov edi, arr            ; put array address in EDI
    mov edi, [edi]          ; get address of first buffer
    xor ecx, ecx            ; zero array counter

    dec esi
  lbl2:
    xor eax, eax
    inc esi
    mov al, [esi]
    cmp al, 13              ; test for line end
    je gaend                ; exit on line end
    cmp BYTE PTR [ebx+eax], 1   ; is it a legal character ?
    jne @F
  backin:
    mov [edi], al
    inc edi
    jmp lbl2
  @@:
  ; -----------------------------------------------------
    mov BYTE PTR [edi], 0   ; append terminator on buffer
    add ecx, 4              ; add 4 to array counter
    mov edi, arr            ; put array address in EDI
    add edi, ecx            ; add counter to get next offset
    mov edi, [edi]          ; set address of next buffer
    add pcnt, 1
  ; -----------------------------------------------------
  @@:
    xor eax, eax
    inc esi
    mov al, [esi]
    cmp BYTE PTR [ebx+eax], 0   ; is it a legal character ?
    jne backin              ; if it is, jump back to 1st loop
    cmp al, 13              ; test for line end
    jne @B

  gaend:

    mov eax, pcnt           ; return arg count in EAX

    pop edi
    pop esi
    pop ebx

    ret

get_args endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл
