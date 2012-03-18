; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

GetArgs proc src:DWORD,buffer:DWORD,lplb:DWORD,lprb:DWORD

  ; ---------------------------------------------------
  ; procedure reads the first argument between inner
  ; brackets reading from left to right. it writes the
  ; argument including the brackets to "buffer" and
  ; the starting and ending location for the argument 
  ; inclding brackets to the address of "lb" and "rb".
  ; ---------------------------------------------------

    push ebx
    push esi
    push edi

    mov esi, src
    mov edx, -1
    mov ecx, lplb
    mov DWORD PTR [ecx], -1
    mov edi, lprb
    mov DWORD PTR [edi], -1

    mov ebx, buffer             ; buffer address in EBX

  lpStart:
    inc edx
    mov al, [esi+edx]
    cmp al, 0
    je lpEnd
    cmp al, "("
    jne @F
    mov [ecx], edx              ; write current location to address of "lplb"
    mov ebx, buffer             ; reset buffer address each "("
  @@:
    cmp al, ")"
    jne @F
    mov [edi], edx              ; write current location to address of "lprb"
    mov [ebx], al               ; write ")"
    inc ebx
    mov BYTE PTR [ebx], 0       ; append terminator after ")"
    jmp lpEnd                   ; then exit loop
  @@:
    mov [ebx], al               ; write each byte
    inc ebx
    jmp lpStart
  lpEnd:

    pop edi
    pop esi
    pop ebx

    ret

GetArgs endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл
