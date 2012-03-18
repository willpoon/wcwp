; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

ReplArg proc src:DWORD,repl:DWORD,lb:DWORD,rb:DWORD

  ; ------------------------------------------------------
  ; replace inner bracketed argument with variable "repl"
  ; ------------------------------------------------------
    push esi
    push edi

    mov esi, repl
    mov edi, src
    add edi, lb     ; set starting position to write

  @@:
    mov al, [esi]
    inc esi
    cmp al, 0
    je @F
    mov [edi], al
    inc edi
    jmp @B

  @@:
    mov esi, src
    add esi, rb
    inc esi

  @@:
    mov al, [esi]
    inc esi
    mov [edi], al
    inc edi
    cmp al, 0
    jne @B
    
    pop edi
    pop esi

    ret

ReplArg endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл
