; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

; get decimal number after "#" character and return it in EAX

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

getindex proc lpvar:DWORD

    mov edx, lpvar
    mov ecx, edx
    inc edx             ; get next character after #
  @@:
    mov al, [edx]
    inc edx
    mov [ecx], al
    inc ecx
    cmp al, 0
    jne @B

    invoke atodw,lpvar  ; result is integer index in EAX

    ret

getindex endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл
