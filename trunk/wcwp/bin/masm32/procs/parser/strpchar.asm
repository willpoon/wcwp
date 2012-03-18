; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

stripchar proc src:DWORD,char:BYTE
  ; ------------------------------------------
  ; strip any single character from zero
  ; terminated string passed as buffer "src".
  ; ------------------------------------------
    mov ecx, src
    mov edx, src

  @@:
    mov al, [ecx]
    inc ecx
    cmp al, char
    je @B
    mov [edx], al
    inc edx
    cmp al, 0
    jne @B
  @@:

    ret

stripchar endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл
