; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

ccount proc src:DWORD, char:BYTE
  ; -----------------------------------------------------
  ; count any single character in zero terminated string
  ; -----------------------------------------------------
    mov ecx, src
    xor eax, eax        ; use as counter
    mov dl, char
    dec ecx

  @@:
    inc ecx
    cmp BYTE PTR [ecx], 0
    je @F
    cmp [ecx], dl
    jne @B
    inc eax
    jmp @B
  @@:

    ret

ccount endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл
