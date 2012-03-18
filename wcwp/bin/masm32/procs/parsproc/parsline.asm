; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

comment * -----------------------------------------------------------
    The "wcnt" parameter is the number of leading words to skip to
    reach the arguments on the line.
    KEYWORD QUALIFIER arg1,"text",arg3 etc ...
    The "arr" parameter is an array of addresses of memory buffers.
    The "src" parameter is the address of the line of args.
        ----------------------------------------------------------- *

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

parse_line proc wcnt:DWORD,src:DWORD,arr:DWORD

    push ebx
    push esi
    push edi

    xor edx, edx
    mov esi, src

  ; ----------------------------------------------------
  ; skip the count of leading words delimited by spaced
  ; ----------------------------------------------------
  gast:
    inc esi
    cmp BYTE PTR [esi], 32  ; strip leading spaces
    je gast
  @@:
    inc esi
    xor eax, eax            ; return zero if end of line
    cmp BYTE PTR [esi], 13  ; exit on end of line
    je plend
    cmp BYTE PTR [esi], ";" ; exit on end of line
    je plend
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

  ; src is already in ESI

    sub esi, 1
    mov edi, arr            ; load array address in EDI
    mov edi, [edi]          ; dereference to get string buffer

  lbl1:
    add esi, 1
    mov al, [esi]
    cmp al, ","             ; normal delimiter
    je next_arg
    cmp al, 13              ; line end
    je last_arg
    cmp al, ";"             ; comment = line end
    je last_arg
    cmp al, 34              ; quotation
    je quotes
  jumpin:
    mov [edi], al
    add edi, 1
    jmp lbl1
  next_arg:
    mov BYTE PTR [edi], 0   ; terminate buffer
    add edx, 1
    mov eax, edx
    mov edi, arr            ; reload array address into EDI
    mov edi, [edi+eax*4]
    jmp lbl1

  last_arg:
    mov BYTE PTR [edi], 0   ; terminate buffer
    add edx, 1
    mov eax, edx            ; return parameter count in EAX

    mov ecx, esi
    sub ecx, src            ; return last read location in ECX
    add ecx, 2              ; plus 2 extra bytes for CRLF

    jmp plend

; -------------------------------------------------------------------------
  quotes:
    mov [edi], al           ; write 1st quote
    add edi, 1
  @@:
    add esi, 1
    mov al, [esi]
    cmp al, 13              ; end of line error, no closing quote
    je quote_error
    mov [edi], al           ; write each byte to buffer
    add edi, 1
    cmp al, 34              ; is it a closing quote ?
    jne @B
    jmp lbl1

  quote_error:
    mov eax, -1             ; quotation error returns -1
    jmp plend
; -------------------------------------------------------------------------
 
  plend:

    pop edi
    pop esi
    pop ebx

    ret

parse_line endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл
