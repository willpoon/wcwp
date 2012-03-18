; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

; --------------------------------
; put this table in .data section
; --------------------------------

  parsetable dd zero,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr
             dd errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr         ; 1 = number
             dd spac,errr,errr,hash,errr,errr,errr,errr,spac,spac,mult,addt,errr,subt,numz,divd         ; mult = mul
             dd numz,numz,numz,numz,numz,numz,numz,numz,numz,numz,errr,errr,errr,errr,errr,errr         ; 3 = add
             dd errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr         ; 4 = min
             dd errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr         ; 5 = div
             dd errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr         ; 6 = period
             dd errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr         ; 7 = # character
             dd errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr
             dd errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr         ; 9 = space
             dd errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr
             dd errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr
             dd errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr
             dd errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr
             dd errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr
             dd errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr,errr


; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

parseargs proc lptxt:DWORD

    LOCAL vflag     :DWORD
    LOCAL oflag     :DWORD
    LOCAL buffer[32]:BYTE

    push ebx
    push esi
    push edi

    mov vflag, 0                ; set variable flag to zero

    mov esi, lptxt              ; address of text
    mov edi, OFFSET str1        ; temporary write buffer
    dec esi
    mov ebx, OFFSET parsetable  ; address of table

  spac::
    xor eax, eax                ; prevent stall
    inc esi
    mov al, [esi]               ; loop through string and
    jmp DWORD PTR [ebx+eax*4]   ; jmp to address of each character class

  numz::                        ; numbers
    mov BYTE PTR [edi], al      ; write BYTE to buffer
    inc edi
    jmp spac

  mult::                        ; *
    mov oflag, 1
    jmp setop

  addt::                        ; +
    mov oflag, 2
    jmp setop

  subt::                        ; -
    mov oflag, 3
    jmp setop

  divd::                        ; /
    mov oflag, 4
    jmp setop

  hash::                        ; #
    mov BYTE PTR [edi], al
    inc edi
    jmp spac

  setop:
    mov BYTE PTR [edi], 0
    AtoFP ADDR buffer,ADDR arg1
    mov edi, OFFSET str2
    jmp spac

  zero::
    mov BYTE PTR [edi], 0
    AtoFP ADDR buffer,ADDR arg2
    jmp paquit

  errr::
    invoke MessageBox,hWnd,SADD("Invalid character"),SADD("Error"),MB_OK
    mov oflag, 0                ; return zero for error

  paquit:

    mov eax, oflag              ; return the operator number * + - /

    pop edi
    pop esi
    pop ebx

    ret

parseargs endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл
