; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

      create_prime_table PROTO
      hashkey PROTO :DWORD,:DWORD,:DWORD
      create_hash_table PROTO :DWORD,:DWORD
      write_to_table PROTO :DWORD,:DWORD,:DWORD,:DWORD

comment * ------------------------------------------------------------
    the "alloc" and "free" macros are required for the memory
    handling. Allocate the array of primes and the hash table
    with its associated array of pointers to the string locations
    in the hash table.

    You must specify the number of array elements for the hash table
    and the length of the fields.

    The following two macros are normally in the macros.asm file

      alloc MACRO bytecount
        invoke GlobalAlloc,GMEM_FIXED or GMEM_ZEROINIT,bytecount
        EXITM <eax>
      ENDM

      free MACRO hmemory
        invoke GlobalFree,hmemory
      ENDM
    ---------------------------------------------------------------- *

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

create_hash_table proc acnt:DWORD,asize:DWORD

comment * -------------------------------
    acnt  = number of items in hash table
    asize = byte count for each item ---- *

    LOCAL lparr:DWORD
    LOCAL lpmem:DWORD

    mov ecx, acnt
    shl ecx, 2                  ; multiply by 4
    mov lparr, alloc(ecx)       ; allocate array

    mov ecx, asize
    mov eax, acnt
    imul ecx                    ; multiply count by BYTE size for string memory length
    mov lpmem, alloc(eax)       ; allocate string memory

    mov eax, lpmem              ; string memory start address
    mov edx, lparr              ; array address
    mov ecx, acnt               ; item count
  @@:
    mov [edx], eax              ; load address in EAX into location in array
    add eax, asize              ; add "asize" for next start address
    add edx, 4                  ; set next array location
    sub ecx, 1
    jnz @B

comment * ----------------------------------
    deallocate both of the returned memory
    handles when the hash table is no longer
    required ------------------------------- *

    mov eax, lparr              ; return address of array of pointers in EAX
    mov ecx, lpmem              ; return string memory address in ECX

    ret

create_hash_table endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

create_prime_table proc

    mov eax, alloc(128)
    push eax

    mov DWORD PTR [eax],   103
    mov DWORD PTR [eax+4], 101
    mov DWORD PTR [eax+8],  97
    mov DWORD PTR [eax+12], 91
    mov DWORD PTR [eax+16], 89
    mov DWORD PTR [eax+20], 87
    mov DWORD PTR [eax+24], 83
    mov DWORD PTR [eax+28], 79
    mov DWORD PTR [eax+32], 73
    mov DWORD PTR [eax+36], 71
    mov DWORD PTR [eax+36], 67
    mov DWORD PTR [eax+40], 61
    mov DWORD PTR [eax+44], 59
    mov DWORD PTR [eax+48], 57
    mov DWORD PTR [eax+52], 53
    mov DWORD PTR [eax+56], 51
    mov DWORD PTR [eax+60], 47
    mov DWORD PTR [eax+64], 43
    mov DWORD PTR [eax+68], 41
    mov DWORD PTR [eax+72], 39
    mov DWORD PTR [eax+76], 37
    mov DWORD PTR [eax+80], 31
    mov DWORD PTR [eax+84], 29
    mov DWORD PTR [eax+88], 23
    mov DWORD PTR [eax+92], 19
    mov DWORD PTR [eax+96], 17
    mov DWORD PTR [eax+100], 13
    mov DWORD PTR [eax+104], 11
    mov DWORD PTR [eax+108], 7
    mov DWORD PTR [eax+112], 5
    mov DWORD PTR [eax+116], 2
    mov DWORD PTR [eax+120], 1

    pop eax

    ret

create_prime_table endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

hashkey proc src:DWORD,cnt:DWORD,prms:DWORD

    LOCAL ttl:DWORD

    push ebx
    push esi
    push edi

    mov ttl, 0                  ; total

    mov esi, src
    mov edi, prms               ; array of primes address in EDI
    xor ecx, ecx                ; zero character counter
    xor ebx, ebx                ; prevent stall in EBX
  @@:
    movzx eax, BYTE PTR [esi+ecx]
    add ecx, 1                  ; increment the character position counter
    mov ebx, [edi+ecx*4]        ; get value of prime from character position
    imul bx                     ; mul char ascii value by prime
    add ttl, eax                ; add result to total
    cmp eax, 0
    jne @B

  ; -------------------------------
  ; added to deliver larger numbers
  ; -------------------------------
    mov ecx, ttl
    shl ecx, 13
    add ttl, ecx

  ; ------------------------------------------------------------
  ; divide total by array count and return the remainder in EAX
  ; ------------------------------------------------------------
    xor edx, edx
    mov eax, ttl
    div cnt
    mov eax, edx

    pop edi
    pop esi
    pop ebx

    ret

hashkey endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

write_to_table proc array:DWORD,src:DWORD,acnt:DWORD,prms:DWORD

comment * ----------------------------------------------
    array = array of pointers to string array locations
    src   = string to hash and write to table
    acnt  = is the number of items in the array
    prms  = the array of primes for the hash function
        ------------------------------------------------ *

    LOCAL item:DWORD
    LOCAL hkey:DWORD

    mov hkey, FUNC(hashkey,src,acnt,prms)
  wttstart:
    mov eax, hkey
    cmp eax, acnt                   ; compare the hash key to the array
    jl @F                           ; count. loop around to start if it
    mov hkey, 0                     ; is at the end of the hash table
  @@:
    mov edx, array
    mov eax, hkey
    lea edx, [edx+eax*4]            ; get offset of array plus hash key in EDX
    push [edx]
    pop item

comment * -------------------------------------------------------
    The logic here is as follows, if the table location selected
    by using the hash key is empty, write the string to it, if
    it has the same string written to it as is being tested, exit
    the procedure, if the string is different to the one at the
    tested location, try the nwext higher string location to see
    if its blank.
        --------------------------------------------------------- *

    mov eax, item
    cmp BYTE PTR [eax], 0           ; if first byte is zero,
    je write_string                 ; jump to "write string"
    cmp FUNC(szCmp,item,src), 0     ; if string is aready written
    jnz wtEnd                       ; exit procedure
    add hkey, 1                     ; if collision, 
    jmp wttstart                    ; try next location in hash table

  write_string:
    invoke szCopy,src, item         ; write string to location in hash table
    add wcnt, 1

  wtEnd:
    ret

write_to_table endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл
