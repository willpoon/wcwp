; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

comment * -------------------------------------------------------------
        memory is allocated for both the main array and the array of
        pointers using GlobalAlloc().

        The return address in EAX is the array of pointers
        The return address in ECX is the main memory buffer

        Each pointer is a location in the main memory buffer and each
        following pointer is stepped by the size nominated with the
        "asize" parameter.

        Both return values MUST be deallocated using GlobalFree() when
        they are no longer required.
        ------------------------------------------------------------- *

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

create_array proc acnt:DWORD,asize:DWORD

comment * ---------------------------------
    acnt  = number of array items
    asize = byte count for each item
    ------------------------------------- *

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

create_array endp

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл
