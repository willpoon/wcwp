; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

    .486                      ; force 32 bit code
    .model flat, stdcall      ; memory model & calling convention
    option casemap :none      ; case sensitive

    include \masm32\include\user32.inc
    include \masm32\macros\macros.asm

    ucLen     PROTO :DWORD

    .code

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

OPTION PROLOGUE:ProLogueDef
OPTION EPILOGUE:EpilogueDef

align 4

ucLower proc wsrc:DWORD

    invoke CharLowerBuffW,wsrc,ulen$([esp+4])

    ret 4

ucLower endp

OPTION PROLOGUE:ProLogueDef
OPTION EPILOGUE:EpilogueDef

; ллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллллл

end
