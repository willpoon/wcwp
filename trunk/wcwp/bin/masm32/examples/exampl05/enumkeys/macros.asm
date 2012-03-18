;Macros

LOWORD	MACRO 	bigword	;; Retrieves the low word from double word argument

	mov	eax,bigword
	and	eax,0FFFFh	;; Set to low word 
	ENDM

HIWORD	MACRO bigword  		;; Retrieves the high word from double word 

	mov	eax,bigword
	shr	eax,16		;; Shift 16 for high word to set to high word
	ENDM

MAX	MACRO   word1, word2	;; Return the maximum of 2 integers in eax

	mov	eax,word1
	.IF	eax < word2
	mov	eax,word2	;; Set word2 as max
	.ENDIF
	ENDM

MIN	MACRO	word1, word2	;; Return the minimum of 2 integers in eax

	mov	eax,word1
	.IF	eax > word2
	mov	eax,word2	;; Set word2 as min
	.ENDIF
	ENDM

M2M 	MACRO M1, M2		;; Copy word from memory to memory
        push M2
        pop  M1
        ENDM

RGB	MACRO red, green, blue	;; Get composite number from red green and blue bytes 

	mov	al,blue			;; ,,,blue	
	shl	eax,8			;; ,,blue,

	add	al,green		;; ,,blue,green
	shl	eax,8			;; ,blue,green,
	add	al,red			;; ,blue,green,red
	and	eax,0FFFFFFh		;; Mask out top byte to complete COLORREF dword 
	ENDM

M2M	MACRO   arg1, arg2
	
	push	arg2
	pop	arg1

	ENDM
	
return  MACRO arg

        mov eax, arg
        ret
        ENDM


