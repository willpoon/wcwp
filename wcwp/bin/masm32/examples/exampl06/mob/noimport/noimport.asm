; =======================================
; NO_IMPORT by mob aka drcmda
; this program demonstrates how to write
; portable code... this code could be
; added to other executables with no prob.
; i'm over that virus shit so don't waste
; your time... i'm working on something
; like a executable patcher right now so
; portable code was very interesting for
; me...................................
; if you want to use other apis or other
; dll's then use this structure:
; 00      db ?? ;lenght of name
; 01 - ?? db ?? ;API name
; ??      dd ?? ;pointer
; then use 'GetApis' to find their
; pointers so you don't have to search
; the pointers with GetModuleHandle
; write to drcmda@gmx.de 
; =======================================

; ---------------------------------------------------
; Build with MAKEIT.BAT to merge the .text and .data
; sections. Result is a 1024 byte length EXE file.
; ---------------------------------------------------

.486
.Model      Flat, Stdcall
Option      Casemap:None

.Data

; kernel32.dll api's
___Kernel32         db 14,"GetProcAddress"
_Getprocaddress     dd 0
                    db 11,"LoadLibrary"
_Loadlibrary        dd 0
                    db 11,"ExitProcess"
_Exitprocess        dd 0

; user32.dll api's
___User32           db 11,"MessageBeep"
_Messagebeep        dd 0
                    db 10,"MessageBox"
_MessageBox         dd 0                    

_Kernel             Dd 0
_User32             db "USER32",0
_Default            Dd 0   

.Code       
    
Start:
            Call    Delta
Delta:
            Pop     Ebp                                 ; get deltaofs    
            Sub     Ebp,Offset Delta                    ; for portability         
            
            Call    Get_Kernel                          ; get kernel base/set default
            
            Push    3                                   ; 3 api's in the kernel32 struc
            pop     Ecx
            Lea     Esi,[Ebp+Offset ___Kernel32]                                           
            Call    Get_Apis                            ; get kernel apis
                       
            Lea     Eax,[Ebp+Offset _User32]            ; load user32.dll
            Push    Eax
            Call    [Ebp+_Loadlibrary]            
            
            test    Eax,Eax
            jz      Error_Exit

            Mov     [Ebp+Offset _Default],Eax           ; store result in 'default'

            push    2                                   ; 4 api's in the user32 struc
            pop     Ecx
            Lea     Esi, [Ebp+Offset ___User32]                                           
            Call    Get_Apis                            ; get user32 apis           
                                  
            Push    -1
            Call    [Ebp+_Messagebeep]                  ; beep
            
            Push    0
            Call    _t02
            db      "little test",0
_t02:       Call    _t01
            db      "MessageBox without imports, funny eh?",0
_t01:       Push    0
            Call    [Ebp+_MessageBox]                   ; messagebox

Error_Exit:                                
            Push    0
            Call    [Ebp+_Exitprocess]                  ; get out


; ######################## get kernel ########################
; returns kernelbase and stores it in 'default' and 'kernel'
Get_Kernel:
            Mov     Ecx,[Esp+4] ; get kerneladdr from stack
            
Kernel_Loop:
            Xor     Edx,Edx
            Dec     Ecx
            Mov     Dx,[Ecx+3Ch] 
            Test    Dx,0F800H
            Jnz     Kernel_Loop
            Cmp     Ecx,[Ecx+Edx+34H]
            Jnz     Kernel_Loop            
            Mov     [Ebp+Offset _Kernel],Ecx
            Mov     [Ebp+Offset _Default],Ecx                                             
            Ret

; ######################## get apis   ########################
; default   = dll base
; ecx       = number of api's in the structure
; esi       = pointer to structure
Get_Apis:  
            Xor     Ebx,Ebx
Api_Loop:
            Inc     Esi         ; scan through the api
            Push    Ecx         ; table and try to
            Movzx   ecx, byte ptr [Esi-1] ; addresses...                
            Push    Ecx
            Call    Get_Api
            Pop     Ebx
            Pop     Ecx
            Add     Esi,Ebx            
            Mov     [Esi],Eax
            Add     Esi,4
            Loop    Api_Loop
            Ret       

; ######################## get api    ########################
; default = dll base
; ecx     = structure entry
Get_Api:   
            Mov     Edx, [Ebp+Offset _Default]
            Add     Edx, [Edx+3Ch] ; get default module        
            Mov     Edx, [Edx+78H]
            Add     Edx, [Ebp+Offset _Default]

            Mov     Edi, [Edx+32] ;Get Addrofnames
            Add     Edi, [Ebp+Offset _Default]
            Mov     Edi, [Edi] ;Get Addrofnames
            Add     Edi, [Ebp+Offset _Default]
            Mov     Eax, [Edx+24] ;Get Numberofnames                                   
            Xor     Ebx,Ebx
Next_One:            
            Push    Ecx
            Inc     Ebx          
            Push    Esi
            Push    Edi
            Repz    Cmpsb ; compare api with export
            Pop     Edi
            Pop     Esi
            Jnz     Not_Found            
            Pop     Ecx
            Mov     Ecx, [Edx+36] ;Get Addrnameord
            Add     Ecx, [Ebp+Offset _Default]
            Dec     Ebx
            Movzx   eax, word ptr [Ecx+Ebx*2]                        
            Mov     Ebx, [Edx+28] ;Get Addroffunctions
            Add     Ebx, [Ebp+Offset _Default]
            Mov     Eax, [Ebx+Eax*4]                
            Add     Eax, [Ebp+Offset _Default]
            Ret
Not_Found:  
            Dec     Edi            
Loop_1:
            Inc     Edi
            Cmp     Byte Ptr [Edi],0
            Jnz     Loop_1

            Inc     Edi            
            Dec     Eax
            Jz      Exit_Search            
            Pop     Ecx
            Jmp     Next_One
Exit_Search:  
            Jmp     Error_Exit
            Ret                        
        
End         Start