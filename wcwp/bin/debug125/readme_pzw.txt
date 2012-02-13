1.进入 C:\poon\wcwp\wcwp\bin\debug125
2.scite运行本文件
3.打开调试窗口
4.执行
>cmd /c debug.com
5. or *.asm ctrl+5
debug:
e1000:0
23
e1000:1
11
e1000:2
22
e1000:3
66

##or :
e1000:0 23 11 22 66
e1000:0 17 11 16 42


mov ax,1000h
mov ds,ax
mov ax,[0]
mov bx,[2]
mov cx,[1]
add bx,[1]
add cx,[2]

