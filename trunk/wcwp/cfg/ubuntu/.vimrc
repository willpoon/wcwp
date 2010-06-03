set nu
filetype plugin on 
helptags ~/.vim/doc
"let s:C_CFlags                                  = '-Wall -g -O0 -c'
let g:C_CFlags  = '-Wall -g -o0 -c -std=c99'
"insert time stamp
nmap <F6> :execute "normal i" . strftime("//%x %X (%Z) ")<Esc>
imap <F6> <Esc>:execute "normal i" . strftime("//%x %X (%Z) ")<Esc>i
