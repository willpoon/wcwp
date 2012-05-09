#ident	"@(#)local.cshrc	1.2	00/05/01 SMI"
umask 022
set path=(/bin /usr/bin /usr/ucb /etc .)
if ( $?prompt ) then
	set history=32
endif
