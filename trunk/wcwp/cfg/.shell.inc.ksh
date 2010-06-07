######################################################################################################
#common settings
set -o vi
PS1=['${PWD} ']
export PS1
#alias for ls
alias lh='ls -al'
#alias lt='ls -alrt'
alias l1='ls -a1'
alias le='ls -alh'
alias lk='ls -als'
alias la='ls -arlt'
alias ldf='ls -l --group-directories-first'
alias ldl='ls -l -r --group-directories-first '
#alias for cd
#alias for directories operation
#alias for cd
alias ..='cd ..'
alias ..2='cd ../..' 
alias ..3='cd ../../..' 
alias ..4='cd ../../../..' 
alias pzh='cd ~/'
alias f='cd /media/OTHER'
#alias for dirs
alias rd='rmdir'
alias md='mkdir'
alias fr='rm -rf'
alias rr='rm -r' 
alias cpv='cp -v' 
alias cls='clear'

#alias for find command
alias fe='find -executable -type f -exec rm {} \;'
alias fo='find -name  *.o -type f ;'
#alias for svn command
alias svst='svn status'
alias svci='svn ci -m '
alias sva='svn add '
#alias for coding 
svnbase="poonzref"
devroot="${myroot}/${svnbase}/dev"
alias gc='cd ${devroot}/gcc'
alias cod='vim -p main.c myc.c myh.h'
#alias rl='gcc -o main main.c myc.c -std=c99'
#alias for local system
alias sshar='ssh arpoonin@74.220.215.245'
alias mys='sh ~/.wcwp/mys.sh'
alias rf='. $cfgroot/cfg/.pz_settings;cp $cfgroot/cfg/.pz_settings $cfgroot/cfg/.pz_settings.bak;'
alias cfg='vi $cfgroot/cfg/.pz_settings'
