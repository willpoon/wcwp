#	This is the default standard profile provided to a user.
#	They are expected to edit it to meet their own needs.

MAIL=/usr/mail/${LOGNAME:?}
stty erase '^H'

if [ -f /bassdb1/db2home/db2inst1/sqllib/db2profile ]; then
    . /bassdb1/db2home/db2inst1/sqllib/db2profile
fi


PATH=.:/usr/sbin:/usr/local/bin:$PATH
export PATH

JRE_HOME=/usr/j2se;  export JRE_HOME

PATH=$JRE_HOME/bin:/usr/sbin:/usr/local/bin:$PATH

export PATH
LANG=zh_CN.GBK;         export NLS_LANG
LC_ALL=zh_CN.GBK; export LC_ALL
