#!/bin/sh

. $HOME/bin/xferprofile
nohup $HOME/bin/xfer -i $HOME/config/xfer.cfg -c 5 -f &
