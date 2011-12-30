#!/bin/sh
　　# next line restarts using tclsh in path \
　　exec tclsh ${1+"$@"}
　　# echo server that can handle multiple 
　　# simultaneous connections.
　　proc newConnection { sock addr port } {
　　# client connections will be handled in
　　# line-buffered, non-blocking mode
　　fconfigure $sock -blocking no -buffering line
　　# call handleData when socket is readable
　　fileevent $sock readable [ list handleData $sock ]
　　}
　　proc handleData {
　　puts $sock [ gets $sock ]
　　if { [ eof $sock ] } {
　　close $sock
　　}
　　}
　　# handle all connections to port given
　　# as argument when server was invoked
　　# by calling newConnection
　　set port [ lindex $argv 0 ]
　　socket -server newConnection $port
　　# enter the event loop by waiting
　　# on a dummy variable that is otherwise
　　# unused.
　　vwait forever


#proc every {ms body} {eval $body; after $ms [info level 0]}
　#　pack [label .clock -textvar time]
　　#every 1000 {set ::time [clock format [clock sec] -format %H:%M:%S]} ;# RS
