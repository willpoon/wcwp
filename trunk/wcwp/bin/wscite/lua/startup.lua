PLATFORM = 'windows' -- or 'windows'
if PLATFORM == 'linux' then
  LUA_PATH = props['SciteDefaultHome']..'/scripts/?.lua'
elseif PLATFORM == 'windows' then
  LUA_PATH = props['SciteDefaultHome']..'\\scite-debug\\scite_lua\\scripts\\?.lua'..';'.. \
end
--~ EXTMAN_PATH = props['SciteDefaultHome']..'\\scite-debug\\?.lua'
package.path  = package.path..';'..LUA_PATH
require 'scite/scite' -- load scite module
require 'scite/theme' -- load scite module
--~ require 'scite/extman' -- load scite module


local CLIDBG = ';C:\\poon\\wcwp\\wcwp\\bin\\wscite\\scite-debug\\lua_clidebugger\\?.'
package.path = package.path..CLIDBG..'lua'
package.cpath = package.cpath..CLIDBG..'dll'
WIN=true
GDB=true
require "debugger"
io.stdout:setvbuf("no")
pause('debug')
