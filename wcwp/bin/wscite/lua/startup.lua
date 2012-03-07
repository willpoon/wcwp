--~ startup.lua
--~ http://hi.baidu.com/perlifect/blog/item/fe559baf4aac9bf1faed5062.html
--~ 下面是我的自启动 Lua 脚本文件 
--~ 自动补全括号
local toClose = { ['('] = ')', ['{'] = '}', ['['] = ']', ['"'] = '"', ["'"] = "'" }
function OnChar(charAdded)
    if toClose[charAdded] ~= nil then
        editor:InsertText(editor.CurrentPos,toClose[charAdded])
    end
end
--选择括号
function Selecttobraceplus(vs)
    if editor:GetSelText()~="" then
        editor:GotoPos(editor.CurrentPos+1)
    end
    local p = editor.CurrentPos
    while true do
        local fs,fe = editor:findtext("[\]\)\}]", SCFIND_REGEXP, editor.CurrentPos)
        if fe ~= nil then
            editor:GotoPos(fe-vs)
            scite.MenuCommand(IDM_MATCHBRACE)
            if editor.CurrentPos <= p then
                scite.MenuCommand(IDM_SELECTTOBRACE)
                break
            else
                editor:GotoPos(fe)
            end
        else
            break
        end
    end
end

--选择词
function SelectWord()
    editor:WordRight()
    editor:WordLeftExtend()
end

--插入行
function insertline(iline)
    if iline=='1' then
        editor:LineEnd()
        editor:NewLine()
    end
    if iline=='2' then
        editor:NewLine()
        editor:NewLine()
        editor:LineUp()
    end

end

--跳出括号
function Jump()

 local fs,fe = editor:findtext("[\]\)\}\'\"\>]", SCFIND_REGEXP,editor.CurrentPos)
 if fe ~= nil then
  editor:GotoPos(fe)
 end
end
-- evening 配色方案
local function SetColours(scheme)
 editor:SetFoldMarginColour(1, 3355443) --折叠状态栏颜色
 editor:SetFoldMarginHiColour(1, 0) --折叠状态栏高亮颜色
  local function dec(s) return tonumber(s, 16) end
  for i, style in pairs(scheme) do
          for prop, value  in pairs(style) do
            if (prop == "StyleFore" or prop == "StyleBack")
               and type(value) == "string" then
              local hex, hex, r, g, b =
                string.find(value, "^(%x%x)(%x%x)(%x%x)$")
              value = hex and (dec(r) + dec(g)*256 + dec(b)*65536) or 0
            end
            editor[prop][i] = value
          end
  end
end
local ColourScheme = {  
--{StyleFore = "008000", StyleBack = "E0E0E0", StyleItalic = true,StyleBold = true,},
[0] = {StyleFore = "FFFFFF", StyleBack = "333333",}, -- 空白!
[1] = {StyleFore = "80A0FF", StyleBack = "333333", }, -- 注释!
[2] = {StyleFore = "80A0FF", StyleBack = "333333", }, -- 注释!
[3] = {StyleFore = "80A0FF", StyleBack = "333333", }, -- 注释!
[4] = {StyleFore = "FFA0A0", StyleBack = "0D0D0D", }, -- 数字!
[5] = {StyleFore = "FFFF60", StyleBack = "333333", StyleBold = true,}, -- 关键字!
[6] = {StyleFore = "FFA0A0", StyleBack = "0D0D0D", }, -- 双引号!
[7] = {StyleFore = "FFA0A0", StyleBack = "0D0D0D", }, -- 单引号!
[8] = {StyleFore = "FFFFFF", StyleBack = "333333", StyleBold = true,}, -- 
[9] = {StyleFore = "FF80FF", StyleBack = "333333", }, -- 
[10] = {StyleFore = "FFFFFF", StyleBack = "333333", }, -- 括号,等号，运算符!
[11] = {StyleFore = "FFFFFF", StyleBack = "333333", }, -- 变量!
[12] = {StyleFore = "40FFFF", StyleBack = "333333", }, -- 系统函数
[13] = {StyleFore = "60FF60", StyleBack = "333333", }, -- 系统函数
[14] = {StyleFore = "60FF60", StyleBack = "333333", }, -- 系统函数
[15] = {StyleFore = "80A0FF", StyleBack = "333333", }, -- 注释
[16] = {StyleFore = "60FF60", StyleBack = "333333", }, -- 
[17] = {StyleFore = "60FF60", StyleBack = "333333", }, -- 
[18] = {StyleFore = "60FF60", StyleBack = "333333", }, -- 
[19] = {StyleFore = "60FF60", StyleBack = "333333", }, -- 
[32] = {StyleFore = "FFFFFF", StyleBack = "333333", }, -- 默认!
[33] = {StyleFore = "FFFF00", StyleBack = "333333", }, -- 行号!
[34] = {StyleFore = "FFFFFF", StyleBack = "008080", StyleBold = true,}, -- 括号高亮!
[35] = {StyleFore = "EE0000", StyleBold = true,}, -- 
[36] = {StyleFore = "EE0000", StyleBack = "000000", StyleBold = true,}, -- 
[37] = {StyleFore = "FFFFFF", }, -- 缩进线
[38] = {StyleFore = "EE0000", StyleBack = "000000", StyleBold = true,}, -- 
}
function OnUpdateUI()
    SetColours(ColourScheme)
end


--~ define the commment sign for asm files
  function asmComment()
    local asm_comment_ext = props['comment.block.asm']
    local comment_nasm = '#'
    local comment_masm = ';'
--~     local f = props['FileName']    -- e.g 'test'
    local ext = props['FileExt']   -- e.g 'cpp'
--~     local path = props['FileDir']  -- e.g. '/home/steve/progs'
    if ext == 's' then
       props['comment.block.asm'] = comment_nasm
    else
       props['comment.block.asm'] = comment_masm
    end
--~     scite.Open(path..'/'..f..'.'..ext)
  end


--~ function make_uppercase()
--~   local sel = editor:GetSelText()
--~   editor:ReplaceSel(string.upper(sel))
--~ end
--~   


 function SetAsmComment()
    local  ext = props['FileExt']
		if 	ext == 's' then
			props['comment.block.asm'] = '#'
		elseif ext == 'asm' then
			props['comment.block.asm'] = ';'
		end
   print ("switch comment to : ",props['comment.block.asm'] ,"for .",ext)

end

