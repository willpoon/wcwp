 
 function make_uppercase()
--~   local sel = editor:GetSelText()
--~   editor:ReplaceSel(string.upper(sel))
--~    ext = props['file.patterns.lua']
--~ ext='a'
local	comment_asm=props['comment.block.asm']
    local f = props['FileName']    -- e.g 'test'
    local ext = props['FileExt']   -- e.g 'cpp'
    local path = props['FileDir']  -- e.g. '/home/steve/progs'
	print (comment_asm)
	props['comment.block.asm'] = '#'
	print (comment_asm)
--~ 	props['comment.block.asm'] = '#'
	print (comment_asm)
   print ("Hello, World!")
   print (comment_asm,f,ext,path)
end

 