.data
TreeViewClass 	db "SysTreeView32",0
HKCR		db "HKEY_CLASSES_ROOT",0
HKCU		db "HKEY_CURRENT_USER",0
HKLM		db "HKEY_LOCAL_MACHINE",0
HKU		db "HKEY_USERS",0
HKCC		db "HKEY_CURRENT_CONFIG",0
HKDD		db "HKEY_DYN_DATA",0
Child1		db "child1",0
Child2		db "child2",0
DragMode		dd FALSE
tvinsert	TV_INSERTSTRUCT <>
tvhit		TV_HITTESTINFO <>

.data?
hwndTreeView	dd ?
hParent		dd ?
hImageList	dd ?
hDragImageList	dd ?
hBitmap	dd ?
