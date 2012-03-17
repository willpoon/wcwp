注意：这是命令行的编译器，没有任何图形界面！没有命令行编译经验如cl, nmake，gcc等的最好不要下。高手一定要下！初学者推荐下！体积19.2MB

从Microsoft Visual Studio 6.0中Ripper出来的VC++编译器，花了很多精力测试它，成功！（配合ActivePerl编译openssl-0.9.8k.tar.gz完全OK）

还添加了从MASM32 V10中Ripper出来的汇编编译器ML 6.14.8444经典版。

苦于网上VC++6.0的各种版本非常之多，却没有一个纯编译器版本（主要用来编译下载的VC6源码），因此专门研究制作。

（lauey出品，CSDN首发，2009-07-07）

使用方法：

把下载的文件解压到C盘根目录下，进入命令提示符后，运行c:\cl6\setvars.bat一次就可以正式开工了。

如果解压到其他路径，比如D:\或D:\Tools的话，需要修改setvars.bat中的CLDIR，例如set CLDIR=D:\Tools\cl6即可。