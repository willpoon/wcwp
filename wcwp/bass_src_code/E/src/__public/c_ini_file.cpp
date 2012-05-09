//---------------------------------------------------------------------------
#pragma hdrstop

#include "cmpublic.h"
//---------------------------------------------------------------------------
#pragma package(smart_init)

//==========================================================================
// 函数 : TCIniFile
// 用途 : TCIniFile类构造函数
// 原型 : TCIniFile(const TCString FileName) ;
// 参数 : 文件名称
// 返回 :
// 说明 : 取文件名称，初始化m_load,m_modify变量
//==========================================================================
TCIniFile::TCIniFile()
{
  m_FileName = "" ;
  m_Strings.Clear() ;
  m_load = -1 ;
  m_modify = -1;
}
//==========================================================================
// 函数 : ~TCIniFile
// 用途 : TCIniFile类析构函数
// 原型 : ~TCIniFile() ;
// 参数 :
// 返回 :
// 说明 : 判断INI文件设置是否已经修改过，如果修改则存盘！
//==========================================================================
TCIniFile::~TCIniFile()
{
  if ( (m_load == 0)&&( m_modify == 0 ) )  //INI文件内容已经修改
  {
    m_Strings.SaveToFile(m_FileName);
  }
}

void TCIniFile::CreateNew(const TCString FileName) 
{
   TCFileStream fNewIniFile ;
   m_load = fNewIniFile.Open(FileName,omWrite) ;
   m_Strings.Clear() ;
   ASSERT( m_load != -1 ) ;
   if ( m_load != -1 )
   {
      m_FileName = FileName ;
      m_modify = -1;
   }
}

//==========================================================================
// 函数 : Load
// 用途 : 读INI文件到m_Strings 并去掉两头空格
// 原型 : int TCIniFile::Load(void)
// 参数 :
// 返回 : 读取成功或者失败
// 说明 : 成功返回0 ,否则返回-1
//==========================================================================
int TCIniFile::Load(const TCString FileName)
{
   m_Strings.Clear() ;
   m_load = m_Strings.LoadFromFile(FileName) ;
   ASSERT( m_load != -1 ) ;
   if ( m_load != -1 )
   {
      m_FileName = FileName ;
      m_modify = -1;
   }
   for ( long i = 0; i < m_Strings.GetCount() ; i++ )
   {
     m_Strings[i].TrimRight();
     m_Strings[i].TrimLeft();
   }
   return m_load ;
}

//==========================================================================
// 函数 : DeleteKey
// 用途 : 删除一条参数设置
// 原型 : int TCIniFile::DeleteKey(TCString Section,TCString Ident) ;
// 参数 : 参数组名,参数名
// 返回 : 成功或者失败
// 说明 : 成功返回0 ,如果指定的参数不存在则返回-1
//==========================================================================
int TCIniFile::DeleteKey(TCString Section,TCString Ident)
{
   long Index ;
   if ( m_load == 0 )
   {
     Index = SectionExists(Section);
     if ( Index != -1 )
     {
       Index = KeyExists(Index,Ident);
       if (Index != -1 )
       {
         m_Strings.Delete(Index) ;
         m_modify = 0 ;
         return 0 ;
       }
     }
   }
   return -1 ;
}

//==========================================================================
// 函数 : EraseSection
// 用途 : 删除一条参数设置
// 原型 : int TCIniFile::EraseSection(TCString Section);
// 参数 : 参数组名
// 返回 : 成功或者失败
// 说明 : 成功返回0 ,如果指定的组不存在则返回-1
//==========================================================================
int TCIniFile::EraseSection(TCString Section)
{
   long Index ;
   if ( m_load == 0 )
   {
     Index = SectionExists(Section);
     if ( Index != -1 )
     {
       m_modify = 0 ;
       m_Strings.Delete(Index) ;
       for ( long i=Index;i<m_Strings.GetCount();i++ )
       {
         if (m_Strings[i].GetLength() > 0 )
         {
            if ( m_Strings[i][1] != '[' )
                m_Strings.Delete(Index) ;
            else return 0 ;
         }
       }
       return 0 ;
     }
   }

   return -1 ;
}

//==========================================================================
// 函数 : ReadSection
// 用途 : 取得指定设置组的所有参数名！
// 原型 : int TCIniFile::ReadSection(const TCString Section,TCStringList * pStrings) ;
// 参数 : 参数组名 ， 参数串指针
// 返回 : -1,0
// 说明 : 如果指定的参数组不存在则返回-1
//==========================================================================
int TCIniFile::ReadSection(const TCString Section,TCStringList & pStrings)
{
   long Index ;
   TCString S ;
   if ( m_load == 0 )
   {
     Index = SectionExists(Section);
     if ( Index != -1 )
     {
       for ( long i=Index + 1;i<m_Strings.GetCount();i++ )
       {
         if (m_Strings[i].GetLength() > 0 )
         {
           if ( m_Strings[i][1] != '[' )
           {
             S = GetKeyName(m_Strings[i]) ;
             if (S != TCString("") )
               if ( (S[1] != ';')&&(S[1] != '#') )
                   pStrings.Add(S) ;
           }
           else return 0 ;
         }
       }
       return 0 ;
     }
   }
   return -1 ;
}

//==========================================================================
// 函数 : ReadSections
// 用途 : 取得所有的参数组名
// 原型 : void TCIniFile::ReadSections(TCStringList * pStrings)
// 参数 : 存贮参数组名的String List指针
// 返回 :
// 说明 :
//==========================================================================
void TCIniFile::ReadSections(TCStringList & pStrings)
{
   long nPos ;
   for ( long i = 0; i < m_Strings.GetCount() ; i++ )
   {
     if (m_Strings[i].GetLength() > 0 )
       if ( m_Strings[i][1] == '[' )
       {
         nPos = m_Strings[i].Find(']') ;
         if ( nPos != -1 )
         {
           pStrings.Add(m_Strings[i].Mid(2,nPos - 2)) ;
         }
       }
   }
}

//==========================================================================
// 函数 : ReadSectionValues
// 用途 : 取得指定参数组的所有设置值
// 原型 : int TCIniFile::ReadSectionValues(const TCString Section,TCStringList * pStrings)
// 参数 : 参数组名，存贮设置值的String List指针
// 返回 : 0或者-1
// 说明 :  如果指定的组不存在则返回-1
//==========================================================================
int TCIniFile::ReadSectionValues(const TCString Section,TCStringList & pStrings)
{
   long Index ;
   if ( m_load == 0 )
   {
     Index = SectionExists(Section);
     if ( Index != -1 )
     {
       for ( long i= Index + 1;i<m_Strings.GetCount();i++ )
       {
         if (m_Strings[i].GetLength() > 0 )
         {
            if ( m_Strings[i][1] != '[' )
            {
              if ( (m_Strings[i][1] != ';')&&(m_Strings[i][1] != '#')
                   &&(m_Strings[i] != TCString("") ) )
                 pStrings.Add( GetValue(m_Strings[i]) ) ;
            }
            else return 0 ;
         }
       }
       return 0 ;
     }
   }
   return -1 ;
}

//==========================================================================
// 函数 : ReadString
// 用途 : 读一指定的设置串
// 原型 : ReadString(const TCString Section, const TCString Ident,
//                   const TCString s_Default)
// 参数 : 参数组名，参数名，缺省设置
// 返回 : 设置值的字符串
// 说明 : 如果指定的设置不存在或为空则返回缺省设置串
//==========================================================================
TCString TCIniFile::ReadString(const TCString Section, const TCString Ident,
                               const TCString s_Default)
{
  long Index ;
  TCString S ;
  Index = SectionExists(Section);
  if ( Index != -1 )
  {
    Index = KeyExists(Index,Ident) ;
    if ( Index != -1 )
       S = GetValue(m_Strings[Index]) ;
  }
  if ( S == TCString("") )
     S = s_Default ;
  return S ;
}

//==========================================================================
// 函数 : WriteString
// 用途 : 写入一项参数设置串
// 原型 : void TCIniFile::WriteString(const TCString Section, const TCString Ident,
//                  const TCString srcString)
// 参数 : 参数组名，参数名，设置串
// 返回 :
// 说明 : 如果指定的组不存在则新建！
//==========================================================================
void TCIniFile::WriteString(const TCString Section, const TCString Ident,
                  const TCString srcString)
{
  long Index_s ,Index ;
  TCString S ;
  m_modify = 0 ;
  Index_s = SectionExists(Section) ;
  if ( Index_s != -1 )
  {
    Index = KeyExists(Index_s,Ident) ;
    if ( Index != -1 )
    {
       m_Strings[Index] = Ident + "=" + srcString ;
    }
    else
    {                                              
       if( Index_s == (m_Strings.GetCount()-1) )
           m_Strings.Add( Ident + "=" + srcString ) ;
       else
           m_Strings.Insert(Index_s + 1 ,Ident + "=" + srcString ) ;
    }
  }
  else
  {
    m_Strings.Add( "[" + Section + "]" ) ;
    m_Strings.Add( Ident + "=" + srcString) ;
  }
}

//==========================================================================
// 函数 : SectionExists
// 用途 : 搜索指定的参数组并返回位置索引
// 原型 : long TCIniFile::SectionExists(const TCString Section) ;
// 参数 : 参数组名
// 返回 : 参数组在m_Strings中的位置
// 说明 : 返回 -1 则说明不存在
//==========================================================================
long TCIniFile::SectionExists(const TCString Section)
{
   long nPos ;
   for ( long i = 0; i < m_Strings.GetCount() ; i++ )
   {
     if (m_Strings[i].GetLength() > 0 )
       if ( m_Strings[i][1] == '[' )
       {
         nPos = m_Strings[i].Find(']') ;
         if ( nPos != -1 )
         {
           if ( Section.CompareNoCase(m_Strings[i].Mid(2,nPos-2)) == 0 )
             return i ;
         }
       }
   }
   return -1 ;
}

//==========================================================================
// 函数 : KeyExists
// 用途 : 搜索指定的参数并返回位置索引
// 原型 : KeyExists(TCString Section,TCString Ident) ;
// 参数 : 参数组第一个参数索引，参数名
// 返回 : 参数在m_Strings中的位置
// 说明 : 返回 -1 则说明不存在
//==========================================================================
long TCIniFile::KeyExists(long Index_s ,TCString Ident)
{
  TCString s_KeyName ;
  if ( Index_s != -1 )
    for ( long i = Index_s + 1; i < m_Strings.GetCount() ; i++ )
    {
      if (m_Strings[i].GetLength() > 0 )
      {
         if ( m_Strings[i][1] != '[' )
         {
           s_KeyName = GetKeyName( m_Strings[i] ) ;
           if ( Ident.CompareNoCase(s_KeyName) == 0 )
              return i ;
         }
         else return  -1 ;
      }
    }
  return -1 ;
}
//==========================================================================
// 函数 : GetKeyName
// 用途 : 从设置行中取得参数名
// 原型 : TCString TCIniFile::GetKeyName(const TCString S_line) ;
// 参数 : 源字符串
// 返回 : 参数名
// 说明 : 参数和值之间以'=' 分隔 ';'和'#'表示为注释
//        没有'='符号则说明未设置值
//==========================================================================
TCString TCIniFile::GetKeyName(const TCString S_line)
{
  long nPos ;
  TCString S ;
  if ( (S_line.GetLength() > 0)&&
        (S_line[1] != ';')&&(S_line[1] != '#') )
  {
    nPos = S_line.Find('=') ;
    if ( nPos != -1 )
    {
      S = S_line.Mid(1,nPos-1) ;
      S.TrimRight() ;
    }
    else
    {
       nPos = S_line.Find(';') ;
       if ( nPos != -1 )
       {
          S = S_line.Mid(1,nPos-1) ;
          S.TrimRight() ;
       }
       else
       {
          nPos = S_line.Find('#') ;
          if ( nPos != -1 )
          {
             S = S_line.Mid(1,nPos-1) ;
             S.TrimRight() ;
          }
          else
             S = S_line ;
       }
    }
  }
  return S ;
}

//==========================================================================
// 函数 : GetValue
// 用途 : 从设置行中取得设置值
// 原型 : TCString TCIniFile::GetValue(const TCString S_line)
// 参数 : 源字符串
// 返回 : 设置值的字符串
// 说明 : 参数和值之间以'='分隔 ，';'或者'#'后面表示为注释！
//==========================================================================
TCString TCIniFile::GetValue(const TCString S_line)
{
  long nPos ;
  TCString S ;
  nPos = S_line.Find('=') ;
  if ( nPos != -1 )
  {
     S = S_line.Mid( nPos + 1) ;
     S.TrimLeft() ;
     if (S.GetLength() > 0 )
     {
       if ( S[1] == '"' )
       {
         S = (char *)S + 1 ;
         nPos = S.Find('"') ;
         if ( nPos != -1 )
            S = S.Mid(1,nPos-1) ;
       }
       else
       {
         nPos = S.Find(';') ;   //后面是注释
         if ( nPos != -1 )
           S = S.Mid(1,nPos-1) ;
         else
         {
           nPos = S.Find('#') ;   //后面是注释
           if ( nPos != -1 )
              S = S.Mid(1,nPos-1) ;
         }
       }
     }
     S.TrimRight() ;
  }
  return S ;

}

//==========================================================================
// 函数 : ProfileBool
// 用途 : 得到配置文件的配置Bool值
// 原型 : bool ProfileBool(const TCString FileName,
//              const TCString Section,const TCString Ident, bool bDefault,
//              bool bThrowException = false);
// 参数 : 文件名，段值，键值，缺省布尔值, 如果没有设置是否抛出例外
// 返回 : 配置文件的布尔值
// 说明 : 1. 如果文件未配置该项或配置的值不符合要求，则返回缺省布尔值;
//        2. 判断配置项的第一个字符，为1,T,t,Y,y则返回真，为0,F,f,N,n返回假
//==========================================================================
bool ProfileBool(const TCString FileName,const TCString Section,
        const TCString Ident, bool bDefault,
        bool bThrowException)
{
    TCString sReadString;
    char cFirstChar;

    sReadString = ProfileString(FileName, Section, Ident, "__S_DEF");

    if (sReadString == TCString("__S_DEF") )
        goto NotSetValue;

    if (Length(sReadString) < 1)
        goto NotSetValue;
    else
    {
        cFirstChar = sReadString[1];
        if (cFirstChar == '1' || cFirstChar == 'T' || cFirstChar == 't'
                || cFirstChar == 'Y' || cFirstChar== 'y')
            return true;

        if (cFirstChar == '0' || cFirstChar == 'F' || cFirstChar == 'f'
                || cFirstChar == 'N' || cFirstChar== 'n')
            return false;

        else
            goto NotSetValue;
    }

NotSetValue:
    if (bThrowException)
        throw TCException("ProfileBool() : Value Not Set - "
                + FileName + " * " + Section + " * " + Ident);
    else
        return bDefault;
}

long ProfileInt(const TCString FileName,const TCString Section,const TCString Ident,
                               long nDefault)
{
    TCString sReadString;

    sReadString = ProfileString(FileName, Section, Ident, IntToStr(nDefault));

    return StrToInt(sReadString);
}

TCString ProfileString(const TCString FileName,const TCString Section,const TCString Ident,
                               const TCString s_Default)
{
    TCIniFile CIniFile ;
    CIniFile.Load(FileName) ;
    return CIniFile.ReadString(Section,Ident,s_Default) ;
}

void ProfileSession(const TCString FileName,const TCString Section,
                        TCStringList & pStrings)
{
    TCIniFile CIniFile ;
    CIniFile.Load(FileName) ;
    CIniFile.ReadSection(Section,pStrings) ;
}

void ProfileWriteString(const TCString FileName,const TCString Section,const TCString Ident,
                               const TCString sValue)
{
    TCIniFile CIniFile ;
    CIniFile.Load(FileName) ;
    CIniFile.WriteString(Section,Ident,sValue) ;
}

