//---------------------------------------------------------------------------
#pragma hdrstop

#include "cmpublic.h"
//---------------------------------------------------------------------------
#pragma package(smart_init)

//==========================================================================
// ���� : TCIniFile
// ��; : TCIniFile�๹�캯��
// ԭ�� : TCIniFile(const TCString FileName) ;
// ���� : �ļ�����
// ���� :
// ˵�� : ȡ�ļ����ƣ���ʼ��m_load,m_modify����
//==========================================================================
TCIniFile::TCIniFile()
{
  m_FileName = "" ;
  m_Strings.Clear() ;
  m_load = -1 ;
  m_modify = -1;
}
//==========================================================================
// ���� : ~TCIniFile
// ��; : TCIniFile����������
// ԭ�� : ~TCIniFile() ;
// ���� :
// ���� :
// ˵�� : �ж�INI�ļ������Ƿ��Ѿ��޸Ĺ�������޸�����̣�
//==========================================================================
TCIniFile::~TCIniFile()
{
  if ( (m_load == 0)&&( m_modify == 0 ) )  //INI�ļ������Ѿ��޸�
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
// ���� : Load
// ��; : ��INI�ļ���m_Strings ��ȥ����ͷ�ո�
// ԭ�� : int TCIniFile::Load(void)
// ���� :
// ���� : ��ȡ�ɹ�����ʧ��
// ˵�� : �ɹ�����0 ,���򷵻�-1
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
// ���� : DeleteKey
// ��; : ɾ��һ����������
// ԭ�� : int TCIniFile::DeleteKey(TCString Section,TCString Ident) ;
// ���� : ��������,������
// ���� : �ɹ�����ʧ��
// ˵�� : �ɹ�����0 ,���ָ���Ĳ����������򷵻�-1
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
// ���� : EraseSection
// ��; : ɾ��һ����������
// ԭ�� : int TCIniFile::EraseSection(TCString Section);
// ���� : ��������
// ���� : �ɹ�����ʧ��
// ˵�� : �ɹ�����0 ,���ָ�����鲻�����򷵻�-1
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
// ���� : ReadSection
// ��; : ȡ��ָ������������в�������
// ԭ�� : int TCIniFile::ReadSection(const TCString Section,TCStringList * pStrings) ;
// ���� : �������� �� ������ָ��
// ���� : -1,0
// ˵�� : ���ָ���Ĳ����鲻�����򷵻�-1
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
// ���� : ReadSections
// ��; : ȡ�����еĲ�������
// ԭ�� : void TCIniFile::ReadSections(TCStringList * pStrings)
// ���� : ��������������String Listָ��
// ���� :
// ˵�� :
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
// ���� : ReadSectionValues
// ��; : ȡ��ָ�����������������ֵ
// ԭ�� : int TCIniFile::ReadSectionValues(const TCString Section,TCStringList * pStrings)
// ���� : ������������������ֵ��String Listָ��
// ���� : 0����-1
// ˵�� :  ���ָ�����鲻�����򷵻�-1
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
// ���� : ReadString
// ��; : ��һָ�������ô�
// ԭ�� : ReadString(const TCString Section, const TCString Ident,
//                   const TCString s_Default)
// ���� : ������������������ȱʡ����
// ���� : ����ֵ���ַ���
// ˵�� : ���ָ�������ò����ڻ�Ϊ���򷵻�ȱʡ���ô�
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
// ���� : WriteString
// ��; : д��һ��������ô�
// ԭ�� : void TCIniFile::WriteString(const TCString Section, const TCString Ident,
//                  const TCString srcString)
// ���� : ���������������������ô�
// ���� :
// ˵�� : ���ָ�����鲻�������½���
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
// ���� : SectionExists
// ��; : ����ָ���Ĳ����鲢����λ������
// ԭ�� : long TCIniFile::SectionExists(const TCString Section) ;
// ���� : ��������
// ���� : ��������m_Strings�е�λ��
// ˵�� : ���� -1 ��˵��������
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
// ���� : KeyExists
// ��; : ����ָ���Ĳ���������λ������
// ԭ�� : KeyExists(TCString Section,TCString Ident) ;
// ���� : �������һ������������������
// ���� : ������m_Strings�е�λ��
// ˵�� : ���� -1 ��˵��������
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
// ���� : GetKeyName
// ��; : ����������ȡ�ò�����
// ԭ�� : TCString TCIniFile::GetKeyName(const TCString S_line) ;
// ���� : Դ�ַ���
// ���� : ������
// ˵�� : ������ֵ֮����'=' �ָ� ';'��'#'��ʾΪע��
//        û��'='������˵��δ����ֵ
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
// ���� : GetValue
// ��; : ����������ȡ������ֵ
// ԭ�� : TCString TCIniFile::GetValue(const TCString S_line)
// ���� : Դ�ַ���
// ���� : ����ֵ���ַ���
// ˵�� : ������ֵ֮����'='�ָ� ��';'����'#'�����ʾΪע�ͣ�
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
         nPos = S.Find(';') ;   //������ע��
         if ( nPos != -1 )
           S = S.Mid(1,nPos-1) ;
         else
         {
           nPos = S.Find('#') ;   //������ע��
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
// ���� : ProfileBool
// ��; : �õ������ļ�������Boolֵ
// ԭ�� : bool ProfileBool(const TCString FileName,
//              const TCString Section,const TCString Ident, bool bDefault,
//              bool bThrowException = false);
// ���� : �ļ�������ֵ����ֵ��ȱʡ����ֵ, ���û�������Ƿ��׳�����
// ���� : �����ļ��Ĳ���ֵ
// ˵�� : 1. ����ļ�δ���ø�������õ�ֵ������Ҫ���򷵻�ȱʡ����ֵ;
//        2. �ж�������ĵ�һ���ַ���Ϊ1,T,t,Y,y�򷵻��棬Ϊ0,F,f,N,n���ؼ�
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

