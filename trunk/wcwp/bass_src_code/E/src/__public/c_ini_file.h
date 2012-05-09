//---------------------------------------------------------------------------
#ifndef _c_ini_fileH
#define _c_ini_fileH
//---------------------------------------------------------------------------
#include "cmpublic.h"

class TCIniFile
{
 private :
   TCStringList m_Strings ;
   TCString m_FileName ;
   int m_load ;
   int m_modify ;
   TCString GetKeyName(const TCString S_line) ;
   TCString GetValue(const TCString S_line) ;
 public :
   TCIniFile() ;
   ~TCIniFile() ;
   int Load(const TCString FileName) ;
   void CreateNew(const TCString FileName) ;
   virtual int DeleteKey(TCString Section,TCString Ident) ;
   virtual int EraseSection(TCString Section) ;
   virtual int ReadSection(const TCString Section,TCStringList & pStrings) ;
   virtual void ReadSections(TCStringList & pStrings) ;
   virtual int ReadSectionValues(const TCString Section,TCStringList & pStrings) ;

   virtual TCString ReadString(const TCString Section, const TCString Ident,
                               const TCString s_Default);
   virtual void WriteString(const TCString Section, const TCString Ident,
                  const TCString srcString) ;
   long SectionExists(const TCString Section) ;
   long KeyExists(long Index_s ,TCString Ident) ;
   TCString GetFileName()
   {
     return m_FileName ;
   };
};

TCString ProfileString(const TCString FileName,const TCString Section,const TCString Ident,
                               const TCString s_Default);
bool ProfileBool(const TCString FileName, const TCString Section,
        const TCString Ident, bool bDefault, bool bThrowException = false);
long ProfileInt(const TCString FileName,const TCString Section,const TCString Ident,
                               long nDefault);
void ProfileSession(const TCString FileName,const TCString Section,TCStringList & pStrings) ;
void ProfileWriteString(const TCString FileName,const TCString Section,const TCString Ident,
                               const TCString sValue);


#endif
