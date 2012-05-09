#ifndef c_mzcompress_lzw15vH 
#define c_mzcompress_lzw15vH

void CompressFile(FILE *input, BIT_FILE *output);
void ExpandFile(BIT_FILE *input, FILE *output);

#endif
