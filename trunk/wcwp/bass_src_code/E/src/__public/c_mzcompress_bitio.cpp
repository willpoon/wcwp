#include <stdio.h>
#include <stdlib.h>

#include "c_mzcompress.h"
#include "c_mzcompress_bitio.h"

BIT_FILE *OpenOutputBitFile(char *name)
{
	BIT_FILE *bit_file;
	
	bit_file = (BIT_FILE *)calloc(1, sizeof(BIT_FILE));
	if(bit_file == NULL)
		return(bit_file);
	bit_file->file = fopen(name, "wb");
	bit_file->rack = 0;
	bit_file->mask = 0x80;
	bit_file->pacifier_counter = 0;
	return(bit_file);
}

BIT_FILE *OpenInputBitFile(char *name)
{
	BIT_FILE *bit_file;
	
	bit_file = (BIT_FILE *)calloc(1, sizeof(BIT_FILE));
	if(bit_file == NULL)
		return(bit_file);
	bit_file->file = fopen(name, "rb");
	bit_file->rack = 0;
	bit_file->mask = 0x80;
	bit_file->pacifier_counter = 0;
	return(bit_file);
}

void CloseOutputBitFile(BIT_FILE *bit_file)
{
	if(bit_file->mask != 0x80)
		if(putc(bit_file->rack, bit_file->file) != bit_file->rack)
			throw TCCompressException("Fatal error in ClosebitFile!");
	fclose(bit_file->file);

	free((char *)bit_file);
}

void CloseInputBitFile(BIT_FILE *bit_file)

{
	fclose(bit_file->file);
	free((char *)bit_file);
}

void OutputBit(BIT_FILE *bit_file,int bit)
{
	if(bit)
		bit_file->rack |= bit_file->mask;
	bit_file->mask>>=1;
	if(bit_file->mask == 0)
	{
		if(putc(bit_file->rack, bit_file->file) != bit_file->rack)
			throw TCCompressException("Fatal error in OutputBits");
		else
			if((bit_file->pacifier_counter++&4095) == 0)
				TCMZCompress::Putc('.');
		bit_file->rack = 0;
		bit_file->mask = 0x80;
	}
}

void OutputBits(BIT_FILE *bit_file,unsigned long code,int count)
{
	unsigned long mask;

	mask = 1L<<(count - 1);
	while(mask != 0)
	{
		if(mask&code)
			bit_file->rack |= bit_file->mask;
		bit_file->mask >>= 1;
		if(bit_file->mask == 0)
		{
			if(putc(bit_file->rack, bit_file->file) != bit_file->rack)
				throw TCCompressException("Fatal error in OutputBit");
			else
				if((bit_file->pacifier_counter++&2047) == 0)
					TCMZCompress::Putc('.');
			bit_file->rack = 0;
			bit_file->mask = 0x80;
		}
		mask>>=1;
	}
}

int InputBit(BIT_FILE *bit_file)
{
	int value;
	
	if(bit_file->mask == 0x80)
	{
		bit_file->rack = getc(bit_file->file);
		if(bit_file->rack == EOF)
			throw TCCompressException("Fatal Error in InputBit");
		if((bit_file->pacifier_counter++&2047) == 0)
			TCMZCompress::Putc('.');
	}
	value = bit_file->rack&bit_file->mask;
	bit_file->mask>>=1;
	if(bit_file->mask == 0)
		bit_file->mask = 0x80;
	return(value?1:0);
}

unsigned long InputBits(BIT_FILE *bit_file,int bit_count)
{
	unsigned long mask;
	unsigned long return_value;
	
	mask = 1L<<(bit_count - 1);
	return_value = 0;
	while(mask != 0)
	{
		if(bit_file->mask == 0x80)
		{
			bit_file->rack = getc(bit_file->file);
			if(bit_file->rack == EOF)
				throw TCCompressException("Fatal error in InputBit");
			if((bit_file->pacifier_counter++&2047) == 0)
				TCMZCompress::Putc('.');
		}
		if(bit_file->rack&bit_file->mask)
			return_value |= mask;
		mask >>= 1;
		bit_file->mask >>= 1;
		if(bit_file->mask == 0)
			bit_file->mask = 0x80;
	}
	return(return_value);
}

void FilePrintBinary(FILE *file,unsigned int code,int bits)
{
	unsigned int mask;
	mask = 1<<(bits - 1);
	while(mask != 0)
	{
		if(code&mask)
			fputc('1', file);
		else
			fputc('0', file);
		mask>>=1;
	}
}

