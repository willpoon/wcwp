//-------------------------------------------------------------------
// This is the LZW module which implements a more powerful version of
// the algorithm.  This version of the program has three major
// improvements over LZW12.CPP.  First, it expands the maximum code
// size to 15 bits.  Second, it starts encodeing with 9 bit codes,
// owrking its way up in bit size only as necessary.  Finally, it
// flushes the dictionary when done.
//-------------------------------------------------------------------

#include "cmpublic.h"
#include "c_mzcompress.h"
#include "c_mzcompress_bitio.h"

//-------------------------------------------------------------------
// Constants used throughout the program. BITS defines the maximum
// number of bits that can be used in the output code.  TABLE_SIZE
// defines the size of the dictionary.  TABLE_BANKS are the number of
// 256 element dictionary pages needed.  The code defines should be
// self-expanatory.
//-------------------------------------------------------------------
#define TABLE_SIZE  35023L
#define TABLE_BANKS  ((TABLE_SIZE >> 8) + 1)

const int BITS = 15;
const int MAX_CODE = ((1 << BITS) - 1);
const int END_OF_STREAM = 256;
const int BUMP_CODE = 257;
const int FLUSH_CODE = 258;
const int FIRST_CODE = 259;
const int UNUSED = -1;

//-------------------------------------------------------------------
// Local prototypes
//-------------------------------------------------------------------
void InitializeDictionary(void);
void InitializeStorage(void);

//====(2000.07.03)Add This Routine to release storage=======
void ReleaseStorage(void);

WORD find_child_node(int parent_code, int child_character);
WORD decode_string(WORD offset, WORD code);

char *CompressionName = "LZW 15 Bit Variable Rate Encoder";
char *Usage = "in_file out_file \n\n";

//-------------------------------------------------------------------
// This data structure defines the dictionary. Each entry in the
// dictionary has a code value. This is the code emitted by the
// compressor. Each code is actually made up of two pieces: a
// parent_code, and a character. Code values of less than 256 are
// actually plain text codes.
// Note that in order to handle 16 bit segmented compilers, such as
// most of the MS-DOS compilers, it was necessary to break up the
// dictionary into a table of smaller dictionary pointers.  Every
// reference to the dictionary was replaced by a macro that did a
// pointer dereference first.  By breaking up the index along byte
// boundaries we should be as efficient as possible.
//-------------------------------------------------------------------
struct dictionary
{	int code_value;
	int parent_code;
	char character;
} *dict[TABLE_BANKS];

#define DICT(i) dict[i>>8][i&0xff]

//-------------------------------------------------------------------
// Other global data structures.  The decode_stack is used to reverse
// strings that come out of the tree during decoding, next_code is
// the next code to be added to the dictionary, both during compression
// and decompression, current_code_bits defines how many bits are
// currently being used for output, and next_bump_code defines the code
// that will trigger the next jump in word size.
//-------------------------------------------------------------------
char decode_stack[TABLE_SIZE];
WORD next_code;
int current_code_bits;
WORD next_bump_code;

//-------------------------------------------------------------------
// This routine is used to initialize the dictionary, both when the
// compressor or decompressor first starts up, and also when a flush
// code comes in.  Note that even thought the decompressor sets all
// the code_value elements to UNUSED, it doesn't really need to.
//-------------------------------------------------------------------
void InitializeDictionary(void)
{	WORD i;
	for (i=0; i<TABLE_SIZE; i++)
		DICT(i).code_value = UNUSED;
	next_code = FIRST_CODE;
	TCMZCompress::Putc('F');
	current_code_bits = 9;
	next_bump_code = 511;
}

//-------------------------------------------------------------------
// This routine allocates the dictionary.  Since the total size of the
// dictionary is much larger than 64K, it can't be allocated as a single
// object.  Instead, it is allocated as a set of pointers to smaller
// dictionary objects.  The special DICT() macro is used to translate
// indices into pairs of references.
//-------------------------------------------------------------------
void InitializeStorage(void)
{
	int i;

	for (i=0; i<TABLE_BANKS; i++)
	{
		dict[i] = (struct dictionary *)calloc(256,sizeof(struct dictionary));
		if (dict[i] == NULL)
			throw TCCompressException("Error allocating dictionary space");
	}
}

void ReleaseStorage(void)
{
	int i;

	for (i=0; i<TABLE_BANKS; i++)
		free(dict[i]);
}

//-------------------------------------------------------------------
// The compressor is short and simple.  It reads in new symbols one
// at a time from the input file.  It then checks to see if the
// combination of the current symbol and the current code are already
// defined in the dictionary.  If they are not, they are added to the
// dictionary, and we start over with a new one symbol code.  If they
// are, the code for the combination of the code and character becomes
// our new code.  Note that in this enhanced version of LZW, the
// encoder needs to check the codes for boundary conditions.
//-------------------------------------------------------------------
void CompressFile(FILE *input, BIT_FILE *output)
{
	int character, string_code;
	WORD index;

	InitializeStorage();
	InitializeDictionary();
	if ((string_code = getc(input)) == EOF)
		string_code = END_OF_STREAM;
	while ((character = getc(input)) != EOF)
	{	index = find_child_node(string_code, character);
		if (DICT(index).code_value != -1)
			string_code = DICT(index).code_value;
		else
		{	DICT(index).code_value = next_code++;
			DICT(index).parent_code = string_code;
			DICT(index).character = (char)character;
			OutputBits(output, (DWORD)string_code, current_code_bits);
			string_code = character;
			if (next_code > MAX_CODE)
			{	OutputBits(output, (DWORD)FLUSH_CODE, current_code_bits);
				InitializeDictionary();
			}
			else
				if (next_code > next_bump_code)
				{
					OutputBits(output, (DWORD)BUMP_CODE, current_code_bits);
					current_code_bits++;
					next_bump_code <<= 1;
					next_bump_code |= 1;
					TCMZCompress::Putc('B');
				}
		}
	}
	OutputBits(output, (DWORD)string_code, current_code_bits);
	OutputBits(output, (DWORD)END_OF_STREAM, current_code_bits);

    ReleaseStorage();
}

//-------------------------------------------------------------------
// The file expander operates much like the encoder.  It has to read
// in codes, then convert the codes to a string of characters.  The
// only catch in the whole operation occurs when the encoder
// encounters a CHAR+STRING+CHAR+STRING+CHAR sequence.   When this
// occurs, the encoder outputs a code that is not presently defined in
// the table.  This is handled as an exception.  All of the special
// input codes are handled in various ways.
//-------------------------------------------------------------------
void ExpandFile(BIT_FILE *input, FILE *output)
{
	WORD new_code, old_code;
	int character;
	WORD count;

	InitializeStorage();
	for (;;)
	{	InitializeDictionary();
		old_code = (WORD)InputBits(input, current_code_bits);
		if (old_code == END_OF_STREAM)
        {
            ReleaseStorage();
			return;
        }
		character = old_code;
		putc(old_code, output);
		for (;;)
		{	new_code = (WORD)InputBits(input, current_code_bits);
			if (new_code == END_OF_STREAM)
            {
                ReleaseStorage();
				return;
            }
			if (new_code == FLUSH_CODE)
				break;
			if (new_code == BUMP_CODE)
			{
				current_code_bits++;
				TCMZCompress::Putc('B');
				continue;
			}
			if (new_code >= next_code)
			{
				decode_stack[0] = (char)character;
				count = decode_string(1, old_code);
			}
			else
				count = decode_string(0, new_code);
			character = decode_stack[count-1];
			while (count > 0)
				putc(decode_stack[--count], output);
			DICT(next_code).parent_code = old_code;
			DICT(next_code).character = (char)character;
			next_code++;
			old_code = new_code;
		}
	}
}

//-------------------------------------------------------------------
// This hashing routine is responsible for finding the table location
// for a string/character combination.  This table index is created by
// using an exclusive OR combination of the prefix and character.  This
// code also has to check for collisions, and handles them by jumping
// around in the table.
//-------------------------------------------------------------------
WORD find_child_node(int parent_code, int child_character)
{
#pragma warn -8071
	WORD index;
	int offset;
	index = (child_character<<(BITS-8))^parent_code;
	if (index == 0)
		offset = 1;
	else
		offset = TABLE_SIZE - index;
	for (;;)
	{
		if (DICT(index).code_value == UNUSED)
			return (WORD)index;
		if (DICT(index).parent_code == parent_code &&
				DICT(index).character == (char)child_character)
			return (index);
		if (index >= offset)
			index -= offset;
		else
			index += TABLE_SIZE-offset;
	}
#pragma warn +8071
}

//-------------------------------------------------------------------
// This routine decodes a string from the dictionary, and strores it
// in the decode_stack data structure.  It returns a count to the
// calling program of how many characters were placed in the stack.
//-------------------------------------------------------------------
WORD decode_string(WORD count, WORD code)
{
	while (code > 255)
	{
		decode_stack[count++] = DICT(code).character;
#pragma warn -8071
		code = DICT(code).parent_code;
#pragma warn +8071
	}
	decode_stack[count++] = (char)code;
	return (count);
}
