
#include <string.h>
#include <assert.h>
#include "sortMerge.h"

// Error Protocall:

static const char* ErrMsgs[] = 	{
	"Error: Sort Failed.",
	"Error: HeapFile Failed."
};

static error_string_table ErrTable( JOINS, ErrMsgs );

sortMerge::sortMerge(
		char*           filename1,      // Name of heapfile for relation R
		int             len_in1,        // # of columns in R.
		AttrType        in1[],          // Array containing field types of R.
		short           t1_str_sizes[], // Array containing size of columns in R
		int             join_col_in1,   // The join column of R 

		char*           filename2,      // Name of heapfile for relation S
		int             len_in2,        // # of columns in S.
		AttrType        in2[],          // Array containing field types of S.
		short           t2_str_sizes[], // Array containing size of columns in S
		int             join_col_in2,   // The join column of S

		char*           filename3,      // Name of heapfile for merged results
		int             amt_of_mem,     // Number of pages available
		TupleOrder      order,          // Sorting order: Ascending or Descending
		Status&         s               // Status of constructor
		){
}

sortMerge::~sortMerge()
{

}
