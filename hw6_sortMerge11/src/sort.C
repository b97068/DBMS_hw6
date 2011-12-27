
#include <string.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <memory.h>

#include "sort.h"
#include "heapfile.h"
#include "new_error.h"

int   tupleCmp (const void* t1, const void* t2);
int   key_size,
      key1_pos,
      key2_pos;

TupleOrder s_order;
AttrType key_type;

enum sortErrors { OPEN_HPFILE_FAILED, 	// open heap file for read or write.
	OPEN_SCAN_FAILED, 	// open scanner object fail
	INSERT_RECORD_FAILED,  // attempted to insert records to heap file
	GET_NEXT_FAILED,  	// getNext failed before end of file.
	DELETE_FILE_FAILED,	// deleteFile failed
	MERGE_MANY_TO_ONE_FAILED,	
	MERGE_NEG_NUMBER,	// tried to merge a negative number of files
	MERGE_ONE,		// merge one file -- performance issue.
	ONE_MERGE_PASS_FAILED,
	PASS_ZERO_FAILED };  	

const char* sortMsgs[] = {"Unable to open heap file.",
	"Unable to open a scan for the heap file.",
	"Failure to insert record to heap file.",
	"Get next failed before the end of the heapfile.",
	"deleteFile returned an error",
	"Failed to merge several files to one file.",
	"An attempt was made to merge a negative number of files.",
	"One file is being merged by itself.  Bad performance - FIX THIS!",
	"One of the merging passes failed.",
	"The initial pass failed."};


Sort::Sort( char*		inFile,		// Name of unsorted heapfile.
		char*		outFile,			// Name of sorted heapfile.
		int      	len_in,				// Number of fields in input records.
		AttrType 	in[],				// Array containing field types of input records.
		// i.e. index of in[] ranges from 0 to (len_in - 1)
		short    	str_sizes[],		// Array containing field sizes of input records.
		int       	fld_no,				// The number of the field to sort on.
		// fld_no ranges from 0 to (len_in - 1).
		TupleOrder 	sort_order,			// ASCENDING, DESCENDING
		int       	amt_of_buf,			// Number of buffer pages available for sorting.
		Status& 	s
	  ){
	// prepare for errors, only register errors first time sort is called
	static int messagesAdded=0;
	if (messagesAdded == 0) {
		messagesAdded=1;
		/*		if (minirel_error->registerErrorMsgs(sortMsgs,JOINS) != OK){
				cout << "Couldn't register error messages."<<endl;
				exit(1);
				}  */
	}


	int num_temp_files = 0;
	_fld_no = fld_no;
	_str_sizes = str_sizes;
	_sort_order = sort_order;
	_rec_length= 0;                //  compute the record length.
	for(int i=0;i<len_in;i++){
		_str_sizes[i] = str_sizes[i];
		_rec_length +=str_sizes[i];
	}

	key1_pos = 0;
	for (int i=0;i<fld_no;i++){
		key1_pos+=str_sizes[i];
	}
	key2_pos = key1_pos;
	_in_file = inFile;		// save the file names and how much space we have.
	_out_file = outFile;		// other info is superfluous...
	_amt_of_buf = amt_of_buf;
	s_order = _sort_order;
	key_size = _str_sizes[_fld_no];
	key_type = in[_fld_no];

	s = _pass_one(num_temp_files);   // does the quick sort pass
	if (s!=OK) {
		MINIBASE_CHAIN_ERROR(JOINS,s);
		return;
	}

	if (num_temp_files != 1) s = _merge(num_temp_files);  // does the merges
	// any error in _merge will be registered in _merge, and we're exiting anyway...
}

//*************************************************************************
// 	_pass_one does the quicksorting into runs pass.  Returns the number of 
//		temporary files created in num_temp_file.
//*************************************************************************
Status Sort::_pass_one(int& num_temp_file)
{
	Status status;
	num_temp_file = 0;	// how many sorted runs does this pass create	
	RID sortRID; 		// only used as place holder
	int sortlen = _amt_of_buf*PAGESIZE; 			// The byte size of memory allocated.
	char* _sort_area = new char[sortlen]; 			// Allocated memory.


	HeapFile hpfile(_in_file, status);				// open heap file.
	if(status !=OK){                                // Error test.
		MINIBASE_CHAIN_ERROR(JOINS,status);
		delete _sort_area; 
		return status;
	}
	int num_recds_infile = hpfile.getRecCnt();

	Scan* _scan_hpfile = hpfile.openScan(status);     // open scan for heap file.
	if(status !=OK){
		MINIBASE_CHAIN_ERROR(JOINS,status);
		delete _sort_area;
		delete _scan_hpfile;  
		return status;
	}

	int num_recds_per_run = sortlen/_rec_length;

	int num_left = num_recds_infile;

	// each pass through loop writes one run.
	while(num_left > 0){
		int index = 0;
		int num_in_this_file = (num_left < num_recds_per_run) ? num_left : num_recds_per_run;

		// read in the records for this run
		for(int i=0; i<num_in_this_file;i++, index +=_rec_length){
			status = _scan_hpfile->getNext(sortRID,&_sort_area[index],_rec_length);
			if (status != OK) {
				MINIBASE_CHAIN_ERROR(JOINS,status);
				// get next failed BEFORE the end of the file.  This is fatal.
				delete _sort_area;
				delete _scan_hpfile; 
				return status;
			}		
		}

		qsort(_sort_area,num_in_this_file,_rec_length,tupleCmp); // qsort.

		// write the records to the temporary file.  (If all fit into one file, write directly
		//	to the output file)
		Status sss;
		HeapFile *tmphpfile;	
		if (num_recds_infile<=num_recds_per_run){
			tmphpfile = new HeapFile(_out_file,sss); // create heap file
			num_temp_file = 1;
		} else {		
			char* tempname;
			tempname = _temp_name(0,num_temp_file++,_out_file); // create temp filename.
			tmphpfile = new HeapFile(tempname,sss); // create heap file
			delete tempname;
		}
		if(sss!=OK){
			MINIBASE_CHAIN_ERROR(JOINS,sss);
			delete _sort_area;
			delete tmphpfile;
			delete _scan_hpfile; 
			return sss;
		}
		index = 0;
		for (int i=0; i<num_in_this_file;i++, index += _rec_length){
			sss = tmphpfile->insertRecord(&_sort_area[index],_rec_length,sortRID);
			if(sss!=OK){
				MINIBASE_CHAIN_ERROR(JOINS,sss);
				delete _sort_area;
				delete tmphpfile;
				delete _scan_hpfile; 
				return FAIL;
			}
		}
		delete tmphpfile;	
		num_left -= num_in_this_file;
	}
	delete _sort_area;
	delete _scan_hpfile;
	return OK;
}

//*********************************************************************************
//	_temp_name : given an output file, a pass number, and a file number within the
//		pass, this creates the unique name for that file.
//	File 7 in pass 3 for output file FOO will be named
//		FOO.sort.temp.3.7
//*********************************************************************************       
char* Sort::_temp_name(int pass, int run, char* file_name)
{
	char* name = new char[strlen(file_name)+20];
	sprintf(name,"%s.sort.temp.%d.%d",file_name,pass,run);
	return name;
}

// Beginning pass 2.

//*********************************************************************************
//	_merge_many_to_one : given source heapfiles, the number of heapfiles and a 
// 		destination file, this merges the source heapfiles into the destination
//		heapfile.  The main workhorse for the merging.
//	NOTE : Due to concern that getNext might return a non-OK value for
//		some reason other than the end of file, we explicitly count through
//		the records.  This cound is only marginally different from the code
//		that relies on getNext, and is more robust.  
//*********************************************************************************       
Status Sort::_merge_many_to_one(unsigned int number, 
		HeapFile** source, HeapFile* dest) {
	Scan *scan[_amt_of_buf-1];  // array of pointers to scans
	char nextRecord[_amt_of_buf-1][_rec_length]; // array of records
	RID rid[_amt_of_buf-1];

	int num_left[_amt_of_buf-1];   // how many records in each file to be merged
	unsigned int i;
	for (i=0; i<number; i++){
		num_left[i]=source[i]->getRecCnt();
	}

	i=0;	
	Status status;
	while(i<number){
		scan[i] = source[i]->openScan(status);
		if (status != OK) {
			MINIBASE_CHAIN_ERROR(JOINS,status);
			break;  // will clean up and return below
		}
		status = scan[i]->getNext(rid[i],nextRecord[i],_rec_length);
		if (status !=OK){
			MINIBASE_CHAIN_ERROR(JOINS,status);
			break;  // will cleanup and return below
		}
		i++;
	}
	if (status != OK) {
		// already registered the error above.  This is just for cleanup.
		for(i=0; i<number; i++){
			if (scan[i] != NULL) delete scan[i];	
		}
		return status;
	}

	// now all set up, so start looping through
	while(1){
		// each time check to see which is least
		int leastIndex = -1;
		for (i=0; i<number; i++){
			if ((leastIndex == -1) && (num_left[i] != 0)){
				// i is first non-empty file
				leastIndex = i;
			} else if (num_left[i] != 0){
				if (tupleCmp(nextRecord[leastIndex],nextRecord[i]) > 0 ){
					// i is better than leastIndex
					leastIndex = i;
				}

			}
		}
		if (leastIndex == -1) break;  // done 

		// write nextRecord[leastIndex] to dest
		status = dest->insertRecord(nextRecord[leastIndex],_rec_length,rid[leastIndex]);
		if (status != OK) {
			MINIBASE_CHAIN_ERROR(JOINS,status);
			for (i=0; i<number; i++)
				if (scan[i] != NULL) delete scan[i];
			return FAIL;
		}

		num_left[leastIndex]--;   // one less record left
		if (num_left[leastIndex] != 0) {
			// records left in this file, so read in one more
			status = scan[leastIndex]->getNext(rid[leastIndex],
					nextRecord[leastIndex],_rec_length);
			if (status != OK) {
				// We're not at end of file since num_left != 0
				// So this is a real error
				MINIBASE_CHAIN_ERROR(JOINS,status);
				return status;	
			}
		}
	}
	for(i=0; i<number; i++){
		delete scan[i];	
	}
	return OK;
}

//*********************************************************************************
//	This goes through one later pass, merging the files from the previous pass into
// a smaller number of larger files.  It needs to know how many files were in the previous
// pass and what the pass number is (for naming purposes).  It returns the number of files
// it created in "numberDest"
//	This just sets cycles through setting up the heapfiles and then calls 
// _merge_many_to_one.
//*********************************************************************************       
Status Sort::_one_later_pass(int numberTempFiles, int passNum, int &numberDest){
	int first;
	int numberToMerge;
	Status status;
	HeapFile *source[_amt_of_buf -1]; // pointers to HeapFile's
	HeapFile *dest;  // the file to write out to.

	first = 0;
	numberDest=0;
	while (first < numberTempFiles){
		numberToMerge =(numberTempFiles-first < _amt_of_buf -1) ?
			numberTempFiles-first: _amt_of_buf-1;
		// get the heap files to merge together
		for (int i=first; i<first+numberToMerge; i++){
			char* name = _temp_name(passNum-1,i,_out_file);
			source[i-first] = new HeapFile (name, status);  // open heap file.
			if (status != OK) {
				MINIBASE_CHAIN_ERROR(JOINS,status);
				for (int j = first; j<=i; j++ ) delete source[j-first];
				return status;
			}
			delete name;
		}

		if (numberTempFiles <= _amt_of_buf-1){
			dest = new HeapFile(_out_file,status);  // merge into final output
		} else {
			char* name = _temp_name(passNum,numberDest,_out_file);
			dest = new HeapFile (name, status);  // open heap file.
			delete name;
		}

		if (status != OK) {
			MINIBASE_CHAIN_ERROR(JOINS,status);
			for (int i=first; i<first+numberToMerge; i++) delete source[i-first];
			delete dest;
			return status;
		}
		status = _merge_many_to_one(numberToMerge,source,dest);
		for (int i = first; i<first+numberToMerge;i++) {
			Status s = source[i-first]->deleteFile();
			if (s != OK) {
				MINIBASE_CHAIN_ERROR(JOINS,s);
				// log error, but keep going?
			}
			delete source[i-first];
		}
		delete dest;
		if (status != OK) {
			MINIBASE_CHAIN_ERROR(JOINS,status);
			return status;
		}
		first += numberToMerge;
		numberDest++;
	}
	return OK;
}


//*********************************************************************************************
// 	_merge repeats calling _one_later_pass until only one file is left
//*********************************************************************************************
Status Sort::_merge(int numFiles){
	if (numFiles <=0) {
		// big error
		MINIBASE_CHAIN_ERROR(JOINS,FAIL);
		return FAIL;
	}
	if (numFiles == 1){
		// take care of this in pass 1.  Performance issue, not correctness issue.  This should
		// never happen, but keeping internal error code just in case
		MINIBASE_CHAIN_ERROR(JOINS,FAIL);
		// go ahead and merge this into the output file, but log error so that this
		// problem can be addressed
	}
	int lastPass = 0;
	while (numFiles >=1){  // we allow =1 in case pass one only returned one file...
		int numNewFiles;
		Status s = _one_later_pass(numFiles,lastPass+1,numNewFiles);	
		if (s != OK) {	
			MINIBASE_CHAIN_ERROR(JOINS,s);
			return s;
		}
		numFiles = numNewFiles;
		lastPass++;
		if (numFiles == 1) break;
	}
	return OK;
}


int tupleCmp (const void* t1, const void* t2)
{
	if (s_order == Ascending) {
		switch(key_type) {
			case attrInteger:
				if (*((int*)((char*)t1 + key1_pos)) < *((int*)((char*)t2 + key2_pos)))
					return -1;
				else if (*((int*)((char*)t1 + key1_pos)) > *((int*)((char*)t2 + key2_pos)))
					return 1;
				else 
					return 0;
				break;

			case attrString:
				return strncmp((char*)t1 + key1_pos,(char*)t2 + key2_pos,key_size);
				break;
			default:
				cout << "unhandled key type";
				exit(1);
		}
	}
	else {
		switch(key_type) {
			case attrInteger:
				if (*((int*)((char*)t1 + key1_pos)) < *((int*)((char*)t2 + key2_pos)))
					return 1;
				else if (*((int*)((char*)t1 + key1_pos)) > *((int*)((char*)t2 + key2_pos)))
					return -1;
				else 
					return 0;
				break;

			case attrString:
				return strncmp((char*)t2 + key2_pos,(char*)t1 + key1_pos,key_size);
				break;
			default:
				cout << "unhandled key type";
				exit(1);
		}
	}
}



