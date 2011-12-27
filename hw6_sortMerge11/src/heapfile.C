/*
 * implementation of class HeapFile
 * $Id: heapfile.C,v 1.1 1997/01/02 12:46:38 flisakow Exp $
 *
 * A heapfile is an unordered set of tuples stored on pages.
 */

#include <stdio.h>

#include "heapfile.h"
#include "hfpage.h"
#include "scan.h"
#include "buf.h"
#include "db.h"

#ifndef offsetof
#    define offsetof(ty,mem) ((size_t)&(((ty*)0)->mem))
#endif


// ******************************************************
// Error messages for the heapfile layer

static const char *hfErrMsgs[] = {
    "bad record id",
    "bad record pointer",
    "end of file encountered",
    "invalid update operation",
    "no space on page for record",
    "page is empty - no records",
    "last record on page",
    "invalid slot number",
    "file has already been deleted",
};

static error_string_table hfTable( HEAPFILE, hfErrMsgs );

extern "C" int getpid();

// ******************************************************
//  HeapFile::HeapFile (char *name, Status& returnStatus)
//
//  If the heapfile already exists in the database, get the first page.
//  If the heapfile does not yet exist, create it, get the first page.
//
HeapFile::HeapFile( const char *name, Status& returnStatus )
{
     // Give us a prayer of destructing cleanly if construction fails.
    _file_deleted = true;
    _fileName = NULL;

	
      // If the name is NULL, allocate a temporary name
    if ( name == NULL) {
        char tempname[MAX_NAME];
        static unsigned numtemps;
        sprintf( tempname, "*** temp HeapFile #%d:%u ***",
                 getpid(), numtemps++ );
        _fileName = new char[strlen(tempname)+1];
        strcpy(_fileName,tempname);
        _ftype = TEMP;
    } else {
        _fileName = new char[strlen(name)+1];
        strcpy( _fileName,name );
        _ftype = ORDINARY;
    }
    Status status;
    Page  *pagePtr;

      // The constructor gets run in two different cases.
      // In the first case, the file is new and the header page
      // must be initialized.  This case is detected via a failure
      // in the db->get_file_entry() call.  In the second case, the
      // file already exists and all that must be done is to fetch
      // the header page into the buffer pool.

      // try to open the file

	if (_ftype == ORDINARY){		
        status = MINIBASE_DB->get_file_entry(_fileName, _firstPageId);		// gets the start page number for the specified file
	}
	else{
        status = DBMGR;
	}

    if (status != OK) {
          // file doesn't exist. First create it.
        status = MINIBASE_BM->newPage(_firstPageId, pagePtr);
        if (status != OK) {
#ifdef DEBUG
            cerr << "Allocation of header page failed.\n";
#endif
            returnStatus = MINIBASE_CHAIN_ERROR( HEAPFILE, status );
            return;
        }

        status = MINIBASE_DB->add_file_entry(_fileName, _firstPageId);
        if (status != OK) {
#ifdef DEBUG
            cerr << "Could not add file entry.\n";
#endif
            returnStatus = status;
            return;
        }

        HFPage *firstPage = (HFPage*) pagePtr;
        firstPage->init(_firstPageId);

		// === new a datapage ===
		PageId  nextPageId;
		HFPage *nextPage;
		Status st;

        st = MINIBASE_BM->newPage(nextPageId, (Page *&) nextPage);
		if (st != OK){
            returnStatus = MINIBASE_CHAIN_ERROR( HEAPFILE, st );
			return;
		}
        nextPage->init(nextPageId);

        assert( firstPage->getNextPage() == INVALID_PAGE );

          // Link it into the end of the list.
        nextPage->setNextPage(INVALID_PAGE);
        firstPage->setNextPage(nextPageId);

        st = MINIBASE_BM->unpinPage(nextPageId, TRUE /*dirty*/);
		if (st != OK){
            returnStatus = MINIBASE_CHAIN_ERROR( HEAPFILE, st );
			return;
		}
		// === end ===


        status = MINIBASE_BM->unpinPage(_firstPageId, true /*dirty*/ );
        if (status != OK) {
#ifdef DEBUG
            cerr << "Unpinning of new header page failed.\n";
#endif
            returnStatus = MINIBASE_CHAIN_ERROR( HEAPFILE, status );
            return;
        }
    }

    _file_deleted = false;
    returnStatus = OK;
    // ASSERTIONS:
    // - ALL private data members of class Heapfile are valid:
    //
    //  - _firstPageId valid
    //  - _fileName valid
    //  - no datapage pinned yet
}

// *******************************************
// Destructor
HeapFile::~HeapFile()
{
    // ASSERTIONS:
    // - no pages are pinned
    // - private members of class Heapfile are valid

    if ((_ftype == TEMP) && !_file_deleted ) {
#ifdef DEBUG
        Status status =
#endif
            deleteFile();
#ifdef DEBUG
        if ( status != OK )
            cerr << "Error in deleting temporary file" << endl;
        delete [] _fileName;
#endif
        return;
    }

    delete [] _fileName;
    _fileName = NULL;
}

// *******************************************
// Wipes out the heapfile from the database permanently.  This function
// is also used by the destructor for temporary files.
Status HeapFile::deleteFile()
{
    Status status;

      // If file has already been deleted, return an error status
    if ( _file_deleted )
        return MINIBASE_FIRST_ERROR( HEAPFILE, ALREADY_DELETED );

      // Mark the deleted flag (even if it doesn't get all the way done).
    _file_deleted = true;

      // Deallocate all data pages
    PageId currentPageId = _firstPageId, nextPageId = INVALID_PAGE;
    HFPage *currentPage;

    while (currentPageId != INVALID_PAGE) {

        status = MINIBASE_BM->pinPage(currentPageId, (Page*&)currentPage);
        if ( status != OK )
            return MINIBASE_CHAIN_ERROR( HEAPFILE, status );

        nextPageId = currentPage->getNextPage();

        status = MINIBASE_BM->freePage(currentPageId);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR( HEAPFILE, status );

        currentPageId = nextPageId;
    }

      // Deallocate the file entry and header page.
    status = MINIBASE_DB->delete_file_entry( _fileName );
    if ( status != OK )
        return MINIBASE_CHAIN_ERROR( HEAPFILE, status );

    return OK;
}

// *******************************************
// Return number of records in heap file
int HeapFile::getRecCnt()
{
    Status  status = OK;
    int     answer = 0;
    PageId  currentPageId = _firstPageId, nextPageId = INVALID_PAGE;
    HFPage *currentPage;

    while ((status == OK) && (currentPageId != INVALID_PAGE)) {

        status = MINIBASE_BM->pinPage(currentPageId,(Page*&)currentPage);
        if ( status != OK )
            break;

        answer += currentPage->num_recs();

        nextPageId = currentPage->getNextPage();

        status = MINIBASE_BM->unpinPage( currentPageId );

        currentPageId = nextPageId;
    }

    if ( status != OK ) {
        MINIBASE_CHAIN_ERROR( HEAPFILE, status );
        answer = -1;
    }

    return answer;
}

// *******************************************
// Insert a record into the file
Status HeapFile::insertRecord(char *recPtr, int recLen, RID& outRid)
{
    Status st, status;

    PageId  currentPageId = _firstPageId;
    HFPage *currentPage;
    PageId  nextPageId, lastPageId = INVALID_PAGE;
    HFPage *nextPage;

	st = MINIBASE_BM->pinPage(currentPageId, (Page *&) currentPage);
    if (st != OK)
        return MINIBASE_CHAIN_ERROR( HEAPFILE, st );
    nextPageId = currentPage->getNextPage();

	st = MINIBASE_BM->unpinPage(currentPageId,(status == OK));
    if (st != OK)
        return MINIBASE_CHAIN_ERROR( HEAPFILE, st );
	currentPageId = nextPageId;

    // Search for a datapage with enough free space.
    while (currentPageId != INVALID_PAGE) {

        st = MINIBASE_BM->pinPage(currentPageId, (Page *&) currentPage);
        if (st != OK)
            return MINIBASE_CHAIN_ERROR( HEAPFILE, st );

        status = currentPage->insertRecord(recPtr, recLen, outRid);

        nextPageId = currentPage->getNextPage();

        st = MINIBASE_BM->unpinPage(currentPageId,(status == OK));
        if (st != OK)
            return MINIBASE_CHAIN_ERROR( HEAPFILE, st );

        if (status == OK)
            break;

        lastPageId = currentPageId;
        currentPageId = nextPageId;
    }

        // We didn't find a data page to insert the record on.
        // Make a new one, and link it in at the end of the list.
    if (currentPageId == INVALID_PAGE) {

        currentPageId = lastPageId;

        st = MINIBASE_BM->pinPage(currentPageId, (Page *&) currentPage);
        if (st != OK)
            return MINIBASE_CHAIN_ERROR( HEAPFILE, st );

        st = MINIBASE_BM->newPage(nextPageId, (Page *&) nextPage);
        if (st != OK)
            return MINIBASE_CHAIN_ERROR( HEAPFILE, st );
        nextPage->init(nextPageId);

        assert( currentPage->getNextPage() == INVALID_PAGE );

          // Link it into the end of the list.
        nextPage->setNextPage(INVALID_PAGE);
        currentPage->setNextPage(nextPageId);

        st = MINIBASE_BM->unpinPage(currentPageId,TRUE /*dirty*/);
        if (st != OK)
            return MINIBASE_CHAIN_ERROR( HEAPFILE, st );

        status = nextPage->insertRecord(recPtr, recLen, outRid);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR( HEAPFILE, st );

        st = MINIBASE_BM->unpinPage(nextPageId,TRUE /*dirty*/);
        if (st != OK)
            return MINIBASE_CHAIN_ERROR( HEAPFILE, st );
    }

    return OK;
}

// *******************************************
// read record from file, returning pointer and length
Status HeapFile::getRecord (const RID& rid, char *recPtr, int& recLen)
{
  Status st;
  PageId  dataPageId = _firstPageId, nextPageId;
  HFPage *datapage = NULL;
  bool    found = false;

  while (!found && (dataPageId != INVALID_PAGE)) {
      st = MINIBASE_BM->pinPage(dataPageId,(Page*&)datapage);
      if (st != OK)
          return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );

      nextPageId = datapage->getNextPage();

      if (dataPageId == rid.pageNo) {
          st = datapage->getRecord(rid, recPtr, recLen);
          if (st != OK) {
              MINIBASE_BM->unpinPage(dataPageId);
              return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );
          }
          found = true;
      }

      st = MINIBASE_BM->unpinPage(dataPageId);
      if (st != OK)
          return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );

      dataPageId = nextPageId;
  }

  if (found)
      return OK;
  else
      return DONE;
}

// *******************************************
// delete record from file
Status HeapFile::deleteRecord (const RID& rid)
{
  Status st = OK;

  HFPage *dataPage;
  PageId  dataPageId = _firstPageId, prevPageId, nextPageId;
  bool    found = false;

  prevPageId = nextPageId = INVALID_PAGE;

  while (!found && (dataPageId != INVALID_PAGE)) {
      st = MINIBASE_BM->pinPage(dataPageId,(Page*&)dataPage);
      if (st != OK)
          return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );

      nextPageId = dataPage->getNextPage();

      if (dataPageId == rid.pageNo) {
          st = dataPage->deleteRecord(rid);
          if (st != OK) {
              MINIBASE_BM->unpinPage(dataPageId);
              return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );
          }
          found = true;
          break;
      }

      st = MINIBASE_BM->unpinPage(dataPageId);
      if (st != OK)
          return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );

      prevPageId = dataPageId;
      dataPageId = nextPageId;
  }

  if (found) {
      if (dataPage->num_recs() > 0) {
          // more records remain on the datapage
          st = MINIBASE_BM->unpinPage(dataPageId, TRUE /*dirty*/);
          if (st != OK)
              return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );
      } else {
          // delete this empty datapage
          st = MINIBASE_BM->unpinPage(dataPageId);
          if (st != OK)
              return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );

          st = MINIBASE_BM->freePage(dataPageId);
          if (st != OK)
              return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );

          // Now need to fix next pointer of previous page.
          st = MINIBASE_BM->pinPage(prevPageId, (Page*& )dataPage);
          if (st != OK)
              return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );

          dataPage->setNextPage(nextPageId);
          st = MINIBASE_BM->unpinPage(prevPageId, TRUE /*dirty*/);
          if (st != OK)
              return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );
      }
  } else {
      return DONE;
  }

  return OK;
}

// *******************************************
// updates the specified record in the heapfile.
Status HeapFile::updateRecord (const RID& rid, char *recPtr, int recLen)
{
  Status st;

  HFPage *datapage;
  PageId  dataPageId = _firstPageId, nextPageId;
  bool    found = false;

  char *oldRecPtr = NULL;
  int   oldRecLen = 0;

  while (!found && (dataPageId != INVALID_PAGE)) {
      st = MINIBASE_BM->pinPage(dataPageId,(Page*&)datapage);
      if (st != OK)
          return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );

      nextPageId = datapage->getNextPage();

      if (dataPageId == rid.pageNo) {
          st = datapage->returnRecord(rid, oldRecPtr, oldRecLen);
          if (st != OK) {
              MINIBASE_BM->unpinPage(dataPageId);
              return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );
          }
          found = true;
          break;
      }

      st = MINIBASE_BM->unpinPage(dataPageId);
      if (st != OK)
          return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );

      dataPageId = nextPageId;
  }

  if (!found)
      return DONE;

  if (recLen != oldRecLen) {
      st = MINIBASE_BM->unpinPage(dataPageId);
      if (st != OK)
          return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );

      return MINIBASE_FIRST_ERROR( HEAPFILE, INVALID_UPDATE );
  }

    // Update the record contents
  memcpy(oldRecPtr, recPtr, recLen);

  st = MINIBASE_BM->unpinPage(dataPageId, TRUE /* = DIRTY */);
  if (st != OK)
      return  MINIBASE_CHAIN_ERROR( HEAPFILE, st );

  return OK;
}

// *******************************************
// initiate a sequential scan
Scan *HeapFile::openScan(Status& status)
{
    Scan *newScan;
    newScan = new Scan(this, status);
    if (status == OK)
        return newScan;
    else {
        delete newScan;
        return NULL;
    }
}

// *******************************************
