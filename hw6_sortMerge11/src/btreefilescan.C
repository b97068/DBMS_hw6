/*
 * btreefilescan.cc - function members of class BTreeFileScan
 *
 * Johannes Gehrke & Gideon Glass  951022  CS564  UW-Madison
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
//#include "btfile.h"
#include "btreefilescan.h"

/*
 * Note: BTreeFileScan uses the same errors as BTREE since its code basically
 * BTREE things (traversing trees).
 */

/*
 * BTreeFileScan::~BTreeFileScan ()
 *
 * unpin current page if it's not unpinned already.
 */

BTreeFileScan::~BTreeFileScan ()
{
	if (leafp != NULL) {
		Status st = MINIBASE_BM->unpinPage(leafp->page_no());
		if (st != OK) {
			MINIBASE_FIRST_ERROR(BTREE, BTreeFile::CANT_UNPIN_PAGE);
		}
	}
}

int BTreeFileScan::keysize()
{
	return treep->keysize();
}


/*
 * Status BTreeFileScan::get_next (RID & rid, void* keyptr)
 *
 * Iterate once (during a scan).
 *
 * Special handling: if we are at the very start, or if we deleted_current(),
 * don't use BTLeafPage::get_next, use get_current instead.
 *
 * Special handling for possibly empty leaf pages (since we don't do
 * the cool delete).
 *
 * Returns DONE (not NOMORERECS) when DONE, in accordance with what
 * main (not written by us) wants to see.
 */

Status BTreeFileScan::get_next (RID & rid, void* keyptr)
{
	RID answerRid;
	Status st;
	PageId nextpage;

	if (leafp == NULL)
		return DONE;

	if ((deletedcurrent && didfirst) || (!deletedcurrent && !didfirst)) {
		didfirst = true;
		deletedcurrent = false;
		st = leafp->get_current(curRid, keyptr, answerRid);
	}
	else {
		st = leafp->get_next(curRid, keyptr, answerRid);
	}

	while (st == NOMORERECS) {
		nextpage = leafp->getNextPage();
		st = MINIBASE_BM->unpinPage(leafp->page_no());
		if (st != OK) {
			MINIBASE_FIRST_ERROR(BTREE, BTreeFile::CANT_UNPIN_PAGE);
			return FAIL;
		}

		if (nextpage == INVALID_PAGE) {
			leafp = NULL;
			return DONE;
		}

		st = MINIBASE_BM->pinPage(nextpage, (Page *&) leafp);
		if (st != OK) {
			MINIBASE_FIRST_ERROR(BTREE, BTreeFile::CANT_PIN_PAGE);
			return FAIL;
		}

		st = leafp->get_first(curRid, keyptr, answerRid);
	}

	if (endkey && keyCompare(keyptr, endkey, treep->headerPage->key_type) > 0) {
		// went past right end of scan
		st = MINIBASE_BM->unpinPage(leafp->page_no());
		if (st != OK)
			MINIBASE_FIRST_ERROR(BTREE, BTreeFile::CANT_UNPIN_PAGE);
		return DONE;
	}

	rid = answerRid;
	return OK;
}

/*
 * Status BTreeFileScan::delete_current ()
 *
 * Delete currently-being-scanned data entry.  (Surprising, eh?)
 *
 * Real work done by BTLeafPage::delUserRid; we just have to
 * pin and unpin the page to effect a dirty bit being clocked into
 * the buffer manager.
 *
 * Also, set the deletedcurrent flag so get_next knows how to advance.
 */

Status BTreeFileScan::delete_current ()
{
	Status st;
	Keytype curkey;
	bool deleted;
	RID dataRid;
	BTLeafPage *dupPagePtr;

	if (leafp == NULL) {
		return MINIBASE_FIRST_ERROR(BTREE, BTreeFile::INVALID_SCAN);
	}

	st = MINIBASE_BM->pinPage(leafp->page_no(), (Page *&) dupPagePtr);
	if (st != OK)
		return MINIBASE_FIRST_ERROR(BTREE, BTreeFile::CANT_PIN_PAGE);
	assert(dupPagePtr == leafp);

	st = leafp->get_current(curRid, &curkey, dataRid);
	// if st != OK, they tried to delete after going past all the scanned recs
	if (st != OK) {
		MINIBASE_FIRST_ERROR(BTREE, BTreeFile::DELETE_CURRENT_FAILED);
		MINIBASE_BM->unpinPage(leafp->page_no());  // undo above 2nd pin
		return st;
	}

	deleted = leafp->delUserRid(&curkey, treep->headerPage->key_type, dataRid);
	assert(deleted == true);  // we know curRid is on this page, and that
	// get_current must return the key corresponding to it

	st = MINIBASE_BM->unpinPage(leafp->page_no(), 1 /* DIRTY */);
	if (st != OK)
		return MINIBASE_FIRST_ERROR(BTREE, BTreeFile::CANT_UNPIN_PAGE);

	deletedcurrent = true;
	return OK;
}
