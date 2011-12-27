/*
 * btindex_page.cc - implementation of class BTIndexPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 */

#include "btindex_page.h"
#include <stdio.h>
#include <cstring>

const char* BTIndexPage::Errors[BTIndexPage::INDEXNR_ERRORS] = {
	"OK",
	"Insert Record Failed (BTIndexPage::insertRecord)",
};



/*
 * Status BTIndexPage::insertKey (const void *key, AttrType key_type,
 *                                PageId pageNo,
 *                                RID& rid)
 *
 * Inserts a key, page pointer value into the index node.
 * This is accomplished by a call to SortedPage::insertRecord()
 * The function also sets up the recPtr field for the call to
 * SortedPage::insertRecord().
 */

Status BTIndexPage::insertKey (const void *key,
		AttrType key_type,
		PageId pageNo,
		RID& rid)
{
	KeyDataEntry entry;
	int entry_len;

	Datatype d;
	d.pageNo = pageNo;
	make_entry(&entry, key_type, key, (nodetype) type, d, &entry_len);

	if (SortedPage::insertRecord(key_type, (char*)&entry,
				entry_len, rid ) != OK) {
		return MINIBASE_FIRST_ERROR(BTINDEXPAGE, INDEXINSERTRECFAILED);
	}

	return OK;
}

Status BTIndexPage::deleteKey (const void *key, AttrType key_type, RID& curRid)
{
	PageId pageno;
	Keytype curkey;
	Status st;

	st = get_first(curRid, (void*)&curkey, pageno);
	assert(st == OK);
	while (keyCompare(key, (void*)&curkey, key_type) > 0) {
		st = get_next(curRid, (void*)&curkey, pageno);
		if (st != OK)
			break;
	}
	if (keyCompare(key, (void*)&curkey, key_type) != 0)
		curRid.slotNo--; // we want to delete the previous key

	st = deleteRecord(curRid);
	assert(st == OK);
	return OK;
}

/*
 * Status BTIndexPage::get_page_no (const void *key, AttrType key_type,
 *                                  PageId & pageNo)
 *
 *
 * This function encapsulates the search routine to search a
 * BTIndexPage. It uses the standard search routine as
 * described on page 77 of the text.
 */

Status BTIndexPage::get_page_no(const void *key,
		AttrType key_type,
		PageId & pageNo)
{
	int i;
	for (i=slotCnt-1; i >= 0; i--) {
		if (keyCompare(key, (void*)(data+slot[-i].offset), key_type) >= 0)
		{
			get_key_data(NULL, (Datatype *) &pageNo,
					(KeyDataEntry *)(data+slot[-i].offset),
					slot[-i].length, (nodetype)type);
			return OK;
		}
	}

	pageNo = getPrevPage();
	return OK;
}

bool BTIndexPage::get_sibling(const void *key, AttrType key_type,
		PageId &pageNo, int &left)
{
	if (slotCnt == 0) // there is no sibling
		return false;

	int i;
	for (i=slotCnt-1; i >= 0; i--) {
		get_key_data(NULL, (Datatype *) &pageNo,
				(KeyDataEntry *)(data+slot[-i].offset),
				slot[-i].length, (nodetype)type);
		if (keyCompare(key, (void*)(data+slot[-i].offset), key_type) >= 0) {
			left = 1;
			if (i != 0) {
				get_key_data(NULL, (Datatype *) &pageNo,
						(KeyDataEntry *)(data+slot[-(i-1)].offset),
						slot[-(i-1)].length, (nodetype)type);
				left = 1;
				return true;
			}
			else {
				pageNo = getLeftLink();
				return true;
			}
		}
	}

	left = 0;
	get_key_data(NULL, (Datatype *) &pageNo,
			(KeyDataEntry *)(data+slot[0].offset),
			slot[0].length, (nodetype)type);

	return true;
}

/*
 * Status BTIndexPage::get_first (const void *key, PageId & pageNo)
 * Status BTIndexPage::get_next (const void *key, PageId & pageNo)
 *
 * The two functions get_first and get_next provide an
 * iterator interface to the records on a BTIndexPage.
 * get_first returns the first key, pageNo from the page,
 * while get_next returns the next key on the page.
 *
 * They simply iterate through the slot directory and extract
 * each <key,pageNo> as they go.  (Note: this could be
 * done using RecordPage::get_first/get_next, but since we
 * already know the internal page structure, we simply use that.)
 */

Status BTIndexPage::get_first(RID& rid,
		void *key,
		PageId & pageNo)
{
	if (slotCnt == 0) {
		pageNo = INVALID_PAGE;
		return NOMORERECS;
	}

	rid.pageNo = curPage;
	rid.slotNo = 0; // begin with first slot

	get_key_data(key, (Datatype *) &pageNo,
			(KeyDataEntry *)(data+slot[0].offset), slot[0].length,
			(nodetype)type);

	return OK;
}

Status BTIndexPage::get_next(RID& rid, void *key, PageId & pageNo)
{
	rid.slotNo++;

	if (rid.slotNo >= slotCnt)
	{
		pageNo = INVALID_PAGE;
		return NOMORERECS;
	}

	get_key_data(key, (Datatype *) &pageNo,
			(KeyDataEntry *)(data+slot[-rid.slotNo].offset),
			slot[-rid.slotNo].length,
			(nodetype)type);

	return OK;
}

Status BTIndexPage::adjust_key(const void *newKey, const void *oldKey,
		AttrType key_type)
{
	for (int i = slotCnt-1; i >= 0; i--) {
		if (keyCompare(oldKey, (void*)(data+slot[-i].offset), key_type) >= 0) {
			memcpy(data+slot[-i].offset, newKey, get_key_length(newKey,key_type));
			return OK;
		}
	}
	return FAIL;
}

bool BTIndexPage::redistribute(BTIndexPage *pptr, BTIndexPage *parentPtr,
		AttrType key_type, int left, const void *deletedKey)
{
	// assertion: pptr and parentPtr are  pinned

	if (left) { // 'this' is the left sibling of pptr
		if (slot[-(slotCnt-1)].length + free_space() > (MAX_SPACE-DPFIXED)/2) {
			// cannot spare a record for its underflow sibling
			return false;
		}
		else {
			// get its sibling's first record's key
			Status st;
			RID dummyRid;
			PageId dummyPageId;
			Keytype oldKey;
			pptr->get_first(dummyRid, (void*)&oldKey, dummyPageId);

			// get the entry pointing to the right sibling
			Keytype entry;
			st = parentPtr->findKey((void*)&oldKey, (void*)&entry, key_type);
			assert(st == OK);

			// get the leftmost child pointer of the right sibling
			PageId leftMostPageId = pptr->getLeftLink();

			// insert  <entry,leftMostPageId>  to its sibling
			st = pptr->insertKey((void*)&entry, key_type,
					leftMostPageId, dummyRid);
			if (st != OK)
				return false;

			// get the last record of itself
			PageId lastPageId;
			Keytype lastKey;
			get_key_data(&lastKey, (Datatype*)&lastPageId,
					(KeyDataEntry*)(data+slot[-(slotCnt-1)].offset),
					slot[-(slotCnt-1)].length, (nodetype)type);

			// set sibling's leftmostchild to be lastPageId
			pptr->setLeftLink(lastPageId);

			// delete the last record from the old page
			RID delRid;
			delRid.pageNo = page_no();
			delRid.slotNo = slotCnt-1;
			st = deleteRecord(delRid);
			assert(st == OK);

			// adjust the entry pointing to sibling in its parent
			if (deletedKey)
				st = parentPtr->adjust_key((void*)&lastKey, deletedKey,
						key_type);
			else
				st = parentPtr->adjust_key((void*)&lastKey,
						(void*)&oldKey, key_type);
			assert (st == OK);
		}
	}
	else { // 'this' is the right sibling of pptr
		if (slot[0].length + free_space() > (MAX_SPACE-DPFIXED)/2) {
			// cannot spare a record for its underflow sibling
			return false;
		}
		else {
			// get the first record
			Status st;
			PageId firstPageId;
			Keytype firstKey;
			get_key_data(&firstKey, (Datatype*)&firstPageId,
					(KeyDataEntry*)(data+slot[0].offset),
					slot[0].length, (nodetype)type);

			// get its leftmost child pointer
			PageId leftMostPageId = getLeftLink();

			// get the entry in its parent pointing to itself
			Keytype entry;
			st = parentPtr->findKey((void*)&firstKey, (void*)&entry, key_type);
			assert(st == OK);

			// insert <entry, leftMostPageId> to its left sibling
			RID dummyRid;
			st = pptr->insertKey((void*)&entry, key_type,
					leftMostPageId, dummyRid);
			if (st != OK)
				return false;

			// set its new leftmostchild
			setLeftLink(firstPageId);

			// delete the first record
			RID delRid;
			delRid.pageNo = page_no();
			delRid.slotNo = 0;
			st = deleteRecord(delRid);
			assert(st == OK);

			// adjust the entry pointing to itself in its parent
			st = parentPtr->adjust_key((void*)&firstKey,
					(void*)&entry, key_type);
			assert(st == OK);
		}
	}

	return true;
}

Status BTIndexPage::findKey(void *key, void *entry, AttrType key_type)
{
	for (int i = slotCnt-1; i >= 0; i--) {
		if (keyCompare(key, (void*)(data+slot[-i].offset), key_type) >= 0) {
			memcpy(entry, data+slot[-i].offset, get_key_length(key,key_type));
			return OK;
		}
	}
	return FAIL;
}
