/*
 * btleaf_page.cc - implementation of class BTLeafPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 */

#include "btleaf_page.h"

const char* BTLeafPage::Errors[BTLeafPage::LEAFNR_ERRORS] = {
	"OK",
	"Insert Record Failed (BTLeafPage::insertRec)",
};


/*
 * Status BTLeafPage::insertRec(const void *key,
 *                             AttrType key_type,
 *                             RID dataRid,
 *                             RID& rid)
 *
 * Inserts a key, rid value into the leaf node. This is
 * accomplished by a call to SortedPage::insertRecord()
 * The function also sets up the recPtr field for the call
 * to SortedPage::insertRecord()
 *
 * Parameters:
 *   o key - the key value of the data record.
 *
 *   o key_type - the type of the key.
 *
 *   o dataRid - the rid of the data record. This is
 *               stored on the leaf page along with the
 *               corresponding key value.
 *
 *   o rid - the rid of the inserted leaf record data entry.
 */

Status BTLeafPage::insertRec(const void *key,
		AttrType key_type,
		RID dataRid,
		RID& rid)
{
	KeyDataEntry entry;
	int entry_len;

	Datatype d; d.rid = dataRid;
	make_entry(&entry, key_type, key, (nodetype)type, d, &entry_len);

	if (SortedPage::insertRecord(key_type, (char*)&entry,
				entry_len, rid) != OK) {
		return MINIBASE_FIRST_ERROR(BTLEAFPAGE, LEAFINSERTRECFAILED);
	}

	return OK;
}


#if NOT_USED
/*
 *
 * Status BTLeafPage::get_data_rid(const void *key,
 *                                 AttrType key_type,
 *                                 RID & dataRid)
 *
 * This function performs a binary search to look for the
 * rid of the data record. (dataRid contains the RID of
 * the DATA record, NOT the rid of the data entry!)
 */

Status BTLeafPage::get_data_rid(const void *key,
		AttrType key_type,
		RID & dataRid)
{
	int lower = 0;           // vars used for binary search
	int upper = slotCnt-1;
	int mid;
	int result;

	while (lower <= upper)
	{
		mid = (lower + upper)/2;

		result = keyCompare(key, (void*)(data+slot[mid].offset), key_type);

		if (result == 0)    // key == ...
		{
			Keytype tmpKey;

			get_key_data((void*)&tmpKey, (Datatype *) &dataRid,
					(KeyDataEntry *)(data+slot[mid].offset),
					slot[mid].length, (nodetype)type);
			return OK;
		}
		else if (result < 0) // key < ...
			lower = mid+1;
		else    // result > 0 : key > ...
			upper = mid-1;
	}

	return RECNOTFOUND;
}
#endif

/*
 * Status BTLeafPage::get_first (const void *key, RID & dataRid)
 * Status BTLeafPage::get_next (const void *key, RID & dataRid)
 * Status BTLeafPage::get_current (const void *key, RID & dataRid)
 *
 * These functions provide an
 * iterator interface to the records on a BTLeafPage.
 * get_first returns the first key, RID from the page,
 * while get_next returns the next key on the page.
 * These functions make calls to RecordPage::get_first() and
 * RecordPage::get_next(), and break the flat record into its
 * two components: namely, the key and datarid.
 * get_current is like get_next except it does not advance the
 * internal iterator.
 *
 * Note: even though we have an entire RID (page + slotno) available to
 * maintain iteration, we use only the slotno and access the slot
 * directory directly (other methods of BTLeafPage need to know the
 * slot dir format, so this isn't breaking any abstractions).
 *
 * BUG: since BTLeafPage maintains its own iterator state, only one
 * iteration can be active at one time.  This is probably bad design --
 * the call should provide its own opaque iterator variable for us
 * to tweak.
 */

Status BTLeafPage::get_first (RID& rid,
		void *key,
		RID & dataRid)
{
	rid.pageNo = curPage;
	rid.slotNo = 0; // begin with first slot

	if (slotCnt == 0) {
		dataRid.pageNo = INVALID_PAGE;
		dataRid.slotNo = INVALID_SLOT;
		return NOMORERECS;
	}
	get_key_data(key, (Datatype *) &dataRid,
			(KeyDataEntry *)(data+slot[0].offset), slot[0].length,
			(nodetype)type);
	return OK;
}

Status BTLeafPage::get_next (RID& rid,
		void *key,
		RID & dataRid)
{
	rid.slotNo++;

	if (rid.slotNo == slotCnt)
	{
		dataRid.pageNo = INVALID_PAGE;
		dataRid.slotNo = INVALID_SLOT;
		return NOMORERECS;
	}

	get_key_data(key, (Datatype *) &dataRid,
			(KeyDataEntry *)(data+slot[-rid.slotNo].offset),
			slot[-rid.slotNo].length,
			(nodetype) type);

	return OK;
}

Status BTLeafPage::get_current (RID rid,
		void *key,
		RID & dataRid)
{
	if (rid.slotNo == slotCnt)
	{
		dataRid.pageNo = INVALID_PAGE;
		dataRid.slotNo = INVALID_SLOT;
		return NOMORERECS;
	}

	get_key_data(key, (Datatype *) &dataRid,
			(KeyDataEntry *)(data+slot[-rid.slotNo].offset),
			slot[-rid.slotNo].length,
			(nodetype) type);

	return OK;
}


/*
 * bool BTLeafPage::delUserRid (const void *key, AttrType key_type,
 *                              const RID& dataRid)
 *
 * Delete an occurrence of data entry <key, dataRid> from a page.
 * Useful if caller doesn't know (or doesn't want to know) internal
 * RID of data entry itself.
 */

bool BTLeafPage::delUserRid (const void *key, AttrType key_type,
		const RID& dataRid)
{
	int i;

	for (i=slotCnt-1; i >= 0; i--) {
		Keytype tmpKey;  // key & user-rid for this slot
		RID     tmpRid;
		get_key_data(&tmpKey, (Datatype *) &tmpRid,
				(KeyDataEntry *)(data+slot[-i].offset),
				slot[-i].length, (nodetype)type);
		if (tmpRid == dataRid && keyCompare(key, &tmpKey, key_type) == 0) {
			// found record to delete; so do_it()
			RID delRid;
			Status st;
			delRid.pageNo = page_no();
			delRid.slotNo = i;
			st = deleteRecord(delRid);  // SortedPage::deleteRecord
			assert(st == OK);
			return true;
		}
	}


	return false;
}

bool BTLeafPage::redistribute(BTLeafPage *pptr, BTIndexPage *parentPtr, AttrType key_type, int left, const void *deletedKey)
{
	// assertion: pptr pinned

	if (left) { // 'this' is the left sibling of pptr
		if (slot[0].length + free_space() > (MAX_SPACE-DPFIXED)/2) {
			// cannot spare a record for its underflow sibling
			return false;
		}
		else {
			// move the last record to its sibling

			// get the last record
			RID lastRid;
			Keytype lastKey;
			Status st;
			get_key_data(&lastKey, (Datatype*)&lastRid,
					(KeyDataEntry*)(data+slot[-(slotCnt-1)].offset),
					slot[-(slotCnt-1)].length, (nodetype)type);

			// get its sibling's first record's key for adjusting parent pointer
			RID dummyRid, dummydummyRid;
			Keytype oldKey;
			pptr->get_first(dummyRid, (void*)&oldKey, dummydummyRid);

			// insert it into its sibling
			st = pptr->insertRec((void*)&lastKey, key_type, lastRid, dummyRid);
			if (st != OK)
				return false;

			// delete the last record from the old page
			RID delRid;
			delRid.pageNo = page_no();
			delRid.slotNo = slotCnt-1;
			st = deleteRecord(delRid);
			assert(st == OK);

			// adjust the entry pointing to sibling in its parent
			if (deletedKey)
				st = parentPtr->adjust_key((void*)&lastKey,
						deletedKey, key_type);
			else
				st = parentPtr->adjust_key((void*)&lastKey,
						(void*)&oldKey, key_type);
			assert (st == OK);
		}
	}
	else { // 'this' is the right sibling of pptr
		if (slot[(slotCnt-1)].length + free_space() > (MAX_SPACE-DPFIXED)/2) {
			// cannot spare a record for its underflow sibling
			return false;
		}
		else {
			// move the first record to its sibling

			// get the first record
			RID firstRid;
			Keytype firstKey;
			get_key_data(&firstKey, (Datatype*)&firstRid,
					(KeyDataEntry*)(data+slot[0].offset),
					slot[0].length, (nodetype)type);

			// insert it into its sibling
			RID dummyRid, dummydummyRid;
			Status st = pptr->insertRec((void*)&firstKey, key_type,
					firstRid, dummyRid);
			if (st != OK)
				return false;

			// delete the first record from the old page
			RID delRid;
			delRid.pageNo = page_no();
			delRid.slotNo = 0;
			st = deleteRecord(delRid);
			assert(st == OK);

			// get the current first record of the old page
			// for adjusting parent pointer.
			Keytype newKey;
			st = get_first(dummyRid, (void*)&newKey, dummydummyRid);
			assert(st == OK);

			// adjust the entry pointing to itself in its parent
			st = parentPtr->adjust_key((void*)&newKey,
					(void*)&firstKey, key_type);
			assert(st == OK);
		}
	}

	return true;
}

