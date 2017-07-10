/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include <string>



namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	// constructing an index name
	std :: ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std :: string indexName = idxStr.str();

	// Initialize
	outIndexName = indexName;
	this-> bufMgr = bufMgrIn;
	this->attrByteOffset = attrByteOffset;
	leafOccupancy = INTARRAYLEAFSIZE;
	nodeOccupancy = INTARRAYNONLEAFSIZE;
	Page * metaPage;
	headerPageNum  = 1;

	// check if the index file exists
	if (File::exists(outIndexName)){
		// create new blob file
		file = new BlobFile(outIndexName, false);

		// read the meta page
		bufMgr->readPage(file, headerPageNum, metaPage);

		// cast the meta page to meta node
		IndexMetaInfo* metaNode = (IndexMetaInfo*) metaPage;

		// get the root pageNo
		rootPageNum = metaNode->rootPageNo;

		// unPin the head page
		bufMgr->unPinPage(file, headerPageNum, false);
	}

	// if the index file not exist, create a new index file
	else {

		file = new BlobFile(outIndexName, true);

		Page * rootPage;

		// allocate new meta page
		bufMgr->allocPage(file, headerPageNum, metaPage);
		IndexMetaInfo* metaNode = (IndexMetaInfo*)metaPage;

		// allocate new root page
		bufMgr->allocPage(file, rootPageNum, rootPage);

		// initialize IndexmetaInfo with the rootPageNum and attrByteOffset
		metaNode->attrByteOffset = this-> attrByteOffset;
		metaNode->rootPageNo = this-> rootPageNum;
		// copy the relation name
		strcpy(metaNode->relationName, relationName.c_str());

		// get the root and initialize
		LeafNodeInt *root = (LeafNodeInt *)rootPage;
		root -> level =0;
		root-> numKeys =0;

		// unPin pages that have been pinned
		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file,  rootPageNum, true);

		// scan records and insert into the B+ Tree
		FileScan fscan(relationName, bufMgr);
		RecordId scanRid;

		try{

			while(true){
				fscan.scanNext(scanRid);
				std::string recordStr = fscan.getRecord();
				const char *record = recordStr.c_str();
				insertEntry((void*)(record+attrByteOffset), scanRid);
			}
		}

		catch(EndOfFileException & e){
			// do nothing
		}
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	// flushing the index file, 
	bufMgr->flushFile(file);
	// delete file instance thereby close the index file
	delete file;
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{
	// the initial level of the root
	int rootLevel = 0;
	// returnPageNo is 0 if no split occurred
	PageId returnPageNo =0;
	void *middleKey = new int;

	// recursively insert the key to each node
	returnPageNo = insert(key, rid, middleKey, rootPageNum);

	// if the root need to split, create new root
	if (returnPageNo != 0) {
		// once root split,increment the root level
		rootLevel++;
		Page *newRootPage;
		PageId newRootPageNo;

		// allocate a new page for root
		bufMgr ->allocPage(file, newRootPageNo, newRootPage);

		// create a new root node
		NonLeafNodeInt *newRoot = (NonLeafNodeInt *)newRootPage;
		// push up the middle key from the child
		newRoot->keyArray[0] = *((int*) middleKey);
		// the first pageNo is the old root page number
		newRoot->pageNoArray[0] = rootPageNum;
		// the second pageNo is the pageNo of the new sibling node of old root
		newRoot->pageNoArray[1] = returnPageNo;
		newRoot->numKeys = 1;
		newRoot->level = rootLevel;

		// update the meta data information
		Page *metaPage;
		rootPageNum = newRootPageNo;
		bufMgr->readPage(file, headerPageNum, metaPage);
		IndexMetaInfo * metaNode = (IndexMetaInfo* ) metaPage;
		metaNode->rootPageNo = newRootPageNo;

		// unPin the pages
		bufMgr -> unPinPage (file, headerPageNum, true);
		bufMgr -> unPinPage (file, newRootPageNo, true);

	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::insert method
// -----------------------------------------------------------------------------

PageId BTreeIndex::insert(const void *key, const RecordId rid,
		void *middleKey, PageId currPageNo)
{
	//the pageNo to return 
	PageId returnPageNo = 0;
	Page *currPage;
	//read the current page using the page number 
	bufMgr->readPage(file, currPageNo, currPage);
	//cast the current page to node 
	NonLeafNodeInt *currNode = (NonLeafNodeInt *)currPage;

	// if it is leaf node
	if(currNode->level == 0){

		LeafNodeInt *leafNode = (LeafNodeInt *)currPage;

		// if the leaf node is not full, just add it.
		if(leafNode->numKeys < leafOccupancy){
			// call insertNonFullNode method to insert the key and rid
			insertNonFullNode(leafNode, key, (void*)&rid, leafNode->level);
			(leafNode->numKeys)++;

			bufMgr->unPinPage(file, currPageNo, true);

			return returnPageNo;
		}
		// if the leaf node is full, need to split
		else{
			Page *newLeafPage;
			PageId newPageNo = 0;
			// allocate new leaf page
			bufMgr->allocPage(file, newPageNo, newLeafPage);
			//cast it into new leaf node
			LeafNodeInt *newLeafNode = (LeafNodeInt *)newLeafPage;

			// link the new leaf node with the current leaf node
			newLeafNode->rightSibPageNo = leafNode ->rightSibPageNo;
			leafNode ->rightSibPageNo = newPageNo;

			// initialed the new leaf node
			newLeafNode -> numKeys =0;
			newLeafNode -> level = 0;

			// get the middle key
			int* midKeyValue = (int*) middleKey;
			int mid = leafOccupancy/2-1;
			*midKeyValue = leafNode -> keyArray[mid];
			// set the new leaf node pageNo as the return pageNo
			returnPageNo = newPageNo;

			// insert the second half of key/rid to the new leaf node
			for(int i = mid; i < leafOccupancy; i++) {

				insertNonFullNode(newLeafNode,(void*)&(leafNode->keyArray[i]),
						(void*)&(leafNode->ridArray[i]), leafNode->level);

				// decrement the number of keys in leaf node
				(leafNode-> numKeys)--;
				// increment the number of keys in the new leaf node
				(newLeafNode->numKeys)++;
			}

			// if the key less than middle key, add the key/rid to the current leaf node
			if ( *(int*)key < *(int *)middleKey) {

				insertNonFullNode(leafNode, key, (void*)&rid, leafNode->level);
				(leafNode-> numKeys)++;
			}
			// else, add the key/rid to the new leaf node
			else {

				insertNonFullNode(newLeafNode, key, (void*)&rid, newLeafNode->level);
				(newLeafNode-> numKeys)++;

			}

			// unPin the pages
			bufMgr->unPinPage(file, newPageNo, true);
			bufMgr->unPinPage(file, currPageNo, true);
			return returnPageNo;
		}

	}
	// if it is non leaf node
	else{

		PageId targetPageNo = 0;

		// search the correct the child node to insert using the search method
		// key < the first
		if (*(int *)key < (currNode->keyArray[0])) {
			targetPageNo = currNode->pageNoArray[0];
		}

		// key > the last
		else if (*(int *)key > (currNode->keyArray[currNode->numKeys-1])){
			// index of pageNo == index of key +1
			targetPageNo = currNode->pageNoArray[currNode->numKeys];
		}

		// in between
		else {
			for (int i =0; i < currNode->numKeys; i++) {
				if (*(int *)key>= currNode-> keyArray[i] && *(int *)key < currNode-> keyArray[i+1]) {
					targetPageNo = currNode -> pageNoArray [i+1];
				}
			}
		}

		bufMgr->unPinPage(file, currPageNo, false);

		// recursively insert into the child
		returnPageNo = insert(key, rid, middleKey, targetPageNo);

		// check is the split occurred
		// if no split occurred, just return 0
		if (returnPageNo == 0) {

			return returnPageNo;
		}
		// else if the split occurred
		else  {
			// read the current page
			bufMgr -> readPage (file, currPageNo,currPage);
			// cast it into non leaf node
			NonLeafNodeInt *nonLeafNode = (NonLeafNodeInt *) currPage;

			// if is not full, insert PageNoKey pair
			if ((nonLeafNode->numKeys) < nodeOccupancy) {
				insertNonFullNode(nonLeafNode,middleKey, (void*)& returnPageNo, nonLeafNode->level);
				(nonLeafNode-> numKeys)++;

				returnPageNo =0;
				bufMgr->unPinPage(file, currPageNo, true);
				return returnPageNo;
			}

			// else if the node is full
			// split the node, 2d keys and 2d+1 node pointers
			else {

				Page *newNonLeafPage;
				PageId newNonLeafPageNo = 0;
				// allocate a new non leaf page
				bufMgr ->allocPage(file,newNonLeafPageNo, newNonLeafPage);
				// cast the page to node
				NonLeafNodeInt *newNonLeafNode = (NonLeafNodeInt *) newNonLeafPage;

				// initialed the new leaf node
				newNonLeafNode -> numKeys =0;
				// new leaf node has the same level as the current leaf node
				newNonLeafNode -> level = nonLeafNode -> level;

				// get the middle key
				int mid = nodeOccupancy/2-1;
				middleKey = & (nonLeafNode -> keyArray[mid]);
				// set the new non leaf node pageNo as the return pageNo
				returnPageNo = newNonLeafPageNo;

				// split the node
				// first d key values and d node pointers stay in the current nonLeaf node
				// last d keys and d + 1 pointers move to new nonLeaf node
				for(int i = mid; i < nodeOccupancy; i++) {

					insertNonFullNode(newNonLeafNode,(void*)&(nonLeafNode->keyArray[i]),
							(void*)&(nonLeafNode->pageNoArray[i]), nonLeafNode->level);
					// decrement the number of keys in the current non leaf node.
					(nonLeafNode-> numKeys)--;
					// increment the number of keys in the new leaf node
					(newNonLeafNode->numKeys)++;
				}

				// insert the new entry pair
				// if key < middle key, add the key/pageNo to the current non leaf node
				if ( *(int*)key < *(int *)middleKey) {
					insertNonFullNode(nonLeafNode, middleKey, (void*)&returnPageNo, nonLeafNode->level);
					(nonLeafNode-> numKeys)++;
				}
				// else, add the key/pageNo to new non leaf node
				else {
					insertNonFullNode(newNonLeafNode, middleKey, (void*)&returnPageNo, newNonLeafNode->level);
					(newNonLeafNode-> numKeys)++;
				}
				// unPin the pages
				bufMgr->unPinPage(file,newNonLeafPageNo, true);
				bufMgr->unPinPage(file, currPageNo, true);
				return returnPageNo;

			}
		}

	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertNonFullNode method
// -----------------------------------------------------------------------------

const void BTreeIndex::insertNonFullNode(void *targetNode, const void *newKey, void *newId, int nodeLevel)
{

	// if it is leaf node
	if(nodeLevel == 0){
		// leaf nodeLevel == 0
		LeafNodeInt *leafNode = (LeafNodeInt *) targetNode;
		int pos = 0;
		// key value to insert
		int key = *(int *)newKey;
		// Record id to insert
		RecordId rid = *(RecordId *) newId;

		// find the position to insert
		for (int i = 0; i < leafNode->numKeys; i++){
			if (leafNode->keyArray[i] < key){
				pos = i+1;
			}
		}

		// move over the rid /key in array to the right and allow the new entry insert into the array
		for (int j = leafNode-> numKeys; j> pos; j--) {

			leafNode-> keyArray[j] =  leafNode-> keyArray[j-1];
			leafNode-> ridArray[j] =  leafNode-> ridArray[j-1];
		}

		// insert the key and rid to the right position
		leafNode-> keyArray[pos] = key;
		leafNode-> ridArray[pos] = rid;

	}

	// else if it is non leaf node
	else {
		NonLeafNodeInt * NonLeafNode = (NonLeafNodeInt *) targetNode;

		int pos = 0;
		// key value to insert
		int key = *(int *)newKey;
		// PageNo to insert
		PageId pageNo = *(PageId *) newId;

		// find the position
		for (int i = 0; i < NonLeafNode->numKeys; i++){
			if (NonLeafNode->keyArray[i] < key){
				pos = i+1;
			}
		}

		// move over the key/pageNo in array to the right and allow the new entry insert into the array
		for (int j = NonLeafNode-> numKeys; j> pos; j--) {
			// memmove(..)
			NonLeafNode-> keyArray[j] =  NonLeafNode-> keyArray[j-1];
		}

		// move over the pageNo in array to the right and allow the new pageNo insert into the array
		for (int j = NonLeafNode-> numKeys+1; j> pos+1; j--) {

			NonLeafNode-> pageNoArray[j] =  NonLeafNode-> pageNoArray[j-1];
		}

		// insert the key and pageNo to the right position
		NonLeafNode-> keyArray[pos] = key;
		NonLeafNode-> pageNoArray[pos+1] = pageNo;
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
		const Operator lowOpParm,
		const void* highValParm,
		const Operator highOpParm)
{
	// set a flag to determine whether the first RecordID has been found or not
	bool isFound = false;

	// if low operator does not support GT and GTE or high operator does not support LT and LTE
	// throw BadOpcodesException
	if ((lowOpParm !=GT && lowOpParm !=GTE) || (highOpParm != LT && highOpParm != LTE)) {
		scanExecuting = false;
		throw BadOpcodesException();
	}
	// If another scan is already executing, that needs to be ended here.
	if (scanExecuting) {
		endScan();
	}

	lowValInt = *(int *)lowValParm;
	highValInt = *(int *)highValParm;

	// if lowValue > highValue, throw BadScanrangeException
	if(lowValInt > highValInt){
		scanExecuting = false;
		throw BadScanrangeException();
	}

	// Set up all the variables for scan.
	scanExecuting = true;
	lowOp = lowOpParm;
	highOp = highOpParm;
	currentPageNum = rootPageNum;

	// Start from root to find out the leaf page that contains the first RecordID
	while(!isFound){
		// read the current page
		bufMgr->readPage(file, currentPageNum, currentPageData);
		NonLeafNodeInt *nonLeafNode = (NonLeafNodeInt *) currentPageData;

		// if the current node is Leaf node
		if(nonLeafNode->level == 0){
			LeafNodeInt *leafNode = (LeafNodeInt *) currentPageData;

			for(int i = 0; i < leafNode->numKeys; i++){
				if(lowOpParm == GTE && leafNode->keyArray[i] >= lowValInt) {
					nextEntry = i;
					isFound = true;
					break;
				}
				else if(lowOpParm == GT && leafNode->keyArray[i] > lowValInt){
					nextEntry = i;
					isFound = true;
					break;
				}

			}

		}

		// for non leaf node
		else{
			PageId previousPageNum = currentPageNum;

			//  if low value < the first
			if(lowValInt < nonLeafNode->keyArray[0]) {
				currentPageNum = nonLeafNode->pageNoArray[0];
			}
			// else if the low value > the last
			else if(lowValInt > nonLeafNode->keyArray[nonLeafNode->numKeys-1] ){
				currentPageNum = nonLeafNode->pageNoArray[nonLeafNode->numKeys];

			}
			// in between
			else {
				for(int i = 0; i < nonLeafNode->numKeys; i++){
					if (lowValInt >= nonLeafNode->keyArray[i] && lowValInt <= nonLeafNode->keyArray[i+1]) {

						currentPageNum = nonLeafNode->pageNoArray[i+1];

					}
				}
			}

			bufMgr->unPinPage(file, previousPageNum,false);
		}
	}

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid)
{

	// If no scan has been initialized throws ScanNotInitializedException
	if(!scanExecuting){
		throw ScanNotInitializedException();
	}
    // cast into leaf node
	LeafNodeInt *currLeafNode = (LeafNodeInt *) currentPageData;

	// if the next entry in the range
	if(nextEntry < (currLeafNode->numKeys)-1){

		if( highOp == LT && currLeafNode->keyArray[nextEntry] < highValInt) {
			outRid = currLeafNode->ridArray[nextEntry];
		}

		else if( highOp == LTE && currLeafNode->keyArray[nextEntry] <= highValInt) {
			outRid = currLeafNode->ridArray[nextEntry];
		}
		// If no more records, satisfying the scan criteria, are left to be scanned.
		else {

			throw IndexScanCompletedException();
		}
		nextEntry ++;
	}
	//  If current page has been scanned to its entirety, move on to the right sibling of current page
	else {
		nextEntry = 0;
		// move to right sibling of the current page
		PageId nextPageNo = currLeafNode->rightSibPageNo;
		// unPin any pages that are no longer required
		bufMgr->unPinPage(file, currentPageNum,false);

		// read the next page
		currentPageNum = nextPageNo;
		if(currentPageNum == 0){
			throw IndexScanCompletedException();
		}
		bufMgr->readPage(file, currentPageNum, currentPageData);
	}

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() {

	// If no scan has been initialized throws ScanNotInitializedException
	if(!scanExecuting) {
		throw ScanNotInitializedException();
	}
	// unPin any pinned pages
	if(currentPageNum != 0){
		bufMgr->unPinPage(file, currentPageNum,false);
	}
	scanExecuting = false;
}
}
