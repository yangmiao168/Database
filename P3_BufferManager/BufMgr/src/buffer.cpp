/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

/**
 * @name&studentid  Miao Yang(9071882121)  Qimeng Han (9073368152)      Group 8
 * @file            buffer.cpp
 * @description     build a buffer manager, on top of an I/O Layer that we provide.
 * @course          CS564
 * @date            11/08/2016 etc.
 * @version         2.0 
 */


#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

using namespace std;

namespace badgerdb { 

//constructor
BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	// create a BufDesc object--keep track the state of each frame in the buffer pool
	bufDescTable = new BufDesc[bufs];
  // set the frame number 
  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }
  // create an array for buffer pool
  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  // allocate the buffer hash table
  hashTable = new BufHashTbl (htsize);  

  clockHand = bufs - 1;
}


/** 
  *   @brief  Flushes out all dirty pages and deallocates the buffer pool and the BufDesc table.   
  */ 

BufMgr::~BufMgr() {
    for(uint32_t i= 0; i<numBufs; i++) {
	// if BufDesc table is valid and dirty
	    if (bufDescTable[i].valid && bufDescTable[i].dirty) {
		// wirte page 
		    bufDescTable[i].file -> writePage(bufPool[bufDescTable[i].frameNo]);
	    }
    }

    // delete bufDescTable 
    delete []bufDescTable; 
    // delete buffer pool
    delete []bufPool; 
    // delete hashTable 
    delete hashTable;  
}



 /** 
  *   @brief  advance clock to the next frame in the buffer pool
  *   @return void
  */
void BufMgr::advanceClock()
{
  // increment the clockhand in a circle
  clockHand = (clockHand+1)%numBufs;
}


/** 
  *   @brief  allocate a free frame using the clock algorithm
  *   @param  FrameId & frame is the frame in the buffer pool
  *   @throws BufferExceededException If no such buffer is found which can be allocated
  *   @return void
  */
void BufMgr::allocBuf(FrameId & frame) 
{
	
	uint32_t numPinnedPages = 0;	///<count hwo many pages have been pinned in the frame
	bool found = false;	///<record whether an appropriate pgae can be replaced has been found 

	//the while loop continues when numpinned < numFrames and not found the appropriate page
    while (numPinnedPages <numBufs && !found) {
	    //continue the clockhand
 	    advanceClock();

		// if the page is not valid, cleared the frame
		if (bufDescTable[clockHand].valid == false) {
 		bufDescTable[clockHand].Clear();
		//return the newly allocated frame
 		frame = clockHand;
 		found = true;
		}	

		// else if the page is valid and refbit is true, clear refbit
		else if (bufDescTable[clockHand].refbit) {
		// set the refbit to false
		bufDescTable[clockHand].refbit = false;
		}

		// else if the page is valid and the refbit is flase and the page has been pinned
		else if (bufDescTable[clockHand].pinCnt > 0) {
		//increase the number of pages that have been pinned 
		numPinnedPages++;
		}

		// the page is valid and the refbit is flase and the page has not been pinned
		else  {
			// if the page is dirty
			if (bufDescTable[clockHand].dirty) {
    		// flush page to disk
			bufDescTable[clockHand].file -> writePage(bufPool[bufDescTable[clockHand].frameNo]);
    		}
			// remove the entry from the hashtable
			hashTable -> remove (bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
			bufDescTable[clockHand].Clear();
			// set the frame to the current clock hand
			frame = clockHand;
			//the appropraite frame has been found 
			found = true;
		}
	}

	// if all buffer frames are pinned,throws BufferExceededException
	if (numPinnedPages == numBufs) {
		//when the number of pinned frames has exceed the number of available frames
		//thrwo an exception 	
		throw BufferExceededException();
	}
}


/** 
  *   @brief  Reads the given page from the file into a frame and returns the pointer to page.
  *           If the requested page is already present in the buffer pool pointer to that frame is returned
  *           otherwise a new frame is allocated from the buffer pool for reading the page.
  *   @param  file file is the pointer to the file object
  *   @param  pageNo is the page number within th file
  *   @param  Page page is the page 
  *   @return void
  */
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{	
	
	FrameId frameNo;	///<The frame number

	try {
	 	//check whether the page is already in the buffer pool
	 	hashTable ->lookup(file, pageNo, frameNo);
       	//Case 2: the page is in the buffer pool
	 	//set the refbit to true
	 	bufDescTable[frameNo].refbit = true;
        //increase the pintCnt
	 	bufDescTable[frameNo].pinCnt ++;
	 	// return a pointer to the frame containg the page via the page parameter
	    page = &bufPool[frameNo];
	}

	//Case 1:the page is not in the buffer pool
	catch(const HashNotFoundException& e){
		//allocated frame via frameNo
	    allocBuf(frameNo);
		//read page from the disk 
	 	Page diskPage = file->readPage(pageNo);
	 	// set the page from the disk to the buffer pool 
        bufPool[frameNo] = diskPage;
        // insert the page into hash table
		hashTable->insert(file, pageNo, frameNo);
		// after read page from the disk, set up the frame with pinCnt=1
		bufDescTable[frameNo].Set(file, pageNo);
		// Return a pointer to the frame containing the page via the page parameter.			
		page = &bufPool[frameNo];
	}	
		
}

/** 
  *   @brief  Unpin a page from memory since it is no longer required for it to remain in memory.
  *   @param  file file is the pointer to the file object
  *   @param  pageNo is the page nubmer within file
  *   @param  dirty True if the page to be unpinned needs to be marked dirty	
  *   @throws PageNotPinnedException If the page is not already pinned
  *   @return void
  */

void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{

    FrameId frameNo;	///<The frame number

	try{
		//check if the page is in the hash table 
		hashTable->lookup(file, pageNo, frameNo);
		// if is dirty, set dirty bit to true
        if(dirty){		  
	          bufDescTable[frameNo].dirty = true;
	      }
	    // Throws PageNotPinnedException if the pin count is already 0
		if (bufDescTable[frameNo].pinCnt == 0){
			throw PageNotPinnedException(file-> filename(),pageNo,frameNo);
		}
		// decrements the pinCnt of the frame
		bufDescTable[frameNo].pinCnt --;
			
	}

	catch (const HashNotFoundException e ){
		// do nothing if page is not found in the hash table
	}
		
}


/** 
  *   @brief  scan the buffer pool for pages belonging to the file
  *   @param  file file is the pointer to the file object
  *   @return void
  */

void BufMgr::flushFile(const File* file) {

	// scan the buffer pool
	for (uint32_t frameNo = 0; frameNo < numBufs; frameNo++){
	  // if pages from buffer pool belonging to the file
	  if (bufDescTable[frameNo].file == file){
        
        // if the page is dirty
	  	if (bufDescTable[frameNo].dirty){
          // flush the page to the disk
          Page newPage = bufPool[frameNo];
	      bufDescTable[frameNo].file ->writePage(newPage);
	      // set the dirty bit for the page to false
	      bufDescTable[frameNo].dirty = false;
	 		     
	    }
        
        // if some page of the file is pinned, throws PagePinnedException
	    if (bufDescTable[frameNo].pinCnt != 0){
	      throw PagePinnedException(file->filename(),bufDescTable[frameNo].pageNo,frameNo);
	    }

	    // if an invalid page belonging to the file is encountered throws BadBufferException 
	    if (!(bufDescTable[frameNo]).valid){
	      throw BadBufferException(frameNo, bufDescTable[frameNo].dirty, false, bufDescTable[frameNo].refbit);
	    }
	   
        // remove the page from the hashTable
        hashTable->remove( bufDescTable[frameNo].file, bufDescTable[frameNo].pageNo);
        // clear the page frame
	    bufDescTable[frameNo].Clear();
	  }
	}
}


/** 
  *   @brief  allocate an empty page in the specified file
  *   @param  file file is the pointer to the file object
  *   @param  pageNo is the page number within file
  *   @param  page Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
  *   @return void
  */

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{

    FrameId frameNo;	///<The frame number
	// allocaet an empty page in the specified file
	Page newPage = file->allocatePage();
	// get the page number of the newpage
	pageNo = newPage.page_number();
	// obtain a buffer pool frame
	allocBuf(frameNo);
	// add the new page to the buffer pool
	bufPool[frameNo] = newPage;
	// insert an entry to the hash table
	hashTable->insert(file, pageNo, frameNo);
	// set up the frame
	bufDescTable[frameNo].Set(file, pageNo);
	// get a pointer to the buffer frame allocated for the page via the page parameter		    
	page = &bufPool[frameNo];
    
}


/** 
  *   @brief  deletes a particular page from the file
  *   @param  file file is the pointer to the file object
  *   @param  pageNo is the page number within file
  *   @return void
  */

void BufMgr::disposePage(File* file, const PageId PageNo)
{

    FrameId frameNo;	///<The frame number
	try{		
	//check if the page to be deleted is allocated a frame in the buffer pool
        hashTable->lookup(file, PageNo, frameNo);
        // clear the buffer pool frame
        bufDescTable[frameNo].Clear();         
        //remove the corresponding entry from hashtable
	    hashTable->remove(file, PageNo);            
        // delete the page from the file
        file ->deletePage(PageNo);	
	}

	catch(const HashNotFoundException& e){
	// do nothing if the file not in the hash table
	}
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}


