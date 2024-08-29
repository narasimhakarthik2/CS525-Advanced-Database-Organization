#include <stdlib.h>
#include <stdint.h>
#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"
#include "dberror.h"
#include "dt.h"
#include "storage_mgr.h"

#define CONSTANT_VALUE_ZERO 0
#define CONSTANT_VALUE -1

// Constant representing the initial page number
const int INITIAL_PAGE_NUMBER = CONSTANT_VALUE;
int LogicClockloadToMemory = CONSTANT_VALUE_ZERO; //Initializing logical clock for loading pages into memory
int LogicClockOperations = CONSTANT_VALUE_ZERO; //Initializing logical clock for operations

/**
 * @author      : Narasimha Karthik
 * @function    : initBufferPool
 * @description : This function initializes the Buffer Pool with its attributes like number of pages, page file name, and replacement strategy
 */

RC initBufferPool(BM_BufferPool *const bufferPool, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData) {

    bufferPool->pageFile=(char *)pageFileName; // Set the page file name
    bufferPool->numPages=numPages; // Set the number of pages in the buffer pool
    bufferPool->strategy=strategy; // Set the replacement strategy
    bufferPool->readOperationsCount=0; // Initialize read operations count
    bufferPool->writeOperationsCount=0; // Initialize write operations count

    // Allocate memory for page array
    BM_PageFrame *pageArray=malloc(sizeof(BM_PageFrame) * numPages);
    
    // Initialize loop counter
    int i=0;

    if (pageArray != NULL) {
    while (i < bufferPool->numPages) {
    // Initialize page properties
    
    // Initialize page number on disk, dirty flag, page data pointer,
    // client count, last operation timestamp, and load timestamp
    pageArray[i] = (BM_PageFrame){ 
                           .pageNumOnDisk = INITIAL_PAGE_NUMBER,
                           .isPageDirty = false,
                           .pageData = NULL,
                           .clientCount = 0,
                           .lastOperationTimestamp = 0,
                           .loadTimestamp = 0 
                    };
    
    // Move to the next page
    i++;
    }

    }

    // Set management data to point to the allocated page array
    bufferPool->mgmtData=pageArray;
    // Return success status
    return RC_OK;
}

/**
 * @author      : Narasimha Karthik
 * @function    : shutdownBufferPool
 * @description : This function shutdowns the Buffer Pool
 */

RC shutdownBufferPool(BM_BufferPool *const bufferPool) {
    // Write all dirty pages to disk if the buffer pool is not NULL
if (bufferPool != NULL) {
    forceFlushPool(bufferPool);
}

// Get the start address of the pages array if the buffer pool management data is not NULL
BM_PageFrame *pageArray = NULL;
if (bufferPool != NULL && bufferPool->mgmtData != NULL) {
    pageArray = (BM_PageFrame *) bufferPool->mgmtData;
}

    // Free memory for page data and buffer manager
    int i = 0;
    while (i < bufferPool->numPages) {
        // Check if the page has been loaded from disk
        if (pageArray[i].pageNumOnDisk != INITIAL_PAGE_NUMBER) {
            // Free memory for page data
            free(pageArray[i].pageData);
        }
        i++;
    }

    // Check if buffer manager data is not NULL
    if (bufferPool->mgmtData != NULL) {
        // Free buffer manager
        free(bufferPool->mgmtData);
    }

    // Return success status
    return RC_OK;
}

/**
 * @author      : Narasimha Karthik
 * @function    : initBufferPool
 * @description : Function to write a page to disk
 */

void writePageToDisk(BM_BufferPool *bufferPool, BM_PageFrame *pageArray, int pageIndexInMemory) {
    // Increment write operations count
    bufferPool->writeOperationsCount += 1;

    // Open the page file if it exists
    SM_FileHandle fileHandle;
    if (openPageFile(bufferPool->pageFile, &fileHandle) != RC_OK) {
        printf("Failed to open the page file for writing\n");
        return;
    }

    // Check if the page data is valid before writing
    if (pageArray[pageIndexInMemory].pageData == NULL) {
        printf("Invalid page data, skipping write operation\n");
        closePageFile(&fileHandle);
        return;
    }

    // Write the page to disk
    if (writeBlock(pageArray[pageIndexInMemory].pageNumOnDisk, &fileHandle, pageArray[pageIndexInMemory].pageData) != RC_OK) {
        printf("Failed to write page to disk\n");
        closePageFile(&fileHandle);
        return;
    }

    // Close the page file
    if (closePageFile(&fileHandle) != RC_OK) {
        printf("Failed to close the page file after writing\n");
    }

    // Mark the page as not dirty since it has been successfully written to disk
    pageArray[pageIndexInMemory].isPageDirty = false;
}

/**
 * @author      : Narasimha Karthik
 * @function    : initBufferPool
 * @description : Function to flush all dirty pages in the buffer pool to disk
 */

RC forceFlushPool(BM_BufferPool *const bufferPool) {
    // Get the start address of the pages array
    BM_PageFrame *pageArray = (BM_PageFrame *) bufferPool->mgmtData;

    // Initialize loop counter
    int i = 0;

    // Flush dirty pages not being accessed by any client
    while (i < bufferPool->numPages) {
        // Check if the page is dirty and not being accessed by any client
        if (pageArray[i].isPageDirty == true) {
            if(pageArray[i].clientCount == 0){
                // Mark the page as not dirty after flushing
                pageArray[i].isPageDirty = false;
                // Write the dirty page to disk
                writePageToDisk(bufferPool, pageArray, i);
            }
        }
        i++;
    }

    // Return success status
    return RC_OK;
}

/**
 * @author      : Narasimha Karthik
 * @function    : initBufferPool
 * @description : Mark a page as dirty in the buffer pool
 */
 
RC markDirty(BM_BufferPool *const bufferPool, BM_PageHandle *const page) {
    // Get the start address of the pages array
    BM_PageFrame *pageArray = (BM_PageFrame *) bufferPool->mgmtData;

    // Check if the page exists
    bool pageFound = false;
    int pageIndexInMemory = -1;

    // Search for the page in memory
    for (int i = 0; i < bufferPool->numPages; i++) {
        if (pageArray[i].pageNumOnDisk == page->pageNum) {
            // Set flag indicating page is found
            pageFound = true;
            // Store the index of the page in memory
            pageIndexInMemory = i;
            break;
        }
    }

    if ( !pageFound) {
        printf("cannot find page %d in markDirty, ignore", page->pageNum); // Print message indicating page not found
    }

    else {
        printf("markDirty %d", page->pageNum); // Print message indicating page marked as dirty
        pageArray[pageIndexInMemory].isPageDirty=true; // Mark the page as dirty4
    }

    // Return success status
    return RC_OK;
}

/**
 * @author      : Ananya Menon
 * @function    : unpinPage
 * @description : Unpin a page in the buffer pool
 */
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
  
  // Get the start address of the pages array
BM_PageFrame *head;
head = (BM_PageFrame *) bm->mgmtData;


  // Check if the page exists
  bool pageFound;
  pageFound = false;
  int pageIndexInMemory;
  pageIndexInMemory = -1;
  for (int i = 0; i < bm->numPages; i++) {
    if (head[i].pageNumOnDisk == page->pageNum) {
      pageFound = true;     // Set flag indicating page is found
      pageIndexInMemory = i;    // Store the index of the page in memory
      break;
    }
  }

  // Check if the page does not exist or has fixCount equal to 0
  if (!pageFound || head[pageIndexInMemory].clientCount == 0) {
    printf("cannot find page %d with fixCount > 0 in memory in unpinPage, ignore", page->pageNum);  // Print message indicating page not found or already unpinned
  } else {
    printf("unpinPage %d", page->pageNum);  // Print message indicating page unpinned
    head[pageIndexInMemory].clientCount--;  // Decrement the client count for the page
  }

  // Return success status
  return RC_OK;
}

/**
 * @author      : Ananya Menon
 * @function    : forcePage
 * @description : Function to force a page to disk
 */

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    
    // Get the start address of the pages array
    BM_PageFrame *head = (BM_PageFrame *) bm->mgmtData;

    // Check if the page exists
    bool pageFound = false;
    int pageIndexInMemory = -1;
    for (int i = 0; i < bm->numPages; i++) {

        if (head[i].pageNumOnDisk != page->pageNum) {
            continue;
        }

        // Set flag indicating page is found and store the index of the page in memory
        pageFound = true;
        pageIndexInMemory = i;
        break;
    }

    // Check if the page is not dirty or does not exist
    if (!(pageFound && head[pageIndexInMemory].isPageDirty)) {
        printf("cannot find dirty page %d in memory in forcePage, ignore", page->pageNum); // Print message indicating dirty page not found or not dirty
    } else {
        printf("write page %d back to disk in forcePage", page->pageNum);   // Print message indicating page is being written back to disk
        writePageToDisk(bm, head, pageIndexInMemory);   // Write the dirty page back to disk
    }

    // Return success status
    return RC_OK;
}

/**
 * @author      : Ananya Menon
 * @function    : evict
 * @description : Function to evict a page from the buffer pool
 */

int evict(BM_BufferPool *const bm) {
    // Cast management data to page frame pointer
    BM_PageFrame *head = (BM_PageFrame *)bm->mgmtData;

    int toEvictPageIndex ;
    toEvictPageIndex= -1; // Index of the page to evict
    int *noClientPages = NULL; // Array to store indices of pages with no clients
    int noClientPagesSize;
    noClientPagesSize= 0; // Size of the array

    // Find pages with no clients using them
    int i = 0;
    while (i < bm->numPages) {
    if (head[i].clientCount == 0) {
        // Increase the size of the array
        noClientPagesSize++;
        noClientPages = realloc(noClientPages, noClientPagesSize * sizeof(int));
        // Store the index of the page without clients
        noClientPages[noClientPagesSize - 1] = i;
    }
    i++;
}

    // Determine eviction based on different policies
    if (bm->strategy != RS_FIFO && bm->strategy != RS_LRU) {
        printf("Eviction strategy not implemented yet: %d\n", bm->strategy);
    } else {
        // Variables to track timestamps for FIFO and LRU strategies
        int earliestLoadTimestamp = INT32_MAX;
        int oldestLastOperationTimestamp = INT32_MAX;
        
        // Determine the page to evict based on the eviction strategy
        int i = 0;
    while (i < noClientPagesSize) {
    if (bm->strategy != RS_FIFO) {

        int pageT = -2;
    if (head[noClientPages[i]].lastOperationTimestamp < oldestLastOperationTimestamp) {
        if(pageT < 0){
            oldestLastOperationTimestamp = head[noClientPages[i]].lastOperationTimestamp;
            pageT = pageT + 2;
        }

        if(pageT >= 0){
            toEvictPageIndex = noClientPages[i];
            pageT--;
        }
        
        
    }
    } else { // bm->strategy == RS_FIFO
    int pageInd = -1;
    if (head[noClientPages[i]].loadTimestamp < earliestLoadTimestamp) {
        if(pageInd < 0){
            earliestLoadTimestamp = head[noClientPages[i]].loadTimestamp;
            pageInd = pageInd + 1;
        }

        if(pageInd >= 0){
            toEvictPageIndex = noClientPages[i];
        }
        
       
    }
}

    i++;
}

    }

    // Perform eviction actions if a page to evict is found
    if (toEvictPageIndex != -1) {
        if (head[toEvictPageIndex].isPageDirty) {
            writePageToDisk(bm, head, toEvictPageIndex);
        }
        free(head[toEvictPageIndex].pageData);
    }

    // Free memory allocated for the array of pages with no clients
    free(noClientPages);

    return toEvictPageIndex;
}

/**
 * @author      : Ananya Menon
 * @function    : pinPage
 * @description : Function to pin a page in the buffer pool
 */

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    // Set the page number in the page handle
    page->pageNum = pageNum;

    // Update logic clock with read operation
    LogicClockOperations =LogicClockOperations +1;

    // Get the start address of the pages array
    BM_PageFrame *head = (BM_PageFrame *)bm->mgmtData;

    // Check if the page is already in memory
    int pageIndexInMemory = -1;
    for (int i = 0; i < bm->numPages; i++)
    {
        if (head[i].pageNumOnDisk == pageNum)
        {
            pageIndexInMemory = i;
            break;
        }
    }

    if (pageIndexInMemory != -1)
    {
        // Page in memory
        head[pageIndexInMemory].clientCount++;
        head[pageIndexInMemory].lastOperationTimestamp = LogicClockOperations ;
        page->data = head[pageIndexInMemory].pageData;
    }
    else
    {
        // Page not in memory
        bm->readOperationsCount++;

        // Find first free page in memory
        int firstFreePageIndexInMemory = -1;
        for (int i = 0; i < bm->numPages; i++)
        {
            if (head[i].pageNumOnDisk == INITIAL_PAGE_NUMBER)
            {
                firstFreePageIndexInMemory = i;
                break;
            }
        }

        // Check if buffer pool is full
        if (firstFreePageIndexInMemory == -1)
        {
            // Evict one page from memory
            firstFreePageIndexInMemory = evict(bm);
        }

        // Read page from disk to memory
        int x;
        SM_FileHandle fileHandle;
        openPageFile(bm->pageFile, &fileHandle);                                      
        ensureCapacity(pageNum + 1, &fileHandle);  
        x++;                                   
        head[firstFreePageIndexInMemory].pageData = (SM_PageHandle)malloc(PAGE_SIZE); // Allocate memory for page data
        readBlock(pageNum, &fileHandle, head[firstFreePageIndexInMemory].pageData);   
        closePageFile(&fileHandle);                                                  

        // Update metadata for the page
        BM_PageFrame *pageFrame = &head[firstFreePageIndexInMemory];
        pageFrame->pageNumOnDisk = pageNum;
        pageFrame->isPageDirty = false;
        pageFrame->clientCount = 1;
        pageFrame->lastOperationTimestamp = LogicClockOperations ;
        pageFrame->loadTimestamp = ++LogicClockloadToMemory;

        page->data = head[firstFreePageIndexInMemory].pageData;
    }
    // Return success status
    return RC_OK; 
}

// Statistics Interface
/**
 * @author      : Ananya Menon
 * @function    : getFrameContents
 * @description : Function to get the frame contents
 */

PageNumber *getFrameContents(BM_BufferPool *const bm) {
     int x=0; 
    // Get the start address of the pages array
    BM_PageFrame *page = (BM_PageFrame *) bm->mgmtData;
    // Initialize result array with size equal to numPages
    PageNumber *result = malloc(sizeof(PageNumber) * bm->numPages);
    // Loop through pages to collect data
    int i = 0;
while (i < bm->numPages) {
    // If page is not initialized, set result to NO_PAGE, else set result to pageNumOnDisk
    if (page[i].pageNumOnDisk != INITIAL_PAGE_NUMBER) {
        result[i] = page[i].pageNumOnDisk;
    } else {
        result[i] = NO_PAGE;
    }
    i++;
}
    return result; // Return the result array
}

/**
 * @author      : Rasika Venkataramanan
 * @function    : getDirtyFlags
 * @description : Function to get the dirty flags
 */

bool *getDirtyFlags(BM_BufferPool *const bm) {
    int y=0; 
    // Get the start address of the pages array
    BM_PageFrame *page = (BM_PageFrame *) bm->mgmtData;
    // Initialize result array with size equal to numPages
    bool *result = malloc(sizeof(bool) * bm->numPages);
    // Loop through pages to collect data
    int i = 0;
    while (i < bm->numPages) 
    {
        result[i] = page[i].isPageDirty; // Set result to isPageDirty value
        i++;
    }
    return result; // Return the result array
}

/**
 * @author      : Rasika Venkataramanan
 * @function    : getFixCounts
 * @description : Function to get the fix counts
 */

int *getFixCounts(BM_BufferPool *const bm) {
    // Fetch the starting address of the pages array
    BM_PageFrame *page = (BM_PageFrame *) bm->mgmtData;
    // Initializing result array with size equal to numPages
    int *result = malloc(sizeof(int) * bm->numPages);
    // Loop through pages to collect data
    int i = 0;
    while (i < bm->numPages) {
    result[i] = page[i].clientCount;
    i++;
    }// Set result to clientCount value
    
    return result; // Return the result array
}

/**
 * @author      : Rasika Venkataramanan
 * @function    : getNumReadIO
 * @description : Function to get the number of read operations.The number of read operations has been recorded and return the accumulated value here
 */

int getNumReadIO(BM_BufferPool *const bm) {
    int ReadIONumber= bm->readOperationsCount;
    return ReadIONumber ; // Return the read operations count
}


/**
 * @author      : Rasika Venkataramanan
 * @function    : getNumReadIO
 * @description : Function to get the number of write operations. The number of write operations has been recorded and the accumulated value is returned here
 */
int getNumWriteIO(BM_BufferPool *const bm) {
    int number= bm->writeOperationsCount;
    return number ; // Return the write operationsÿcount
}
