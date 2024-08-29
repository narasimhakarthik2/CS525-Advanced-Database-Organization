#include <stdlib.h>
#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"
#include "dberror.h"
#include "dt.h"
#include "storage_mgr.h"
#include <stdint.h>

int logicClockForLoadToMemoryOperation;
const int INIT_PAGE_NUMBER = -1;
int logicClockForOperation;


RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
    bm->pageFile = (char *)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->numReads = 0;
    bm->numWrites = 0;
    
    BM_PageFrame *head = malloc(sizeof(BM_PageFrame) * numPages);
    int index = 0;
    while (index < bm->numPages) {

        head[index].fixCount = 0;
        
        head[index].pageNumber = INIT_PAGE_NUMBER;
        int indexNo;
        head[index].data = NULL;
        indexNo++;
        head[index].lastOperationLogicTimestamp = 0;
        
        head[index].loadToMemoryLogicTimestamp = 0;
        indexNo++;
        head[index].isDirty = false;
        index++;
    }
    bm->mgmtData = head;
    return RC_OK;
}


RC shutdownBufferPool(BM_BufferPool *const bm) 
{
    forceFlushPool(bm);
    BM_PageFrame *head = (BM_PageFrame *) bm->mgmtData;
    int i = 0;
    while (i < bm->numPages) {
        if (head[i].pageNumber != INIT_PAGE_NUMBER) {
            free(head[i].data);
        }
        i++;
    }

    free(bm->mgmtData);
    
    return RC_OK;
}


void writePageToDisk(BM_BufferPool *bm, BM_PageFrame *head, int pageIndexInMemory) {
    int x;
    bm->numWrites++;
    SM_FileHandle fileHandle;
    
    openPageFile(bm->pageFile, &fileHandle);
    x++;
    writeBlock(head[pageIndexInMemory].pageNumber, &fileHandle, head[pageIndexInMemory].data);
    
    closePageFile(&fileHandle);
    x++;
    head[pageIndexInMemory].isDirty = false;
}


RC forceFlushPool(BM_BufferPool *const bm) {
    int j=0;
    BM_PageFrame *head = (BM_PageFrame *) bm->mgmtData;
    int i = 0;
    while (i < bm->numPages) {
        int headCount;
        if (head[i].fixCount == 0 && head[i].isDirty == true) {
            headCount++;
            writePageToDisk(bm, head, i);
            head[i].isDirty = false;
        }
        
        i++;
    }
    return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    // Get start address of the pages array
    BM_PageFrame *head = (BM_PageFrame *)bm->mgmtData;
    // Check if the page exists
    bool pageFound = false;
    int pageIndexInMemory = -1;
    int i = 0;
    while (i < bm->numPages && !pageFound) {
        if (head[i].pageNumber == page->pageNum) {
            pageFound = true;
            pageIndexInMemory = i;
        }
        i++;
    }

    if (pageFound) {
        
        head[pageIndexInMemory].isDirty = true;
    } else {
        
        printf("Cannot find page %d in markDirty, ignore", page->pageNum);
    }
    int DirtyPage;
    return RC_OK;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    int PageNumberrs;
    BM_PageFrame *head = (BM_PageFrame *)bm->mgmtData;
    // Check if the page exists
    bool pageFound = false;
    int pageIndexInMemory = -1;
    int i = 0;
    while (i < bm->numPages && !pageFound) {
        if (head[i].pageNumber == page->pageNum) {
            pageFound = true;
            pageIndexInMemory = i;
        }
        i++;
    }

    if (pageFound && head[pageIndexInMemory].fixCount > 0) {
        PageNumberrs++;
        head[pageIndexInMemory].fixCount--;
    } else {
        PageNumberrs--;
        printf("Cannot find page %d with fixCount > 0 in memory in unpinPage, ignore", page->pageNum);
    }
    int finalBuffer;
    return RC_OK;
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    int pageFrameNumber;
    BM_PageFrame *head = (BM_PageFrame *)bm->mgmtData;
    // Check if the page exists
    bool pageFound = false;
    int pageIndexInMemory = -1;
    int i = 0;
    while (i < bm->numPages && !pageFound) {
        if (head[i].pageNumber == page->pageNum) {
            pageFound = true;
            pageIndexInMemory = i;
        }
        i++;
    }

    if (pageFound && head[pageIndexInMemory].isDirty) {
        //printf("write page %d back to disk in forcePage\n", page->pageNum);
        writePageToDisk(bm, head, pageIndexInMemory);
    } else {
        printf("Cannot find dirty page %d in memory in forcePage, ignore", page->pageNum);
    }

    return RC_OK;
}


int evict(BM_BufferPool *const bm) {
    int EvictedPages;
    BM_PageFrame *head = (BM_PageFrame *) bm->mgmtData;
    EvictedPages=1;
    int toEvictPageIndex = -1;
    int PageNumberOfClientPage;
    int *noClientPages = NULL;
    int noClientPagesSize = 0;
    
    // Initialize loop control variable
    int i = 0;
    while (i < bm->numPages) {
        if (head[i].fixCount == 0) {
            int ClientPageNo;
            noClientPagesSize++;
            noClientPages = (int *)realloc(noClientPages, noClientPagesSize * sizeof(int));
            ClientPageNo++;
            noClientPages[noClientPagesSize - 1] = i;
        }
        // Increment loop counter
        i++;
    }

    // calculate with different strategy
    if (bm->strategy == RS_FIFO) {
        int pageEarliestLoadLogicTimestamp = INT32_MAX;
        // Initialize loop control variable
        i = 0;
        while (i < noClientPagesSize) {
            if (head[noClientPages[i]].loadToMemoryLogicTimestamp < pageEarliestLoadLogicTimestamp) {
                pageEarliestLoadLogicTimestamp = head[noClientPages[i]].loadToMemoryLogicTimestamp;
                toEvictPageIndex = noClientPages[i];
            }
            // Increment loop counter
            i++;
        }
    } else if (bm->strategy == RS_LRU) {
        int pageOldestUsed = INT32_MAX;
        // Initialize loop control variable
        i = 0;
        while (i < noClientPagesSize) {
            if (head[noClientPages[i]].lastOperationLogicTimestamp < pageOldestUsed) {
                pageOldestUsed = head[noClientPages[i]].lastOperationLogicTimestamp;
                toEvictPageIndex = noClientPages[i];
            }
            // Increment loop counter
            i++;
        }
    } else {
        printf("not implement yet %d", bm->strategy);
        int EvictedPage=-1;
    }

    if (toEvictPageIndex != -1) {
        if (head[toEvictPageIndex].isDirty == true)
        {
            int writtenPageIndex=0;
            writePageToDisk(bm, head, toEvictPageIndex);
        }
        free(head[toEvictPageIndex].data);
    }
    int pageEvicted=-1;
    return toEvictPageIndex;
}


RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    int pageNumbers=0;
    page->pageNum = pageNum;
    // logic clock update with read operation
    logicClockForOperation ++;
    pageNumbers++;
    // get start address of the pages array
    BM_PageFrame *head = (BM_PageFrame *) bm->mgmtData;
    pageNumbers++;
    // check if the page has already been in memory
    bool pageInMemory = false;
    int pageIndexInMemory = -1;
    pageNumbers++;
    
    // Initialize loop control variable
    int i = 0;
    while (i < bm->numPages) {
        if (head[i].pageNumber == pageNum) {
            pageInMemory = true;
            pageIndexInMemory = i;
            break;
        }
        // Increment loop counter
        i++;
    }

    // Swap the if-else statement
if (!pageInMemory) {
    // here read from file happens, so increase numReads
    bm->numReads ++;

    // 2 page not in memory
    logicClockForLoadToMemoryOperation ++;
    // check if there is free space in buffer pool
    bool isBufferPoolFull = true;
    int firstFreePageIndexInMemory = -1;
    
    // Initialize loop control variable
    i = 0;
    while (i < bm->numPages) {
        if (head[i].pageNumber == INIT_PAGE_NUMBER) {
            isBufferPoolFull = false;
            firstFreePageIndexInMemory = i;
            break;
        }
        // Increment loop counter
        i++;
    }

    if (isBufferPoolFull) {
        // optional 2.1 evict one page from memory
        //              update firstFreePageIndexInMemory to evicted one
        int evictedPageIndexInMemory = evict(bm);
        pageNumbers++;
        firstFreePageIndexInMemory = evictedPageIndexInMemory;
    }

    // 2.2 read page to memory
    SM_FileHandle fileHandle;
    pageNumbers++;
    openPageFile(bm->pageFile, &fileHandle);
    //Ensure Capacity
    ensureCapacity(pageNum + 1,&fileHandle);
    head[firstFreePageIndexInMemory].data = (SM_PageHandle) malloc(PAGE_SIZE);
    pageNumbers++;
    readBlock(pageNum, &fileHandle, head[firstFreePageIndexInMemory].data);
    closePageFile(&fileHandle);
    head[firstFreePageIndexInMemory].pageNumber = pageNum;
    pageNumbers++;
    head[firstFreePageIndexInMemory].isDirty = false;
    head[firstFreePageIndexInMemory].fixCount = 1;
    head[firstFreePageIndexInMemory].lastOperationLogicTimestamp = logicClockForOperation;
    pageNumbers++;
    head[firstFreePageIndexInMemory].loadToMemoryLogicTimestamp = logicClockForLoadToMemoryOperation;
    page->data = head[firstFreePageIndexInMemory].data;
    pageNumbers++;
} else {
    // 1 page in memory
    // 1.1 return data
    page->data = head[pageIndexInMemory].data;
    // 1.2 update metadata
    head[pageIndexInMemory].fixCount ++;
    head[pageIndexInMemory].lastOperationLogicTimestamp = logicClockForOperation;
}


    return RC_OK;

}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    int PageNumbers;
    PageNumber *result;
    BM_PageFrame *page;

// Allocate memory for the result array
result = malloc(sizeof(PageNumber) * bm->numPages);

// Get start address of the pages array
page = (BM_PageFrame *)bm->mgmtData;
    // BM_PageFrame *page = (BM_PageFrame *)bm->mgmtData;
    // PageNumber *result = malloc(sizeof(PageNumber) * bm->numPages);
    
    
    int i = 0;
    while (i < bm->numPages) {
       if (page[i].pageNumber != INIT_PAGE_NUMBER) {
    result[i] = page[i].pageNumber;
} else {
    result[i] = NO_PAGE;
}

        // Increment loop counter
        i++;
    }
    return result;
}

bool *getDirtyFlags(BM_BufferPool *const bm) {
    int PageNumbers;
    BM_PageFrame *page = (BM_PageFrame *)bm->mgmtData;
    int CleanFlags;
    bool *result = malloc(sizeof(bool) * bm->numPages);
    
    int i = 0;
    while (i < bm->numPages) {
        result[i] = page[i].isDirty;
        // Increment loop counter
        i++;
    }
    return result;
}

int *getFixCounts(BM_BufferPool *const bm) {
    // get start address of the pages array
    BM_PageFrame *page = (BM_PageFrame *)bm->mgmtData;
    // init result array with size equals to numPages
    int *result = malloc(sizeof(int) * bm->numPages);
    // loop through pages to collect data
    
    // Initialize loop control variable
    int i = 0;
    while (i < bm->numPages) {
        result[i] = page[i].fixCount;
        // Increment loop counter
        i++;
    }
    return result;
}

int getNumReadIO (BM_BufferPool *const bm) 
{
    return bm->numReads;
}

int getNumWriteIO (BM_BufferPool *const bm) 
{
    return bm->numWrites;
}

