#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

// Include return codes and methods for logging errors
#include "dberror.h"
// storage manager
#include "storage_mgr.h"
// Include bool DT
#include "dt.h"

// Replacement Strategies
typedef enum ReplacementStrategy {
	RS_FIFO = 0,
	RS_LRU = 1,
	RS_CLOCK = 2,
	RS_LFU = 3,
	RS_LRU_K = 4
} ReplacementStrategy;

// Data Types and Structures
typedef int PageNumber;
#define NO_PAGE -1

typedef struct BM_BufferPool {
	char *pageFile;
	int numPages;
	ReplacementStrategy strategy;
	void *mgmtData; // use this one to store the bookkeeping info your buffer
	// manager needs for a buffer pool
	// Extended properties
	int readOperationsCount;
	int writeOperationsCount;
} BM_BufferPool;


// Structure representing a page in memory containing both data and metadata
typedef struct BM_PageFrame {
	
	SM_PageHandle pageData;		// Data read from disk
	
	PageNumber pageNumOnDisk;	// Page number in the disk file of the current page frame in memory

	bool isPageDirty;			// Flag indicating whether the page content has been modified

	int clientCount;			// Number of clients currently using the page

	int totalClientCount;		// Total count of clients that have used the page

	int lastOperationTimestamp;	// Logical timestamp indicating when the page was last operated on

	int loadTimestamp;			// Logical timestamp indicating when the page was loaded into memory
} BM_PageFrame;

typedef struct BM_PageHandle {
	PageNumber pageNum;
	char *data;
} BM_PageHandle;

// convenience macros
#define MAKE_POOL()					\
		((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
		((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData);
RC shutdownBufferPool(BM_BufferPool *const bm);
RC forceFlushPool(BM_BufferPool *const bm);

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page);
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum);

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm);
bool *getDirtyFlags (BM_BufferPool *const bm);
int *getFixCounts (BM_BufferPool *const bm);
int getNumReadIO (BM_BufferPool *const bm);
int getNumWriteIO (BM_BufferPool *const bm);

#endif
