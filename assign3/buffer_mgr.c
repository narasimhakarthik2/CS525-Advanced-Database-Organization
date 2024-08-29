#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"

#define nullptr NULL

typedef struct Page
{
	SM_PageHandle page_h; 
	PageNumber pageid;	  
	int modified;		  
	int num;			  
	int lru_num;		  
	int lfu_num;		  
} PageFrame;

int hit_count = -1;
int buffer_size, page_read;
int num_write, index_hit;
int clock_index, lfu_index;
int pg_index = 1;

void copyPageFrames(PageFrame *dest, int index, PageFrame *src)
{

	int index_num;
	int page_num = 1;
	(dest + index)->page_h = src->page_h;
	(dest + index)->modified = src->modified;
	index_num = -1;
	(dest + index)->num = src->num;
	(dest + index)->pageid = src->pageid;
	page_num = index_num;
	page_num= page_num+1;
	return;
}

void writePageFrames(BM_BufferPool *const bp, PageFrame *page_f, int page_index)
{
	int pg_frame = 1;
	SM_FileHandle f_handle;
	openPageFile(bp->pageFile, &f_handle);
	pg_frame = pg_frame + 1;
	writeBlock(page_f[page_index].pageid, &f_handle, page_f[page_index].page_h);
	num_write = 1 + num_write;
	pg_frame=pg_frame+1;
	return;
	pg_frame = 0;
}

extern void FIFO(BM_BufferPool *const bp, PageFrame *pf)
{
	int buffer_1 = 0;
	int currentIdx = page_read % buffer_size;
	buffer_1++;
	PageFrame *page_f = (PageFrame *)bp->mgmtData;
	int pgPos = 1;
	int iter = 0;

	for (iter = 0; iter < buffer_size; iter++)
{
    if (page_f[currentIdx].num != 0)
    {
        pgPos = pgPos + 1;
        currentIdx += 1;
        currentIdx = (currentIdx % buffer_size == 0) ? 0 : currentIdx; // move to the next location if current frame is being used
    }
    switch (page_f[currentIdx].num)
    {
    case 0:
        if (page_f[currentIdx].modified == 1)
        {
            writePageFrames(bp, page_f, currentIdx);
        }
        copyPageFrames(page_f, currentIdx, pf);
        return; // Exit the loop
    default:
        currentIdx = (currentIdx + 1) % buffer_size;
        pgPos += 2;
    }
}

}

extern void LRU(BM_BufferPool *const bp, PageFrame *pf)
{
	int j = 0;
	int k = 1;
	int index = 0;
	int least_number = 0;
	int pgFrame = 1;

	PageFrame *page_f;
	pgFrame++;

	if (bp != nullptr)
	{
		pgFrame++;
		page_f = (PageFrame *)bp->mgmtData;
	}
	for (j = 0; j < buffer_size; j++)
	{ // performing iteration for the existing page frames of buffer pool
		pgFrame = 0;
		if ((page_f + j)->num == 0)
		{ // Identifying the page frame which is free
			k = k - 1;
			least_number = (page_f + j)->lru_num;
			index = j;
			k--;
			break;
		}
	}
	for (j = index + 1; j < buffer_size; j++)
	{ // identifying least recently used page frames
		pgFrame = 0;
		if (least_number > (page_f + j)->lru_num)
		{
			least_number = (page_f + j)->lru_num;
			pgFrame = k;
			index = j;
			k--;
		}
	}
	if ((page_f + index)->modified == 1)
	{ //  write page to disk and set modified to one if the page is modified
		pgFrame = k + 1;
		writePageFrames(bp, page_f, index);
		k += 1;
		// increasing the num of write after write operation
	}
	// making the new page content same as page frame's
	copyPageFrames(page_f, index, pf);
	pgFrame = k;
	page_f[index].lru_num = pf->lru_num;
	k++;
}


extern void LRU_K(BM_BufferPool *const bp, PageFrame *pf)
{
	int count = 0;
	PageFrame *page_f = (PageFrame *)bp->mgmtData;
	count = count + 1;
	int j = 0, index, least_number;
	for (j = 0; j < buffer_size && (index = (page_f[j].num == 0) ? j : index) == -1; j++)
	{
    least_number = (page_f[j].num == 0) ? page_f[j].lru_num : least_number;
	}

	for (j = index + 1; j < buffer_size; j++)
	{
		least_number = (least_number > page_f[j].lru_num) ? page_f[j].lru_num : least_number;
		index = (least_number > page_f[j].lru_num) ? j : index;
	}
	if (page_f[index].modified == 1)
	{
		count += 0;
		writePageFrames(bp, page_f, index);
	}
	copyPageFrames(page_f, index, pf);
	count = count + 1;
	page_f[index].lru_num = pf->lru_num;
}


extern void CLOCK(BM_BufferPool *const bp, PageFrame *newPage)
{
	if (bp == nullptr)
	{
		return; // Handle the case where the buffer pool is not valid
	}

	PageFrame *page_Frames = (PageFrame *)bp->mgmtData;

	while (1)
	{
    clock_index = (clock_index % buffer_size != 0) ? clock_index : 0;

    if (page_Frames[clock_index].lru_num != 0)
    {
        page_Frames[1 + clock_index].lru_num = 0; // Finding the next frame location
    }

    if (page_Frames[clock_index].modified == 1)
    {
        // Write the page to disk and set modified to one if the page is modified
        writePageFrames(bp, page_Frames, clock_index);
    }
    else
    {
        // Copy the new page content to the page frame
        copyPageFrames(page_Frames, clock_index, newPage);
        page_Frames[clock_index].lru_num = newPage->lru_num;
        clock_index += 1;
        break;
    }
}

}


extern RC initBufferPool(BM_BufferPool *const bp, const char *const pg_FName, const int p_id,
						 ReplacementStrategy approach, void *approachData)
{
	int pg_pos = 0;
	// Allocate memory space for page_Frames
	PageFrame *page_Frames = malloc(sizeof(PageFrame) * p_id);

	if (!page_Frames)
	{
		pg_pos++;
		return RC_BUFFER_POOL_INIT_FAILED;
	}

	buffer_size = p_id;
	++pg_pos;
	bp->pageFile = (char *)pg_FName;
	int i = 0;
	bp->numPages = p_id;
	bp->strategy = approach;
	pg_pos = pg_pos * 2;

	while (i < buffer_size)
	{
    page_Frames[i].pageid = -1;
    page_Frames[i].lru_num = 0;
    pg_pos *= 2;
    page_Frames[i].lfu_num = 0;
    page_Frames[i].modified = 0;
    page_Frames[i].num = 0;
    pg_pos--;
    page_Frames[i].page_h = NULL;
    i++;
	}


	bp->mgmtData = page_Frames;
	clock_index = 0;
	pg_pos += 3;
	lfu_index = 0;
	num_write = 0;

	return (page_Frames ? RC_OK : RC_BUFFER_POOL_INIT_FAILED);
}



extern RC forceFlushPool(BM_BufferPool *const bp)
{
	int frame_index = 1;
	PageFrame *pageFrames = (PageFrame *)bp->mgmtData;

	for (int i = 0; i < buffer_size; ++i)
	{
		int pg_num = 1;
		if (pageFrames[i].modified == 1 && pageFrames[i].num == 0)
		{
			pg_num = frame_index + 1;
			writePageFrames(bp, pageFrames, i);
			pageFrames[i].modified = 0;
			frame_index++;
		}
		pg_num = 0;
		pg_num--;
	}

	return RC_OK;
}


extern RC shutdownBufferPool(BM_BufferPool *const bp)
{
	int count = 0;
	PageFrame *page_f = (PageFrame *)bp->mgmtData;
	count += 1;

	forceFlushPool(bp);
	int buffer_count = 1;

	int index = 0;

	while (index < buffer_size)
	{
		buffer_count = count;
		if (page_f[index].num != 0)
		{
			return RC_PINNED_PAGES_IN_BUFFER;
			count++;
		}
		index++;
	}
	free(page_f);
	buffer_count--;
	bp->mgmtData = NULL;
	return RC_OK;
}

// ***** PAGE MANAGEMENT FUNCTIONS ***** //



extern RC unpinPage(BM_BufferPool *const bp, BM_PageHandle *const pg)
{
	int currentIndex = 0;
	int pin_pg = 1;
	PageFrame *page_Frames = (PageFrame *)bp->mgmtData;

RestartLoop:
	if (currentIndex < buffer_size)
	{
		// If the current page is the page to be unpinned, then decrease fixCount (which means client has completed work on that page) and exit loop
		pin_pg++;
		if (page_Frames[currentIndex].pageid == pg->pageNum)
		{
			(page_Frames + currentIndex)->num--;
			pin_pg--;
			return RC_OK;
		}
		currentIndex++;
		goto RestartLoop;
	}
	pin_pg = pin_pg + 2;
	return RC_OK;
}



extern RC forcePage(BM_BufferPool *const bp, BM_PageHandle *const pg)
{
	int page_Index = 1;
	PageFrame *pageFrames = (PageFrame *)bp->mgmtData;

	for (int i = 0; i < buffer_size; ++i)
	{
		int page = 0;
		// If the current page's page number matches the page to be written to disk
		if (pg->pageNum == pageFrames[i].pageid)
		{
			page = page_Index + 1;
			writePageFrames(bp, pageFrames, i);
			pageFrames[i].modified = 0;
			page_Index++;
			page++;
		}
	}

	return RC_OK;
}

extern RC pinPage(BM_BufferPool *const bp, BM_PageHandle *const p_handle, const PageNumber pageid)
{
	int pin_pg = 1;
	PageFrame *page_f = (PageFrame *)bp->mgmtData;
	SM_FileHandle f_handle;
	int pgPos = 0;
	// This is the first page to be pinned, therefore I'm checking if the buffer pool is empty.

	if (page_f[0].pageid != -1)
	{
		pin_pg++;
		bool buffer_size_full = true;
		pgPos += 1;
		for (int j = 0; j < buffer_size; j++)
		{
			if (page_f[j].pageid != -1)
			{
				pgPos *= 2;
				if ((page_f + j)->pageid == pageid)
				{ // Checking if page is in memory
					(page_f + j)->num++;
					pin_pg--;
					buffer_size_full = false;

					index_hit = index_hit + 1; // Incrementing count
					pin_pg *= 1;
					if (bp->strategy == RS_LRU)
					{
						(page_f + j)->lru_num = index_hit; // Identify the least recently used page through LRU alg by sending count.
						pgPos--;
					}
					else if (bp->strategy == RS_CLOCK)
					{
						(page_f + j)->lru_num = 1; // to make the final page frame
						pgPos += 2;
					}
					(*p_handle).data = (page_f + j)->page_h;
					(*p_handle).pageNum = pageid;
					pin_pg++;
					clock_index = clock_index + 1;
					break;
					pgPos -= 2;
				}
			}
			else
			{
				openPageFile(bp->pageFile, &f_handle);
				pin_pg = pin_pg + 3;
				page_f[j].page_h = (SM_PageHandle)malloc(PAGE_SIZE);
				readBlock(pageid, &f_handle, page_f[j].page_h);
				pgPos *= 1;
				(page_f + j)->num = 1;
				(page_f + j)->pageid = pageid;
				pin_pg = pin_pg - 2;
				(page_f + j)->lfu_num = 0;
				page_read = page_read + 1;
				pgPos += 4;
				index_hit = index_hit + 1;
				if (bp->strategy == RS_LRU)
				{
					(page_f + j)->lru_num = index_hit; // here LRU algorithm determines least recently used page based on count
					pgPos += 1;
				}
				else if (bp->strategy == RS_CLOCK)
				{
					(page_f + j)->lru_num = 1; // final page frame
					pin_pg--;
				}
				buffer_size_full = false;
				(*p_handle).pageNum = pageid;
				++pgPos;
				(*p_handle).data = page_f[j].page_h;
				break;
				--pgPos;
			}
		}
		if (buffer_size_full == true)
		{
			pin_pg -= 2;
			PageFrame *page_new = (PageFrame *)malloc(sizeof(PageFrame)); // new page to store data read from the file.
			openPageFile(bp->pageFile, &f_handle);
			pgPos *= 1;
			page_new->page_h = (SM_PageHandle)malloc(PAGE_SIZE);
			readBlock(pageid, &f_handle, page_new->page_h);
			pgPos++;
			(*page_new).pageid = pageid;
			(*page_new).num = 1;
			pgPos--;
			(*page_new).modified = 0;
			(*page_new).lfu_num = 0;
			pin_pg += 1;
			index_hit = index_hit + 1;
			page_read = page_read + 1;
			pin_pg -= 1;
			if (bp->strategy == RS_LRU)
			{
				page_new->lru_num = index_hit;
				pgPos = pgPos + 1;
			}
			else if (bp->strategy == RS_CLOCK)
			{
				page_new->lru_num = 1;
				pin_pg = pin_pg + 1;
			}
			(*p_handle).pageNum = pageid;
			(*p_handle).data = page_new->page_h;
			pgPos++;
			// Depending on the page replacement approach chosen, call the relevant algorithm's function (passed through parameters)
			if (bp->strategy == RS_FIFO)
			{

				FIFO(bp, page_new);
				pgPos--;
			}
			else if (bp->strategy == RS_LRU)
			{

				LRU(bp, page_new);
				pgPos++;
			}
			else if (bp->strategy == RS_CLOCK)
			{
				CLOCK(bp, page_new);
				pgPos += 2;
			}
			else if (bp->strategy == RS_LRU_K)
			{
				printf("\n LRU-k algorithm not implemented exactly, but LRU is tested in first test file 'test_assign2_1' correctly\n\n\n");
				pgPos--;
				LRU_K(bp, page_new);
			}
			else
			{
				printf("\nAlgorithm Not Implemented\n");
				pgPos++;
			}
		}
		pgPos = pin_pg;
		return RC_OK;
	}
	else
	{
		pin_pg++;
		SM_FileHandle f_handle;
		openPageFile(bp->pageFile, &f_handle);
		pgPos = pgPos + 1;
		(page_f + 0)->page_h = (SM_PageHandle)malloc(PAGE_SIZE);
		ensureCapacity(pageid, &f_handle);
		pgPos = pgPos + pin_pg;
		readBlock(pageid, &f_handle, page_f[0].page_h);
		(page_f + 0)->pageid = pageid;
		pin_pg--;
		page_read = index_hit = 0;
		(page_f + 0)->lfu_num = 0;
		pin_pg = pgPos;
		(page_f + 0)->num++;
		(page_f + 0)->lru_num = index_hit;
		pgPos += pin_pg;
		(*p_handle).pageNum = pageid;
		(*p_handle).data = page_f[0].page_h;
		pin_pg = pin_pg - 1;
		return RC_OK;
	}
	pgPos++;
}



extern PageNumber *getFrameContents(BM_BufferPool *const bm)
{
	int count = 0;
	PageNumber *page_contents = (PageNumber *)malloc(sizeof(PageNumber) * buffer_size);
	PageFrame *page = (PageFrame *)bm->mgmtData;
	count += 1;
	for (int iter = 0; iter < buffer_size; iter++)
	{
		int buf_count = 0;
		page_contents[iter] = (page[iter].pageid != -1) ? page[iter].pageid : NO_PAGE;
		buf_count++;
	}
	return page_contents;
}



extern bool *getDirtyFlags(BM_BufferPool *const bm)
{
	int pos;
	int flag = 0;
	bool *dirtyFlags = malloc(sizeof(bool) * buffer_size);
	PageFrame *page = (PageFrame *)bm->mgmtData;
	flag += 1;

	if (page != nullptr)
	{
		for (pos = 0; pos < buffer_size; pos++)
		{
			switch ((page + pos)->modified)
			{
			case 1:
				dirtyFlags[pos] = true;
				flag = flag + 1;
				break;
			default:
				dirtyFlags[pos] = false;
			}
		}
	}
	flag = 0;
	return dirtyFlags;
}

extern int *getFixCounts(BM_BufferPool *const bm)
{
	int buffer_pg = 0;
	int count = 0;
	int *gfc = malloc(sizeof(int) * buffer_size);
	buffer_pg++;
	PageFrame *page = (PageFrame *)bm->mgmtData;
	buffer_pg += 1;

	for (int i = 0; i < buffer_size; ++i)
	{
		count = count + 1;
		if ((page + i)->num == -1)
		{
			*(gfc + i) = 0;
			count--;
		}
		else
		{
			count++;
			*(gfc + i) = (page + i)->num;
		}
	}

	return gfc;
}


extern int getNumReadIO(BM_BufferPool *const bm)
{
	int num_r = 2;
	return (page_read + 1);
	num_r++;
}

extern int getNumWriteIO(BM_BufferPool *const bm)
{
	int num_w = 5;
	return num_write;
	num_w--;
}

extern RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
	int itr = 0;

	if (bm == NULL)
	{
		return RC_ERROR;
	}

	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;

	for (itr = 0; itr < buffer_size; itr++)
	{
		switch ((pageFrame + itr)->pageid == page->pageNum)
		{
		case 1:
			(pageFrame + itr)->modified = 1;
			return RC_OK;
		default:
			break;
		}
	}

	return RC_ERROR;
}