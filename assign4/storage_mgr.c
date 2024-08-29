#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sys/stat.h"
#include "storage_mgr.h"
#include "dberror.h"
#include <unistd.h>
#include <sys/types.h>

FILE *filePtr;

extern void initStorageManager(void)
{
    printf("Storage manager initialization.\n");
    filePtr = NULL;
}

extern RC createPageFile(char *fileName)
{
    FILE *filePtr = fopen(fileName, "w+");

    // Checks if the file was successfully opened
    if (filePtr == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }

    SM_PageHandle new_pagefile = (char *)calloc(PAGE_SIZE, sizeof(char));
    if (new_pagefile == NULL)
    {
        fclose(filePtr); // Closes the file before returning
        printf("File Closed");
        return RC_WRITE_FAILED;
    }

    // Writes the initialized page to the file
    if (fwrite(new_pagefile, 1, PAGE_SIZE, filePtr) != PAGE_SIZE)
    {
        fclose(filePtr);    // Closes the file before returning
        free(new_pagefile); // Frees the memory allocated for the new page
        printf("File Freed");
        return RC_WRITE_FAILED;
    }
    free(new_pagefile);

    if (fclose(filePtr) != 0)
    {
        return RC_CLOSE_FAILED;
    }

    return RC_OK;
}

extern RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    FILE *filePtr = fopen(fileName, "r+");

    if (filePtr == NULL)
    {
        printf("FIle not found");
        return RC_FILE_NOT_FOUND;
    }

    // File attribute initialization
    int curPagePos = 0;
    fHandle->mgmtInfo = filePtr;
    fHandle->curPagePos = curPagePos;
    fHandle->fileName = fileName;

    struct stat buffer; // File information obtained from the fstat function.
    if (fstat(fileno(filePtr), &buffer) != 0)
    {
        fclose(filePtr);
        return RC_FETCH_FAILED;
    }

    off_t pageSize = buffer.st_size;
    fHandle->totalNumPages = (int)pageSize / PAGE_SIZE;

    return RC_OK;
}

extern RC closePageFile(SM_FileHandle *fHandle)
{
    if (fHandle == NULL || fHandle->mgmtInfo == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    else
    {
        int closeFileResult = fclose(fHandle->mgmtInfo);
        fHandle->mgmtInfo = NULL; // Ensures mgmtInfo is set to NULL regardless of fclose result
        if (closeFileResult != 0)
        {
            return RC_CLOSE_FAILED;
        }
        return RC_OK;
    }
}

extern RC destroyPageFile(char *fileName)
{
    int removeResult = remove(fileName);
    if (removeResult = 0)
    {
        // Handle the failure to remove the file
        return RC_OK;
    }
}

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int fileTest;

    if (fHandle == NULL || fHandle->mgmtInfo == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT; // Checks if the file handle is initialized
    }

    FILE *filePtr = (FILE *)fHandle->mgmtInfo;

    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) // Checks if the page number is within bounds
    {
        fileTest++;
        // printf(fileTest);
        return RC_READ_NON_EXISTING_PAGE;
    }

    if (fseek(filePtr, pageNum * PAGE_SIZE, SEEK_SET) != 0) // Moves the file pointer to the beginning of the requested page
    {
        return RC_READ_NON_EXISTING_PAGE;
    }

    if (fread(memPage, sizeof(char), PAGE_SIZE, filePtr) != PAGE_SIZE) // Reads the page into memory
    {
        return RC_READ_FAILED;
    }

    fHandle->curPagePos = pageNum;

    return RC_OK;
}

extern int getBlockPos(SM_FileHandle *fHandle)
{
    int blockPos = fHandle->curPagePos;
    return blockPos;
}

extern RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    fseek(filePtr, 0, SEEK_SET);
    if (fread(memPage, sizeof(char), PAGE_SIZE, filePtr) != PAGE_SIZE) // Reads the first block into memPage
    {
        return RC_READ_FAILED;
    }
    fHandle->curPagePos = 0;
    return RC_OK;
}

extern RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int previousPage = getBlockPos(fHandle) - 1;
    return readBlock(previousPage, fHandle, memPage); // Calls the readBlock function and return its result
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    printf("Reading current block...\n");
    RC readBlockRes = readBlock(fHandle->curPagePos, fHandle, memPage);
    if (readBlockRes != RC_OK)
    {
        printf("Failed to read the current block.\n");
        return readBlockRes;
    }
    return RC_OK;
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    //Number of blocks read
    int blockCount = 0;

    RC readBlockRes = readBlock(fHandle->curPagePos + 1, fHandle, memPage);
    if (readBlockRes != RC_OK)
    {
        return readBlockRes;
    }
    blockCount++;
    fHandle->curPagePos += 1;

    return RC_OK;
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int lastBlockIndex = fHandle->totalNumPages - 1;
    RC readBlockRes;
    switch (readBlockRes = readBlock(lastBlockIndex, fHandle, memPage))
    {
    case RC_OK:
        fHandle->curPagePos = lastBlockIndex;
        return RC_OK;
    default:
        return readBlockRes;
    }
}

/* Write blocks - page file */

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fHandle == NULL || fHandle->mgmtInfo == NULL || fHandle->totalNumPages <= pageNum)
    {
        printf("File handle is not initialized/ pageNum is invalid.\n");
        return RC_FILE_HANDLE_NOT_INIT;
    }

    if (fseek((FILE *)fHandle->mgmtInfo, PAGE_SIZE * pageNum, SEEK_SET) != 0)
    {
        printf("RC write Failure");
        return RC_WRITE_FAILED;
    }

    size_t bytesWritten = fwrite(memPage, PAGE_SIZE, 1, (FILE *)fHandle->mgmtInfo);
    if (bytesWritten != 1)
    {
        return RC_WRITE_FAILED;
    }

    fHandle->curPagePos = pageNum;
    return RC_OK;
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    printf("Writing content from memory to disk...\n");
    RC writeBlockRes = writeBlock(fHandle->curPagePos, fHandle, memPage);
    if (writeBlockRes != RC_OK)
    {
        return writeBlockRes;
    }
    return RC_OK;
}

RC appendEmptyBlock(SM_FileHandle *fHandle)
{

    if (fHandle == NULL || fHandle->mgmtInfo == NULL) // Checks if the file handle is initialized
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    FILE *filePtr = (FILE *)fHandle->mgmtInfo;

    fseek(filePtr, 0, SEEK_END);

    for (int i = 0; i < PAGE_SIZE; i++)
    {
        fwrite("\0", 1, 1, filePtr); // Writes null to create an empty page
    }

    fHandle->totalNumPages++;
    fHandle->curPagePos++;

    return RC_OK;
}

extern RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
    if (fHandle == NULL || fHandle->mgmtInfo == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    if (fHandle->totalNumPages >= numberOfPages)
    {
        return RC_OK;
    }

    int additionalPages = numberOfPages - fHandle->totalNumPages; // Calculates the number of additional pages needed
    printf("Additional Pages added %d", +additionalPages);

    for (int i = 0; i < additionalPages; i++)
    {
        RC appendResult = appendEmptyBlock(fHandle);
        if (appendResult != RC_OK)
        {
            return appendResult;
        }
    }
    fHandle->totalNumPages = numberOfPages;
    return RC_OK;
}