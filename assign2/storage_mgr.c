#include "dberror.h"
#include "storage_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

// Global file pointer
FILE *fileRef;

/**
 * @author      : Narasimha Karthik
 * @function    : initStorageManager
 * @description : This function initializes the file pointer to NULL.
 */

void initStorageManager(void)
{
    printf("\n <---------------Initializing Storage Manager----------------->\n"); // Print a message indicating the start of initialization
    fileRef = NULL;                                                                // Set the file reference to NULL
}

/**
 * @author      : Narasimha Karthik
 * @function    : createPageFile
 * @description : This function creates a new page file and writes two pages: one filled with zeroes and one with a value of 3.
 * @param       : fileName  // The name of the file to be created
 */

RC createPageFile(char *fileName)
{
    fileRef = fopen(fileName, "w+"); // Open a file with the given filename in write mode

    if (!fileRef)
    {
        return RC_FILE_NOT_FOUND; // If the file couldn't be opened, return an error code
    }
    else
    {
        char *pageZero = (char *)calloc(PAGE_SIZE, sizeof(char)); // Allocate memory for a page filled with zeroes
        if (!pageZero)
        {
            fclose(fileRef); // If memory allocation failed, close the file and return an error code
            return RC_WRITE_FAILED;
        }

        char *pageWithValueOne = (char *)calloc(PAGE_SIZE, sizeof(char)); // Allocate memory for a page with a value of 3
        if (!pageWithValueOne)
        {
            free(pageZero); // If memory allocation failed, free the previously allocated memory, close the file, and return an error code
            fclose(fileRef);
            return RC_WRITE_FAILED;
        }

        fputs("1", fileRef); // Write the character '1' to the first page

        fwrite(pageZero, PAGE_SIZE, 1, fileRef); // Write the page filled with zeroes to the second page

        free(pageZero); // Free the memory allocated for the pageZero

        fwrite(pageWithValueOne, PAGE_SIZE, 1, fileRef); // Write the page with a value of 3 to the third page

        free(pageWithValueOne); // Free the memory allocated for pageWithValueThree

        if (fclose(fileRef) != 0)
        {                                   // Close the file
            return RC_FILE_HANDLE_NOT_INIT; // If the file couldn't be closed, return an error code
        }

        return RC_OK; // Return RC_OK to indicate success
    }
}

/**
 * @author      : Narasimha Karthik
 * @function    : openPageFile
 * @description : This function opens an existing page file and reads the header page to get the total number of pages.
 * @param       : fileName  // The name of the file to be opened
 * @param       : fHandle   // The file handle to be initialized
 */

RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    fileRef = fopen(fileName, "r+"); // Open the file in read/write mode

    if (fileRef == NULL)
    {
        // If the file couldn't be opened, return an error code
        printf("Error! File cannot be opened");
        return RC_FILE_NOT_FOUND; 
    }

    // Initialize fileHandle attributes
    fHandle->fileName = strdup(fileName); // Set the filename in the file handle
    fHandle->totalNumPages = 0;           // Initialize the total number of pages to 0
    fHandle->mgmtInfo = fileRef;          // Set the file reference in the file handle
    fHandle->curPagePos = 0;              // Initialize the current page position to 0

    char page_counter[PAGE_SIZE]; // Read header page to get the total number of pages

    if (fgets(page_counter, PAGE_SIZE, fileRef) == NULL)
    {

        fclose(fileRef);          // If the header page couldn't be read, close the file
        free(fHandle->fileName);  // Free the memory allocated for the filename
        fHandle->fileName = NULL; // Set the filename in the file handle to NULL
        return RC_FILE_NOT_FOUND; // Return an error code
    }
    else
    {

        fHandle->totalNumPages = atoi(page_counter); // Convert the string to an integer and set the total number of pages
        return RC_OK;                                // Return RC_OK to indicate success
    }
}

/**
 * @author      : Narasimha Karthik
 * @function    : closePageFile
 * @description : This function closes an open page file and nullifies the file handle, indicating that the file is no longer in use.
 * @param       : fHandle   // The file handle to be closed
 */

RC closePageFile(SM_FileHandle *fHandle)
{
    if (fHandle == NULL || fHandle->mgmtInfo == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT; // If the file handle or the file reference is NULL, return an error code
    }

    // Attempt to close the file handle

    if (fclose(fHandle->mgmtInfo) == 0)
    { // The fclose function returns 0 upon success

        fHandle->mgmtInfo = NULL; // Nullify the file handle after closing

        return RC_OK; // Return RC_OK to indicate success
    }

    return RC_FILE_NOT_FOUND; // Return RC_FILE_NOT_FOUND to indicate an error
}

/**
 * @author      : Narasimha Karthik
 * @function    : destroyPageFile
 * @description : This function deletes a page file from the file system, freeing up storage space.
 * @param       : fileName  // The name of the file to be deleted
 */

RC destroyPageFile(char *fileName)
{
    if (fileName == NULL)
    {
        return RC_FILE_NOT_FOUND; // If the filename is NULL, return an error code
    }

    // The remove function returns 0 upon success
    // If the file was successfully deleted, return RC_OK
    // If the file couldn't be deleted, return an error code
    return remove(fileName) == 0 ? RC_OK : RC_FILE_NOT_FOUND;
}

/**
 * @author      : Ananya Menon
 * @function    : readBlock
 * @description : This function reads a block from a page file from the file system
 * @param       : pageNum  // Page Number to be read
 * @param       : fHandle // The file handle to be read
 * @param       : memPage // Memory page
 */

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Check if pageNum is valid
    if (pageNum >= fHandle->totalNumPages || pageNum < 0)
    {
        return RC_READ_NON_EXISTING_PAGE; // If page number is out of range, it will throw an error
    }

    FILE *file = fHandle->mgmtInfo;

    // Calculate the byte offset to the beginning of the page
    long offset = (long)(pageNum + 1) * PAGE_SIZE;

    // Seek to the beginning of the page
    if (fseek(file, offset, SEEK_SET) != 0)
    {
        return RC_READ_NON_EXISTING_PAGE; // return an error code
    }
    else
    {
        // Reading the stream from file and to memPage
        size_t bytesRead = fread(memPage, 1, PAGE_SIZE, file);

        if (bytesRead == PAGE_SIZE)
        {
            // Updating the current page position
            fHandle->curPagePos = pageNum;
            return RC_OK;
        }
    }
}

/**
 * @author      : Ananya Menon
 * @function    : getBlockPos
 * @description : This function will return the position of current block associated with the fHandle
 * @param       : fHandle // The file handle to be read
 */

int getBlockPos(SM_FileHandle *fHandle)
{
    return fHandle->curPagePos; // Current Page Postion is returned
}

/**
 * @author      : Ananya Menon
 * @function    : readFirstBlock
 * @description : This function reads the first block associated with the fHandle
 * @param       : fHandle // The file handle to be read
 * @param       : memPage // Memory
 */
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Calling readBlock with page number 0 (the first block)
    RC returnCode = readBlock(0, fHandle, memPage);
    return returnCode;
}
/**
 * @author      : Ananya Menon
 * @function    : readPreviousBlock
 * @description : This function reads the previous block associated with the fHandle
 * @param       : fHandle // The file handle to be read
 * @param       : memPage // Memory
 */

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Calculating the page number of the previous block (currentPage - 1)
    int currentPageNum = getBlockPos(fHandle);
    int previousPageNum = currentPageNum - 1;

    if (previousPageNum < 0)
    {
        return RC_READ_NON_EXISTING_PAGE; // If the position of the previous page is less than zero, throw an error
    }
    else
    {
        // Read the previous block using readBlock
        return readBlock(previousPageNum, fHandle, memPage);
    }
}
/**
 * @author      : Ananya Menon
 * @function    : readCurrentBlock
 * @description : This function reads the current block associated with the fHandle
 * @param       : fHandle // The file handle to be read
 * @param       : memPage // Memory
 */
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Getting the current position of the read block
    int blockPosition = getBlockPos(fHandle);

    if (blockPosition < 0 || blockPosition >= fHandle->totalNumPages)
    {
        return RC_READ_NON_EXISTING_PAGE; // If the fHandle is NULL or blockPosition is out of bounds, throw an error.
    }
    else
    {
        // Reading the current block using readBlock
        return readBlock(blockPosition, fHandle, memPage);
    }
}
/**
 * @author      : Ananya Menon
 * @function    : readNextBlock
 * @description : This function reads the current block associated with the fHandle
 * @param       : fHandle // The file handle to be read
 * @param       : memPage // Memory
 */
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Getting the position of the next block (currentPage + 1)
    int nextBlockPosition = getBlockPos(fHandle) + 1;

    if (nextBlockPosition >= fHandle->totalNumPages)
    {
        return RC_READ_NON_EXISTING_PAGE; // If nextBlockPosition is out of bounds, throw an error
    }
    else
    {
        // Reading the next block using readBlock
        return readBlock(nextBlockPosition, fHandle, memPage);
    }
}

/**
 * @author      : Rasika Venkataramanan
 * @function    : readLastBlock
 * @description : This function enables reading of the last block of a page
 */
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Code to read the last block of a page file
    int lastBlockPos = fHandle->totalNumPages - 1; // Calculates the position of last block

    // Checks if the calculated position is valid (non-negative)
    if (lastBlockPos <= 0)
    {
        // If the calculated position is invalid, returns an error message
        return RC_READ_NON_EXISTING_PAGE;
    }
    else
    {
        // calls readBlock function to read the last block
        return readBlock(lastBlockPos, fHandle, memPage);
    }
}

/**
 * @author      : Rasika Venkataramanan
 * @function    : writeBlock
 * @description : This function is used to write a block to the page file
 */
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Code to write a block to a page file
    int numOfPages = fHandle->totalNumPages; // Get the total number of pages in the file

// Checks if pageNum is within the valid range (0 to numOfPages-1)
if (!(pageNum >= 0 && pageNum < numOfPages)) {
    return RC_WRITE_FAILED; // Returns error code in case of a write error and/or out-of-range pageNum
} else {
    // Calculates the byte offset to the specified page
    long offset = (long)(pageNum + 1) * PAGE_SIZE;

    FILE *fp = fHandle->mgmtInfo; // Finds the file associated with the file handle

    // Seeks to the beginning of the specified page
    if (fseek(fp, offset, SEEK_SET) == 0) {
        if (PAGE_SIZE == fwrite(memPage, sizeof(char), PAGE_SIZE, fp)) {
            // Updates the current page position
            fHandle->curPagePos = pageNum;
            return RC_OK; // Returns Success Code
        }
    }

    // Returns error code in case of a write error and/or out-of-range pageNum
    return RC_WRITE_FAILED;
}
}

/**
 * @author      : Rasika Venkataramanan
 * @function    : writeCurrentBlock
 * @description : This function is used to write the current block to the page file
 */
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Code to write the current block to a page file
    // Writes the current block using writeBlock function
    RC result = writeBlock(getBlockPos(fHandle), fHandle, memPage);

    // Check if the write operation was successful
    if (result != RC_OK)
    {
        return RC_WRITE_FAILED;// Returns Error Status Code
        
    }
    else
    {
         return RC_OK; // Returns Success Status Code
    }
}

/**
 * @author      : Rasika Venkataramanan
 * @function    : appendEmptyBlock
 * @description : This function is used to append an empty block to the page file
 */
RC appendEmptyBlock(SM_FileHandle *fHandle)
{
    // Code to append an empty block to a page file
    char *emptyPage = (char *)calloc(PAGE_SIZE, sizeof(char)); // Allocates memory for an empty page
    
    if(!emptyPage) return RC_WRITE_FAILED;
    
    // Seeks to the end of the file to append an empty page
    fseek(fHandle->mgmtInfo, (fHandle->totalNumPages + 1) * PAGE_SIZE, SEEK_SET);

    // Writes the empty page to the file
    if (fwrite(emptyPage, PAGE_SIZE, 1, fHandle->mgmtInfo))
    {
       // Incase of Success
        int fHandlePages= fHandle->totalNumPages + 1;
        fHandle->totalNumPages = fHandlePages; // Updates the total number of pages in fHandle
        int fHandleCurPos= fHandle->totalNumPages - 1;
        fHandle->curPagePos = fHandleCurPos; // Updates the current page position 

        int offset= 0L;
        int f=  fHandle->totalNumPages;
        // Seeks to the start of the headerPage and updates the page count
        fseek(fHandle->mgmtInfo, offset, SEEK_SET);
        fprintf(fHandle->mgmtInfo, "%d", f);
        fseek(fHandle->mgmtInfo, (fHandle->totalNumPages+1) * PAGE_SIZE, 0); // Positions the pointer back to the previous position

        // Deallocate the memory
        free(emptyPage);

        return RC_OK; // Returns Success Status Code
    }
    else
    {
         // Incase of error, memory is freed (deallocated) and error status code is returned
        free(emptyPage);
        return RC_WRITE_FAILED;
    }
}


/**
 * @author      : Rasika Venkataramanan
 * @function    : ensureCapacity
 * @description : This function is used to ensure that the page file has capacity
 */
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{    // Code to ensure the capacity of a page file

    // Gets the current number of pages in the file
    int currentPages = fHandle->totalNumPages; 
    
    // Opens the file fp in append mode
    FILE *fileRef = fopen(fHandle->fileName, "a"); 
    
    if (fileRef && currentPages < numberOfPages)
    {
        // Calculates the number of pages to add to reach the required capacity
        int pagesToAdd = numberOfPages - currentPages;
        int j;

        // Appends empty blocks to reach the required capacity
        for (j = 0; j < pagesToAdd; j++)
        {
            // Returns error code when append fails
            if (appendEmptyBlock(fHandle) != RC_OK)
            {
                fclose(fileRef);
                return appendEmptyBlock(fHandle);
            }
        }
        fclose(fileRef);
        return RC_OK; // Returns Success Status Code
    }
    else
    {
        // If the current capacity is greater or equal to the required capacity, file fp is closed
        fclose(fileRef);
        return RC_OK; // Returns Success Status Code
        
    }
        return RC_FILE_NOT_FOUND; // Returns error code if file is not found


}