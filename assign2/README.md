# CS525: Advanced Database Organization

## Assignment 2 - Buffer Manager

## Group 17
## Team Members
### Ananya Menon A20538657
### Rasika Venkataramanan A20536353
### G Narasimha Karthik A20536058

## Steps to run the code:

Execute make clean command to clear all the previously compiled files (.o files).

Execute make command to compile the program files, link and create executables.

Execute make run command to run the test cases.

## Functions Overview:
The buffer manager manages a fixed number of pages in memory that represent pages from a page file managed by the storage manager as implemented in assignment 1

Below are the functions created to implement a simple buffer manager.

1. <b> initBufferPool() </b> - This function initializes the Buffer Pool with its attributes like number of pages, page file name, and replacement strategy

2. <b> shutdownBufferPool() </b> - This function shutdowns the Buffer Pool

3. <b> writePageToDisk() </b>- Function to write a page to disk

4. <b> forceFlushPool() </b>- Function to flush all dirty pages in the buffer pool to disk

5. <b> markDirty() </b>- Mark a page as dirty in the buffer pool

6. <b> unpinPage() </b>- Unpin a page in the buffer pool

7. <b> forcePage() </b>- Function to force a page to disk

8. <b> evict() </b>- Function to evict a page from the buffer pool

9. <b> pinPage() </b>  Function to pin a page in the buffer pool

### Statistics Interfaces:
1. <b> getFrameContents() </b> - Function to get the frame contents

2. <b> getDirtyFlags() </b>- Function to get the dirty flags

3. <b> getFixCounts() </b> Function to get the fix counts

4. <b> getNumReadIO() </b>- Function to get the number of read operations.The number of read operations has been recorded and return the accumulated value here

5. <b> getNumWriteIO() </b>- Function to get the number of write operations. The number of write operations has been recorded and the accumulated value is returned here