# CS525: Advanced Database Organization 
## Assignment 1 - Storage Manager
## Group 17

## Team Members
### Ananya Menon A20538657
### Rasika Venkataramanan A20536353 
### G Narasimha Karthik A20536058

## Steps to run the code:
1. Execute ``` make clean ``` command to clear all the previously compiled files (.o files).
2. Execute ``` make ``` command to compile the program files, link and create executables, 
3. Execute ``` make run```  command to run the test cases.

## Functions Overview: 
Below are the functions created to implement a simple storage manager - a module that is capable of reading blocks from a file on disk into memory and writing blocks from memory to a file on disk. Part of the implementation is mainly about memory and file system interactions with standard library methods like fopen and calloc. 

**1. <u>Page handling functions**</u>

a. **createPageFile()**: Creates a new file and writes an empty block of memory to it. <br/>
b. **openPageFile()**: Opens an existing file in read-write mode.<br/>
c. **closePageFile()**: Closes an already open file.<br/>
d. **destroyPageFile()**: Destroys or removes the already created file. Also checks if the file is closed before destroying.<br/>

**2. <u>Read functions**</u>

a. **readBlock()**: Reads a block of data from a specified page number in an open file.<br/>
b. **getBlockPos()**: Retrieves the current page position of an open file.<br/>
c. **readFirstBlock()**: Reads the first block of an open file and stores it in memory page.<br/>
d. **readPreviousBlock()**: Reads previous block of file into memory by setting the pointer position to the previous block.<br/>
e. **readCurrentBlock()**: Gets the current block position by calling getBlockPos() and calls readBlock() to read the block at current position.<br/>
f. **readNextBlock()**: Increments the current block position and calls readBlock() to read the block at next position.<br/>
g. **readLastBlock()**: Decrements the total number of pages by one to get the last block position and calls readBlock() at that position.<br/>

**3. <u>Write functions**</u>

a. **writeBlock()**: Checks whether the page number is valid. Set the pointer position to where we have to write and then write the block at that page.<br/>
b. **writeCurrentBlock()**: Calls writeBlock() after setting the current page number as the pointer position.<br/>
c. **appendEmptyBlock()**: Create an empty block of size which equals PAGE_SIZE to the end of the file.<br/>
d. **ensureCapacity()**: Increases the number of pages in a file to a specified amount by appending empty blocks if the total number of pages is less than the required number.<br/>