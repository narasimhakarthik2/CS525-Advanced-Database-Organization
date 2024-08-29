# CS525: Advanced Database Organization
## Assignment 3 - Record Manager
## Group 17
## Team Members
### Ananya Menon A20538657
### Rasika Venkataramanan A20536353
### G Narasimha Karthik A20536058
## Steps to run the code:
1. Execute `make clean` command to clear all the previously compiled files (.o files).
2. Execute `make` command to compile the program files, link and create executables.
3. Execute `make run` command to run the test cases.
### Record Manager
1. RM_ScanHandle
2. RC initRecordManager (void *mgmtData);
3. RC shutdownRecordManager ();
4. RC createTable (char *name, Schema *schema);
5. RC openTable (RM_TableData *rel, char *name);
6. RC closeTable (RM_TableData *rel);
7. RC deleteTable (char *name);
8. int getNumTuples (RM_TableData *rel);
1. RC insertRecord (RM_TableData *rel, Record *record);
2. RC deleteRecord (RM_TableData *rel, RID id);
3. RC updateRecord (RM_TableData *rel, Record *record);
4. RC getRecord (RM_TableData *rel, RID id, Record *record);
1. RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
2. RC next (RM_ScanHandle *scan, Record *record);
3. RC closeScan (RM_ScanHandle *scan);
1. int getRecordSize (Schema *schema);
2. Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys);
1. RC freeSchema (Schema *schema);
1. RC createRecord (Record **record, Schema *schema);
2. RC freeRecord (Record *record);
3. RC getAttr (Record *record, Schema *schema, int attrNum, Value **value);
4. RC setAttr (Record *record, Schema *schema, int attrNum, Value *value);