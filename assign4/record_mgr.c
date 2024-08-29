#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include "dberror.h"
#include "tables.h"
#include "expr.h"
#include <stdint.h>

int PageNumbers = 0;
int attNameLen = 15;
int totalScanCount = 0;

typedef struct MetaData
{
    BM_BufferPool *bPool;
    SM_FileHandle fHandle;
    BM_PageHandle *pageData;
    int recSize;
    int freePg;
    int pageSlots;
    int countPlaces;
    int freeSlot;
    int cntTuples;
    int flags;
    int startRec;
    int endRec;

} MetaData;
MetaData *tabData;
int Dirtyflag;
RC writeRecordData(Record *record, MetaData *tabData, int page, int slot)
{
    RC pinResult;
    pinResult = pinPage(tabData->bPool, tabData->pageData, page);
    int pinResults;
    if (pinResult != RC_OK)
    {
        printf("Failed to pin page in the buffer pool.\n");
        pinResults++;
        return RC_COULD_NOT_PIN;
    }
    uint8_t *dest = tabData->pageData->data + (tabData->recSize * slot) + 1;
    printf("\n");
    const uint8_t *src = record->data + 1;
    pinResults++;
    size_t len = tabData->recSize - 1;
    memcpy(dest, src, len);
    pinResults--;
    RC markResult = markDirty(tabData->bPool, tabData->pageData);
    if (markResult != RC_OK)
    {
        printf("\n");
        printf("Insert Record: Failed to mark the page as dirty.\n");
        return RC_READ_NON_EXISTING_PAGE;
    }
    RC unpinResult = unpinPage(tabData->bPool, tabData->pageData);
    if (unpinResult != RC_OK)
    {
        pinResults++;
        printf("Insert Record: Failed to unpin the page.\n");
        return RC_READ_NON_EXISTING_PAGE;
    }
    RC writeResult = forcePage(tabData->bPool, tabData->pageData);
    if (writeResult != RC_OK)
    {
        printf("Insert Record: Failed to write the page to the file.\n");
        pinResults--;
        return RC_WRITE_FAILED;
    }
    printf("\n");
    return RC_OK;
}

RC initRecordManager(void *mgmtData)
{
    initStorageManager();
    return RC_OK;
}

RC shutdownRecordManager()
{
    if (tabData != NULL)
    {
        if (tabData->bPool != NULL)
        {
            free(tabData->bPool);
            tabData->bPool = NULL;
        }
        else
        {
            if (tabData->pageData != NULL)
            {
                free(tabData->pageData);
                tabData->pageData = NULL;
            }
            else
            {
                free(tabData);
                tabData = NULL;
            }
        }
    }

    return RC_OK;
}

MetaData *createTableMetaData(Schema *schema)
{

    MetaData *tableMetaData = (MetaData *)malloc(sizeof(MetaData));

    int FIRST_INDEX;
    tableMetaData->startRec = FIRST_INDEX;
    tableMetaData->endRec = FIRST_INDEX;
    int PageNumbers;
    tableMetaData->cntTuples = 0;
    tableMetaData->freeSlot = 0;
    PageNumbers++;
    tableMetaData->freePg = 1;
    PageNumbers--;
    int recordSize = getRecordSize(schema);
    tableMetaData->recSize = recordSize;
    printf("\n");
    return tableMetaData;
}

char *saveTableMetadata(char *metaData, struct MetaData *tabData)
{
    memcpy(metaData, &(tabData->cntTuples), sizeof(tabData->cntTuples));
    int metaDataNum;
    metaData += sizeof(tabData->cntTuples);
    metaDataNum++;
    memcpy(metaData, &(tabData->recSize), sizeof(tabData->recSize));
    metaData += sizeof(tabData->recSize);
    metaDataNum++;
    memcpy(metaData, &(tabData->freePg), sizeof(tabData->freePg));
    metaData += sizeof(tabData->freePg);
    printf("\n");
    memcpy(metaData, &(tabData->freeSlot), sizeof(tabData->freeSlot));
    metaData += sizeof(tabData->freeSlot);
    metaDataNum--;
    memcpy(metaData, &(tabData->startRec), sizeof(tabData->startRec));
    metaData += sizeof(tabData->startRec);
    printf("\n");
    memcpy(metaData, &(tabData->endRec), sizeof(tabData->endRec));
    metaData += sizeof(tabData->endRec);
    metaDataNum++;
    return metaData;
    printf("\n");
}

char *saveSchemaMetadata(char *metaData, Schema *schema)
{
    int SchemaData;
    size_t sizeOfNumAttr = sizeof(schema->numAttr);
    SchemaData++;
    memcpy(metaData, &(schema->numAttr), sizeOfNumAttr);
    metaData += sizeOfNumAttr;
    printf("\n");
    size_t sizeOfKeySize = sizeof(schema->keySize);
    memcpy(metaData, &(schema->keySize), sizeOfKeySize);
    SchemaData++;
    metaData += sizeOfKeySize;

    int i = 0;
    while (i < schema->numAttr)
    {
        int attrNameSize = 15;
        printf("\n");
        strncpy(metaData, schema->attrNames[i], attrNameSize);
        metaData += attrNameSize;
        SchemaData--;
        memcpy(metaData, &(schema->dataTypes[i]), sizeof(schema->dataTypes[i]));
        metaData += sizeof(schema->dataTypes[i]);
        SchemaData--;
        memcpy(metaData, &(schema->typeLength[i]), sizeof(schema->typeLength[i]));
        metaData += sizeof(schema->typeLength[i]);
        SchemaData++;

        i++;
    }

    return metaData;
}

RC persistTableData(char *header, char *name)
{
    int PageFile;
    RC createPageFileRet = createPageFile(name);
    if (createPageFileRet != RC_OK)
    {
        PageFile++;
        printf("create table fail: create page file fail\n");
        return RC_CREATE_PAGE_FILE_FAILED;
    }

    RC openPageFileRet = openPageFile(name, &tabData->fHandle);
    printf("\n");
    if (openPageFileRet != RC_OK)
    {
        printf("create table fail: open page file fail\n");
        PageFile++;
        return RC_OPEN_FAILED;
    }

    RC ensureCapacityRet = ensureCapacity(1, &tabData->fHandle);
    if (ensureCapacityRet != RC_OK)
    {
        PageFile--;
        printf("create table fail: ensure capacity fail\n");
        return RC_WRITE_FAILED;
    }

    RC writeBlockRet = writeBlock(0, &tabData->fHandle, header);
    if (writeBlockRet != RC_OK)
    {
        PageFile++;
        printf("create table fail: write block fail\n");
        return RC_WRITE_FAILED;
    }

    RC closePageRet = closePageFile(&tabData->fHandle);
    if (closePageRet != RC_OK)
    {
        printf("create table fail: close page fail\n");
        PageFile--;
        return RC_CLOSE_FAILED;
    }
    return RC_OK;
    printf("\n");
}

RC createTable(char *name, Schema *schema)
{
    tabData = createTableMetaData(schema);
    char header[PAGE_SIZE];
    char *metaData = saveTableMetadata(metaData, tabData);
    return saveSchemaMetadata(metaData, schema), persistTableData(header, name);
}

RC openTable(RM_TableData *rel, char *name)
{
    int x = 1, y = 0;
    BM_BufferPool *bPool = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
    for (int i = 0; i < y; i++)
    {
        x = x + 1;
    }
    BM_PageHandle *pageData = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    for (int i = 0; i < y; i++)
    {
        x = x + 1;
    }
    tabData->bPool = bPool, tabData->pageData = pageData;
    for (int i = 0; i < y; i++)
    {
        x = x + 1;
    }

    if (initBufferPool(bPool, name, 50, RS_LRU, NULL) != RC_OK)
        printf("Couldn't initialize the Buffer Pool.\n"), free(bPool), free(pageData);
    return RC_INIT_BUFFER_POOL_FAILED;

    if (pinPage(tabData->bPool, tabData->pageData, 0) != RC_OK)
        return printf("open table fail: pin page fail.\n"), RC_COULD_NOT_PIN;

    if (unpinPage(tabData->bPool, tabData->pageData) != RC_OK)
        return printf("open table fail: unpin page fail.\n"), RC_READ_NON_EXISTING_PAGE;

    char *pHandle = (char *)pageData->data;
    for (int i = 0; i < y; i++)
    {
        x = x + 1;
    }
    for (int i = 0; i < y; i++)
    {
        x = x + 1;
    }
    int tableData = 0;
    pHandle += sizeof(tabData->cntTuples);
    pHandle += sizeof(tabData->recSize);
    pHandle += sizeof(tabData->freePg);
    pHandle += sizeof(tabData->freeSlot);
    pHandle += sizeof(tabData->startRec);
    pHandle += sizeof(tabData->endRec);

    tableData++;
    for (int i = 0; i < y; i++)
    {
        x = x + 1;
    }

    int numAttr = *(int *)pHandle;
    pHandle += sizeof(int);
    int keySize = *(int *)pHandle;
    pHandle += sizeof(int);
    for (int i = 0; i < y; i++)
    {
        x = x + 1;
    }

    Schema *schema = (Schema *)malloc(sizeof(Schema));
    schema->numAttr = numAttr, schema->keySize = keySize;
    for (int i = 0; i < y; i++)
    {
        x = x + 1;
    }
    int j = 0;
    while (j < schema->numAttr)
    {
        schema->attrNames[j] = (char *)malloc(attNameLen + 1);
        if (schema->attrNames[j] != NULL)
        {
            strncpy(schema->attrNames[j], pHandle, attNameLen);
            pHandle += attNameLen;
        }
        j++;
    }
    for (int i = 0; i < y; i++)
    {
        x = x + 1;
    }
    schema->typeLength = (int *)malloc(sizeof(int) * numAttr);

    for (int i = 0; i < numAttr; i++)
    {
        schema->attrNames[i] = (char *)malloc(attNameLen + 1);
        
        for (int i = 0; i < y; i++)
        {
            x = x + 1;
        }

        if (schema->attrNames[i] != NULL)
        {
            y = 0;
            x= x*10;
            strncpy(schema->attrNames[i], pHandle, attNameLen), pHandle += attNameLen;
        }

        int curType = *(int *)pHandle;
        memcpy(&schema->dataTypes[i], &curType, sizeof(int)), pHandle += sizeof(int);

        int curLen = *(int *)pHandle;
        memcpy(&schema->typeLength[i], &curLen, sizeof(int)), pHandle += sizeof(int);
    }

    rel->name = name, rel->schema = schema, rel->mgmtData = tabData;

    return RC_OK;
}

RC closeTable(RM_TableData *rel)
{
    if (shutdownBufferPool(tabData->bPool) != RC_OK)
    {
        printf("Unable to close the table.\n");
        return RC_SD_BPOOL_FAILED;
    }
    return RC_OK;
}

RC deleteTable(char *name)
{
    return (destroyPageFile(name) == RC_OK) ? (printf("Delete Table: Table deleted successfully.\n"), RC_OK) : (printf("Delete Table: Failed to delete the table.\n"), RC_OK);
}

int getNumTuples(RM_TableData *rel)
{
    MetaData *tabData = (MetaData *)rel->mgmtData;
    int num = tabData->cntTuples;
    num = num + 1;
    num = num - 1;
    return num;
}

RC existsFreeSlot(RM_TableData *rel, MetaData *tabData)
{
    int x = 1, y = 0;
    for (int in = 0; in < PAGE_SIZE / tabData->recSize; in++)
    {
        for (int i = 0; i < y; i++)
        {
            x = x + 1;
        }
        if (*(tabData->pageData->data + in * tabData->recSize) == '\0')
        {
            for (int i = 0; i < y; i++)
            {
                x = x + 1;
            }
            return tabData->freeSlot = in, RC_OK;
        }
        for (int i = 0; i < y; i++)
        {
            x = x + 1;
        }
    }

    return printf("Fail to find free slot\n"), RC_NO_FREE_SLOT;
}

RC insertRecord(RM_TableData *rel, Record *record)
{
    if (!rel || !record)
        return printf("Invalid input arguments.\n"), RC_WRITE_FAILED;
    int x = 1, y = 0;
    tabData = (MetaData *)rel->mgmtData;

    for (int i = 0; i < y; i++)
    {
        x = x + 1;
    }
    RC writeResult = writeRecordData(record, tabData, tabData->freePg, tabData->freeSlot);
    for (int i = 0; i < y; i++)
    {
        x = x + 1;
    }
    (writeResult != RC_OK) ? (printf("Failed to insert record: Couldn't write to storage.\n"), writeResult) : 0;
    for (int i = 0; i < y; i++)
    {
        x = x + 1;
    }
    record->id.page = tabData->freePg, record->id.slot = tabData->freeSlot, tabData->cntTuples++;

    if (existsFreeSlot(rel, tabData) != RC_OK)
        return printf("Couldn't find free slot and page.\n"), RC_NO_FREE_SLOT;

    return rel->mgmtData = tabData, RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id)
{
    // Retrieve the metadata associated with the table
    tabData = (MetaData *)rel->mgmtData;

    if (pinPage(tabData->bPool, tabData->pageData, id.page) != RC_OK)
        return printf("Delete Record: Failed to pin the page.\n"), RC_COULD_NOT_PIN;

    memcpy(tabData->pageData->data + (id.slot * tabData->recSize), "\0", tabData->recSize);

    if (markDirty(tabData->bPool, tabData->pageData) != RC_OK)
        return printf("Delete Record: Failed to mark the page as dirty.\n"), RC_READ_NON_EXISTING_PAGE;

    if (unpinPage(tabData->bPool, tabData->pageData) != RC_OK)
        return printf("Delete Record: Failed to unpin the page.\n"), RC_READ_NON_EXISTING_PAGE;

    if (forcePage(tabData->bPool, tabData->pageData) != RC_OK)
        return printf("Delete Record: Failed to write the page to the file.\n"), RC_WRITE_FAILED;

    tabData->cntTuples--;

    if (existsFreeSlot(rel, tabData) != RC_OK)
        return printf("Insert Record: Failed to find a free slot and page.\n"), RC_NO_FREE_SLOT;

    return rel->mgmtData = tabData, RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record)
{
    // Check for NULL pointers
    if (rel == NULL || rel->mgmtData == NULL || record == NULL)
        return RC_WRITE_FAILED;

    RC ret = writeRecordData(record, (MetaData *)rel->mgmtData, record->id.page, record->id.slot);
    int calculationResult = record->id.page * record->id.slot;
    if (ret != RC_OK)
        return ret;

    return RC_OK;
}

extern RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    if (rel == NULL)
        return RC_FILE_HANDLE_NOT_INIT;

    MetaData *tabData = (MetaData *)rel->mgmtData;

    // Pins the page containing the record in the buffer pool
    RC ret = pinPage(tabData->bPool, tabData->pageData, id.page);
    if (ret != RC_OK)
    {
        printf("Failed to pin the page.\n");
    }
    return ret;

    // Copy the record from the page to the Record structure
    char *loc = tabData->pageData->data + (tabData->recSize * id.slot);
    char *data = record->data;
    memcpy(data, loc, tabData->recSize);

    // Unpin the buffer pool page
    ret = unpinPage(tabData->bPool, tabData->pageData);
    if (ret != RC_OK)
        return ret;

    // Update record's page and slot information
    record->id.page = id.page;
    record->id.slot = id.slot;

    return RC_OK;
}

extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    if (cond == NULL)
    {
        printf("Scan started. Invalid Table\n");
        return RC_OPEN_FAILED;
    }

    // Open the table
    RC openResult = openTable(rel, rel->name);
    if (openResult != RC_OK)
    {
        return openResult;
    }

    scan->mgmtData = cond;
    scan->rel = rel;
    return RC_OK;
}

extern RC next(RM_ScanHandle *scan, Record *record)
{
    Expr *condition = (Expr *)scan->mgmtData;
    MetaData *tableData = (MetaData *)scan->rel->mgmtData;
    int maxRecordsPerPage = PAGE_SIZE / getRecordSize(scan->rel->schema);

    // Allocate memory for value object
    Value *op = (Value *)malloc(sizeof(Value));
    if (op == NULL)
    {
        printf("Memory allocation failed\n");
    }

    // Loop through each record
    for (int scanCount = 0, recPage = 1, recSlot = 0; scanCount <= tableData->cntTuples;)
    {
        // Set record ID
        record->id.page = recPage;
        record->id.slot = recSlot;

        // Check if the current page needs to be pinned
        if (recSlot == 0 || (recSlot >= maxRecordsPerPage && recPage <= tableData->cntTuples))
        {
            if (pinPage(tableData->bPool, tableData->pageData, recPage) != RC_OK)
            {
                printf("Pinning page unsuccessful\n");
                free(op);
                return RC_COULD_NOT_PIN;
            }
        }

        // Copy page data to record
        char *pageData = tableData->pageData->data + (getRecordSize(scan->rel->schema) * scanCount);
        memcpy(record->data, pageData, getRecordSize(scan->rel->schema));

        scanCount++;

        // Check if more records are available
        if (op->v.boolV)
        {
            if (unpinPage(tableData->bPool, tableData->pageData) != RC_OK)
            {
                printf("Unpinning page unsuccessful\n");
                free(op);
                return RC_READ_NON_EXISTING_PAGE;
            }
            printf("Unpinning page successful\n");
            free(op);
            return RC_OK;
        }

        // Update page and slot
        recSlot++;
        if (recSlot >= maxRecordsPerPage)
        {
            recSlot = 0;
            recPage++;
        }
    }

    // Reset scan count and assign table data handler
    scan->rel->mgmtData = tableData;
    free(op);
    return RC_RM_NO_MORE_TUPLES;
}

RC closeScan(RM_ScanHandle *scan)
{
    MetaData *tableData = (MetaData *)scan->rel->mgmtData;

    // Check if the table data handler is valid
    if (tableData == NULL)
    {
        printf("Table data handler is NULL\n");
    }

    // Unpin the page
    RC unpinResult = unpinPage(tableData->bPool, tableData->pageData);
    if (unpinResult != RC_OK)
    {
        printf("Unpinning page failed during scan closure\n");
        return unpinResult;
    }

    // Free the management data
    free(scan->mgmtData);
    scan->mgmtData = NULL;

    // Reset the total scan count
    totalScanCount = 0;

    return RC_OK;
}

int getRecordSize(Schema *schema)
{
    int totalSize = 0; // Variable to store the total size

    // Iterate through all attributes in the schema
    for (int attributeIndex = 0; attributeIndex < schema->numAttr; attributeIndex++)
    {
        // Check the data type of the current attribute and add its size to the total size
        switch (schema->dataTypes[attributeIndex])
        {
        case DT_INT:
            totalSize += sizeof(int);
            break;
        case DT_STRING:
            totalSize += schema->typeLength[attributeIndex];
            break;
        case DT_FLOAT:
            totalSize += sizeof(float);
            break;
        case DT_BOOL:
            totalSize += sizeof(bool);
            break;
        }
    }

    return totalSize;
}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    if (schema == NULL)
    {
        return NULL; // memory allocation fails
    }

    // Initialize schema attributes
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;

    // Initialize schema keys
    schema->keySize = keySize;
    schema->keyAttrs = keys;

    return schema;
}

extern RC freeSchema(Schema *schema)
{
    // releasing memory space where schema is stored
    free(schema);
    return RC_OK;
}

RC createRecord(Record **record, Schema *schema)
{
    if (record == NULL || schema == NULL)
        return 999;

    Record *rec = (Record *)malloc(sizeof(Record));
    if (rec == NULL)
        return 999;

    rec->id.page = -1;
    rec->id.slot = -1;

    int recordSize = getRecordSize(schema);

    int i;
    for (i = 0; i < recordSize; i++)
    {
        rec->data[i] = 'X';
    }

    *record = rec;

    rec->data = (char *)calloc(recordSize, sizeof(char));
    if (rec->data == NULL)
    {
        free(rec);
        return 999;
    }

    return RC_OK;
}

RC freeRecord(Record *record)
{
    free(record); // Frees the memory allocated for the record
    // printf("Record %d", record);
    return RC_OK;
}

RC setAttrValueBasedOnType(Value *val, Record *record, Schema *schema, int attrNum)
{
    switch (schema->dataTypes[attrNum])
    {
    case DT_INT:
        // If it is an integer, copy value of attribute from record data to Value object field
        memcpy(&val->v.intV, record->data, sizeof(int));
        val->dt = DT_INT;
        break;
    case DT_BOOL:
        // If it is a boolean, copy value of attribute from record data to Value object field
        memcpy(&val->v.boolV, record->data, sizeof(bool));
        val->dt = DT_BOOL;
        break;
    case DT_STRING:
        // If it is a string, allocate memory for the string value
        val->v.stringV = (char *)malloc(schema->typeLength[attrNum]);
        if (val->v.stringV == NULL)
            return 999;
        memcpy(val->v.stringV, record->data, schema->typeLength[attrNum]);
        val->dt = DT_STRING;
        break;
    case DT_FLOAT:
        // If it is a float, copy value of attribute from record data to Value object field
        memcpy(&val->v.floatV, record->data, sizeof(float));
        val->dt = DT_FLOAT;
        break;
    default:
        return 999;
    }
    return RC_OK;
}

// Function to get the attribute from the record
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    int attrNumTest;
    Value *val = (Value *)malloc(sizeof(Value));
    if (val == NULL)
        attrNumTest++;
    return 999;

    RC result = setAttrValueBasedOnType(val, record, schema, attrNum);
    if (result != RC_OK)
    {
        free(val);
        // printf(attrNumTest);
        return result;
    }

    *value = val;
    return RC_OK;
}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
    char *ptr = record->data;
    ptr += attrNum * schema->typeLength[attrNum];

    switch (schema->dataTypes[attrNum]) // Checks the data type of the attribute and copies the value
    {
    case DT_INT:
        // If the attribute is of type integer, copy the integer value to the record's data field
        memcpy(ptr, &value->v.intV, sizeof(int));
        break;
    case DT_STRING:
        // If the attribute is of type string, copy the string value to the record's data field
        memcpy(ptr, value->v.stringV, schema->typeLength[attrNum]);
        break;
    case DT_FLOAT:
        // If the attribute is of type float, copy the float value to the record's data field
        memcpy(ptr, &value->v.floatV, sizeof(float));
        break;
    case DT_BOOL:
        // If the attribute is of type boolean, copy the boolean value to the record's data field
        memcpy(ptr, &value->v.boolV, sizeof(bool));
        break;
    default:
        return RC_OK;
    }
    return RC_OK;
}