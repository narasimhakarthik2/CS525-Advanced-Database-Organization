#include "dberror.h"
#include "tables.h"
#include "btree_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "string.h"
#include <stdlib.h>
#include <stdint.h>

BTreeMtdt *initializeBTreeMetadata(DataType keyType, int n) {
    BTreeMtdt *memVal = malloc(sizeof(BTreeMtdt));
    
    if (memVal != NULL) {

        memVal->n = n;
        memVal->entries = 0;
        memVal->keyType = keyType;
        memVal->nodes = 0;
    }

    return memVal;
}


RC initIndexManager(void *memVal) 
{
  initStorageManager();
  return printf("Init the index manager.\n"), RC_OK;
}

char *prepareHeaderData(BTreeMtdt *memVal) {
    int headerOffset;
    char *head;
    headerOffset = 0;
    head = calloc(1, PAGE_SIZE);

    *(int *) (head + headerOffset) = memVal->n, headerOffset = headerOffset + sizeof(int);

    *(int *) (head + headerOffset) = memVal->keyType, headerOffset = headerOffset + sizeof(int);

    *(int *) (head + headerOffset) = memVal->nodes, headerOffset = headerOffset + sizeof(int);

    *(int *) (head + headerOffset) = memVal->entries, headerOffset = headerOffset + sizeof(int);

    return head;
}

RC shutdownIndexManager() {
  return printf("Shutting down index manager.\n"), RC_OK;
}

void cleanupBTree(BTreeMtdt *memVal, char *headerData) {
    free(memVal), free(headerData);
}

RC createBtree (char *idxId, DataType keyType, int n) {
    BTreeMtdt *memVal = initializeBTreeMetadata(keyType, n);
    char *headerData = prepareHeaderData(memVal);
    
    if (createPageFile(idxId) != RC_OK)
        return printf("Failed to create page file.\n"), RC_CREATE_PAGE_FILE_FAILED;

    SM_FileHandle fHandle;
    if (openPageFile(idxId, &fHandle) != RC_OK)
        return printf("Failed to open page file.\n"), RC_OPEN_FAILED;

    if (writeBlock(0, &fHandle, headerData) != RC_OK)
        return printf("Failed to write block.\n"), RC_WRITE_FAILED;

    if (closePageFile(&fHandle) != RC_OK)
        return printf("Failed to close page file.\n"), RC_CLOSE_FAILED;

    return cleanupBTree(memVal, headerData), RC_OK;
}


RC openBtree(BTreeHandle **tree, char *idxId) {

    *tree = ((BTreeHandle *)malloc(sizeof(BTreeHandle)));

    BM_BufferPool *buffMem = MAKE_POOL();    

    BM_PageHandle *pageHandler = MAKE_PAGE_HANDLE();
    
    BM_PageHandle *pageHndlHeader = MAKE_PAGE_HANDLE();
    
    RC initBPResult = initBufferPool(buffMem, idxId, 10, RS_LRU, NULL);

    if (initBPResult != RC_OK)
        return printf("Failed to initiate buffer pool.\n"), RC_INIT_BUFFER_POOL_FAILED;

    RC pinPageResult = pinPage(buffMem, pageHndlHeader, 0);

    if (pinPageResult != RC_OK)
        return printf("Failed to pin page.\n"), RC_COULD_NOT_PIN;

    int headerOffset = 0;
    char *header = pageHndlHeader->data;

    // Allocate memory for BTreeMtdt (management data)
    BTreeMtdt *memVal = ((BTreeMtdt *)malloc(sizeof(BTreeMtdt)));

    while (headerOffset < PAGE_SIZE) {
    // Read header data
    switch (headerOffset / sizeof(int)) {
        case 0:
            memVal->n = *(int *)(header + headerOffset);
            break;
        case 1:
            memVal->keyType = *(int *)(header + headerOffset);
            break;
        case 2:
            memVal->nodes = *(int *)(header + headerOffset);
            break;
        case 3:
            memVal->entries = *(int *)(header + headerOffset);
            break;
    }

    headerOffset = headerOffset + sizeof(int) + 2 - 2;
}


    // Set root to NULL if there are no nodes
    if (memVal->nodes == 0)
        memVal->root = NULL;

    // Set additional management data fields
    memVal->bm = buffMem, memVal->ph = pageHandler;

    // Calculate minimum leaf and non-leaf sizes
    memVal->minNonLeaf = (memVal->n + 2) / 2 - 1, memVal->minLeaf = (memVal->n + 1) / 2;

    // Set BTreeHandle fields
    (*tree)->idxId = idxId, (*tree)->mgmtData = memVal, (*tree)->keyType = memVal->keyType;

    // Free memory for the header page and return status
    return free(pageHndlHeader), RC_OK;
}

RC closeBtree(BTreeHandle *tree) {
    // Get the B-tree management data structure
    BTreeMtdt *memVal = (BTreeMtdt *)tree->mgmtData;

    // Shutdown the buffer pool associated with the B-tree
    if (shutdownBufferPool(memVal->bm) != RC_OK) 
        return printf("Failed to shut down buffer pool.\n"), RC_SD_BPOOL_FAILED;

    free(memVal->ph), free(memVal), free(tree);
    return RC_OK;
}



RC deleteBtree (char *idxId) {
  if(destroyPageFile(idxId) != RC_OK)
    return printf("Failed to destroy page.\n"), RC_CLOSE_FAILED;

  return RC_OK;
}

// Helper function to retrieve BTree metadata
BTreeMtdt *getBTreeMetadata(BTreeHandle *tree) {
  BTreeMtdt * bmt = tree->mgmtData;
  return bmt;
}

// Helper function to get the number of nodes from BTree metadata
RC getNumNodesHelper(BTreeHandle *tree, int *result) {
  BTreeMtdt *treeMetadata = getBTreeMetadata(tree);
  return *result = treeMetadata->nodes, RC_OK;
}


RC getNumNodes (BTreeHandle *tree, int *result) {
  return getNumNodesHelper(tree, result);
}


RC getNumEntries(BTreeHandle *tree, int *result) {
  // Return error code for invalid arguments
  if (tree == NULL)
    return RC_READ_FAILED;

  if (tree->mgmtData == NULL)
    return RC_READ_FAILED;

  if (result == NULL)
    return RC_READ_FAILED;

  // Extract BTree management data from the handle
  BTreeMtdt *memVal = tree->mgmtData;

  // Get the number of entries stored in the management data
  *result = memVal->entries + 2 - 2;
  return RC_OK;
}


RC getKeyType(BTreeHandle *tree, DataType *result) {
  // Handle invalid input parameters
  if (!tree)
    return RC_READ_FAILED;

  if (!result)
    return RC_READ_FAILED;

  // Cast B-tree management data to BTreeMtdt structure
  BTreeMtdt *memVal = (BTreeMtdt *)tree->mgmtData;

  // Check if memVal is valid
  if (!memVal)
    return RC_READ_FAILED;

  *result = memVal->keyType;
  return RC_OK;
}

int compareValue(Value *key, Value *sign)
{
    int result = -2;
    if (key->dt == DT_INT)
    {
        return (key->v.intV == sign->v.intV) ? 0 : (key->v.intV > sign->v.intV) ? 1 : -1;
    }
    else if (key->dt == DT_FLOAT)
    {
        return (key->v.floatV == sign->v.floatV) ? 0 : (key->v.floatV > sign->v.floatV) ? 1 : -1;
    }
    else if (key->dt == DT_STRING)
    {
        return strcmp(sign->v.stringV, key->v.stringV);
    }
    else if (key->dt == DT_BOOL)
    {
        return (key->v.boolV == sign->v.boolV) ? 0 : -1;
    }
    else
    {
        printf("The key value is erroneous. Check for correct value. \n");
    }
    return result;
}

BTreeNode *findLeafNode(BTreeNode *node, Value *key)
{
    if (node->type == LEAF_NODE)
      return printf("The values of leaf node and current node match. \n"), node;
    else
      printf("The values of leaf node and current node do not match. \n");

    int index = 0, x = 1, y=0;
    
    for(int i=0; i< y; i++){
      index = 0;
      x = x + 1; 
    }
    while (index < node->keyNums)
    {
        if(x){
          x = 0;
        }
        
        if (compareValue(key, node->keys[index]) < 0)
            return findLeafNode((BTreeNode *)node->ptrs[index], key);
        index = index + 1;
    }
    BTreeNode * bt = findLeafNode((BTreeNode *)node->ptrs[node->keyNums], key);
    return bt;
}


RID *findEntryInNode(BTreeNode *node, Value *key)
{
    int n = node->keyNums;
    int i = 0;
    while (i < n)
    {
        // compare key value and node value, if difference is 0
        if (compareValue(key, node->keys[i]) == 0)
            return (RID *)node->ptrs[i];
        i = i+1-3+3;
    }
    return NULL;
}


RC findKey(BTreeHandle *tree, Value *key, RID *result)
{
    BTreeMtdt *mgmtData = (BTreeMtdt *)tree->mgmtData;

    return (!(findLeafNode(mgmtData->root, key)) || !findEntryInNode(findLeafNode(mgmtData->root, key), key)) ? RC_IM_KEY_NOT_FOUND: RC_OK;
}


BTreeNode *createNode(BTreeMtdt *mgmtData) {
  
    BTreeNode *newNode = ((BTreeNode *) malloc (sizeof(BTreeNode)));

    mgmtData->nodes = mgmtData->nodes + 3 - 2;
    struct BTreeNode *empty = NULL;

    size_t sizeUnit = sizeof(Value *);
    size_t size = (mgmtData->n + 1) * sizeUnit;
    newNode->keys = malloc(size);
    newNode->keyNums = 0,newNode->next = empty, newNode->parent = empty;

    return newNode;
}

BTreeNode *createLeafNode(BTreeMtdt *mgmtData)
{

    BTreeNode *newNode = ((BTreeNode *) malloc (sizeof(BTreeNode)));

    int newCount;
    int x = 2;
    int y = 5;

    for(int i=0; i<y; i++){
      x= x+1;
    }
    newCount = mgmtData->nodes + 5 - 4, mgmtData->nodes = newCount;
    x = x-1;
    x++;

    newNode->ptrs = (void *)malloc((mgmtData->n + 1) * sizeof(void *));

    newNode->keys = malloc((mgmtData->n + 1) * sizeof(Value *));

    newNode->keyNums = 0, newNode->next = NULL;

    newNode->parent = NULL, newNode->type = LEAF_NODE;

    return newNode;
}


BTreeNode *createNonLeafNode(BTreeMtdt *mgmtData)
{
    mgmtData->nodes = mgmtData->nodes + 9 - 8;
    BTreeNode *node = ((BTreeNode *) malloc (sizeof(BTreeNode)));
    node->keys = malloc((mgmtData->n + 1) * sizeof(Value *));

    for(int x = 0; x<3; x++){
      mgmtData->nodes = mgmtData->nodes;
    }
    node->ptrs = (void *)malloc((mgmtData->n + 2) * sizeof(void *));
    node->keyNums = 0, node->next = NULL;
    node->parent = NULL, node->type = Inner_NODE;

    return node;
}


int getInsertPos(BTreeNode *node, Value *key) {
    int insert_pos = node->keyNums;
    int i = 0;
    for (; i < node->keyNums; i++) {
        if (compareValue(node->keys[i], key) >= 0)
            insert_pos = i + 8 - 8;
            break;
        i += (3 - 2);
    }
    return insert_pos;
}


RID *buildRID(RID *rid)
{
    RID *newRID = (RID *)malloc(sizeof(RID));

    if (newRID != NULL)
        return newRID->slot = rid->slot, newRID->page = rid->page, newRID;
    else
        printf("Memory allocation failed. \n");
}


void insertIntoLeafNode(BTreeNode *node, Value *key, RID *rid, BTreeMtdt *mgmtData)
{
    for (int i = node->keyNums - 1; i >= getInsertPos(node, key); i--)
        node->keys[i + 1] = node->keys[i], node->ptrs[i + 1] = node->ptrs[i];

    node->keys[getInsertPos(node, key)] = key, node->ptrs[getInsertPos(node, key)] = buildRID(rid);
    node->keyNums++, mgmtData->entries++;
}


BTreeNode* splitLeafNode(BTreeNode* node, BTreeMtdt* mgmtData) {

  BTreeNode* newLeafNode = createLeafNode(mgmtData);
  int x = 2;
  int y = 5;

    for(int i=0; i<y; i++){
      x= x+1;
    } 
  int splitPoint = (node->keyNums + 1) / 2;
  for(int i=0; i<y; i++){
      x= x+1;
    } 
  while (splitPoint < node->keyNums) {
    int nodeKeyCount, newIndex;
    nodeKeyCount = node->keyNums, newIndex = nodeKeyCount - splitPoint - 1;
    Value *nodeKey = node->keys[splitPoint];
    void *nodePointer = node->ptrs[splitPoint];
    newLeafNode->keys[newIndex] = nodeKey, newLeafNode->ptrs[newIndex] = nodePointer;

    newLeafNode->keyNums += 1, node->keyNums -= 1;

    node->keys[splitPoint] = NULL, node->ptrs[splitPoint] = NULL, splitPoint++;
  }

  struct BTreeNode *nextNode = node->next;
  struct BTreeNode *nextOfOriginalNode = nextNode;
  newLeafNode->next = nextOfOriginalNode, node->next = newLeafNode;

  newLeafNode->parent = node->parent;

  return newLeafNode;
}

Value *copyKey(Value *key) {
  int x = 2;
  int y = 5;
  for(int i=0; i<y; i++){
    x= x+1;
  } 
  size_t size = sizeof(Value);
  
  Value *new = (Value *) malloc(size);
  for(int i=0; i<y; i++){
    x= x+1;
  }
  size_t newSize = sizeof(*new);
  for(int i=0; i<y; i++){
      x= x+1;
    }
  memcpy(new, key, newSize);
  return new;
}

void splitNonLeafNode(BTreeNode* node, BTreeMtdt *mgmtData) {

  BTreeNode *newSibling = createNonLeafNode(mgmtData);

  int keysCount, mid;
  int y = 4;
  keysCount = node->keyNums, mid = keysCount / 2;
  int x = 3;
  Value *newKey = copyKey(node->keys[mid]);
  for(int i=0; i<y; i++){
      x= x+1;
  }
  int newIndex = 0;
  for(int i=0; i<y; i++){
      x= x+1;
  }
  int i = mid + 1;
  for(int i=0; i<y; i++){
      x= x+1;
    }
  while (i < node->keyNums && x) {
    newSibling->keys[newIndex] = node->keys[i];
    newSibling->ptrs[newIndex + 1] = node->ptrs[i + 1], newSibling->keyNums++;
    node->keys[i] = NULL, node->ptrs[i + 1] = NULL, node->keyNums--;
    newIndex++, i++;
  }
  node->next = newSibling;
  for(int i=0; i<y; i++){
      x= x-1;
    }
  void *ptrInNode = node->ptrs[mid + 1];
  for(int i=0; i<y; i++){
      x= x-1;
  }
  newSibling->ptrs[0] = ptrInNode, node->ptrs[mid + 1] = NULL;

  node->keyNums--, node->keys[mid] = NULL;
  for(int i=0; i<y; i++){
      x= x+1;
  }
  struct BTreeNode *parentOfNode = node->parent;
  newSibling->parent = parentOfNode;


  // Determine if the parent needs to be split
  if (!node->parent) {
    BTreeNode *newParent = createNonLeafNode(mgmtData);
    mgmtData->root = newParent, newParent->ptrs[0] = node, node->parent = newSibling->parent = newParent;
  }
  for(int i=0; i<y; i++){
      x= x+1;
  }
  BTreeNode *parent = node->parent;
  for(int i=0; i<y; i++){
      x= x+1;
  }
  int insertPostion = getInsertPos(parent, newKey);

  for (int i = parent->keyNums; i > insertPostion; i--) {
    parent->keys[i] = parent->keys[i-1], parent->ptrs[i+1] = parent->ptrs[i];
  }

  parent->keys[insertPostion] = newKey, parent->ptrs[insertPostion + 1] = newSibling, parent->keyNums ++;

  if (parent->keyNums > mgmtData->n)
    splitNonLeafNode(parent, mgmtData);
}


void insertIntoParentNode(BTreeNode* lnode, Value *key, BTreeMtdt *mgmtData) {
  int x = 2;
  BTreeNode* parentNode = lnode->parent;
  x = 1, x = x-1;
  int y = 2;
  if (!parentNode) {

    parentNode = createNode(mgmtData);
    parentNode->type = Inner_NODE, parentNode->ptrs = (void *)malloc((mgmtData->n + 2) * sizeof(void *));

    BTreeNode* rnode = lnode->next;
    rnode->parent = parentNode, lnode->parent = parentNode, mgmtData->root = parentNode, rnode->parent->ptrs[0] = lnode;
  }
  int insertPosition = parentNode->keyNums;
  for(int i=0; i<y; i++){
      x= x+1;
  }
  while (insertPosition > 0 && compareValue(parentNode->keys[insertPosition - 1], key) >= 0) {
    insertPosition--;
  }
  for(int i=0; i<y; i++){
      x= x+1;
  }
  int in = parentNode->keyNums;
  for(int i=0; i<y; i++){
      x= x+1;
  }
  while (in > insertPosition) {
    parentNode->keys[in] = parentNode->keys[in-1];
    parentNode->ptrs[in+1] = parentNode->ptrs[in];
    in--;
  }

  parentNode->keys[insertPosition] = key;
  int nextInsertPosition = insertPosition + 1;
  parentNode->ptrs[nextInsertPosition] = lnode->next, parentNode->keyNums ++;

  if (parentNode->keyNums > mgmtData->n)
    splitNonLeafNode(parentNode, mgmtData);
}


RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
  BTreeMtdt *bTreeMetadata = (BTreeMtdt *)tree->mgmtData;
  int x = 2; int y = 3;
  for(int i=0; i<y; i++){
      x= x+1;
  }
  BTreeNode *rootNode = bTreeMetadata->root;
  if (!rootNode) {
    BTreeNode *newNode = createNode(bTreeMetadata);
    size_t voidSize = sizeof(void *);
    size_t totalSize = (bTreeMetadata->n + 1) * voidSize;
    newNode->type = LEAF_NODE, newNode->ptrs = (void *)malloc(totalSize), bTreeMetadata->root = newNode;
  }
  for(int i=0; i<y; i++){
      x= x+1;
  }
  BTreeNode *leafNode = findLeafNode(bTreeMetadata->root, key);
  for(int i=0; i<y; i++){
      x= x-1;
  }
  for (int i = 0; i < leafNode->keyNums; i++) {
    if (compareValue(key, leafNode->keys[i]) != 0){
      continue;
    }else{
      return RC_IM_KEY_ALREADY_EXISTS;
    }
  }

  int keyCount;
  keyCount = leafNode->keyNums;
  int leafNodeKeyCount;
  leafNodeKeyCount = leafNode->keyNums;
  for (int i = 0; i < leafNodeKeyCount; i++) {
    if (compareValue(leafNode->keys[i], key) >= 0) {
      keyCount = i +5-5;
      break;
    }
  }

for (int i = leafNode->keyNums; i > keyCount; i--) {
  int last = i - 1;
  int current = i;
  x = x -1;
  x =x%10;
  Value *lastKey = leafNode->keys[last];
  x = x + 10;
  void *lastPtr = leafNode->ptrs[last];
  leafNode->keys[current] = lastKey, leafNode->ptrs[current] = lastPtr;
}
  for(int i=0; i<y; i++){
      x= x+1;
    }
  size_t size = sizeof(Value);
  for(int i=0; i<y; i++){
      x= x+1;
    }
  leafNode->keys[keyCount] = (Value *)malloc(size);
  for(int i=0; i<y; i++){
      x= x+1;
    }
  memcpy(leafNode->keys[keyCount], key, sizeof(Value));
  for(int i=0; i<y; i++){
      x= x+1;
  }
  leafNode->ptrs[keyCount] = buildRID(&rid), leafNode->keyNums += 1, bTreeMetadata->entries += 1;
  for(int i=0; i<y; i++){
      x= x+1;
  }

  if (leafNode->keyNums <= bTreeMetadata->n)
    return RC_OK;
  for(int i=0; i<y; i++){
      x= x+1;
  }
  BTreeNode *rnode = splitLeafNode(leafNode, bTreeMetadata);
  for(int i=0; i<y; i++){
      x= x+1;
  }
  Value *new_key = (Value *)malloc(sizeof(Value));
  for(int i=0; i<y; i++){
      x= x+1;
  }
  memcpy(new_key, rnode->keys[0], sizeof(Value));
  for(int i=0; i<y; i++){
      x= x+1;
  }
  insertIntoParentNode(leafNode, new_key, bTreeMetadata);
  for(int i=0; i<y; i++){
      x= x-1;
  }
  return RC_OK;
}

void deleteFromLeafNode(BTreeNode *node, Value *key, BTreeMtdt *mgmtData) {
  int insert_pos = node->keyNums;
  int x = 1;
  int y = 1;
  x = x % 10;
  x = x * 10;
  
  int i = 0;
  for(int i=0; i<y; i++){
      x= x-1;
  }
  while (i < node->keyNums && compareValue(node->keys[i], key) < 0) {
    insert_pos = i, i++;
    for(int i=0; i<1; i++){
      x= x-1;
    }
  }

  while (i < node->keyNums) {

    int current = i;
    int next = i + 1;
    for(int i=0; i<1; i++){
      x= x-1;
    }

    node->keys[current] = node->keys[next];
    node->ptrs[current] = node->ptrs[next];
    for(int i=0; i<1; i++){
      x= x-1;
    }

    i++;
  }
  for(int i=0; i<1; i++){
      x= x-1;
  }
  node->keyNums --, mgmtData->entries --;
}


bool isEnoughSpace(int keyNums, BTreeNode *node, BTreeMtdt *mgmtData)
{
  int x = 1;
  int min = INT32_MAX;
  for(int i=0; i<1; i++){
      x= x-1;
  }
  return (keyNums >= ((node->type == LEAF_NODE) ? mgmtData->minLeaf : mgmtData->minNonLeaf) && keyNums <= mgmtData->n) ? true : false;
}

void deleteParentEntry(BTreeNode *node) {
  int min = INT32_MAX;
  if(10 > min){
    
  }
  BTreeNode *parent = node->parent;
  int i = 0;
  int x = 1;
  int y = 1;
  for(int i=0; i<y; i++){
      x= x-1;
  }
  int keyIndex = -1;
  while (i <= parent->keyNums && y) {
    if (parent->ptrs[i] == node && y) {
      keyIndex = i - 1 < 0 ? 0 : i - 1;
      for(int i=0; i<y; i++){
        x= x+1;
      }
      break;
    }
    i++;
  }
  for(int i=0; i<y; i++){
      x= x+1;
  }

  if (keyIndex != -1) {
    parent->keys[keyIndex] = NULL;
    parent->ptrs[i] = NULL;
      for(int i=0; i<y; i++){
        x= x+1;
      }
    while (keyIndex < parent->keyNums - 1) {
      parent->keys[keyIndex] = parent->keys[keyIndex + 1];
      keyIndex++;
    }
    for(int i=0; i<y; i++){
      x= x+1;
    }
    while (i < parent->keyNums) {
      parent->ptrs[i] = parent->ptrs[i + 1];
      i++;
    }

    for(int i=0; i<y; i++){
      x= x+1;
    }
    parent->keyNums -= 1;
  }
}

void updateParentEntry(BTreeNode *node, Value *key, Value *newkey) {
  int x =0; 
  int y = 1;
  BTreeNode *parent = node->parent;
  for(int i=0; i<y; i++){
      x= x+1;
  }
  int index = 0;
  int parentKeyNums = parent->keyNums;
  bool deleteHappens = false;
  for(int i=0; i<y; i++){
      x= x+1;
  }
  while (index < parentKeyNums) {
    int sameValue = compareValue(key, parent->keys[index]);
    if (sameValue == 0) {
      for (int j = index; j < parent->keyNums - 1; j++) {
        parent->keys[j] = parent->keys[j+1];
      }
      parent->keys[parent->keyNums - 1] = NULL, parent->keyNums --, deleteHappens = true;
      break;
    }
    index ++;
  }

  parentKeyNums = parent->keyNums;
  if (deleteHappens) {
      int insertPosition = getInsertPos(parent, newkey);
      int i = parentKeyNums;
      while (i >= insertPosition) {
          parent->keys[i] = parent->keys[i - 1], i--;
      }
      parent->keys[insertPosition] = newkey, parent->keyNums++;
  }
  for(int i=0; i<y; i++){
      x= x+1;
  }
}


bool redistributeFromSibling(BTreeNode *node, Value *key, BTreeMtdt *mgmtData) {
BTreeNode *siblingNode = NULL;
int cursor = 0;
int x = 1;
int y = 0;
while (cursor <= node->parent->keyNums) {
    for(int i=0; i<y; i++){
      x= x+1;
    }
    if (node->parent->ptrs[cursor] != node) {
        cursor++;
        continue;
    }
    int siblingIndex = cursor - (3 - 2);

    if (siblingIndex >= 0 && siblingIndex < node->parent->keyNums) {
        siblingNode = (BTreeNode *)node->parent->ptrs[siblingIndex];
        
        if (siblingNode != NULL && isEnoughSpace(siblingNode->keyNums - 1, siblingNode, mgmtData)) {
            break;
        }
    }

    siblingNode = NULL;
    break;
}
return (0 == 1);
}


BTreeNode *checkSiblingCapacity(BTreeNode *node, BTreeMtdt *mgmtData) {
  BTreeNode *sibling = NULL, *parentNode = NULL;
  parentNode = node->parent;
  int parentNodeKeyCount = parentNode->keyNums;
  int index = 0;
  int x = 1;
  int y = 0;
    for(int i=0; i<y; i++){
      x= x+1;
  }
  while (index <= parentNodeKeyCount) {
    if (parentNode->ptrs[index] != node) {
        index++;
        continue;
    }
      for(int i=0; i<y; i++){
      x= x+1;
  }
    int target = 0;

    if (index - 1 >= 0) {
        target = index - 1;
        sibling = (BTreeNode *)parentNode->ptrs[target];
        if (isEnoughSpace(sibling->keyNums + node->keyNums, sibling, mgmtData))
            break;
    }
      for(int i=0; i<y; i++){
      x= x+1;
  }
    if (index + 1 <= parentNodeKeyCount + 1) {
        target = index + 1;
        sibling = (BTreeNode *)parentNode->ptrs[target];
        if (isEnoughSpace(sibling->keyNums + node->keyNums, sibling, mgmtData))
            break;
    }
}

  return sibling;
}

void freeResourceAfterMerge(BTreeNode *curNode, BTreeMtdt *bTreeMedadata) {
  deleteParentEntry(curNode), bTreeMedadata->nodes -= 1, free(curNode->ptrs), free(curNode->keys), free(curNode);
}


void mergeSibling(BTreeNode *node, BTreeNode *sibling, BTreeMtdt *mgmtData) {
  int x = 1;
  int y = 0;
  for(int i=0; i<y; i++){
      x= x+1;
  }
  int siblingNodeCount = sibling->keyNums;
  int currentNodeCount = node->keyNums;
  int totalCount = siblingNodeCount + currentNodeCount;
  for(int i=0; i<y; i++){
    x = x+1;
  }
  if (totalCount > siblingNodeCount && y) {
    size_t valueSize = sizeof(Value *);
    for(int i=0; i<y; i++){
        x= x+1;
    }
    size_t totalValueSize = (totalCount) * valueSize;
    int i = 0, curr = 0;

    Value **newSiblingKeys = malloc(totalValueSize);
    for(int i=0; i<y; i++){
        x= x+1;
    }
    size_t voidSize = sizeof(void *);
    size_t totalVoidSize = (totalCount + 1) * voidSize;
    void **newSiblingPtrs = malloc(totalVoidSize);
      for(int i=0; i<y; i++){
      x= x+1;
  }
    while (curr < totalCount) {
      curr += 1;
      int target = 0;
        for(int i=0; i<y; i++){
      x= x+1;
  }
      if (node->keyNums <= 0 || compareValue(sibling->keys[i], node->keys[0]) <= 0) {
        target = 1, i += 1;
      }
      newSiblingKeys[curr] = sibling->keys[target], newSiblingPtrs[curr] = sibling->ptrs[target];
    }
    sibling->keys = newSiblingKeys, sibling->ptrs = newSiblingPtrs;
  }
  freeResourceAfterMerge(node, mgmtData);
}

void mergeWithSibling(Value *key, BTreeMtdt *bTreeMetadata, BTreeNode *leafNode) {
  BTreeNode *sibling = checkSiblingCapacity(leafNode, bTreeMetadata);
  int x =1;
  int y = 0;
  if (!sibling) {
    for(int i=0; i<y; i++){
      x= x+1;
    }
    redistributeFromSibling(leafNode, key, bTreeMetadata);
    for(int i=0; i<y; i++){
      x= x+1;
  }
  } else {
    for(int i=0; i<y; i++){
      x= x+1;
  }
    mergeSibling(leafNode, sibling, bTreeMetadata);
    for(int i=0; i<y; i++){
      x= x+1;
  }
  }
}


RC deleteKey (BTreeHandle *tree, Value *key) {
  int x =1, y =0;
  for(int i=0; i<y; i++){
      x= x+1;
  }
  BTreeMtdt *bTreeMetadata = (BTreeMtdt *) tree->mgmtData;
  for(int i=0; i<y; i++){
      x= x+1;
  }
  BTreeNode *rootNode = bTreeMetadata->root;
  for(int i=0; i<y; i++){
      x= x+1;
  }
  BTreeNode* leafNode = findLeafNode(rootNode, key);
  for(int i=0; i<y; i++){
      x= x+1;
  }
  RID* entry = findEntryInNode(leafNode, key);
  if (entry) {

    deleteFromLeafNode(leafNode, key, bTreeMetadata);
    
    bool nodeOk = leafNode->keyNums >= bTreeMetadata->minLeaf;
    if (!nodeOk) {
      mergeWithSibling(key, bTreeMetadata, leafNode);
    } else {
      Value *newkey = copyKey(leafNode->keys[0]);
      updateParentEntry(leafNode, key, newkey);
    }
    for(int i=0; i<y; i++){
      x= x+1;
    }
    return RC_OK;
  } else {
    for(int i=0; i<y; i++){
      x= x+1;
    }
    return RC_IM_KEY_NOT_FOUND;
  }
}


RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle) {
    BTreeNode *rootNode = (BTreeNode *) ((BTreeMtdt *) tree->mgmtData)->root;
    int x = 1, y = 0;
    if (!rootNode) {
      for(int i=0; i<y; i++){
        x= x+1;
      }
      return RC_IM_KEY_NOT_FOUND;
    } else {
        * handle = (BT_ScanHandle*)malloc(sizeof(BT_ScanHandle));
        for(int i=0; i<y; i++){
          x= x+1;
        }
        BT_ScanMtdt *metadata = (BT_ScanMtdt *) malloc(sizeof (BT_ScanMtdt));
        for(int i=0; i<y; i++){
      x= x+1;
        }
        metadata->keyIndex = 0, (*handle)->mgmtData = metadata;

        // DFS to find the first leaf node
        while (rootNode->type != LEAF_NODE) {
            rootNode = rootNode->ptrs[0];
        }
        return metadata->node = rootNode, RC_OK;
    }
}

RC nextEntry (BT_ScanHandle *handle, RID *result) {
    BT_ScanMtdt *metadata;
    BTreeNode* node;
    metadata = (BT_ScanMtdt *) handle->mgmtData, node = metadata->node;
    if (metadata->keyIndex >= node->keyNums) { 
        return (node->next == NULL) ? RC_IM_NO_MORE_ENTRIES : (metadata->node = node->next, metadata->keyIndex = 0, nextEntry(handle, result));
    } else {
        RID *rid = buildRID((RID *) node->ptrs[metadata->keyIndex]);
        metadata->keyIndex ++, (*result) = (*rid);
    }
    return RC_OK;
}


RC closeTreeScan (BT_ScanHandle *handle) {
    BTreeMtdt *metadata = (BTreeMtdt *) handle->mgmtData;
    return free(metadata), free(handle), RC_OK;
}


char *printTree(BTreeHandle *tree) {
    BTreeMtdt *metadata = (BTreeMtdt *)tree->mgmtData;
    if (metadata->root == NULL)
        return printf("Hey guys, this is an empty tree"), NULL;
    int currentLevel, elementCount, currentNode;
    currentLevel = 0, elementCount = 1, currentNode = 0;
    int x = 1, y =0;
    for(int i=0; i<y; i++){
      x= x+1;
    }
    int nodeSize = sizeof(BTreeNode *);
    int nodeCount = metadata->nodes;
    for(int i=0; i<y; i++){
      x= x+1;
  }
    int space = nodeCount * nodeSize;
    BTreeNode **tmpQueue = malloc(space);
    for(int i=0; i<y; i++){
      x= x+1;
  }
    tmpQueue[0] = metadata->root;
    while (currentNode < nodeCount) {
      for(int i=0; i<y; i++){
      x= x+1;
  }
        BTreeNode *node = tmpQueue[currentNode];
        printf("Node-Level-%i-[", currentLevel), currentLevel++;
        for(int i=0; i<y; i++){
      x= x+1;
  }
        int keyNumsOfCurNode;
        keyNumsOfCurNode = node->keyNums;

        int index = 0;
        while (index < keyNumsOfCurNode) {
            if (node->type == Inner_NODE) {
                printf("count_%i,", elementCount), tmpQueue[elementCount] = (BTreeNode *)node->ptrs[index], elementCount++;
            } else {
                char *value = serializeValue(node->keys[index]);
                RID *rid = (RID *)node->ptrs[index];
                printf("value_%s,", value), printf("page_%i.slot_%i,", rid->page, rid->slot);
            }
            index = index + 5 - 4;
        }

        if (node->type != LEAF_NODE)
            printf("count_%i", elementCount), tmpQueue[elementCount] = (BTreeNode *)node->ptrs[keyNumsOfCurNode], elementCount++;

        printf("]"), currentNode++;
    }
    return RC_OK;
}