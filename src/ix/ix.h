#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
#define NODE_FLAG_BLOCK 4094
#define FREE_SPACE_BLOCK 4092
#define NUM_OF_INDEX_BLOCK 4090
#define NEXT_PAGE_PTR 4086
#define PAGE_NUM_PTR_SIZE 4
#define RID_BLOCK_SIZE 8

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

public:
	static IndexManager* instance();

	// Create an index file.
	RC createFile(const string &fileName);

	// Delete an index file.
	RC destroyFile(const string &fileName);

	// Open an index and return an ixfileHandle.
	RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

	// Close an ixfileHandle for an index.
	RC closeFile(IXFileHandle &ixfileHandle);

	// Insert an entry into the given index that is indicated by the given ixfileHandle.
	RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
			const void *key, const RID &rid);

	// Delete an entry from the given index that is indicated by the given ixfileHandle.
	RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
			const void *key, const RID &rid);

	// Initialize and IX_ScanIterator to support a range search
	RC scan(IXFileHandle &ixfileHandle, const Attribute &attribute,
			const void *lowKey, const void *highKey, bool lowKeyInclusive,
			bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator);

	// Print the B+ tree in pre-order (in a JSON record format)
	void printBtree(IXFileHandle &ixfileHandle,
			const Attribute &attribute) ;

	RC newPage(IXFileHandle &ixfileHandle, void *new_page_buffer, PageNum page_num,
			bool is_leaf, PageNum next_leaf_page_num,
			PageNum left_child_page_num);
protected:
	IndexManager();
	~IndexManager();

private:
	static IndexManager *_index_manager;
};

class IX_ScanIterator {
public:
    
    // Constructor
    IX_ScanIterator();
    
    // Destructor
    ~IX_ScanIterator();
    
    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);
    
    //added
    RC scanLeafNodes(void * buffer,const void *lowkey,const void *highkey,bool lowkeyinclusive,bool highkeyinclusive,const AttrType attrType,RID &rid, void *key);
    
    // Terminate index scan
    RC close();
    
    IXFileHandle *ixfileHandle;
    
    AttrType attrType;
    
    PageNum currentPageNum;
    int currentOffset;
    void *insert_enrty_buffer;
    
    bool lowKeyInclusive;
    bool highKeyInclusive;
    
    const void *lowKey;
    const void *highKey;
private:
    
    
};


class IXFileHandle {
public:

	// variables to keep counter for each operation
	unsigned ixReadPageCounter;
	unsigned ixWritePageCounter;
	unsigned ixAppendPageCounter;

	// Constructor
	IXFileHandle();

	// Destructor
	~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
			unsigned &appendPageCount);

	FileHandle fileHandle;

private:

};

#endif
