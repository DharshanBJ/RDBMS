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
			const Attribute &attribute) const;

	RC newPage(IXFileHandle &ixfileHandle, void *new_page_buffer, PageNum page_num,
			bool is_leaf, PageNum next_leaf_page_num,
			PageNum left_child_page_num);

	//cout checks
//	RC readRootPage(IXFileHandle &ixfileHandle); //to read root page num
	RC printPage(IXFileHandle &ixfileHandle, int pageNum,const void *key);
protected:
	IndexManager();
	~IndexManager();

private:
	static IndexManager *_index_manager;
	//PagedFileManager *pfm;

//	RC readOverHeads(const void *key, int type, void *buffer,
//			short &free_space_of_page, short &num_of_slots, int &char_len); //to compute (parameters in the function) the overhead
//	RC readRootPage(IXFileHandle &ixfileHandle); //to read root page num
//	RC createNewIntermediatePage(IXFileHandle &ixfileHandle, PageNum leftPage,
//			PageNum rightPage); //to create a new intermediate page with all the overheads set ready to use
//	RC createNewLeafPage(); //to create a new leaf page with all the overheads set ready to use
//	RC updateRootPage(IXFileHandle &ixfileHandle, unsigned root_page_num); //to update Root node
//	RC insertIntoIntermediatePage(IXFileHandle &ixfileHandle, const void *key,
//			PageNum page_ptr, int type, void *buffer); //,short free_space_of_page, short num_of_slots, int char_len);//to insert key and pointer to non-leaf /intermediate page
//	RC insertIntoLeafPage(IXFileHandle &ixfileHandle, const void *key,
//			const RID &rid, int type, void *buffer); //,short free_space_of_page, short num_of_slots, int char_len);//to insert key and pointer to leaf page
//	RC searchLeafNode(const void *key, void *buffer, int type); //to search the key in leaf node
//	RC searchIntermediateNode(const void *key, int &pagePtr, void *buffer,
//			int type); //to search the key in intermediate node
//	RC compareEntryKeyIndex(const void *key, void *comparisonEntry, int type,
//			int compare_len); //, short node_type);//compare the key and entry generic function
//	RC compareDeleteEntryKeyRID(const void *key, void *comparisonEntry,
//			const RID &rid, int type, int compare_len);
//	RC deleteEntryInLeaf(IXFileHandle &ixfileHandle, const void *key,
//			const RID &rid, int type, void *buffer, short free_space_of_page,
//			short num_of_slots, int char_len); //to delete the key index for the matching <key,rid>
//	RC splitLeafPage(IXFileHandle &ixfileHandle, int insert_dest,
//			const void *key, const RID &rid, int type, void *buffer,
//			void *prop_key, RID &prop_page_num, int &prop_key_len); //splits the leaf page and inserts the key requested into it
//	RC splitIntermediatePage(IXFileHandle &ixfileHandle, int insert_dest,
//			const void *key, const RID &rid, int type, void *buffer,
//			void *prop_key, RID &prop_page_num, int &prop_key_len); //splits the intermediate page and inserts the key propagated from new leaf into it
//	RC insertReccursion(unsigned page_num, const void *key,
//			IXFileHandle &ixfileHandle, const RID &rid, int type,
//			void*prop_page_pointer, RID &prop_page_num, int &prop_len); //function called to insert the key (recursive) one;
//	RC splitLeafSearch(void *buffer, short num_of_slots, int type, int &pos); //search helper for splitting leaf nodes sends index position number & offset
//	RC splitIntermediateSearch(void *buffer, short num_of_slots, int type,
//			int &pos); //search helper for splitting intermediate nodes sends index position number & offset
//	RC printPage(IXFileHandle &ixfileHandle, int pageNum,const void *key);
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
    
    IXFileHandle *ixfileHandle;//*
    
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
