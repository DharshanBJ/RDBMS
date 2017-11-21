#include "ix.h"

#include <stdlib.h>
#include <cstring>
#include <iostream>

#include "../rbf/pfm.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance() {
	if (!_index_manager)
		_index_manager = new IndexManager();

	return _index_manager;
}
RC readOverHeads(const void *key, int type, void *buffer,
		short &free_space_of_page, short &num_of_slots, int &char_len);
RC updateRootPage(IXFileHandle &ixfileHandle, unsigned root_page_num); //to update Root node
RC insertIntoIntermediatePage(IXFileHandle &ixfileHandle, const void *key,
		PageNum page_ptr, int type, void *buffer); //,short free_space_of_page, short num_of_slots, int char_len);//to insert key and pointer to non-leaf /intermediate page
RC insertIntoLeafPage(IXFileHandle &ixfileHandle, const void *key,
		const RID &rid, int type, void *buffer); //,short free_space_of_page, short num_of_slots, int char_len);//to insert key and pointer to leaf page
RC searchLeafNode(const void *key, void *buffer, int type); //to search the key in leaf node
RC readRootPage(IXFileHandle &ixfileHandle);
RC searchIntermediateNode(const void *key, int &pagePtr, void *buffer,
		int type);
RC compareEntryKeyIndex(const void *key, void *comparisonEntry, int type,
		int compare_len);
RC compareDeleteEntryKeyRID(const void *key, void *comparisonEntry,
		const RID &rid, int type, int compare_len);
RC deleteEntryInLeaf(IXFileHandle &ixfileHandle, const void *key,
		const RID &rid, int type, void *buffer, short free_space_of_page,
		short num_of_slots, int char_len); //to delete the key index for the matching <key,rid>
RC splitLeafPage(IXFileHandle &ixfileHandle, int insert_dest, const void *key,
		const RID &rid, int type, void *buffer, void *prop_key,
		RID &prop_page_num, int &prop_key_len); //splits the leaf page and inserts the key requested into it
RC splitIntermediatePage(IXFileHandle &ixfileHandle, int insert_dest,
		const void *key, const RID &rid, int type, void *buffer, void *prop_key,
		RID &prop_page_num, int &prop_key_len); //splits the intermediate page and inserts the key propagated from new leaf into it
RC insertReccursion(unsigned page_num, const void *key,
		IXFileHandle &ixfileHandle, const RID &rid, int type,
		void*prop_page_pointer, RID &prop_page_num, int &prop_len); //function called to insert the key (recursive) one;
RC splitLeafSearch(void *buffer, short num_of_slots, int type, int &pos); //search helper for splitting leaf nodes sends index position number & offset
RC splitIntermediateSearch(void *buffer, short num_of_slots, int type,
		int &pos); //search helper for splitting intermediate nodes sends index position number & offset
IndexManager::IndexManager() {
}

IndexManager::~IndexManager() {
	_index_manager = NULL;
}

RC IndexManager::createFile(const string &fileName) {
	PagedFileManager *pfm = PagedFileManager::instance();
	RC result = pfm->createFile(fileName);
	return result;
}

RC IndexManager::destroyFile(const string &fileName) {
	PagedFileManager *pfm = PagedFileManager::instance();
	RC result = pfm->destroyFile(fileName);
	return result;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle) {
	PagedFileManager *pfm = PagedFileManager::instance();
	RC result = pfm->openFile(fileName, ixfileHandle.fileHandle);
	return result;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle) {
	PagedFileManager *pfm = PagedFileManager::instance();
	RC result = pfm->closeFile(ixfileHandle.fileHandle);
	return result;

}

RC updateRootPage(IXFileHandle &ixfileHandle, unsigned root_page_num) {

	//update the root page number in the hidden page0
	ixfileHandle.fileHandle.writeRootPageNumber(root_page_num);
	return 0;
}

RC readRootPage(IXFileHandle &ixfileHandle) {
	int root_page_num = ixfileHandle.fileHandle.readRootPageNumber();
	return root_page_num;
}

RC IndexManager::newPage(IXFileHandle &ixfileHandle, void *new_page_buffer,
		PageNum page_num, bool is_leaf, PageNum next_leaf_page_num,
		PageNum left_child_page_num) {

	const unsigned currentNumPages = ixfileHandle.fileHandle.getNumberOfPages();

	int leftChild = (int) left_child_page_num;
	short isLeafPage = (short) (is_leaf ? 1 : 0);
	short freeSpace = 0;
	short numSlots = 0;
	int next_page_num = -1; //is_leaf ? (int) next_leaf_page_num : -1;

	int page_no = 0;
	//create a buffer for page

	//prepare the buffer with footer values
	memcpy((char *) new_page_buffer + NODE_FLAG_BLOCK, &isLeafPage, 2);
	freeSpace = 4096 - 10; //10 overhead bytes at foot of page for leaf nodes and 4 byte left child pointer and 6 overhead footer of non leaf page
	memcpy((char *) new_page_buffer + FREE_SPACE_BLOCK, &freeSpace, 2);
	memcpy((char *) new_page_buffer + NUM_OF_INDEX_BLOCK, &numSlots, 2);

	//for non-leaf first 4 bytes is left child page num and for leaf -> 4bytes for next page num at head of footer
	if (!is_leaf) {
		memcpy((char *) new_page_buffer, &leftChild, 4);
	} else {
		memcpy((char *) new_page_buffer + NEXT_PAGE_PTR, &next_page_num, 4);
	}

	// If we are 'new'ing a previously allocated page then we are zeroing it and over writing
	int res = 0;

	if (page_num <= currentNumPages) { //check if < | <= makes sense
		res = ixfileHandle.fileHandle.writePage(page_num, new_page_buffer);
		if (res < 0)
			cout << "File->write page at new page() failed" << endl;

	} else { //append page
		//cout checks
//		memcpy( &isLeafPage, (char *) new_page_buffer + NODE_FLAG_BLOCK,2);
//		freeSpace = 4096 - 10; //10 overhead bytes at foot of page for leaf nodes and 4 byte left child pointer and 6 overhead footer of non leaf page
//		memcpy( &freeSpace, (char *) new_page_buffer + FREE_SPACE_BLOCK,2);
//		memcpy( &numSlots, (char *) new_page_buffer + NUM_OF_INDEX_BLOCK,2);
//
//		cout<<"isLeafPage :"<<isLeafPage<<endl;
//		cout<<"freeSpace :"<<freeSpace<<endl;
//		cout<<"numSlots :"<<numSlots<<endl;

		res = ixfileHandle.fileHandle.appendPage(new_page_buffer);
		page_no = ixfileHandle.fileHandle.getNumberOfPages();
		if (res < 0)
			cout << "File->append page at new page() failed" << endl;
	}
	return page_no;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle,
		const Attribute &attribute, const void *key, const RID &rid) {

	// Insert one key at a time, check and store key type
	// get hold of the root page
	// compare the key and read the correct pageNum PTR (if< key read left PTR else (if >= than key read right PTR)
	// while loop until offset is got-> search Intermediate/Leaf based on the flag (call accordingly)
	// contd. read of pageNum PTR the position to insert --all logic happens here
	// contd. return the pageNum PTR(intermediate search) & offset to insert(node page search)
	// based on offset call insertIntoLeafPage()
	// if overflow of leaf node page then create a new leafPage and update the required overheads and move data into new page->
	// contd. insert the <key,rid> and update the upper intermediate nodes with the LOWEST_KEY of the newly created leaf page ->insertIntermediatePage(this lowestkey)
	// contd. if an overflow is also found in the intermediate nodes because of leaf getting split (as usual pass LOWEST_KEY up) and then
	// create a new intermediate parent Node and split by putting half the data into the new intermediate one
	// contd. replace the HIGHEST_KEY of the old (overFlown intermediate node page) with the LOWEST KEY of leafNode Page while
	// contd. ***** copying to its parent intermediate node-> if parent Intermediate Node is not in existence then create a
	// contd. a new rootKey page(HIGEST_KEY of child intermediate Node after split and NOTE: *dont forget about replacing it in child by other to maintain rule )
	// contd. and update the hidden page with the new root pageNum PTR.
	// The new root Key will have left pointer pointing to the old Intermediate Node and right will be to the Newly created Intermediate Node

//	cout << "No. of pages :" << ixfileHandle.fileHandle.getNumberOfPages()
//			<< endl;

	int type = (attribute.type == TypeVarChar) ? 2 :
				(attribute.type == TypeReal) ? 1 : 0;

	// Traverse down the tree to the leaf, using non-leaves along the way
	int insert_dest = readRootPage(ixfileHandle);

	if (insert_dest == 0) {
		// Create the root<leaf> page, mark it as a leaf, it's page 1
		void *new_page_buffer = calloc(PAGE_SIZE, 1);

		unsigned page_no = newPage(ixfileHandle, new_page_buffer, 1, true, 0,
				0);

		free(new_page_buffer);

		// Store the new page to page 0
		updateRootPage(ixfileHandle, page_no);
	}

	insert_dest = readRootPage(ixfileHandle);

	RID prop_page_num;
	int prop_key_len = 0;
	void *prop_page_pointer = calloc(PAGE_SIZE, 1);
	insertReccursion(insert_dest, key, ixfileHandle, rid, type,
			prop_page_pointer, prop_page_num, prop_key_len);

	free(prop_page_pointer);
	return 0;
}

RC insertReccursion(unsigned page_num, const void *key,
		IXFileHandle &ixfileHandle, const RID &rid, int type,
		void *prop_page_pointer, RID &prop_page_num, int &prop_key_len) {

	void *insert_entry_buffer = calloc(PAGE_SIZE, 1);

	//pull the root page
	ixfileHandle.fileHandle.readPage(page_num, insert_entry_buffer);

	// Read overheads for the current page-> page_num
	short free_space_of_page;
	short num_of_slots;
	int char_len;
	readOverHeads(key, type, insert_entry_buffer, free_space_of_page,
			num_of_slots, char_len);

	//read leaf flag on the root page
	short is_leaf_page = 0;
	memcpy(&is_leaf_page, (char *) insert_entry_buffer + NODE_FLAG_BLOCK, 2);

	int page_ptr = 0;
	if (is_leaf_page == 0) {
		searchIntermediateNode(key, page_ptr, insert_entry_buffer, type);
		insertReccursion(page_ptr, key, ixfileHandle, rid, type,
				prop_page_pointer, prop_page_num, prop_key_len);
		if (prop_key_len != -1) {
			if (free_space_of_page > (prop_key_len + 4)) {
				insertIntoIntermediatePage(ixfileHandle, prop_page_pointer,
						prop_page_num.pageNum, type, insert_entry_buffer);
				ixfileHandle.fileHandle.writePage(page_num,
						insert_entry_buffer);
				prop_key_len = -1;
			} else {

				void *insert_key_pointer = calloc(prop_key_len, 1);
				memcpy(insert_key_pointer, prop_page_pointer, prop_key_len);
				RID insert_page_num_ptr;
				insert_page_num_ptr.pageNum = prop_page_num.pageNum;

				//check with cout
//				char str1_key_Array[prop_key_len];
//				memcpy(&str1_key_Array, (char *) prop_page_pointer,
//						prop_key_len);
//				string str1_key(str1_key_Array, prop_key_len);
//				cout << "propagated key from leaf to intermediate:" << str1_key
//						<< endl;
//				cout << "propagated key length  :" << prop_key_len << endl;
//				cout << "propagated page number from leaf to intermediate::"
//						<< prop_page_num.pageNum << endl;

				splitIntermediatePage(ixfileHandle, page_num,
						insert_key_pointer, insert_page_num_ptr, type,
						insert_entry_buffer, prop_page_pointer, prop_page_num,
						prop_key_len);

				//check with cout
//				char str_key_Array[prop_key_len];
//				memcpy(&str_key_Array, (char *) prop_page_pointer,
//						prop_key_len);
//				string str_key(str_key_Array, prop_key_len);
//				cout << "propagated key from intermediate :" << str_key << endl;
//				cout << "propagated key length  :" << prop_key_len << endl;
//				cout << "propagated page number from intermediate :"
//						<< prop_page_num.pageNum << endl;

				unsigned root_node_page_num = readRootPage(ixfileHandle);
				if (page_num == root_node_page_num) {
					void *root_page_buffer = calloc(PAGE_SIZE, 1);

					memcpy((char *) root_page_buffer, &page_num, 4);
					memcpy((char *) root_page_buffer + 4, prop_page_pointer,
							prop_key_len);
					memcpy((char *) root_page_buffer + 4 + prop_key_len,
							&prop_page_num, 4);

					short root_free_space = PAGE_SIZE - 6 - PAGE_NUM_PTR_SIZE
							- prop_key_len - PAGE_NUM_PTR_SIZE;
					short root_node_entry_num = 1;
					short leaf_flag = 0;

					memcpy((char *) root_page_buffer + NODE_FLAG_BLOCK,
							&leaf_flag, 2);
					memcpy((char *) root_page_buffer + FREE_SPACE_BLOCK,
							&root_free_space, 2);
					memcpy((char *) root_page_buffer + NUM_OF_INDEX_BLOCK,
							&root_node_entry_num, 2);

					ixfileHandle.fileHandle.appendPage(root_page_buffer);
					unsigned num_of_pages =
							ixfileHandle.fileHandle.getNumberOfPages();
					updateRootPage(ixfileHandle, num_of_pages);

					free(root_page_buffer);
				}

				free(insert_key_pointer);
			}
		}
	} else if (is_leaf_page == 1) {
		if (free_space_of_page >= (char_len + 8)) {
			insertIntoLeafPage(ixfileHandle, key, rid, type,
					insert_entry_buffer);
			ixfileHandle.fileHandle.writePage(page_num, insert_entry_buffer);
			prop_key_len = -1;
		} else {
			splitLeafPage(ixfileHandle, page_num, key, rid, type,
					insert_entry_buffer, prop_page_pointer, prop_page_num,
					prop_key_len);

			//check with cout
//			char str_key_Array[prop_key_len];
//			memcpy(&str_key_Array, (char *) prop_page_pointer, prop_key_len);
//			string str_key(str_key_Array, prop_key_len);
//			cout << "propagated key from leaf :" << str_key << endl;
//			cout << "propagated key length  :" << prop_key_len << endl;
//			cout << "propagated page number from leaf :"
//					<< prop_page_num.pageNum << endl;

			//if the page number to be inserted was a root then create a new root and update
			unsigned root_node_page_num = readRootPage(ixfileHandle);
			if (page_num == root_node_page_num) {
				void *root_page_buffer = calloc(PAGE_SIZE, 1);

				memcpy((char *) root_page_buffer, &page_num, 4);
				memcpy((char *) root_page_buffer + 4, prop_page_pointer,
						prop_key_len);
				memcpy((char *) root_page_buffer + 4 + prop_key_len,
						&prop_page_num.pageNum, 4);

				short root_free_space = PAGE_SIZE - 6 - PAGE_NUM_PTR_SIZE
						- prop_key_len - PAGE_NUM_PTR_SIZE;
				short root_node_entry_num = 1;
				short leaf_flag = 0;

				memcpy((char *) root_page_buffer + NODE_FLAG_BLOCK, &leaf_flag,
						2);
				memcpy((char *) root_page_buffer + FREE_SPACE_BLOCK,
						&root_free_space, 2);
				memcpy((char *) root_page_buffer + NUM_OF_INDEX_BLOCK,
						&root_node_entry_num, 2);

				ixfileHandle.fileHandle.appendPage(root_page_buffer);
				unsigned num_of_pages =
						ixfileHandle.fileHandle.getNumberOfPages();
				updateRootPage(ixfileHandle, num_of_pages);

				free(root_page_buffer);
			}
		}
	}
	free(insert_entry_buffer);
	return 0;
}
RC splitLeafPage(IXFileHandle &ixfileHandle, int insert_dest, const void *key,
		const RID &rid, int type, void *input_leaf_buffer, void *prop_key,
		RID &prop_page_num, int &prop_key_len) {

	// Definition of useful page overheads
	short free_space_of_page;
	short num_of_slots;
	int char_len;
	readOverHeads(key, type, input_leaf_buffer, free_space_of_page,
			num_of_slots, char_len);

	void *right_page = calloc(PAGE_SIZE, 1);		//new page mapping buffer

	// Get the point to split the existing page at
	int split_index_pos_num;
	int split_page_offset = splitLeafSearch(input_leaf_buffer, num_of_slots,
			type, split_index_pos_num);

	// Manipulations to right page

	// Calculate move length to move into right pointer and move half the records to a new page
	int right_page_mov_len = PAGE_SIZE - 10 - free_space_of_page
			- split_page_offset;
	memmove((char *) right_page, (char *) input_leaf_buffer + split_page_offset,
			right_page_mov_len);

	short right_page_free_space = free_space_of_page + split_page_offset;//int to short?
	short right_page_num_slots = num_of_slots - split_index_pos_num;
	short is_leaf = 1;

	// Copy and move the overheads to the new page
	memcpy((char *) right_page + NEXT_PAGE_PTR,
			(char *) input_leaf_buffer + NEXT_PAGE_PTR, 4);
	memcpy((char *) right_page + NUM_OF_INDEX_BLOCK, &right_page_num_slots, 2);
	memcpy((char *) right_page + FREE_SPACE_BLOCK, &right_page_free_space, 2);
	memcpy((char *) right_page + NODE_FLAG_BLOCK, &is_leaf, 2);

	// Modify/update old page data
	free_space_of_page = free_space_of_page + right_page_mov_len;
	num_of_slots = split_index_pos_num;

	memcpy((char *) input_leaf_buffer + NUM_OF_INDEX_BLOCK, &num_of_slots, 2);
	memcpy((char *) input_leaf_buffer + FREE_SPACE_BLOCK, &free_space_of_page,
			2);

	// Return first key of new page
	prop_key_len = 0;
	if (type == 2)
		memcpy(&prop_key_len, (char *) right_page, 4);
	prop_key_len += 4;
	memcpy(prop_key, right_page, prop_key_len);

	//	To insert the actual key the insert was called for
	int insert_result = compareEntryKeyIndex(key, prop_key, type, prop_key_len);
	if (insert_result == 1) {
		//insert in existing page
		insertIntoLeafPage(ixfileHandle, key, rid, type, input_leaf_buffer);
	} else {
		//insert in new page
		insertIntoLeafPage(ixfileHandle, key, rid, type, right_page);
	}

	// Create a new page for the split page
	ixfileHandle.fileHandle.appendPage(right_page);
	prop_page_num.pageNum = ixfileHandle.fileHandle.getNumberOfPages();

	memcpy((char *) input_leaf_buffer + NEXT_PAGE_PTR, &prop_page_num.pageNum,
			4);

	// Write the buffer back to leaf page
	ixfileHandle.fileHandle.writePage(insert_dest, input_leaf_buffer);

	free(right_page);
	return 0; //return offset of split in input buffer
}

RC splitLeafSearch(void *buffer, short num_of_slots, int type, int &pos) {

	int buffer_offset = 0;
	int entryI;
	float entryF;
	string entryV;

	for (int i = 1; i <= (int) num_of_slots; i++) {

		int one_index_len = 0;

		if (i == (int) num_of_slots / 2) {

			if (type == 2) {
				memcpy(&one_index_len, (char *) buffer + buffer_offset, 4);

				char Str1_array[one_index_len];
				memcpy(&Str1_array, (char *) buffer + buffer_offset + 4,
						one_index_len);

				string Str1(Str1_array, one_index_len);
				entryV = Str1;

			} else if (type == 1) {

				memcpy(&entryF, (char *) buffer + buffer_offset, 4);

			} else {

				memcpy(&entryI, (char *) buffer + buffer_offset, 4);

			}

			one_index_len += 4;	//for int and float and Len indicator for varchar

		} else if (i > (int) num_of_slots / 2) {

			if (type == 2) {

				memcpy(&one_index_len, (char *) buffer + buffer_offset, 4);

				char Str2_array[one_index_len];

				memcpy(&Str2_array, (char *) buffer + buffer_offset + 4,
						one_index_len);
				string Str2(Str2_array, one_index_len);

				if (entryV != Str2) {
					pos = i - 1;
					break;
				}

			} else if (type == 1) {
				float break_entryF = 0;
				memcpy(&break_entryF, (char *) buffer + buffer_offset, 4);

				if (entryF != break_entryF) {
					pos = i - 1;
					break;
				}
			} else {
				int break_entryI = 0;
				memcpy(&break_entryI, (char *) buffer + buffer_offset, 4);

				if (entryI != break_entryI) {
					pos = i - 1;
					break;
				}
			}

			one_index_len += 4;	//for int and float and Len indicator for varchar

		} else {

			//to compute offset to skip to next key
			if (type == 2) {
				memcpy(&one_index_len, (char *) buffer + buffer_offset, 4);
				one_index_len += 4;		//char len of varchar and int
			} else {
				one_index_len = 4;		//for int and float
			}
		}

		//to compute offset to skip to next page number
		one_index_len += RID_BLOCK_SIZE;
		buffer_offset += one_index_len;

	}

	return buffer_offset;
}

RC splitIntermediatePage(IXFileHandle &ixfileHandle, int insert_dest,
		const void *key, const RID &rid, int type, void *input_buffer,
		void *prop_key, RID &prop_page_num, int &prop_key_len) {

	// Definition of page overheads
	short free_space_of_page;
	short num_of_slots;
	int char_len;
	readOverHeads(key, type, input_buffer, free_space_of_page, num_of_slots,
			char_len);
	void *right_page = calloc(PAGE_SIZE, 1);		//new page mapping buffer

	// Get the point to split the existing page at
	int split_index_pos_num;
	int split_page_offset = splitIntermediateSearch(input_buffer, num_of_slots,
			type, split_index_pos_num);

	// Read the first index entry on left page and evict it from buffer and remaining push to right pafe and
	//accordingly update the free space and num of indices carefully.
	// Return the supposed first key of new page
	prop_key_len = 0;
	if (type == 2) {
		memcpy(&prop_key_len, (char *) input_buffer + split_page_offset, 4);
	}
	prop_key_len += 4;
	memcpy(prop_key, (char *) input_buffer + split_page_offset, prop_key_len);

	//do manipulations to right page

	// Calculate move length to move data into right page and move half the records to a new page
	int right_page_mov_len = PAGE_SIZE - 6 - free_space_of_page
			- split_page_offset - prop_key_len;
	memmove((char *) right_page,
			(char *) input_buffer + split_page_offset + prop_key_len,
			right_page_mov_len);

	short right_page_free_space = free_space_of_page + split_page_offset
			+ prop_key_len;
	short right_page_num_slots = num_of_slots - split_index_pos_num - 1;
	short is_leaf = 0;

	// Copy and move the overheads to the new page
	memcpy((char *) right_page + NUM_OF_INDEX_BLOCK, &right_page_num_slots, 2);
	memcpy((char *) right_page + FREE_SPACE_BLOCK, &right_page_free_space, 2);
	memcpy((char *) right_page + NODE_FLAG_BLOCK, &is_leaf, 2);

	// Create a new page for the split page
//	ixfileHandle.fileHandle.appendPage(right_page);
//	prop_page_num.pageNum = ixfileHandle.fileHandle.getNumberOfPages();

	// Modify/update old page data
	free_space_of_page = free_space_of_page + right_page_mov_len;
	num_of_slots = split_index_pos_num;

	memcpy((char *) input_buffer + NUM_OF_INDEX_BLOCK, &num_of_slots, 2);
	memcpy((char *) input_buffer + FREE_SPACE_BLOCK, &free_space_of_page, 2);

	// Write the data back to leaf page
//	ixfileHandle.fileHandle.writePage(insert_dest, input_buffer);

	//	To insert the actual key the insert was called for
	int insert_result = compareEntryKeyIndex(key, prop_key, type, prop_key_len);
	if (insert_result == 1) {
		//insert in existing page
		insertIntoIntermediatePage(ixfileHandle, key, rid.pageNum, type,
				input_buffer);
	} else {
		//insert in new page
		insertIntoIntermediatePage(ixfileHandle, key, rid.pageNum, type,
				right_page);
	}

	// Create a new page for the split page
	ixfileHandle.fileHandle.appendPage(right_page);
	prop_page_num.pageNum = ixfileHandle.fileHandle.getNumberOfPages();

	// Write the buffer back to leaf page
	ixfileHandle.fileHandle.writePage(insert_dest, input_buffer);

	free(right_page);
	return 0;	//return offset of split in input buffer
}

RC splitIntermediateSearch(void *buffer, short num_of_slots, int type,
		int &pos) {

	int buffer_offset = 4;
	for (int i = 0; i < (int) num_of_slots / 2; i++) {
		int one_index_len = 0;

		//to compute offset to skip to next key
		if (type == 2) {
			memcpy(&one_index_len, (char *) buffer + buffer_offset, 4);
		}
		one_index_len += 4;	//for int and float and Len indicator for varchar
		one_index_len += PAGE_NUM_PTR_SIZE;
		buffer_offset += one_index_len;
	}

	pos = (int) num_of_slots / 2;
	return buffer_offset;
}

RC readOverHeads(const void *key, int type, void *buffer,
		short &free_space_of_page, short &num_of_slots, int &char_len) {

	memcpy(&free_space_of_page, (char *) buffer + FREE_SPACE_BLOCK, 2);
	memcpy(&num_of_slots, (char *) buffer + NUM_OF_INDEX_BLOCK, 2);
	if (type == 2) {
		memcpy(&char_len, (char *) key, 4);
		char_len += 4;	//char length + 4 bytes representing it
	} else {
		char_len = 4;
	}
	return 0;
}

RC insertIntoIntermediatePage(IXFileHandle &ixfileHandle, const void *key,
		PageNum page_ptr, int type, void *buffer) {	//,
	//short free_space_of_page, short num_of_slots, int char_len) {//char_len =4 for real and int and 4+var_char for others has to be passed along

	//definition of useful page overheads
	short free_space_of_page;
	short num_of_slots;
	int char_len;
	readOverHeads(key, type, buffer, free_space_of_page, num_of_slots,
			char_len);

	//cout checks
//	cout << "num_of_indexes before leaf insert :" << num_of_slots << endl;
//	cout << "free_space before leaf insert :" << free_space_of_page << endl;

	//offset where the insert should happen
	int page_num_ptr = 0;
	int buffer_offset_to_insert = searchIntermediateNode(key, page_num_ptr,
			buffer, type);

	//find length and move right
	int move_len = PAGE_SIZE - 6 - free_space_of_page - buffer_offset_to_insert;//difference between last data offset - place to be inserted
	memmove(
			(char*) buffer + buffer_offset_to_insert + char_len
					+ PAGE_NUM_PTR_SIZE,
			(char*) buffer + buffer_offset_to_insert, move_len);

	//update the buffer with new key and page pointer to be inserted
	memcpy((char*) buffer + buffer_offset_to_insert, (char*) key, char_len);
	memcpy((char*) buffer + buffer_offset_to_insert + char_len, &page_ptr,
	PAGE_NUM_PTR_SIZE);

	//cout check
//	int entry=0;
//	memcpy(&entry, (char*) key,char_len);
//	cout<<"new:"<<entry<<endl;

	//update the number of indexes slot on the page
	num_of_slots += 1;
	memcpy((char *) buffer + NUM_OF_INDEX_BLOCK, &num_of_slots, 2);

	//update the newly available free space on the page
	free_space_of_page = free_space_of_page - PAGE_NUM_PTR_SIZE - char_len;
	memcpy((char *) buffer + FREE_SPACE_BLOCK, &free_space_of_page, 2);

	//cout checks
//	cout << "num_of_indexes after insert :" << num_of_slots << endl;
//	cout << "free_space after  insert :" << free_space_of_page << endl;

	return 0;
}

RC insertIntoLeafPage(IXFileHandle &ixfileHandle, const void *key,
		const RID &rid, int type, void *buffer) {//, short free_space_of_page,
	//short num_of_slots, int char_len) {	//char_len =4 for int/real n 4+lenght of varchar for varchar

	//definition of useful page overheads
	short free_space_of_page;
	short num_of_slots;
	int char_len;
	readOverHeads(key, type, buffer, free_space_of_page, num_of_slots,
			char_len);

	//cout checks
//	cout << "num_of_slots before leaf insert :" << num_of_slots << endl;
//	cout << "free_space_of_page before leaf insert :" << free_space_of_page
//			<< endl;

	//search the position where the key has to be inserted
	int buffer_offset_to_insert = searchLeafNode(key, buffer, type);
	int move_len = PAGE_SIZE - 10 - free_space_of_page
			- buffer_offset_to_insert;//difference between last data  offset - place to be inserted

	//find length and move right
	memmove(
			(char*) buffer + buffer_offset_to_insert + char_len + RID_BLOCK_SIZE,
			(char*) buffer + buffer_offset_to_insert, move_len);

	//update the buffer with new key and page ptr to be inserted
	memcpy((char*) buffer + buffer_offset_to_insert, (char*) key, char_len);
	memcpy((char*) buffer + buffer_offset_to_insert + char_len, &rid,
	RID_BLOCK_SIZE);


//	//check with cout
//	string str_key((char*)key + 4, char_len - 4);
//	cout << "propagated key from leaf :" << str_key << endl;

	//cout check
//				int entry=0;
//				memcpy(&entry, (char*) key,char_len);
//				cout<<"new:"<<entry<<endl;

	//update the number of indexes slot on the page
	num_of_slots += 1;
	memcpy((char *) buffer + NUM_OF_INDEX_BLOCK, &num_of_slots, 2);

	//update the newly available free space on the page
	free_space_of_page = free_space_of_page - RID_BLOCK_SIZE - char_len;
	memcpy((char*) buffer + FREE_SPACE_BLOCK, &free_space_of_page, 2);

	//cout checks
//	cout << "num_of_slots after leaf insert :" << num_of_slots << endl;
//	cout << "free_space_of_page after leaf insert :" << free_space_of_page
//			<< endl;

	return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle,
		const Attribute &attribute, const void *key, const RID &rid) {

	int result = -1;
//	cout << "No. of pages :" << ixfileHandle.fileHandle.getNumberOfPages()
//			<< endl;

	int type = (attribute.type == TypeVarChar) ? 2 :
				(attribute.type == TypeReal) ? 1 : 0;

	// To keep track of pages we've traversed so that we can find their parent pointers
	std::vector<int> parents;

	// Traverse down the tree to the leaf, using non-leaves along the way
	int insert_dest = readRootPage(ixfileHandle);
	void *buffer = calloc(PAGE_SIZE, 1);

	//pull the root page
	ixfileHandle.fileHandle.readPage(insert_dest, buffer);

	//read leaf flag on the root page
	int is_leaf_page = 0;
	memcpy(&is_leaf_page, (char *) buffer + NODE_FLAG_BLOCK, 2);

	while (is_leaf_page == 0) {
//		parents.push_back(insert_dest);

		searchIntermediateNode(key, insert_dest, buffer, type);

		// pull and read the designated page into memory and refresh the leaf flag
		memset(buffer, 0, PAGE_SIZE);
		ixfileHandle.fileHandle.readPage(insert_dest, buffer);
		memcpy(&is_leaf_page, (char *) buffer + NODE_FLAG_BLOCK, 2);
	}

	//check if we are at leaf page
//	cout << "is_leaf_page :" << is_leaf_page << endl;

	//to delete index entry from leaf node
	short free_space_of_page;
	short num_of_slots;
	int char_len;
	readOverHeads(key, type, buffer, free_space_of_page, num_of_slots,
			char_len);

	if (num_of_slots != 0)
		result = deleteEntryInLeaf(ixfileHandle, key, rid, type, buffer,
				free_space_of_page, num_of_slots, char_len);

	if (result == 0)
		ixfileHandle.fileHandle.writePage(insert_dest, buffer);

	free(buffer);
	return result;
}

RC deleteEntryInLeaf(IXFileHandle &ixfileHandle, const void *key,
		const RID &rid, int type, void *buffer, short free_space_of_page,
		short num_of_slots, int char_key_len) {	//char_key_len =4 for int/real n 4+lenght of varchar for varchar

	int result = -1;

	//corner case test for intermediate page
	short flag;
	memcpy(&flag, (char *) buffer + NODE_FLAG_BLOCK, 2);
	if (flag == 0) {
		return -1;	//if the node is an intermediate node
	}

	int buffer_offset_to_delete = 0;	//searchLeafNode(key, buffer, type);
	int char_len = 0;
	void *comparisonEntry = calloc(PAGE_SIZE, 1);
	for (int i = 0; i < (int) num_of_slots; i++) {
		char_len = 0;

		//to compare the key if var char type ==2 else compare 4 bytes
		if (type == 2) {
			memcpy(&char_len, (char *) buffer + buffer_offset_to_delete, 4);

			memcpy(comparisonEntry,
					(char *) buffer + buffer_offset_to_delete + 4,
					char_len + 8);

		} else {
			memcpy(comparisonEntry, (char *) buffer + buffer_offset_to_delete,
					12);//4+8
			char_len = 4;
		}
		if (compareDeleteEntryKeyRID(key, comparisonEntry, rid, type, char_len)
				== 1) {	//check if key Parameter n rid are matched
			int tombStone = 0;
			if (type == 2) {
				memcpy((char *) buffer + buffer_offset_to_delete + 4 + char_len,
						&tombStone, 4);
				memcpy(
						(char *) buffer + buffer_offset_to_delete + 4 + char_len
								+ 4, &tombStone, 4);
			} else {
				memcpy((char *) buffer + buffer_offset_to_delete + 4,
						&tombStone, 4);
				memcpy((char *) buffer + buffer_offset_to_delete + 8,
						&tombStone, 4);
			}
			result = 0;
			break;
		}
		if (type == 2) {
			buffer_offset_to_delete += 4;
		}
		buffer_offset_to_delete += char_len;	//length of var_char else 4
		buffer_offset_to_delete += RID_BLOCK_SIZE;

	}

//	if (result == 0) {

	//find length and move right
//		int move_len = PAGE_SIZE - 10 - free_space_of_page
//				- buffer_offset_to_delete - char_len - RID_BLOCK_SIZE;//diff between last data  offset - offset of next index place to be deleted
//		memmove((char*) buffer + buffer_offset_to_delete,
//				(char*) buffer + buffer_offset_to_delete + char_len	//char_key_len
//						+ RID_BLOCK_SIZE, move_len);

	//update the number of indexes slot on the page
//		num_of_slots -= 1;
//		memcpy((char *) buffer + NUM_OF_INDEX_BLOCK, &num_of_slots, 2);

	//update the newly available free space on the page
//		free_space_of_page = free_space_of_page + RID_BLOCK_SIZE + char_len;
//		memcpy(&free_space_of_page, (char*) buffer + FREE_SPACE_BLOCK, 2);
//	}

	free(comparisonEntry);
	return result;
}

RC compareDeleteEntryKeyRID(const void *key, void *comparisonEntry,
		const RID &rid, int type, int compare_len) {
	int result = 0;

	if (type == 0) {

		int compare1 = 0;
		memcpy(&compare1, (char *) comparisonEntry, compare_len);

		int compare_key = *(int *) key;
		result = (compare_key == compare1) ? 1 : 0;

	} else if (type == 1) {

		float compare1 = 0;
		memcpy(&compare1, (char *) comparisonEntry, compare_len);

		float compare_key = *(float *) key;
		result = (compare_key == compare1) ? 1 : 0;

	} else if (type == 2) {

		char str1_Array[compare_len];
		memcpy(&str1_Array, (char *) comparisonEntry, compare_len); //copy the varchar entry into string format
		string str1(str1_Array, compare_len); //convert the char array to string

		int str2_len = 0; //parameter key
		memcpy(&str2_len, (char *) key, 4);

		char str2_Array[str2_len];
		memcpy(&str2_Array, (char *) key + 4, str2_len); //copy the key into string format
		string str2_Key(str2_Array, str2_len); //convert the char array to string

		result = (str2_Key == str1) ? 1 : 0;
	}

	//check if RIDS of the given key to delete matches with the one of the index
	if (result == 1) {
//		unsigned compare1_page_num = -1, compare1_slot_num = -1;
//
//		memcpy(&compare1_page_num, (char *) comparisonEntry + compare_len, 4);
//		memcpy(&compare1_slot_num, (char *) comparisonEntry + compare_len + 4,
//				4);
//
//		result =
//				((compare1_page_num == rid.pageNum)
//						&& (compare1_slot_num == rid.slotNum)) ? 1 : 0;
		result =
				(memcmp((char *) comparisonEntry + compare_len, &rid, 8) == 0) ?
						1 : 0;
	}

	return result;
}

RC searchIntermediateNode(const void *key, int &pagePtr, void *buffer,
		int type) {

	//corner case test for intermediate page
	short flag;
	memcpy(&flag, (char *) buffer + NODE_FLAG_BLOCK, 2);
	if (flag) {
		return -1;	//if the node is a leaf node
	}

	//to return the correct page Num for the give Key
	short num_of_index_slots;
	memcpy(&num_of_index_slots, (char*) buffer + NUM_OF_INDEX_BLOCK, 2);

	int buffer_offset = 4;	//4->left pointer of first key
	void *comparisonEntry = calloc(PAGE_SIZE, 1);
	for (int i = 0; i < (int) num_of_index_slots; i++) {
		int char_len = 0;

		//to compare the key if var char type ==2 else compare 4 bytes
		if (type == 2) {
			memcpy(&char_len, (char *) buffer + buffer_offset, 4);

			memcpy(comparisonEntry, (char *) buffer + buffer_offset + 4,
					char_len);

		} else {
			memcpy(comparisonEntry, (char *) buffer + buffer_offset, 4);
			char_len = 4;

			//cout check
//			int entry=0;
//			memcpy(&entry, (char *)comparisonEntry,4);
//			cout<<i<<":"<<entry<<"\t";
		}

		if (compareEntryKeyIndex(key, comparisonEntry, type, char_len) == 1) {//check if key Parameter is right or not
			break;
		}

		if (type == 2) {
			buffer_offset += 4;
		}

		buffer_offset += char_len;	//length of var_char else 4 for int/real
		buffer_offset += PAGE_NUM_PTR_SIZE;

	}
	memcpy(&pagePtr, (char *) buffer + buffer_offset - 4, 4);//read left page number Ptr onto pagePTR
//	cout << "left page ptr: " << pagePtr << endl;

	free(comparisonEntry);
	return buffer_offset;
	//pagePtr is updated with appropriate page num for child intermediate/leaf node and buffer_offset for insert index -> returned (4<default> , max<4090>)
}

RC searchLeafNode(const void *key, void *buffer, int type) {

	//corner case test for intermediate page
	short flag;
	memcpy(&flag, (char *) buffer + NODE_FLAG_BLOCK, 2);
	if (flag == 0) {
		return -1;	//if the node is an intermediate node
	}

	//to loop through until you find the match greater than the of the key
	short num_of_key_slots;
	memcpy(&num_of_key_slots, (char*) buffer + NUM_OF_INDEX_BLOCK, 2);
	int buffer_offset = 0;
	void *comparisonEntry = calloc(PAGE_SIZE, 1);
	for (int i = 0; i < (int) num_of_key_slots; i++) {
		int char_len = 0;

		//to compare the key if var char type ==2 else compare 4 bytes
		if (type == 2) {
			memcpy(&char_len, (char *) buffer + buffer_offset, 4);

			memcpy(comparisonEntry, (char *) buffer + buffer_offset + 4,
					char_len);

		} else {
			memcpy(comparisonEntry, (char *) buffer + buffer_offset, 4);
			char_len = 4;

			//cout check
//			int entry=0;
//			memcpy(&entry, (char *)comparisonEntry,4);
//			cout<<i<<":"<<entry<<"\t";
		}
		if (compareEntryKeyIndex(key, comparisonEntry, type, char_len) == 1) {//check if key Parameter is right or not
			break;
		}
		if (type == 2) {
			buffer_offset += 4;
		}
		buffer_offset += char_len;	//length of var_char else 4
		buffer_offset += RID_BLOCK_SIZE;

	}

	free(comparisonEntry);
	return buffer_offset;
}

RC compareEntryKeyIndex(const void *key, void *comparisonEntry, int type,
		int compare_len) {
	int result = 0;

	if (type == 0) {

		int compare1 = 0;
		memcpy(&compare1, (char *) comparisonEntry, compare_len);

		int compare_key = *(int *) key;
		result = (compare_key < compare1) ? 1 : 0;

	} else if (type == 1) {

		float compare1 = 0;
		memcpy(&compare1, (char *) comparisonEntry, compare_len);

		float compare_key = *(float *) key;
		result = (compare_key < compare1) ? 1 : 0;

	} else if (type == 2) {

		char str1_Array[compare_len];
		memcpy(&str1_Array, (char *) comparisonEntry, compare_len); //copy the varchar entry into string format
		string str1(str1_Array, compare_len); //convert the char array to string

		int str2_len = 0; //parameter key
		memcpy(&str2_len, (char *) key, 4);

		char str2_Array[str2_len];
		memcpy(&str2_Array, (char *) key + 4, str2_len); //copy the key into string format
		string str2_Key(str2_Array, str2_len); //convert the char array to string

		result = (str2_Key < str1) ? 1 : 0;

	}
	return result;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *lowKey, const void *highKey, bool lowKeyInclusive,
		bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator) {

	if (ixfileHandle.fileHandle.getPageFilePtr() == NULL)
		return -1;

	void *currentPageBuffer = calloc(PAGE_SIZE,1);

	ix_ScanIterator.ixfileHandle = &ixfileHandle;
	ix_ScanIterator.attrType = attribute.type;
	ix_ScanIterator.currentPageNum = 0;
	ix_ScanIterator.currentOffset = -1;
	ix_ScanIterator.lowKey = lowKey;
	ix_ScanIterator.highKey = highKey;
	ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
	ix_ScanIterator.highKeyInclusive = highKeyInclusive;
	ix_ScanIterator.insert_enrty_buffer =currentPageBuffer;
	return 0;

}

void IndexManager::printBtree(IXFileHandle &ixfileHandle,
		const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
	//find the root page number
	//find the leaf page and the offset from where i have to start scanning-do this by checking with low key
	//check if the leaf key satisfies the condition >low key and < high key,if it does then return that key and rid

//	void *insert_enrty_buffer = calloc(PAGE_SIZE, 1);

	if (currentPageNum == 0 && currentOffset == -1) {
		// Traverse down the tree to the leaf, using non-leaves along the way
		int insert_dest = readRootPage(*ixfileHandle);

		//pull the root page
		ixfileHandle->fileHandle.readPage(insert_dest, insert_enrty_buffer);

		//read leaf flag on the root page
		int is_leaf_page = 0;
		memcpy(&is_leaf_page, (char *) insert_enrty_buffer + NODE_FLAG_BLOCK,
				2);

		while (is_leaf_page == 0) {
			if (lowKey == NULL) {
				memcpy(&insert_dest, (char *) insert_enrty_buffer, 4);
			} else {
				searchIntermediateNode(lowKey, insert_dest, /// check for lowkey when = NULL
						insert_enrty_buffer, attrType);
			}
			// pull and read the designated page into memory and refresh the leaf flag
			memset(insert_enrty_buffer, 0, PAGE_SIZE);
			ixfileHandle->fileHandle.readPage(insert_dest, insert_enrty_buffer); //insert_dest will have the leaf page number and insert_enrty_buffer will have the leaf page
			memcpy(&is_leaf_page,
					(char *) insert_enrty_buffer + NODE_FLAG_BLOCK, 2);
		}

		int result = -1;

		//search function to update the key and rid
		do {
			result = scanLeafNodes(insert_enrty_buffer, lowKey, highKey,
					lowKeyInclusive, highKeyInclusive, attrType, rid, key);

			if (result == -1) {
//				free(insert_enrty_buffer);
				return -1;
			} else if (result == 1) {
				currentPageNum = insert_dest;
				break;
			} else {
				int overFlowFlag = 0;
				memcpy(&overFlowFlag, (char *) insert_enrty_buffer + 4086,
						sizeof(int));
				if (overFlowFlag != -1) {
					memset(insert_enrty_buffer, 0, PAGE_SIZE);
					ixfileHandle->fileHandle.readPage(overFlowFlag,
							insert_enrty_buffer);
					insert_dest = overFlowFlag;
					currentPageNum = overFlowFlag;
					currentOffset = 0;
				} else {
//					free(insert_enrty_buffer);
					return -1;
				}
			}

		} while (result == 0);
	} else {
		int insert_dest = currentPageNum;

		//directly call the search function here
//		ixfileHandle->fileHandle.readPage(insert_dest, insert_enrty_buffer);
		int result = -1;

		//search function to update the key and rid
		do {
			result = scanLeafNodes(insert_enrty_buffer, lowKey, highKey,
					lowKeyInclusive, highKeyInclusive, attrType, rid, key);

			if (result == -1) {
//				free(insert_enrty_buffer);
				return -1;
			} else if (result == 1) {
				currentPageNum = insert_dest;
				break;
			} else {
				int overFlowFlag = 0;
				memcpy(&overFlowFlag, (char *) insert_enrty_buffer + 4086,
						sizeof(int));
				if (overFlowFlag != -1) {
					memset(insert_enrty_buffer, 0, PAGE_SIZE);
					ixfileHandle->fileHandle.readPage(overFlowFlag,
							insert_enrty_buffer);
					insert_dest = overFlowFlag;
					currentPageNum = overFlowFlag;
					currentOffset = 0;
				} else {
//					free(insert_enrty_buffer);
					return -1;
				}
			}

		} while (result == 0);
	}

//	free(insert_enrty_buffer);
	return 0;
}

RC IX_ScanIterator::scanLeafNodes(void * buffer, const void *lowkey,
		const void *highkey, bool lowkeyinclusive, bool highkeyinclusive,
		const AttrType attribType, RID &rid, void *key) {

//    AttrType attribType;
//    attribType = attribute.type;

	int num_of_slots = 0;
	memcpy(&num_of_slots, (char *) buffer + NUM_OF_INDEX_BLOCK, 2);

	int buffer_offset = 0;

	//seek leftkey and exit based on highkey
	//need to have an exit condition when the highkey
	//while scanning if any rid is -1,u skip that record
	//exitConditions -
	//based on high key,if(highkey !=null && key>=highkey) retutn -1;
	//after iterating through all the nodes,check the leaf flag,if it's -1,again exit

	int breakFlag = false;
	for (int i = 0; i < num_of_slots; i++) {
		int char_len = 0;

		if (attribType != TypeVarChar) {
			char_len += 4;
			//-------addition----------
			int highKeyCheck = 0;
			if(highKey != NULL){
				memcpy(&highKeyCheck, (char *) buffer + buffer_offset, 4);
				if (((int *) highkey) != NULL
						&& highKeyCheck > (*(int *) highkey)) {
					return -1;
			}
				//-----------
			}

		} else {
			memcpy(&char_len, (char *) buffer + buffer_offset, 4); //for varchar
			char_len += 4;
			//----------addition-------
			//convert highkey to string
			if(NULL !=highKey){
				int high_key_check_len = 0;
				memcpy(&high_key_check_len, highKey, 4);
				char high_key_array[high_key_check_len];
				memcpy(&high_key_array, (char *) highKey + 4, high_key_check_len);
				string high_key_string(high_key_array, high_key_check_len);

				//convert the key from buffer to string
				int stringKeyLength = 0;
				memcpy(&stringKeyLength, (char *) buffer + buffer_offset, 4);
				char stringKeyArray[stringKeyLength];
				memcpy(stringKeyArray, (char *) buffer + buffer_offset + 4,
						stringKeyLength);
				string key_string(high_key_array, high_key_check_len);

				if (key_string > high_key_string) {
					return -1;
				}
			}
			//------------------------------
		}

		int pageNum = -1;
		memcpy(&pageNum, (char *) buffer + buffer_offset + char_len, 4);

		if (buffer_offset >= currentOffset && pageNum!=0) {
			if (NULL == lowkey && NULL ==highKey) {
				memcpy(key, (char *) buffer + buffer_offset, char_len); //for int and real
				memcpy(&rid, (char *) buffer + buffer_offset + char_len, 8);
				char_len += 8;
				buffer_offset += char_len;
				breakFlag = true;
				break;
			} else if (NULL == lowkey) {

				if (attribType == TypeInt) {
					int keyCheck = 0;
					memcpy(&keyCheck, (char *) buffer + buffer_offset,
							char_len); //for int and real

					if ((highkeyinclusive && (keyCheck <= *((int*) highkey)))
							|| (!highkeyinclusive
									&& keyCheck < *((int*) highkey))) {
						memcpy(key, &keyCheck, 4);  //for int and real
						memcpy(&rid, (char *) buffer + buffer_offset + char_len,
								8);
						char_len += 8;
						buffer_offset += char_len;
						breakFlag = true;
						break;
					}
				} else if (attribType == TypeReal) {
					float keyCheck = 0;
					memcpy(&keyCheck, (char *) buffer + buffer_offset,
							char_len);  //for int and real

					if ((highkeyinclusive && (keyCheck <= *((float*) highkey)))
							|| (!highkeyinclusive
									&& keyCheck < *((float*) highkey))) {
						memcpy(key, &keyCheck, 4);  //for int and real
						memcpy(&rid, (char *) buffer + buffer_offset + char_len,
								8);
						char_len += 8;
						buffer_offset += char_len;
						breakFlag = true;
						break;
					}
				} else {
					int len_of_string = 0;
					memcpy(&len_of_string, (char *) buffer + buffer_offset, 4);
					char str_key_array[len_of_string];
					memcpy(&str_key_array, (char *) buffer + buffer_offset + 4,
							len_of_string);
					string str1_key(str_key_array, len_of_string);

					int high_key_len = 0;
					memcpy(&high_key_len, highKey, 4);
					char high_key_array[high_key_len];
					memcpy(&high_key_array, (char *) highKey + 4, high_key_len);
					string high_key(high_key_array, high_key_len);

					if (((highkeyinclusive && (str1_key <= high_key)))
							|| (!highkeyinclusive && (str1_key < high_key))) {
						memcpy(&key, (char *) buffer + buffer_offset, char_len);
						memcpy(&rid, (char *) buffer + buffer_offset + char_len,
								8);
						char_len += 8;
						buffer_offset += char_len;
						breakFlag = true;
						break;
					}
				}

			} else if (NULL ==highKey) {
				if (attribType == TypeInt) {
					int keyCheck = 0;
					memcpy(&keyCheck, (char *) buffer + buffer_offset,
							char_len); //for int and real

					if ((lowkeyinclusive && (keyCheck >= *((int*) lowkey)))
							|| (!lowkeyinclusive && keyCheck > *((int*) lowkey))) {
						memcpy(key, &keyCheck, 4);  //for int and real
						memcpy(&rid, (char *) buffer + buffer_offset + char_len,
								8);
						char_len += 8;
						buffer_offset += char_len;
						breakFlag = true;
						break;
					}
				} else if (attribType == TypeReal) {
					float keyCheck = 0;
					memcpy(&keyCheck, (char *) buffer + buffer_offset,
							char_len); //for int and real

					if ((lowkeyinclusive && (keyCheck >= *((float*) lowkey)))
							|| (!lowkeyinclusive
									&& keyCheck > *((float*) lowkey))) {
						memcpy(key, &keyCheck, 4);  //for int and real
						memcpy(&rid, (char *) buffer + buffer_offset + char_len,
								8);
						char_len += 8;
						buffer_offset += char_len;
						breakFlag = true;
						break;
					}
				} else {
					int len_of_string = 0;
					memcpy(&len_of_string, (char *) buffer + buffer_offset, 4);
					char str_key_array[len_of_string];
					memcpy(&str_key_array, (char *) buffer + buffer_offset + 4,
							len_of_string);
					string str1_key(str_key_array, len_of_string);

					int low_key_len = 0;
					memcpy(&low_key_len, lowKey, 4);
					char low_key_array[low_key_len];
					memcpy(&low_key_array, (char *) lowKey + 4, low_key_len);
					string low_key(low_key_array, low_key_len);

					if (((lowkeyinclusive && (str1_key >= low_key)))
							|| (!lowkeyinclusive && str1_key > low_key)) {
						memcpy(key, (char *) buffer + buffer_offset, char_len); //len_of_string+4);  //for int and real
						memcpy(&rid, (char *) buffer + buffer_offset + char_len,
								8);
						char_len += 8;
						buffer_offset += char_len;
						breakFlag = true;
						break;
					}
				}
			} else {
				if (attribType == TypeInt) {
					int keyCheck = 0;
					memcpy(&keyCheck, (char *) buffer + buffer_offset,
							char_len); //for int and real

					if ((!highkeyinclusive && (keyCheck < *((int*) highkey))
							&& ((!lowkeyinclusive
									&& (keyCheck > *((int*) lowkey)))
									|| (lowkeyinclusive
											&& (keyCheck >= *((int*) lowkey)))))
							|| (highkeyinclusive
									&& (keyCheck <= *((int*) highkey))
									&& ((!lowkeyinclusive
											&& (keyCheck > *((int*) lowkey)))
											|| (lowkeyinclusive
													&& (keyCheck
															>= *((int*) lowkey)))))) {
						memcpy(key, &keyCheck, 4);  //for int and real
						memcpy(&rid, (char *) buffer + buffer_offset + char_len,
								8);
						char_len += 8;
						buffer_offset += char_len;
						breakFlag = true;
						break;
					}
				} else if (attribType == TypeReal) {
					float keyCheck = 0;
					memcpy(&keyCheck, (char *) buffer + buffer_offset,
							char_len); //for int and real

					if ((!highkeyinclusive && (keyCheck < *((float*) highkey))
							&& ((!lowkeyinclusive
									&& (keyCheck > *((float*) lowkey)))
									|| (lowkeyinclusive
											&& (keyCheck >= *((float *) lowkey)))))
							|| (highkeyinclusive
									&& (keyCheck <= *((float*) highkey))
									&& ((!lowkeyinclusive
											&& (keyCheck > *((float*) lowkey)))
											|| (lowkeyinclusive
													&& (keyCheck
															>= *((float*) lowkey)))))) {
						memcpy(key, &keyCheck, 4);  //for int and real
						memcpy(&rid, (char *) buffer + buffer_offset + char_len,
								8);
						char_len += 8;
						buffer_offset += char_len;
						breakFlag = true;
						break;
					}
				} else {
					int len_of_string = 0;
					memcpy(&len_of_string, (char *) buffer + buffer_offset, 4);
					char str_key_array[len_of_string];
					memcpy(&str_key_array, (char *) buffer + buffer_offset + 4,
							len_of_string);
					string str1_key(str_key_array, len_of_string);

					int low_key_len = 0;
					memcpy(&low_key_len, lowKey, 4);
					char low_key_array[low_key_len];
					memcpy(&low_key_array, (char *) lowKey + 4, low_key_len);
					string low_key(low_key_array, low_key_len);

					int high_key_len = 0;
					memcpy(&high_key_len, highKey, 4);
					char high_key_array[high_key_len];
					memcpy(&high_key_array, (char *) highKey + 4, high_key_len);
					string high_key(high_key_array, high_key_len);

					if ((!highkeyinclusive && (str1_key < high_key)
							&& ((!lowkeyinclusive && (str1_key > low_key))
									|| (lowkeyinclusive && (str1_key >= low_key))))
							|| (highkeyinclusive && (str1_key <= high_key)
									&& ((!lowkeyinclusive
											&& (str1_key > low_key))
											|| (lowkeyinclusive
													&& (str1_key >= low_key))))) {
						memcpy(key, (char *) buffer + buffer_offset, char_len);
						memcpy(&rid, (char *) buffer + buffer_offset + char_len,////test 13 fails here
								8);
						char_len += 8;
						buffer_offset += char_len;
						breakFlag = true;
						break;
					}
				}
			}

		}

		char_len += 8;
		buffer_offset += char_len;
	}

	if (breakFlag == false) {
		return 0;
	} else {
		currentOffset = buffer_offset;
		return 1;
	}

}

RC IX_ScanIterator::close() {
	free(insert_enrty_buffer);
	return 0;
}

IXFileHandle::IXFileHandle() {
	ixReadPageCounter = 0;
	ixWritePageCounter = 0;
	ixAppendPageCounter = 0;
}

RC IndexManager::printPage(IXFileHandle &ixfileHandle, int pageNum,
		const void *key) {
	void *buffer = calloc(PAGE_SIZE, 1);
	ixfileHandle.fileHandle.readPage(pageNum, buffer);
	int type = 0;
	short free_space_of_page;
	short num_of_slots;
	int char_len;
	readOverHeads(key, type, buffer, free_space_of_page, num_of_slots,
			char_len);

	cout << "root page entry" << endl;
	int buffer_offset = 0;
	for (int i = 0; i < (2 * num_of_slots) + 1; i++) {
		int output_key = 0;
		memcpy(&output_key, (char *) buffer + buffer_offset, 4);
		cout << output_key << "\t";
		buffer_offset += 4;
	}

	free(buffer);
	return 0;
}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount,
		unsigned &writePageCount, unsigned &appendPageCount) {
	fileHandle.collectCounterValues(readPageCount, writePageCount,
			appendPageCount);

	ixReadPageCounter = readPageCount;
	ixWritePageCounter = writePageCount;
	ixAppendPageCounter = appendPageCount;
	return 0;
}

