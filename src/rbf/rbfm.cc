#include "rbfm.h"

#include <math.h>
#include <stdlib.h>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdint.h>
#include<unistd.h>
#include<string.h>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

PagedFileManager* RecordBasedFileManager::_pf_manager =
		PagedFileManager::instance();

RecordBasedFileManager* RecordBasedFileManager::instance() {
	if (!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
}

RecordBasedFileManager::~RecordBasedFileManager() {
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	if (_pf_manager->createFile(fileName) != 0)
		return -1;
	return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	if (_pf_manager->destroyFile(fileName) != 0)
		return -1;
	return 0;
}

RC RecordBasedFileManager::openFile(const string &fileName,
		FileHandle &fileHandle) {
	if (_pf_manager->openFile(fileName, fileHandle) != 0)
		return -1;
	return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	if (_pf_manager->closeFile(fileHandle) != 0)
		return -1;
	return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data, RID &rid) { //only 1 record at a time

	bool append = true;
	int record_size = 0;
	char * buffer = (char *) calloc(PAGE_SIZE, sizeof(char)); //malloc(4096);
	formatRecord(recordDescriptor, data, buffer, record_size); //format the input data to record reqd.
	int check_page_num = fileHandle.getNumberOfPages();
	if (check_page_num == 0) {
		char *write_buffer = (char *) calloc(PAGE_SIZE, 1); //malloc(PAGE_SIZE);
		short num_of_slots = 1;
		short free_space_new_page = (short) (PAGE_SIZE - SIZE_OF_LAST_BLOCK
				- SIZE_OF_LAST_BLOCK - SIZE_OF_SLOT - record_size); //find the free space

		char *slot = (char *) calloc(2, 1); //malloc(2*sizeof(short));
		short slot_record_length = record_size;
		short slot_offset = 0; //*((short*)fileHandle.getPageFilePtr());//+record_size;

		memcpy(write_buffer, buffer, record_size); //write record_format_buffer data onto a new output write buffer

		memcpy(write_buffer + SEEK_FREE_SPACE, &free_space_new_page,
		SIZE_OF_LAST_BLOCK); //modify the data with new record + slot and F + N directory

		memcpy(write_buffer + SEEK_NUM_OF_RECORDS, &num_of_slots,
		SIZE_OF_LAST_BLOCK); //modify write buffer with N:no.of slots

		memcpy(write_buffer + SEEK_NUM_OF_RECORDS - SIZE_OF_LAST_BLOCK,
				&slot_offset, SIZE_OF_LAST_BLOCK); //slot offset put onto write buffer
		memcpy(write_buffer + SEEK_NUM_OF_RECORDS - SIZE_OF_SLOT,
				&slot_record_length, SIZE_OF_LAST_BLOCK); //slot record length put onto write buffer

		//write back and update page nums.
		fileHandle.appendPage(write_buffer); // append a new file and write the record into it , update the last free space , no. of records , and slot number->offset and length of the record
		append = false;
		rid.pageNum = 0;
		rid.slotNum = num_of_slots; //or 1 here
		free(slot);
		free(write_buffer);
	} else { //check for free space in the last page or traverse from first to figure any new space else append new
		int page_num = fileHandle.getNumberOfPages() - 1; //page no.s start from 0
		//to check if the last page has free space?
		short free_space_block = 0;
		char *read_Write_buffer = (char*) calloc(PAGE_SIZE, sizeof(char));//malloc(PAGE_SIZE);

		fileHandle.readPage(page_num, read_Write_buffer);//read into the buffer the page details
		memcpy(&free_space_block, read_Write_buffer + SEEK_FREE_SPACE,
		SIZE_OF_LAST_BLOCK);

		if (free_space_block > (record_size + SIZE_OF_SLOT)) {//if we have free space then update the read_Write_buffer buffer and write.
			append_recordPage(fileHandle, read_Write_buffer, page_num,
					record_size, free_space_block, rid, buffer);
			append = false;
		} else {//if free space in the last page is less than the record length then traverse through all the pages and check for free space: if any write else append new
			for (int i = 0; i < page_num - 1; i++) {
				fileHandle.readPage(i, read_Write_buffer);//read into the buffer the page details
				memcpy(&free_space_block, read_Write_buffer + SEEK_FREE_SPACE,
				SIZE_OF_LAST_BLOCK);

				if (free_space_block >= (record_size + SIZE_OF_SLOT)) {
					int num_page = i;
					append_recordPage(fileHandle, read_Write_buffer, num_page,
							record_size, free_space_block, rid, buffer);
					append = false;
					break;
				}
			}
		}
		if (append) {
			short num_of_slots = 1;
			short free_space_new_page = (short) (PAGE_SIZE - SIZE_OF_LAST_BLOCK
					- SIZE_OF_LAST_BLOCK - SIZE_OF_SLOT - record_size);	//find the free space
			short slot_record_length = record_size;
			short slot_offset = 0;//*((short*)fileHandle.getPageFilePtr());//+record_size;
			memcpy(read_Write_buffer, buffer, record_size * sizeof(char));//write record data(formatted) into a new out buffer

			memcpy(read_Write_buffer + SEEK_FREE_SPACE, &free_space_new_page,
			SIZE_OF_LAST_BLOCK);//modify the data with new record + slot and F + N directory

			memcpy(read_Write_buffer + SEEK_NUM_OF_RECORDS, &num_of_slots,
			SIZE_OF_LAST_BLOCK);	//modify write buffer with N:no.of slots

			memcpy(read_Write_buffer + SEEK_NUM_OF_RECORDS - SIZE_OF_LAST_BLOCK,
					&slot_offset, SIZE_OF_LAST_BLOCK);//slot offset put onto write buffer

			memcpy(read_Write_buffer + SEEK_NUM_OF_RECORDS - SIZE_OF_SLOT,
					&slot_record_length, SIZE_OF_LAST_BLOCK);//slot record put onto write buffer

			//write back and update page nums.
			fileHandle.appendPage(read_Write_buffer); // append a new file and write the record into it , update the last free space , no. of records , and slot number->offset and length of the record

			rid.pageNum = page_num + 1;//fileHandle.getNumberOfPages();
			rid.slotNum = num_of_slots; //or 1 here
		}
		free(read_Write_buffer);
	}
//	printRecord(recordDescriptor, data); //proj 2 test
//	cout << "inserted record---------^" << endl << endl;
	free(buffer);
	return 0;
}

RC RecordBasedFileManager::append_recordPage(FileHandle &fileHandle,
		char *read_Write_buffer, int &page_num, int &record_size,
		short free_space_block, RID &rid, char *buffer) {

	short greatest_slot_offset = -1, greatest_slot_offset_len = 0;
	int deleted_slot_ref = 0;
	free_space_block = free_space_block - record_size - SIZE_OF_SLOT; //free space reduced by record size plus slot directory size of record

	short num_of_slots;
	memcpy(read_Write_buffer + SEEK_FREE_SPACE, &free_space_block,
	SIZE_OF_LAST_BLOCK); //write the updated free space in to the write buffer
	memcpy(&num_of_slots, read_Write_buffer + SEEK_NUM_OF_RECORDS,
	SIZE_OF_LAST_BLOCK); //read num of lots from read page

	short slot_record_length = 0;
	short slot_offset = 0;

	//project two changes to accomodate reuse on deleted slot rec's in slot dir
	for (int j = 0; j < num_of_slots; j++) {
		memcpy(&slot_offset,
				read_Write_buffer + SEEK_NUM_OF_RECORDS
						- ((j + 1) * SIZE_OF_SLOT) + SIZE_OF_LAST_BLOCK,
				SIZE_OF_LAST_BLOCK); //to read the offset of the last record slot directory
		memcpy(&slot_record_length,
				read_Write_buffer + SEEK_NUM_OF_RECORDS
						- ((j + 1) * SIZE_OF_SLOT), SIZE_OF_LAST_BLOCK); //to read the record length of the last record slot directory
		if (slot_record_length == -1) {
			slot_record_length = 8; //2*sizeof(int) => pointer RID
		} else if (slot_record_length == 0) {
			deleted_slot_ref = j + 1; //counter to keep the index of the last occurring delete slot
		}
		if (slot_offset >= greatest_slot_offset) { //to keep track of the largest offset and its corresponding rec len
			greatest_slot_offset = slot_offset;
			greatest_slot_offset_len = slot_record_length; //(slot_record_length ==-1) ? 8:(slot_record_length == 0)?0:slot_record_length;
		}
	}
	//update the slot offset value but just insert the value at the i+1 index ref only
	//the new slot created should have the greatest slot offset & length
	//---
	if (slot_record_length == -1) {
		slot_record_length = 8;
	}

	slot_offset = greatest_slot_offset + greatest_slot_offset_len;

	slot_record_length = record_size;

	memcpy(read_Write_buffer + slot_offset, buffer, slot_record_length); //write the record formatted data into the out buffer

	if (deleted_slot_ref == 0) {
		num_of_slots += 1;
		memcpy(read_Write_buffer + SEEK_NUM_OF_RECORDS, &num_of_slots,
		SIZE_OF_LAST_BLOCK); //write the updated num of slots to write buffer
		memcpy(
				read_Write_buffer + SEEK_NUM_OF_RECORDS
						- (num_of_slots * SIZE_OF_SLOT) + SIZE_OF_LAST_BLOCK,
				&slot_offset, SIZE_OF_LAST_BLOCK); //write the new slot dir with the offset address

		memcpy(
				read_Write_buffer + SEEK_NUM_OF_RECORDS
						- (num_of_slots * SIZE_OF_SLOT), &slot_record_length,
				SIZE_OF_LAST_BLOCK); //write new slot dir with the rec len
	} else {
		memcpy(
				read_Write_buffer + SEEK_NUM_OF_RECORDS
						- (deleted_slot_ref * SIZE_OF_SLOT) + SIZE_OF_LAST_BLOCK,
				&slot_offset, SIZE_OF_LAST_BLOCK); //write the existing slot dir with the offset address

		memcpy(
				read_Write_buffer + SEEK_NUM_OF_RECORDS
						- (deleted_slot_ref * SIZE_OF_SLOT),
				&slot_record_length, SIZE_OF_LAST_BLOCK); //write existing slot dir with the rec len
	}

	fileHandle.writePage(page_num, read_Write_buffer);
	rid.pageNum = page_num;
	rid.slotNum = deleted_slot_ref ? deleted_slot_ref : num_of_slots;//either the num of slots or the updated slot //since latest updated no_of_slots indicates the slot num as well
	return 0;
}

RC RecordBasedFileManager::formatRecord(
		const vector<Attribute> &recordDescriptor, const void* data,
		char *buffer, int &record_size) {

	int fieldCount = recordDescriptor.size();
	int inputNullBytes = ceil((double) fieldCount / 8); //calculate the no. of null bytes

	memcpy(buffer, &fieldCount, sizeof(int)); //to update the first byte of formatted record by no. of fields 1>

	char* null_bytes = (char*) calloc(inputNullBytes, sizeof(char)); //malloc(inputNullBytes);
	memcpy(null_bytes, data, inputNullBytes);

	memcpy(buffer + sizeof(int), data, inputNullBytes * sizeof(char)); //null bytes appended to record format 2>

	//to calculate no. of non-null bits to get so many no. of offset address bytes allocated
	int field_addresses_count = 0;
	for (int i = 0; i < fieldCount; i++) {
		int num_of_null_bit = i % 8;
		int num_of_null_bytes = i / 8;
		if (null_bytes[num_of_null_bytes] & (1 << (7 - num_of_null_bit))) {
			continue;
		} else {
			field_addresses_count++;
		}
	}
	int field_address_byte_offset = 1 * sizeof(int)
			+ inputNullBytes * sizeof(char);

	int buffer_offset = (1 + field_addresses_count) * sizeof(int)
			+ inputNullBytes * sizeof(char); //offset for buffer 1(no.f field)+(null bytes)+(fields offset address bytes)
	int input_stream_offset = inputNullBytes * sizeof(char); //offset for void *data (i/p): initialized to no. of byte for the null bytes
	for (int i = 0; i < fieldCount; i++) {
		int num_of_null_bit = i % 8;
		int num_of_null_bytes = i / 8;
		if (null_bytes[num_of_null_bytes] & (1 << (7 - num_of_null_bit))) {
			continue;	//to escape nulls bits : no-op here
		} else {
			switch (recordDescriptor[i].type) {
			case TypeInt: {
				memcpy(buffer + buffer_offset,
						(char *) data + input_stream_offset, sizeof(int));//load in the field values
				buffer_offset += sizeof(int);//update the buffer offset by 4bytes
				input_stream_offset += sizeof(int);	//update the i/p data offset by 4bytes
				memcpy(buffer + field_address_byte_offset, &buffer_offset,
						sizeof(int));	//update field address
				field_address_byte_offset += sizeof(int);//update field address byte offset
				break;
			}
			case TypeReal: {
				memcpy(buffer + buffer_offset,
						(char *) data + input_stream_offset, sizeof(float));
				buffer_offset += sizeof(float);
				input_stream_offset += sizeof(float);
				memcpy(buffer + field_address_byte_offset, &buffer_offset,
						sizeof(int));	//update field address
				field_address_byte_offset += sizeof(int);
				break;
			}
			case TypeVarChar: {	//check first byte to retrieve the length of the character string
				int len_of_var_char = 0;
				memcpy(&len_of_var_char, (char *) data + input_stream_offset,
						4);
				input_stream_offset += sizeof(int);
				memcpy(buffer + buffer_offset,
						(char *) data + input_stream_offset, len_of_var_char);
				buffer_offset += len_of_var_char;
				input_stream_offset += len_of_var_char;

				memcpy(buffer + field_address_byte_offset, &buffer_offset,
						sizeof(int));	//update field address
				field_address_byte_offset += sizeof(int);
				break;
			}
			}
		}
	}
	record_size = buffer_offset;
	free(null_bytes);

	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

	bool break_flag = false;
	int rid_page_num = rid.pageNum, rid_slot_num = rid.slotNum;

	char *output_read_buffer = (char *) calloc(PAGE_SIZE, sizeof(char));// malloc(PAGE_SIZE);buffer to load the page to be read

	short slot_rec_len = 0, slot_rec_offset = 0;
	do {
		fileHandle.readPage(rid_page_num, output_read_buffer);

		memcpy(&slot_rec_offset,
				output_read_buffer + SEEK_NUM_OF_RECORDS
						- (rid_slot_num * SIZE_OF_SLOT) + SIZE_OF_LAST_BLOCK,
				SIZE_OF_LAST_BLOCK);//copy the value of record start offset from slot dir
		memcpy(&slot_rec_len,
				output_read_buffer + SEEK_NUM_OF_RECORDS
						- (rid_slot_num * SIZE_OF_SLOT), SIZE_OF_LAST_BLOCK);//copy the old/existing record length from slot dir

		if (slot_rec_len == -1) {
			memcpy(&rid_page_num, output_read_buffer + slot_rec_offset,
					sizeof(int));//update with the pointer to new rid page_num
			memcpy(&rid_slot_num,
					output_read_buffer + slot_rec_offset + sizeof(int),
					sizeof(int));	//update with the pointer to rid slot_num
		} else if (slot_rec_len == 0) {
			break_flag = true;
			break;
		} else {
			break;	//break loop if the slot length is not -1
		}
	} while (slot_rec_len < 0);

	if (break_flag) {
		free(output_read_buffer);
		return -1;
	}

	//-----------------
	char *record_buffer = (char *) calloc(slot_rec_len, sizeof(char));// malloc(PAGE_SIZE);//allocate memory to read record from page
	memcpy(record_buffer, output_read_buffer + slot_rec_offset, slot_rec_len);//copy the record from the page buffer

	int field_count;
	memcpy(&field_count, record_buffer, sizeof(int));//copy the first byte representing the field size

	int null_byte_count = ceil((double) field_count / 8);//calculate the no. of null bytes
	memcpy(data, record_buffer + sizeof(int), null_byte_count);	//load the null byte onto the output data

	char *read_null_byte = (char*) malloc(null_byte_count);
	memcpy(read_null_byte, record_buffer + sizeof(int), null_byte_count);

	int non_null_bit_count = 0;	//to count the null bits
	for (int i = 0; i < field_count; i++) {
		int num_of_null_bit = i % 8;
		int num_of_null_bytes = i / 8;
		if (read_null_byte[num_of_null_bytes] & (1 << (7 - num_of_null_bit))) {
			continue;	//to escape nulls bits : no-op here
		} else {
			non_null_bit_count++;
		}
	}

	int left_buffer_offset = sizeof(int) + null_byte_count;	//to move the offset to point to the start of the field offset address
	int right_buffer_offset = sizeof(int) + null_byte_count
			+ (non_null_bit_count * sizeof(int));//to move the offset to point to the start of the fields
	int field_end_offset;
	int out_data_offset = null_byte_count;//offset to keep track of output data stream

	for (unsigned int i = 0; i < (unsigned) field_count; i++) {
		int num_of_null_bit = i % 8;
		int num_of_null_bytes = i / 8;
		if (read_null_byte[num_of_null_bytes] & (1 << (7 - num_of_null_bit))) {
			continue;	//to escape nulls bits : no-op here
		} else {
			memcpy(&field_end_offset, record_buffer + left_buffer_offset,
					sizeof(int));//load the offset address onto the into variable
			left_buffer_offset += sizeof(int);//move offset to next byte(point to next address block)

			switch (recordDescriptor[i].type) {
			case TypeInt: {
				memcpy((char *) data + out_data_offset,
						record_buffer + right_buffer_offset, sizeof(int));//copy the int 4bytes into the data
				right_buffer_offset += sizeof(int);
				out_data_offset += sizeof(int);
				break;
			}
			case TypeReal: {
				memcpy((char *) data + out_data_offset,
						record_buffer + right_buffer_offset, sizeof(float));//copy the real 4bytes into the data
				right_buffer_offset += sizeof(float);
				out_data_offset += sizeof(float);
				break;
			}
			case TypeVarChar: {
				int field_length = field_end_offset - right_buffer_offset;
				memcpy((char *) data + out_data_offset, &field_length,
						sizeof(int));//4 bytes represent of no. of characters on data
				out_data_offset += sizeof(int);

				memcpy((char *) data + out_data_offset,
						record_buffer + right_buffer_offset,
						field_length * sizeof(char));//copy the variable length characters onto the data stream field
				right_buffer_offset += field_length;
				out_data_offset += field_length;
				break;
			}
			}
		}
	}
	free(read_null_byte);
	free(record_buffer);
	free(output_read_buffer);
	return 0;
}

RC RecordBasedFileManager::printRecord(
		const vector<Attribute> &recordDescriptor, const void *data) {
	int offset = 0;
	int recordFieldSize = recordDescriptor.size();
	int inputNullBytes = ceil((double) recordFieldSize / 8);//calculate the no. of null bytes
	char* nullBytes = (char *) calloc(inputNullBytes, sizeof(char));//malloc(inputNullBytes);
	memcpy(nullBytes, data, inputNullBytes);
	offset += inputNullBytes;
	for (int i = 0; i < recordFieldSize; i++) {
		int num_of_null_bit = i % 8;
		int num_of_null_bytes = i / 8;
		if (nullBytes[num_of_null_bytes] & (1 << (7 - num_of_null_bit))) {
			cout << recordDescriptor[i].name << ":" << "NULL" << endl;
		} else {
			switch (recordDescriptor[i].type) {
			case TypeInt: {
				int int_attr;
				memcpy(&int_attr, (char *) data + offset, sizeof(int));
				offset += sizeof(int);
				cout << recordDescriptor[i].name << ":" << int_attr << endl;
				break;
			}
			case TypeReal: {
				float real_attr;
				memcpy(&real_attr, (char *) data + offset, sizeof(float));
				offset += sizeof(float);
				cout << recordDescriptor[i].name << ":" << real_attr << endl;
				break;
			}
			case TypeVarChar: {
				int attr_len;
				memcpy(&attr_len, (char *) data + offset, sizeof(int));
				offset += sizeof(int);
				char* char_attr = (char*) calloc(attr_len, sizeof(char));
				memcpy(char_attr, (char *) data + offset, attr_len);
				offset += attr_len;
				cout << recordDescriptor[i].name << ":" << char_attr << endl;
				free(char_attr);
				break;
			}
			}
		}
	}
	cout << endl;
	free(nullBytes);
	return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data,
		const RID &rid) {

	RID newRid;
	int rid_page_num = rid.pageNum, rid_slot_num = rid.slotNum;	//i/p said record to be updated
	int new_record_size = 0;	//gives the new/updated record length

	char *new_record_buffer = (char *) calloc(PAGE_SIZE, sizeof(char));	//malloc(4096);
	char *page_buffer = (char *) calloc(PAGE_SIZE, sizeof(char));// malloc(PAGE_SIZE);buffer to load the page to be read
	short slot_rec_len = 0, slot_rec_offset = 0;
	do {
		fileHandle.readPage(rid_page_num, page_buffer);
		memcpy(&slot_rec_offset,
				page_buffer + SEEK_NUM_OF_RECORDS
						- (rid_slot_num * SIZE_OF_SLOT) + SIZE_OF_LAST_BLOCK,
				SIZE_OF_LAST_BLOCK);//copy the value of record start offset from slot dir
		memcpy(&slot_rec_len,
				page_buffer + SEEK_NUM_OF_RECORDS
						- (rid_slot_num * SIZE_OF_SLOT), SIZE_OF_LAST_BLOCK);//copy the old/existing record length from slot dir
		if (slot_rec_len == -1) {
			memcpy(&rid_page_num, page_buffer + slot_rec_offset, sizeof(int));//update with the pointer to new rid page_num
			memcpy(&rid_slot_num, page_buffer + slot_rec_offset + sizeof(int),
					sizeof(int));	//update with the pointer to rid slot_num
		} else {
			break;	//break loop if the slot length is not -1
		}
	} while (slot_rec_len < 0);

	formatRecord(recordDescriptor, data, new_record_buffer, new_record_size); //format the updated input data to reqd. record format ready to be inserted/updated

	short num_of_slots;	//num of slots on the page.
	memcpy(&num_of_slots, page_buffer + SEEK_NUM_OF_RECORDS,
	SIZE_OF_LAST_BLOCK);	//copy num of slots value from page buffer

	//could make code generic by having num_of_recs here-----to do----
	if (slot_rec_len == new_record_size) { // updated and existing record len are EQ
		memcpy(page_buffer + slot_rec_offset, new_record_buffer,
				new_record_size); //update the page buffer with newly formatted record
	} else if (slot_rec_len > new_record_size) { // updated new rec len less than existing rec len
		int diff_len = slot_rec_len - new_record_size;

		//now shift all the next records on page to left and update slot offsets for each of slot offsets greater than current slot

		//to calculate how much to move
		int move_len = PAGE_SIZE - (slot_rec_offset + slot_rec_len)
				- ((num_of_slots + 1) * SIZE_OF_SLOT); //length of buffer to be shifted left
		memmove(page_buffer + slot_rec_offset + new_record_size,
				page_buffer + slot_rec_offset + slot_rec_len, move_len); //shift the remaining part of page buffer to left

		memcpy(
				page_buffer + SEEK_NUM_OF_RECORDS
						- (rid_slot_num * SIZE_OF_SLOT), &new_record_size, //&slot_rec_len
				SIZE_OF_LAST_BLOCK);	//update slot len with new rec len

		//similarly update all other slot offsets (only) in the dir
		for (int i = 0; i < (int) num_of_slots; i++) {
			short slot_offset;
			memcpy(&slot_offset,
					page_buffer + SEEK_NUM_OF_RECORDS - (i * SIZE_OF_SLOT)
							- SIZE_OF_LAST_BLOCK, SIZE_OF_LAST_BLOCK);//copy the record start offset from slot dir
			if (slot_offset > slot_rec_offset) {
				slot_offset -= diff_len;
				memcpy(
						page_buffer + SEEK_NUM_OF_RECORDS - (i * SIZE_OF_SLOT)
								- SIZE_OF_LAST_BLOCK, &slot_offset,
						SIZE_OF_LAST_BLOCK);//update the slot offset with its new value (old value -diff in rec len)
			}
		}

		//should this be after slots update or before?-------to-do*******************
		memcpy(page_buffer + slot_rec_offset, new_record_buffer,
				new_record_size); //update the page buffer with newly formatted record

		short free_space_val;
		memcpy(&free_space_val,
				page_buffer + SEEK_NUM_OF_RECORDS + SIZE_OF_LAST_BLOCK,
				SIZE_OF_LAST_BLOCK);		//copy old free space value
		free_space_val += diff_len;
		memcpy(page_buffer + SEEK_NUM_OF_RECORDS + SIZE_OF_LAST_BLOCK,
				&free_space_val, SIZE_OF_LAST_BLOCK);//update free space with new value
	} else {		//when slot_rec_len < new_record_size
					//check if the existing page has space to accommodate new len
		short free_space_val;
		memcpy(&free_space_val,
				page_buffer + SEEK_NUM_OF_RECORDS + SIZE_OF_LAST_BLOCK,
				SIZE_OF_LAST_BLOCK);		//copy old free space value
		int diff_len = new_record_size - slot_rec_len;
		if (free_space_val - diff_len >= 0) {//there is space to accommodate new rec with greater len
			if (rid_slot_num == num_of_slots) {	//to check if the updating rec is the last rec
				memcpy(page_buffer + slot_rec_offset, new_record_buffer,
						new_record_size); //update the page buffer with newly formatted record
				memcpy(
						page_buffer + SEEK_NUM_OF_RECORDS
								- (rid_slot_num * SIZE_OF_SLOT),
						&new_record_size, SIZE_OF_LAST_BLOCK); //update the last slot address with the new rec len

			} else {
				//to find move len for shift right opertn
				short last_slot_offset = 0, last_slot_len = 0;
				memcpy(&last_slot_offset,
						page_buffer + SEEK_NUM_OF_RECORDS
								- (num_of_slots * SIZE_OF_SLOT)+ SIZE_OF_LAST_BLOCK,
						SIZE_OF_LAST_BLOCK);
				memcpy(&last_slot_len,
						page_buffer + SEEK_NUM_OF_RECORDS
								- (num_of_slots * SIZE_OF_SLOT),
						SIZE_OF_LAST_BLOCK);
				if (last_slot_len == -1) {
					last_slot_len = 2 * sizeof(int);
				}
				int move_len = (last_slot_offset + last_slot_len)
						- (slot_rec_offset + slot_rec_len); //new_record_size
				memmove(page_buffer + slot_rec_offset + new_record_size,
						page_buffer + slot_rec_offset + slot_rec_len, move_len); //shift right the block starting next record to end to records on page
				memcpy(
						page_buffer + SEEK_NUM_OF_RECORDS
								- (rid_slot_num * SIZE_OF_SLOT),
						&new_record_size, //&slot_rec_len
						SIZE_OF_LAST_BLOCK); //update slot len with new rec len

				//similarly update all other slot offsets (only) in the dir
				for (int i = 0; i < (int) num_of_slots; i++) {
					short slot_offset;
					memcpy(&slot_offset,
							page_buffer + SEEK_NUM_OF_RECORDS
									- (i * SIZE_OF_SLOT) - SIZE_OF_LAST_BLOCK,
							SIZE_OF_LAST_BLOCK); //copy the record start offset from slot dir of next slot
					if (slot_offset > slot_rec_offset) {
						slot_offset += diff_len;
						memcpy(
								page_buffer + SEEK_NUM_OF_RECORDS
										- (i * SIZE_OF_SLOT)
										- SIZE_OF_LAST_BLOCK, &slot_offset,
								SIZE_OF_LAST_BLOCK); //update the slot offset for next slot with its new value (old value -diff in rec len)
					}
				}
			}
			memcpy(page_buffer + slot_rec_offset, new_record_buffer,
					new_record_size); //update the page buffer with newly formatted record
			free_space_val -= diff_len;
			memcpy(page_buffer + SEEK_NUM_OF_RECORDS + SIZE_OF_LAST_BLOCK,
					&free_space_val, SIZE_OF_LAST_BLOCK); //update free space with new value
		} else {
			insertRecord(fileHandle, recordDescriptor, data, newRid);
			//do shift operation if old/existing record length is greater that 8
			short diff_len = slot_rec_len - 8;
			if (diff_len > 0) { //do shift left operation
				short last_slot_offset = 0, last_slot_len = 0;
				memcpy(&last_slot_offset,
						page_buffer + SEEK_NUM_OF_RECORDS
								- (num_of_slots * SIZE_OF_SLOT)+ SIZE_OF_LAST_BLOCK,
						SIZE_OF_LAST_BLOCK);
				memcpy(&last_slot_len,
						page_buffer + SEEK_NUM_OF_RECORDS
								- (num_of_slots * SIZE_OF_SLOT),
						SIZE_OF_LAST_BLOCK);
				if (last_slot_len == -1) {
					last_slot_len = 2 * sizeof(int);
				}
				int move_len = PAGE_SIZE - (slot_rec_offset + slot_rec_len)
						- ((num_of_slots + 1) * SIZE_OF_SLOT); //length of bytes to be moved left //(last_slot_offset + last_slot_len) - (slot_rec_offset + slot_rec_len);
				memmove(page_buffer + slot_rec_offset + 2 * sizeof(int),
						page_buffer + slot_rec_offset + slot_rec_len, move_len);
				//similarly update all other slot offsets (only) in the dir
				for (int i = 0; i < (int) num_of_slots; i++) {
					short slot_offset;
					memcpy(&slot_offset,
							page_buffer + SEEK_NUM_OF_RECORDS
									- (i * SIZE_OF_SLOT) - SIZE_OF_LAST_BLOCK,
							SIZE_OF_LAST_BLOCK); //copy the record start offset from slot dir
					if (slot_offset > slot_rec_offset) {
						slot_offset -= diff_len;
						memcpy(
								page_buffer + SEEK_NUM_OF_RECORDS
										- (i * SIZE_OF_SLOT)
										- SIZE_OF_LAST_BLOCK, &slot_offset,
								SIZE_OF_LAST_BLOCK); //update the slot offset with its new value (old value -diff in rec len)
					}

				}
				short free_space_val;
				memcpy(&free_space_val,
						page_buffer + SEEK_NUM_OF_RECORDS + SIZE_OF_LAST_BLOCK,
						SIZE_OF_LAST_BLOCK);		//copy old free space value
				free_space_val += diff_len;
				memcpy(page_buffer + SEEK_NUM_OF_RECORDS + SIZE_OF_LAST_BLOCK,
						&free_space_val, SIZE_OF_LAST_BLOCK);//update free space with new value
			}
			int pointerSlotLen = -1;
			memcpy(
					page_buffer + SEEK_NUM_OF_RECORDS
							- (rid_slot_num * SIZE_OF_SLOT), &pointerSlotLen,
					SIZE_OF_LAST_BLOCK); //update slot len with new rec len
			memcpy(page_buffer + slot_rec_offset, &newRid.pageNum, sizeof(int)); //update the new pageNum
			memcpy(page_buffer + slot_rec_offset + sizeof(int), &newRid.slotNum,
					sizeof(int)); //update the new slotNum
		}
	}
	fileHandle.writePage(rid_page_num, page_buffer); //write back to the page on the file

	free(new_record_buffer);
	free(page_buffer);
	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid) {
	//go to the specific page no and slot id
	//calculate the initial and fin

	void * buffer_delete = calloc(PAGE_SIZE, sizeof(char));
	bool break_flag = false;
	short start_point2 = 0;
	short length2 = 0;
	short num_of_slots = 0;
	short move_length = 0;
	short updated_length = 0;
	short slot_offset = 0;
	short freeSpaceoffset = 0;
	short slot_len = 0;
	short slot_rec_len = 0, slot_rec_offset = 0;
	short rid_pagenum = rid.pageNum;
	short rid_slotnum = rid.slotNum;

	do {
		fileHandle.readPage(rid_pagenum, buffer_delete);
		memcpy(&slot_rec_offset,
				(char *) buffer_delete + SEEK_NUM_OF_RECORDS
						- (rid_slotnum * SIZE_OF_SLOT) + SIZE_OF_LAST_BLOCK,
				SIZE_OF_LAST_BLOCK);//copy the value of record start offset from slot dir

		memcpy(&slot_rec_len,
				(char *) buffer_delete + SEEK_NUM_OF_RECORDS
						- (rid_slotnum * SIZE_OF_SLOT), SIZE_OF_LAST_BLOCK);//copy the old/existing record length from slot directory

		if (slot_rec_len == -1) {

			int pointerSlotLen = 0;
			memcpy(
					(char*) buffer_delete + SEEK_NUM_OF_RECORDS
							- (rid_slotnum * SIZE_OF_SLOT), &pointerSlotLen,
					SIZE_OF_LAST_BLOCK);	//mark the record slot length as 0

			memcpy(&rid_pagenum, (char *) buffer_delete + slot_rec_offset,
					sizeof(int));//update with the pointer to new rid page_num
			memcpy(&rid_slotnum,
					(char *) buffer_delete + slot_rec_offset + sizeof(int),
					sizeof(int));	//update with the pointer to rid slot_num
		} else if (slot_rec_len == 0) {
//			cout << "Delete Operation failed while deleting already deleted record" << endl;
			break_flag = true;
			break;
		} else {
			break;	//break loop if the slot length is not -1
		}
	} while (slot_rec_len < 0);

	if (break_flag) {
		free(buffer_delete);
		return -1;
	}
	//perform delete operation to delete record

	memcpy(&num_of_slots, (char *) buffer_delete + SEEK_NUM_OF_RECORDS, 2); //no of slots=last slot
//	cout << "num_of_slots :" << num_of_slots << endl;
	memcpy(&start_point2, (char *) buffer_delete + (4094 - (num_of_slots * 4)),
			2); //last record - initial position
	memcpy(&length2,
			(char *) buffer_delete + SEEK_NUM_OF_RECORDS - (num_of_slots * 4),
			2); //last record - length
	if (length2 == -1) {
		length2 = 2 * sizeof(int);
	}
	move_length = (start_point2 + length2) - (slot_rec_offset + slot_rec_len);
	memmove((char *) buffer_delete + slot_rec_offset,
			(char *) buffer_delete + slot_rec_offset + slot_rec_len,
			move_length); //memmove(destination,source,size)

	memcpy((char *) buffer_delete + SEEK_NUM_OF_RECORDS - (rid_slotnum * 4),
			&updated_length, 2); //4096 - (rid.slotNum * 4) - 2 //delete record's length set to 0

	//update the initial positions of slots in slot directory
	for (int i = 0; i <= num_of_slots; i++) {
		//check to see if length of any slot is 0
		memcpy(&slot_offset,
				(char *) buffer_delete + SEEK_NUM_OF_RECORDS - ((i + 1) * 4)
						+ 2, 2);
		memcpy(&slot_len,
				(char *) buffer_delete + SEEK_NUM_OF_RECORDS - ((i + 1) * 4),
				2);
		if (slot_offset > slot_rec_offset) {
			slot_offset -= slot_rec_len;
			memcpy(
					(char *) buffer_delete + SEEK_NUM_OF_RECORDS - ((i + 1) * 4)
							+ 2, &slot_offset, 2);
		}
	}

	//update the free space offset
	memcpy(&freeSpaceoffset, (char *) buffer_delete + SEEK_NUM_OF_RECORDS, 2);
	freeSpaceoffset += slot_rec_len; // increment the free space offset by deleted record size

	memcpy((uint8_t*) buffer_delete + SEEK_NUM_OF_RECORDS + SIZE_OF_LAST_BLOCK,
			&freeSpaceoffset, 2);	// Update the free space offset at 4094

	fileHandle.writePage(rid_pagenum, buffer_delete); //write back to the page on the file where updated or existing record was deleted

	free(buffer_delete);
	return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid,
		const string &attributeName, void *data) {

	bool break_flag = false;
	int rid_page_num = rid.pageNum, rid_slot_num = rid.slotNum;

	//buffer to load the page to be read
	char *output_read_buffer = (char *) calloc(PAGE_SIZE, sizeof(char));

	//check for updated(rid present in records place) records as well
	short slot_rec_len = 0, slot_rec_offset = 0;

	do {
		fileHandle.readPage(rid_page_num, output_read_buffer);

		memcpy(&slot_rec_offset,
				output_read_buffer + SEEK_NUM_OF_RECORDS
						- (rid_slot_num * SIZE_OF_SLOT) + SIZE_OF_LAST_BLOCK,
				SIZE_OF_LAST_BLOCK); //copy the value of record start offset from slot dir

		memcpy(&slot_rec_len,
				output_read_buffer + SEEK_NUM_OF_RECORDS
						- (rid_slot_num * SIZE_OF_SLOT), SIZE_OF_LAST_BLOCK); //copy the old/existing record length from slot dir

		if (slot_rec_len == -1) {
			memcpy(&rid_page_num, output_read_buffer + slot_rec_offset,
					sizeof(int)); //update with the pointer to new rid page_num
			memcpy(&rid_slot_num,
					output_read_buffer + slot_rec_offset + sizeof(int),
					sizeof(int));	//update with the pointer to rid slot_num
		} else if (slot_rec_len == 0) {
			//break if trying to read a deleted record
			break_flag = true;
			break;
		} else {
			//break loop if the slot length is not -1
			break;
		}
	} while (slot_rec_len < 0);

	if (break_flag) {
		free(output_read_buffer);
		return -1;
	}

	//allocate memory and read record from page
	char *record_buffer = (char *) calloc(slot_rec_len,1);
	memcpy(record_buffer, output_read_buffer + slot_rec_offset, slot_rec_len);

	//copy the first byte representing the field size
	int field_count;
	memcpy(&field_count, record_buffer, sizeof(int));

	//calculate the no. of null bytes
	int null_byte_count = ceil((double) field_count / 8);

	//to set all bits in the o/p null byte (only one byte) to zeros
	char setZero =0;
	memcpy(data, &setZero, 1);

	char *read_null_byte = (char*) malloc(null_byte_count);
	memcpy(read_null_byte, record_buffer + sizeof(int), null_byte_count);

	int non_null_bit_count = 0;	//to count the null bits
	for (int i = 0; i < field_count; i++) {
		int num_of_null_bit = i % 8;
		int num_of_null_bytes = i / 8;
		if (read_null_byte[num_of_null_bytes] & (1 << (7 - num_of_null_bit))) {
			continue;	//to escape nulls bits : no-op here
		} else {
			non_null_bit_count++;	//count of non null bits
		}
	}

	//to move the offset to point to the start of the field offset address (that points to the end of each field)
	int left_buffer_offset = sizeof(int) + null_byte_count;

	//to move the offset to point to the start of the fields
	int right_buffer_offset = sizeof(int) + null_byte_count
			+ (non_null_bit_count * sizeof(int));

	int field_end_offset;

	//offset to keep track of output data stream
	int out_data_offset =1;

	for (int i = 0; i < field_count; i++) {
		int num_of_null_bit = i % 8;
		int num_of_null_bytes = i / 8;
		if (read_null_byte[num_of_null_bytes] & (1 << (7 - num_of_null_bit))) {
			if (recordDescriptor[i].name == attributeName) {
				//to set the first bit of the null byte to 1
				char n = 0;
				n |= (1 << 7);
				memcpy(data, &n, 1);
				break;
			}
			continue;
		} else {
			//load the offset address onto the into variable
			memcpy(&field_end_offset, record_buffer + left_buffer_offset,
					sizeof(int));

			//move offset to next byte(point to next address block)
			left_buffer_offset += sizeof(int);

			switch (recordDescriptor[i].type) {
			case TypeInt: {
				if (recordDescriptor[i].name == attributeName) {
					memcpy((char *) data + out_data_offset,
							record_buffer + right_buffer_offset, sizeof(int));

					free(read_null_byte);
					free(record_buffer);
					free(output_read_buffer);
					return 0;
				}
				right_buffer_offset += 4;
				break;
			}
			case TypeReal: {
				if (recordDescriptor[i].name == attributeName) {
					memcpy((char *) data + out_data_offset,
							record_buffer + right_buffer_offset, sizeof(float));
					free(read_null_byte);
					free(record_buffer);
					free(output_read_buffer);
					return 0;
				}
				right_buffer_offset += 4;
				break;
			}
			case TypeVarChar: {
				int field_length = field_end_offset - right_buffer_offset;
				if (recordDescriptor[i].name == attributeName) {
					//4 bytes represents length of characters on data
					memcpy((char *) data + out_data_offset, &field_length,
							sizeof(int));
					out_data_offset += 4;

					//copy the variable length characters onto the data stream field
					memcpy((char *) data + out_data_offset,
							record_buffer + right_buffer_offset,
							field_length * sizeof(char));

					free(read_null_byte);
					free(record_buffer);
					free(output_read_buffer);
					return 0;
				}
				right_buffer_offset += field_length;
				break;
			}
			}
		}
	}
	free(read_null_byte);
	free(record_buffer);
	free(output_read_buffer);
	return 0;
}
//Scan returns an iterator to allow the caller to go through the resultant records one by one.
//Given a record descriptor, scan a file, i.e., sequentially read all the entries in the file.
//A scan has a filter condition associated with it, e.g., it consists of a list of attributes to project out as well as a predicate on an attribute ("Sal > 40000").
//Specifically, the parameter conditionAttribute here is the attribute's name that you are going to apply the filter on.
//The compOp parameter is the comparison type that is going to be used in the filtering process.
//The value parameter is the value of the conditionAttribute that is going to be used to filter out records.
//Note that the retrieved records should only have the fields that are listed in the vector attributeNames.
//Please take a look at the test cases for more information on how to use this method

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const string &conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
		const void *value,                    // used in the comparison
		const vector<string> &attributeNames, // a list of projected attributes
		RBFM_ScanIterator &rbfm_ScanIterator) {

	rbfm_ScanIterator.fileHandle = fileHandle;
	rbfm_ScanIterator.recordDescriptor = recordDescriptor;
	rbfm_ScanIterator.compOp = compOp;
	rbfm_ScanIterator.attributeNames = attributeNames;

	checkConditionAttributeValue(recordDescriptor, conditionAttribute, compOp,
			value, rbfm_ScanIterator);

//	rbfm_ScanIterator.condition_Attr_name = conditionAttribute; //donno if this is required or not?
	rbfm_ScanIterator.remainingRID.pageNum = 0;
	rbfm_ScanIterator.remainingRID.slotNum = 1;

	return 0;
}

RC RecordBasedFileManager::checkConditionAttributeValue(
		const vector<Attribute> &recordDescriptor,
		const string &conditionAttribute, const CompOp compOp,
		const void *value, RBFM_ScanIterator &rbfm_ScanIterator) {

	if (conditionAttribute == "") { //if condition attribute is empty ->. NO_OP
		rbfm_ScanIterator.condition_Attr_Id = -2;
	} else {
		rbfm_ScanIterator.condition_Attr_Id = -1; //it condition attribute doesn't belong in recordAttributes
	}

	for (unsigned i = 0; i < recordDescriptor.size(); i++) {
		if (recordDescriptor[i].name == conditionAttribute) { //define the condition attribute details onto the instance
			rbfm_ScanIterator.condition_Attr_Type = recordDescriptor[i].type;
			rbfm_ScanIterator.condition_Attr_Id = i;
			rbfm_ScanIterator.conditionAttribute = conditionAttribute;
			break;
		}
	}

	//**************to do---handle NULL values******************************

	if (compOp != NO_OP) { //&& rbfm_ScanIterator.condition_Attr_Id >= 0) {
		if (rbfm_ScanIterator.condition_Attr_Type != TypeVarChar) { //if attribute is of type int or float the 4bytes
			rbfm_ScanIterator.value = calloc(1, sizeof(int)); //to be freed--now done in rmsi->close()--to----do********************
			memcpy(rbfm_ScanIterator.value, value, sizeof(int));
		} else {
			int len_var_char;
			memcpy(&len_var_char, (char*) value, sizeof(int));

			rbfm_ScanIterator.value = calloc(PAGE_SIZE, sizeof(char));
			memcpy(rbfm_ScanIterator.value, (char*) value, len_var_char + 4); //copy the length +varchar string
		}
	}

	return 0;
}

//////////////////////////////////////////////////////////////////
///////////RBFM Scan Iterator Functions///////////////////////////
//////////////////////////////////////////////////////////////////

PagedFileManager* RBFM_ScanIterator::_pf_manager_for_iterator =
		PagedFileManager::instance();

RBFM_ScanIterator::RBFM_ScanIterator() {
}

RBFM_ScanIterator::~RBFM_ScanIterator() {
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {

	int res = 0; //return cached
	char *read_page_buffer = (char*) calloc(PAGE_SIZE, sizeof(char)); //create the page-buffer

	if (fileHandle.readPage(remainingRID.pageNum, read_page_buffer) < 0) { //if read fails due to pageNum being greater that last page Num
		free(read_page_buffer);
		return RBFM_EOF;
	}

	int num_of_slots_on_page = 0;
	memcpy(&num_of_slots_on_page, read_page_buffer + 4092, 2);

	//validate next record
	//if not valid->got to next RID for this see if the slot num != total num of slots in the page else pageNum++ slotNum=0
	//also keep a check on if the pageNum doesn't exceed the last page on the file. else mark EOF

	char *record_buffer = (char *) calloc(PAGE_SIZE, sizeof(char)); //create the record-buffer

	while (readAndValidateNextRecord(remainingRID, read_page_buffer,
			record_buffer, num_of_slots_on_page) < 0) { //read And Validate Next Record on the given page //--to do --num_of_slots_on_page
		//go ahead until u find one
		getNextRID(rid);		//update the present rid to the cached rid
		if (remainingRID.slotNum > num_of_slots_on_page) {
			remainingRID.pageNum++;	//if the slot num = mum of slots on page -> move to the next page
			if ( (unsigned) remainingRID.pageNum >= fileHandle.getNumberOfPages()) {//if the page num exceeds the pages in a file
				res = RBFM_EOF;
				break;
			}
			remainingRID.slotNum = 1;	//initialize to 0 on next page num
			fileHandle.readPage(remainingRID.pageNum, read_page_buffer);//read the new page on to the read page buffer //reducing as it is used in readAndValidateNextRecord()
			memcpy(&num_of_slots_on_page, read_page_buffer + 4092, 2);
		}
	}

	if (res != RBFM_EOF) {
		//retrieve the required attributes for the selected record that returns true for compare operation and update the page and slot nums.
		getRequiredRecord(data, record_buffer);	//to set the data for the function referenced parameters

		getNextRID(rid);//to set the rid for the function referenced parameters
	}

	free(record_buffer);
	free(read_page_buffer);
	return res;
}

RC RBFM_ScanIterator::getNextRID(RID &tempRID) {
	tempRID.slotNum = remainingRID.slotNum;
	tempRID.pageNum = remainingRID.pageNum;
	remainingRID.slotNum++;
	return 0;
}

RC RBFM_ScanIterator::validateRID(RID &tempRID,
		const int num_of_slots_on_page) {

	if (remainingRID.slotNum > num_of_slots_on_page) {
		return -1;
	}
	return 0;
}

RC RBFM_ScanIterator::readAndValidateNextRecord(RID &rid, void *buffer,
		char *record_buffer, const int num_of_slots_on_page) {

	if (validateRID(remainingRID, num_of_slots_on_page) < 0) {
		return -1;
	}

	//validate RID without checking for updated record
	int slot_offset = 0, slot_len = 0;
	memcpy(&slot_len,
			(char *) buffer + SEEK_NUM_OF_RECORDS
					- (rid.slotNum * SIZE_OF_SLOT), SIZE_OF_LAST_BLOCK);
	memcpy(&slot_offset,
			(char *) buffer + SEEK_NUM_OF_RECORDS
					- (rid.slotNum * SIZE_OF_SLOT)+ SIZE_OF_LAST_BLOCK,
			SIZE_OF_LAST_BLOCK);
	if (slot_len <= 0) {//if trying to access a deleted record or an updated record ....to be handled----*************
		return -1;
	} else {
		memcpy((char *) record_buffer, (char *) buffer + slot_offset, slot_len);//copy record in page onto record buffer
		if (checkConditionRecord(record_buffer) < 0) {
			return -1;
		}
	}
	return 0;
}

RC RBFM_ScanIterator::checkConditionRecord(void *record) {
	//to check if the condition attribute is equal to the respective attribute in the record if yes then retrieve the required attributes
	//compare the condition attribute with the value and return true or false accordingly

	if (compOp == NO_OP || condition_Attr_Id == -2) {//if NO_OP then return true and all the records
		return 0;
	}

	bool result = false;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	void *compare = calloc(PAGE_SIZE, 1);
	rbfm->readAttribute(fileHandle, recordDescriptor, remainingRID,
			conditionAttribute, compare);

	//check for null bit
	char nullCheck=0;
	memcpy(&nullCheck,(char *) compare,1);
	if((nullCheck & (1 << 7)) == true){
		free(compare);
		return -1;
	}

	if (condition_Attr_Type == TypeInt) {
		int compare1 = 0;
		memcpy(&compare1, (char *) compare + 1, sizeof(int));
		int compare2 = *((int*) value);

		result = compareIntFloat<int>(compare1, compare2, compOp);
	} else if (condition_Attr_Type == TypeReal) {
		float compare1 = 0;
		memcpy(&compare1, (char *) compare + 1, sizeof(float));
		float compare2 = *((float*) value);

		result = compareIntFloat<float>(compare1, compare2, compOp);
	} else if (condition_Attr_Type == TypeVarChar) {

		char null_check;
		memcpy(&null_check, (char *) compare, sizeof(char));
		if (null_check & (1 << (7))) {	//check for null from read attribute
			free(compare);
			return -1;
		} else {
			int str1_len = 0;
			memcpy(&str1_len, (char *) compare + 1, sizeof(int));

			char str1_Array[str1_len];
			memcpy(&str1_Array, (char *) compare + 5, str1_len); //copy the column name in string format
			string str1(str1_Array, str1_len); //convert the char array to string

			int str2_len = 0;
			memcpy(&str2_len, (char *) value, sizeof(int));

			char str2_Array[str2_len];
			memcpy(&str2_Array, (char *) value + sizeof(int), str2_len); //copy the column name in string format
			string str2(str2_Array, str2_len); //convert the char array to string

			result = compareIntFloat<string>(str1, str2, compOp); // compareString(str1, str2, compOp);//
		}
	}

	free(compare);
	return result ? 0 : -1;
}

template<class T>
bool RBFM_ScanIterator::compareIntFloat(T a, T b, CompOp compOp) {

	if (compOp == EQ_OP)
		return a == b;
	if (compOp == LT_OP)
		return a < b;
	if (compOp == LE_OP)
		return a <= b;
	if (compOp == GT_OP)
		return a > b;
	if (compOp == GE_OP)
		return a >= b;
	if (compOp == NE_OP)
		return a != b;
	if (compOp == NO_OP)
		return 0;
	return false;
}

RC RBFM_ScanIterator::getRequiredRecord(void *data, char *record) {

	int attr_count = attributeNames.size();
	int null_bytes_count = ceil((double) attr_count / 8);

	char *write_null_byte = (char*) calloc(null_bytes_count, sizeof(char));
	memset(write_null_byte, 0, null_bytes_count);    //write out data null bytes

	int descriptor_size = recordDescriptor.size();
	int record_null_bytes_count = ceil((double) descriptor_size / 8);

	int out_data_offset = 0;	//offset to keep track of output data stream

	char *read_null_byte = (char*) calloc(record_null_bytes_count,//to read null byte count of the record read
			sizeof(char));
	memcpy(read_null_byte, record + sizeof(int), record_null_bytes_count);//copy for read manipulations

	int non_null_bit_count = 0;	//to count the null bits
	for (int i = 0; i < descriptor_size; i++) {
		int num_of_null_bit = i % 8;
		int num_of_null_bytes = i / 8;
		if (read_null_byte[num_of_null_bytes] & (1 << (7 - num_of_null_bit))) {
			continue;	//to escape nulls bits : no-op here
		} else {
			non_null_bit_count++;
		}
	}
//	memset(data,0,null_bytes_count);//setting all null bytes in the out buffer to zeros

	for (int j = 0; j < attr_count; j++) {
		int num_of_null_bit_on_write_data = j % 8;
		int num_of_null_bytes_on_write_data = j / 8;

		int left_buffer_offset = sizeof(int) + record_null_bytes_count;	//to move the offset to point to the start of the field offset address
		int right_buffer_offset = sizeof(int) + record_null_bytes_count
				+ (non_null_bit_count * sizeof(int));//to move the offset to point to the start of the fields
		int field_end_offset = 0;

		int pos = getAttributeNameId(attributeNames[j], recordDescriptor);
		if (pos < 0) {//if the attr_names list doesn't match with record descriptor
			write_null_byte[num_of_null_bytes_on_write_data] |= (1
					<< (7 - num_of_null_bit_on_write_data));
			free(write_null_byte);
			free(read_null_byte);
			return -1;
		} else {
			for (int i = 0; i < descriptor_size; i++) {
				int result = false;	//to break from for loop
				int num_of_null_bit = i % 8;
				int num_of_null_bytes = i / 8;
				if (read_null_byte[num_of_null_bytes]
						& (1 << (7 - num_of_null_bit))) {
					if (attributeNames[j] == recordDescriptor[i].name) {
						write_null_byte[num_of_null_bytes_on_write_data] |= (1
								<< (7 - num_of_null_bit_on_write_data));//set the bit to null indication for the given attribute name
						break;
					}
					continue;	//to escape nulls bits : no-op here
				} else {
					memcpy(&field_end_offset, record + left_buffer_offset,
							sizeof(int));//load the offset address onto the into variable
					left_buffer_offset += sizeof(int);//move offset to next byte(point to next address block)

					switch (recordDescriptor[i].type) {
					case TypeInt: {
						if (pos == i) {
							memcpy(
									(char *) data + null_bytes_count
											+ out_data_offset,
									record + right_buffer_offset, sizeof(int));	//copy the int 4bytes into the data
							int value = 0;
							memcpy(&value, record + right_buffer_offset,
									sizeof(int));

							out_data_offset += sizeof(int);
							result = true;
						}
						right_buffer_offset += sizeof(int);
						break;
					}
					case TypeReal: {
						if (pos == i) {
							memcpy(
									(char *) data + null_bytes_count
											+ out_data_offset,
									record + right_buffer_offset,
									sizeof(float));	//copy the real 4bytes into the data
							float value = 0;
							memcpy(&value, record + right_buffer_offset,
									sizeof(float));

							out_data_offset += sizeof(float);
							result = true;
						}
						right_buffer_offset += sizeof(float);
						break;
					}
					case TypeVarChar: {
						int field_length = field_end_offset
								- right_buffer_offset;
						if (pos == i) {
							memcpy(
									(char *) data + null_bytes_count
											+ out_data_offset, &field_length,
									sizeof(int));//4 bytes represent of no. of characters on data
							out_data_offset += sizeof(int);

							memcpy(
									(char *) data + null_bytes_count
											+ out_data_offset,
									record + right_buffer_offset,
									field_length * sizeof(char));//copy the variable length characters onto the data stream field
							out_data_offset += field_length;

							result = true;
						}
						right_buffer_offset += field_length;
						break;
					}
					}
				}
				if (result) {
					break;
				}
			}
		}

	}
	//write_null_bytes updating for output data
	memcpy((char *) data, write_null_byte, null_bytes_count);

	free(write_null_byte);
	free(read_null_byte);
	return 0;
}

RC RBFM_ScanIterator::getAttributeNameId(const string &attributeName,
		const vector<Attribute> &recordDescriptor) {

	int result = -1;
	for (unsigned int k = 0; k < recordDescriptor.size(); k++) {
		if (recordDescriptor[k].name == attributeName) {
			result = k;
		}
	}
	return result;
}
RC RBFM_ScanIterator::close() {
	if (compOp != NO_OP) {
		free(value);
	}

	return 0;
}

