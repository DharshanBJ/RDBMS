#include "rbfm.h"

#include <math.h>
#include <stdlib.h>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdint.h>
#include<unistd.h>

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

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) { //only 1 record at a time


	bool append = true;
	int record_size = 0;
	int size_of_slot = 2 * sizeof(short);
	int size_of_last_block = sizeof(short);
	int seek_free_space_slot = 4094;
	int seek_num_of_records = 4092;
	char * buffer = (char *) malloc(4096);
	formatRecord(recordDescriptor, data, buffer, record_size); //format the input data to record reqd.
	int check_page_num=fileHandle.getNumberOfPages() ;
	if (check_page_num == 0) {
		char *write_buffer = (char *) calloc(PAGE_SIZE, sizeof(char)); //malloc(PAGE_SIZE);
		short num_of_slots = 1;
		short free_space_new_page = (short) (PAGE_SIZE - size_of_last_block
				- size_of_last_block - size_of_slot - record_size); //find the free space

		char *slot = (char *) calloc(2, sizeof(char)); //malloc(2*sizeof(short));
		short slot_record_length = record_size;
		short slot_offset = 0; //*((short*)fileHandle.getPageFilePtr());//+record_size;

		memcpy(write_buffer, buffer, record_size); //write record_format_buffer data onto a new output write buffer

		memcpy(write_buffer + seek_free_space_slot, &free_space_new_page,
				size_of_last_block); //modify the data with new record + slot and F + N directory

		memcpy(write_buffer + seek_num_of_records, &num_of_slots,
				size_of_last_block); //modify write buffer with N:no.of slots

		memcpy(write_buffer + seek_num_of_records - size_of_last_block,
				&slot_offset, size_of_last_block); //slot offset put onto write buffer

		memcpy(write_buffer + seek_num_of_records - size_of_slot,
				&slot_record_length, size_of_last_block); //slot record length put onto write buffer

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
		memcpy(&free_space_block, read_Write_buffer + seek_free_space_slot,
				size_of_last_block);

		if (free_space_block > (record_size + size_of_slot)) {//if we have free space then update the read_Write_buffer buffer and write.
			append_recordPage(fileHandle, read_Write_buffer, page_num,
					record_size, free_space_block, rid, buffer);
			append = false;
		} else {//if free space in the last page is less than the record length then traverse through all the pages and check for free space: if any write else append new
			for (int i = 0; i < page_num - 1; i++) {
				fileHandle.readPage(page_num, read_Write_buffer);//read into the buffer the page details
				memcpy(&free_space_block,
						read_Write_buffer + seek_free_space_slot,
						size_of_last_block);
				;
				if (free_space_block >= (record_size + size_of_slot)) {
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
			short free_space_new_page = (short) (PAGE_SIZE - size_of_last_block
					- size_of_last_block - size_of_slot - record_size);	//find the free space
			short slot_record_length = record_size;
			short slot_offset = 0;//*((short*)fileHandle.getPageFilePtr());//+record_size;
			memcpy(read_Write_buffer, buffer, record_size * sizeof(char));//write record data(formatted) into a new out buffer

			memcpy(read_Write_buffer + seek_free_space_slot,
					&free_space_new_page, size_of_last_block);//modify the data with new record + slot and F + N directory

			memcpy(read_Write_buffer + seek_num_of_records, &num_of_slots,
					size_of_last_block);//modify write buffer with N:no.of slots

			memcpy(read_Write_buffer + seek_num_of_records - size_of_last_block,
					&slot_offset, size_of_last_block);//slot offset put onto write buffer

			memcpy(read_Write_buffer + seek_num_of_records - size_of_slot,
					&slot_record_length, size_of_last_block);//slot record put onto write buffer

			//write back and update page nums.
			fileHandle.appendPage(read_Write_buffer); // append a new file and write the record into it , update the last free space , no. of records , and slot number->offset and length of the record
			rid.pageNum = page_num + 1;
			rid.slotNum = num_of_slots; //or 1 here
		}
		free(read_Write_buffer);
	}
	free(buffer);
	return 0;
}

RC RecordBasedFileManager::append_recordPage(FileHandle &fileHandle,
	char *read_Write_buffer, int &page_num, int &record_size,
	short free_space_block, RID &rid, char *buffer) {
	int size_of_slot = 2 * sizeof(short);
	int size_of_last_block = sizeof(short);
	int seek_free_space_slot = 4094;
	int seek_num_of_records = 4092;
	free_space_block = free_space_block - record_size - size_of_slot; //free space reduced by record size plus slot directory size of record

	short num_of_slots;
	memcpy(read_Write_buffer + seek_free_space_slot, &free_space_block,
			size_of_last_block); //write the updated free space in to the write buffer
	memcpy(&num_of_slots, read_Write_buffer + seek_num_of_records,
			size_of_last_block); //read num of lots from read page

	short slot_record_length; //first set it to prev slot dir rec len and then add to offset to get new offset value and then assign record_size here
	short slot_offset = 0; //should be set to point to first of record address
	memcpy(&slot_record_length,
			read_Write_buffer + seek_num_of_records
					- (num_of_slots * size_of_slot), size_of_last_block); //to read the record length of the last record slot directory
	memcpy(&slot_offset,
			read_Write_buffer + seek_num_of_records
					- (num_of_slots * size_of_slot) + size_of_last_block,
			size_of_last_block); //to read the offset of the last record slot directory

	slot_offset = slot_offset + slot_record_length; //is it the right way?
	slot_record_length = record_size;

	num_of_slots += 1;

	memcpy(read_Write_buffer + slot_offset, buffer, slot_record_length); //write the record formatted data into the out buffer

	memcpy(read_Write_buffer + seek_num_of_records, &num_of_slots,
			size_of_last_block); //write the updated num of slots to write buffer

	memcpy(
			read_Write_buffer + seek_num_of_records
					- (num_of_slots * size_of_slot) + size_of_last_block,
			&slot_offset, size_of_last_block); //write the new slot dir with the offset address

	memcpy(
			read_Write_buffer + seek_num_of_records
					- (num_of_slots * size_of_slot), &slot_record_length,
			size_of_last_block); //write new slot dir with the rec len
	fileHandle.writePage(page_num, read_Write_buffer);
	rid.pageNum = page_num;
	rid.slotNum = num_of_slots; //since latest updated no_of_slots indicates the slot num as well
	return 0;
}

RC RecordBasedFileManager::formatRecord( const vector<Attribute> &recordDescriptor, const void* data, char *buffer, int &record_size) {
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
			+ inputNullBytes * sizeof(char);//offset for buffer 1(no.f field)+(null bytes)+(fields offset address bytes)
	int input_stream_offset = inputNullBytes * sizeof(char);//offset for void *data (i/p): initialized to no. of byte for the null bytes
	for (int i = 0; i < fieldCount; i++) {
		int num_of_null_bit = i % 8;
		int num_of_null_bytes = i / 8;
		if (null_bytes[num_of_null_bytes] & (1 << (7 - num_of_null_bit))) {
			continue;	//to escape nulls bits : no-op here
		} else {
			switch (recordDescriptor[i].type) {
			case TypeInt: {
				memcpy(buffer + buffer_offset, data + input_stream_offset,
						sizeof(int));	//load in the field values
				int data_to_print;// =
				memcpy(&data_to_print,data +input_stream_offset, 4);
				buffer_offset += sizeof(int);//update the buffer offset by 4bytes
				input_stream_offset += sizeof(int);	//update the i/p data offset by 4bytes
				memcpy(buffer + field_address_byte_offset, &buffer_offset,
						sizeof(int));	//update field address
				field_address_byte_offset += sizeof(int);//update field address byte offset
				break;
			}
			case TypeReal: {
				memcpy(buffer + buffer_offset, data + input_stream_offset,
						sizeof(float));
				buffer_offset += sizeof(float);
				input_stream_offset += sizeof(float);
				memcpy(buffer + field_address_byte_offset, &buffer_offset,
						sizeof(int));	//update field address
				field_address_byte_offset += sizeof(int);
				break;
			}
			case TypeVarChar: {	//check first byte to retrieve the length of the character string
				char* len_string_data = (char*) malloc(sizeof(int));//malloc(sizeof(int));
				memcpy(len_string_data, data + input_stream_offset,
						sizeof(int));//get 4byte stream representing the length of var chars
				input_stream_offset += sizeof(int);

				int len_of_var_char = *((int*) len_string_data);//to retrieve the length of the variable characters in the i/p data stream
				memcpy(buffer + buffer_offset, data + input_stream_offset,
						len_of_var_char);
				buffer_offset += len_of_var_char;
				input_stream_offset += len_of_var_char;

				memcpy(buffer + field_address_byte_offset, &buffer_offset,
						sizeof(int));	//update field address
				field_address_byte_offset += sizeof(int);
				free(len_string_data);
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
	int size_of_slot = 2 * sizeof(short);
	int size_of_last_block = sizeof(short);
	int seek_num_of_records = 4092;
	PageNum num_page = rid.pageNum;

	char *output_read_buffer = (char *) calloc(PAGE_SIZE, sizeof(char));// malloc(PAGE_SIZE);buffer to load the page to be read
	fileHandle.readPage(num_page, output_read_buffer);//read the page into the buffer

	short slot_rec_len = 0, slot_rec_offset = 0;
	memcpy(&slot_rec_offset,
			output_read_buffer + seek_num_of_records
					- (rid.slotNum * size_of_slot) + size_of_last_block,
			size_of_last_block);	//copy the value of slot rec address
	memcpy(&slot_rec_len,
			output_read_buffer + seek_num_of_records
					- (rid.slotNum * size_of_slot), size_of_last_block);//copy the rec length from slot dir

	char *record_buffer = (char *) calloc(slot_rec_len, sizeof(char));// malloc(PAGE_SIZE);//allocate memory to read record from page
	memcpy(record_buffer, output_read_buffer + slot_rec_offset, slot_rec_len);//copy the record from the page buffer

	char* char_attr = (char*) malloc(slot_rec_len);	//ouput buffer copied
	memcpy(char_attr, record_buffer, slot_rec_len);
	string character_array = char_attr;
	string record =record_buffer;
	free(char_attr);

	int field_count;
	memcpy(&field_count, record_buffer, sizeof(int));//copy the first byte representing the field size

	int null_byte_count = ceil((double) field_count / 8);//calculate the no. of null bytes
	memcpy(data, record_buffer + sizeof(int), null_byte_count);	//load the null byte onto the output data

	char *read_null_byte = (char*)malloc(null_byte_count);
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

	for (unsigned int i = 0; i < field_count; i++) {
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
				memcpy(&int_attr, data + offset, sizeof(int));
				offset += sizeof(int);
				cout << recordDescriptor[i].name << ":" << int_attr << endl;
				break;
			}
			case TypeReal: {
				float real_attr;
				memcpy(&real_attr, data + offset, sizeof(float));
				offset += sizeof(float);
				cout << recordDescriptor[i].name << ":" << real_attr << endl;
				break;
			}
			case TypeVarChar: {
				int attr_len;
				memcpy(&attr_len, data + offset, sizeof(int));
				offset += sizeof(int);
				char* char_attr = (char*) calloc(attr_len,sizeof(char));
				memcpy(char_attr, data + offset, attr_len);
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
