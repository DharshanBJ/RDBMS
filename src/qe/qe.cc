#include "qe.h"

#include <stdlib.h>
#include <climits>
#include <cmath>
#include <cstring>
#include <iterator>
#include <string>

#include "../rbf/pfm.h"

RC size_In_Bytes(AttrType type, const void* value) {
	if (type == TypeReal)
		return sizeof(float);
	if (type == TypeInt)
		return sizeof(int);
	if (type == TypeVarChar)
		return sizeof(unsigned) + sizeof(char) * (*(unsigned*) value);

	return 0;
}

// Linearly search through the attributes to match the name
RC findAttributeIndexByName(const string& conditionAttribute,
		const vector<Attribute>& recordDescriptor, unsigned& index) {
	index = 0;
	for (unsigned int k = 0; k < recordDescriptor.size(); k++) {

		if (recordDescriptor[k].name == conditionAttribute) {
			return 0;
		}
		index++;
	}
	return -1;
}

template<class T>
bool compareIntFloat(T a, T b, CompOp compOp) {

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

bool Condition::compare(const void* left, const void* right,
		AttrType type) const {

	switch (type) {
	case TypeInt: {
		int leftInt = 0, rightInt = 0; //*((int*) left)
		memcpy(&leftInt, left, 4);
		memcpy(&rightInt, right, 4);
		return compareIntFloat<int>(leftInt, rightInt, op);
	}
	case TypeReal: {
		float leftFloat = 0, rightFloat = 0; //*((float*) left)
		memcpy(&leftFloat, left, 4);
		memcpy(&rightFloat, right, 4);
		return compareIntFloat<float>(leftFloat, rightFloat, op);
	}
	case TypeVarChar: {
		int str1_len = 0;
		memcpy(&str1_len, (char *) left, 4);
		string str1((char *) left + 4, str1_len); //convert the char array to string

		int str2_len = 0;
		memcpy(&str2_len, (char *) right, 4);
		string str2((char *) right + 4, str2_len); //convert the char array to string

		return compareIntFloat<string>(str1, str2, op); // compareString(str1, str2, compOp);
	}
	}
	return false;
}

Filter::Filter(Iterator* input, const Condition &condition) { //: _ipt(input),_cndtn(condition)

	this->_ipt = input;
	this->_cndtn = condition;

	//to do changes here... not tested
	getAttributes(lhs_Attr_list);
	findAttributeIndexByName(_cndtn.lhsAttr, lhs_Attr_list, lhs_Attr_Index);

	if (_cndtn.bRhsIsAttr) {
		findAttributeIndexByName(_cndtn.rhsAttr, lhs_Attr_list, rhs_Attr_Index);
		_cndtn.rhsValue.type = lhs_Attr_list.at(rhs_Attr_Index).type;
	}

}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	return this->_ipt->getAttributes(attrs);
}

RC Filter::getNextTuple(void *data) {

	while (_ipt->getNextTuple(data) != QE_EOF) {

		// Find where the attribute we care about is in the returned tuple
		unsigned lhs_Offset = ceil((double) lhs_Attr_list.size() / 8);

		for (unsigned int i = 0; i < lhs_Attr_Index; ++i) {
			Attribute l_attr = lhs_Attr_list[i];
			lhs_Offset += size_In_Bytes(l_attr.type, (char*) data + lhs_Offset);
		}

		// If the RHS is an attribute, load in the attribute's data
		if (_cndtn.bRhsIsAttr) {
			unsigned rhsDataOffset = ceil((double) lhs_Attr_list.size() / 8); //check this?
			for (unsigned int i = 0; i < rhs_Attr_Index; ++i) {
				Attribute r_attr = lhs_Attr_list[i];
				rhsDataOffset += size_In_Bytes(r_attr.type,
						(char*) data + rhsDataOffset);
			}
			_cndtn.rhsValue.data = (char*) data + rhsDataOffset;
		}

		//traverse through the data to reach to the particular offset where the value for attribute exists
		if (_cndtn.compare((char*) data + lhs_Offset, _cndtn.rhsValue.data,
				_cndtn.rhsValue.type)) {
			return 0;
		}
	}
	return -1;
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {

	this->input = input;
	input->getAttributes(attrs);
	this->aggAttr = aggAttr;
	this->op = op;
	getend = false;
}

RC Aggregate::getNextTuple(void *data) {
	if (getend) {
		return QE_EOF;
	}

	void *key = calloc(4096, 1);
	void *data1 = calloc(4096, 1);
	float tempfloat, tempint;
	float sum = 0;
	int count = 0;
	int minval = INT_MAX;
	int maxval = INT_MIN;

	if (aggAttr.type == TypeInt || aggAttr.type == TypeReal) {

		//while loop, fetch next tuple from input
		while (input->getNextTuple(data1) != QE_EOF) {

			//fetch key value for aggAttr
			getAttrValue(attrs, aggAttr.name, data1, key);

			if (aggAttr.type == TypeReal) {
				tempfloat = *(float *) key;

			} else if (aggAttr.type == TypeInt) {
				//if attr.type is TypeInt
				//assign *(int *)key tempint

				tempint = *((int *) key);

				//type conversion, (float) tempint
				tempfloat = (float) tempint;
			}

			//switch on op
			switch (op) {

			case COUNT:
				//COUNT, count++
				count++;
				break;
			case SUM:
				//SUM, sum++
				sum += tempfloat;
				break;
			case AVG:
				//AVG, sum++ and count ++
				count++;
				sum += tempfloat;
				break;
			case MIN:
				//MIN, if new value < stored value, replace it
				if (tempfloat < minval) {
					minval = tempfloat;
				}
				break;
			case MAX:
				//MAX, if new value > stored value, replace it
				if (tempfloat > maxval) {
					maxval = tempfloat;
				}
				break;
			}
		}
		//if op is AVG, calculate result
		if (op == AVG) {
			tempfloat = sum / count;
		}
		if (op == COUNT) {
			tempfloat = count;
		}
		if (op == SUM) {
			tempfloat = sum;
		}
		if (op == MIN) {
			tempfloat = minval;
		}
		if (op == MAX) {
			tempfloat = maxval;
		}
	}
	memcpy((char *) data + 1, &tempfloat, sizeof(float));

	free(data1);
	free(key);
	getend = true;
	return 0;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const {
	input->getAttributes(attrs);
}

AttrType Aggregate::getAttrValue(vector<Attribute> attrs, string attr,
		void *data, void *value) {

	int nullSize = ceil((double) attrs.size() / 8);

	unsigned char nullIndicator[nullSize];

	memcpy(nullIndicator, data, nullSize);

	int offset = nullSize; // offset to find value

	for (unsigned i = 0; i < attrs.size(); i++) {
		//size only used in this scope
		int size;

		// check if attrs[i] is desired attribute
		if (attrs[i].name.compare(attr) == 0) {
			// if null indicator is 1, no value for desired attribute

			if (nullIndicator[i / 8] & (1 << (7 - (i % 8)))) {

				return attrs[i].type;
			}
			// get attribute value size
			size = size_In_Bytes(attrs[i].type, (char*) data + offset);

			memcpy(value, (char*) data + offset, size);

			return attrs[i].type;
		} else {

			// skip null field for increasing offset
			if (nullIndicator[i / 8] & (1 << (7 - (i % 8))))
				continue;

			// calculate size for value
			size = size_In_Bytes(attrs[i].type, (char*) data + offset);
			offset += size;
		}

	}

}

Project::Project(Iterator *input,                    // Iterator of input R
		const vector<string> &attrNames) :
		inpt(input), attr_names(attrNames) {

	//to get attributes from the input iterator
	inpt->getAttributes(inpt_attrs);

	//	getAttributes(project_attrs);

	// calculate the size on a Projected record
	record_size = 0;
	for (unsigned j = 0; j < inpt_attrs.size(); j++) {
		record_size += inpt_attrs[j].length;
	}
	record_buffer = calloc(record_size, 1);
}

void Project::getAttributes(vector<Attribute> &attrs) const {

	// to capture projection attributes
//	attrs.clear();
//
//	for (unsigned i = 0; i < attr_names.size(); i++) {
//		for (unsigned j = 0; j < inpt_attrs.size(); j++) {
//			if (attr_names[i] == inpt_attrs[j].name) {
//				Attribute attr;
//				attr.name = inpt_attrs[j].name;
//				attr.type = inpt_attrs[j].type;
//				attr.length = inpt_attrs[j].length;
//				attrs.push_back(attr);
//			}
//		}
//	}

}

RC Project::getNextTuple(void *data) {

	//clear the record buffer
	memset(record_buffer, 0, record_size);

	unsigned data_offset = 0;
	unsigned record_offset = 0;
	unsigned len = 0;
	bool nullBit = false;
	unsigned nullBitCount = 0;
//	vector<bool>bit_arr;

	while (inpt->getNextTuple(record_buffer) != QE_EOF) {

		int null_byte_count = ceil((double) inpt_attrs.size() / 8); //calculate the no. of null bytes
		int write_buffer_null_byte = ceil((double) attr_names.size() / 8);

		bool bitArr[write_buffer_null_byte*8] ={0};
//		for(int z=0;z<(write_buffer_null_byte*8);z++){
//			bit_arr.push_back(false);
//		}

		memset(data, 0, write_buffer_null_byte);
		data_offset += write_buffer_null_byte;

		char *null_bytes = (char*) calloc(null_byte_count, 1);
		memcpy(null_bytes, record_buffer, null_byte_count);
		record_offset += null_byte_count;

		//keep the track of the required attrs for projection by caching offset and len of each matched values
		for (unsigned i = 0; i < inpt_attrs.size(); i++) {
			Attribute attr = inpt_attrs[i];
			int num_of_null_bit = i % 8;
			int num_of_null_bytes = i / 8;
			if (null_bytes[num_of_null_bytes] & (1 << (7 - num_of_null_bit))) {
				nullBit = true;
			} else {
				len = size_In_Bytes(attr.type,
						(char*) record_buffer + record_offset);
			}

			if (!nullBit) {
				for (unsigned k = 0; k < attr_names.size(); k++) {
					if (attr.name == attr_names[k]) {
						memcpy((char*) data + data_offset,
								(char*) record_buffer + record_offset, len);
						data_offset += len;
					}
				}
				record_offset += len;
			} else {
//				bit_arr[nullBitCount] = true;
				bitArr[nullBitCount] = 1;
			}

			nullBitCount++;

		}
		memcpy(data,&bitArr,write_buffer_null_byte);
		free(null_bytes);
		return 0;
	}
	return -1;
}

Project::~Project() {
	if (record_buffer != NULL) {
		free(record_buffer);
	}
}
// ... the rest of your implementations go here
