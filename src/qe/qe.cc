#include "qe.h"

#include <cstring>

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
	for (vector<Attribute>::const_iterator it = recordDescriptor.begin();
			it != recordDescriptor.end(); ++it, ++index) {
		if (it->name == conditionAttribute) {
			return 0;
		}
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
		memcpy(&rightInt, left, 4);
		return compareIntFloat<int>(leftInt, rightInt, op);
	}
	case TypeReal: {
		float leftFloat = 0, rightFloat = 0; //*((float*) left)
		memcpy(&leftFloat, left, 4);
		memcpy(&rightFloat, left, 4);
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

Filter::Filter(Iterator* input, const Condition &condition) {//: _ipt(input),_cndtn(condition) {

	_ipt = input;
	_cndtn = condition;

	//to do changes here... not tested
	getAttributes(lhs_Attr_list);
	findAttributeIndexByName(_cndtn.lhsAttr, lhs_Attr_list, lhs_Attr_Index);

	if (_cndtn.bRhsIsAttr) {
		findAttributeIndexByName(_cndtn.rhsAttr, lhs_Attr_list, rhs_Attr_Index);
		_cndtn.rhsValue.type = lhs_Attr_list.at(rhs_Attr_Index).type;
	}

}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	return _ipt->getAttributes(attrs);
}

RC Filter::getNextTuple(void *data) {
	while (_ipt->getNextTuple(data) != QE_EOF) {

		// Find where the attribute we care about is in the returned tuple
		unsigned lhs_Offset = 0;

		for (unsigned int i = 0; i < lhs_Attr_Index; ++i) {
			Attribute l_attr = lhs_Attr_list[i];
			lhs_Offset += size_In_Bytes(l_attr.type, (char*) data + lhs_Offset);
		}

		// If the RHS is an attribute, load in the attribute's data
		if (_cndtn.bRhsIsAttr) {
			unsigned rhsDataOffset = 0;
			for (unsigned int i = 0; i < rhs_Attr_Index; ++i) {
				Attribute r_attr = lhs_Attr_list[i];
				rhsDataOffset += size_In_Bytes(r_attr.type,
						(char*) data + rhsDataOffset);
			}
			_cndtn.rhsValue.data = (char*) data + rhsDataOffset;
		}

		if (_cndtn.compare(_cndtn.rhsValue.data, (char*) data + lhs_Offset,
				_cndtn.rhsValue.type)) {
			return 0;
		}
	}
	return -1;
}

// ... the rest of your implementations go here
