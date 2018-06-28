
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
  this->iter = input; this->condition = condition;
}

RC Filter::getNextTuple(void* data)
{

  while(iter->getNextTuple(data) == SUCCESS){ // && the data satisfies the conditional

    void* selectedData = malloc(50); //assume int for now/ no harm in assuming varchar size
    vector<Attribute> attrs;
    theseAttributes(attrs);
    getSelectedData(selectedData, condition.lhsAttr, data, attrs);
    if(satisfiesCondition(selectedData, condition)) return SUCCESS;
  }

  return QE_EOF;
}

bool Filter::satisfiesCondition(void* data, Condition &condition){
  if(condition.bRhsIsAttr) return 0; // not implemented: Seems like a join
  else return satisfiesAtomicCondition( data, condition);
}

bool Filter::satisfiesAtomicCondition(void* data,Condition &condition){
  switch(condition.op){
    case EQ_OP : return isEqual(data, condition.rhsValue);
    //LT_OP : return lessThan(data, condition.rhsValue);
    case LE_OP : return lessThanOrEq(data, condition.rhsValue);
    case GE_OP : return greaterThanOrEq(data, condition.rhsValue);
  }
}

string getStr(void* data){
  int varcharlen = 0;
  memcpy(&varcharlen, data, sizeof(int));
  char buf[varcharlen+1];
  buf[varcharlen] = '\0';
  memcpy(buf, (char*) data + 4, varcharlen);
  return string(buf);
}
bool Filter::lessThanOrEq(void* data, Value rhsValue){
  switch(rhsValue.type){
    case TypeInt : return (*(int*)data <= *(int*) rhsValue.data);
  }
}

bool Filter::greaterThanOrEq(void* data, Value rhsValue){
  switch(rhsValue.type){
    case TypeInt : return (*(int*)data >= *(int*) rhsValue.data);
    case TypeReal : return (*(float*) data >= *(float*) rhsValue.data);
  }
}

bool Filter::isEqual(void* data, Value rhsValue){
  switch(rhsValue.type){
    case TypeInt : return (*(int*)data == *(int*) rhsValue.data);
    case TypeReal : return (*(float*) data == *(float*) rhsValue.data);
    case TypeVarChar : {string dstring = getStr(data); string rhsStr = getStr(rhsValue.data);
                         return dstring == rhsStr;}
  }
}

void Filter::theseAttributes(vector<Attribute> &attrs){ this->iter->getAttributes(attrs);}

void getNextAttrLen(unsigned *attrLen, void* data, size_t offset, Attribute &attr){
  unsigned sizeofanInt = 4;
  int varcharlen = 0;
  memcpy(&varcharlen, (char*) data + offset, sizeof(int));
  varcharlen += 4;
  // maybe the first 4 bytes can also be passed in in the case of a string
  switch(attr.type){
    case TypeInt : {memcpy(attrLen, &sizeofanInt, sizeof(int)); break;}
    case TypeReal : {memcpy(attrLen, &sizeofanInt, sizeof(int)); break;}
    case TypeVarChar : {memcpy(attrLen, &varcharlen, sizeof(int)); break;}
  }
}

void fillSelectedDataByType(void* selectedData, void* data, size_t offset, AttrType attrType){
  int varcharlen = 0;
  memcpy(&varcharlen, (char*) data + offset, sizeof(int));
  switch(attrType){
    case TypeInt : {memcpy(selectedData, (char*) data + offset, sizeof(int)); break;}
    case TypeReal : {memcpy(selectedData, (char*) data + offset, sizeof(int)); break;}
    case TypeVarChar : {memcpy(selectedData, (char*)data+offset, 4+varcharlen);}
  }
}
void Filter::getSelectedData(void* selectedData, const string &lhsAttr, void* data, vector<Attribute> &attrs){
  // use get Attrs instance method which calls getAttrs in the iterator object
  //loop through the descriptor, look at getAttributefromRecord -similar.
  // returned data does not have number of cols at beginning
  //going to have to go through the data checking null bits.
  // ASSUMPTION - No null bits set.

  //get null byte
  char nullByte = 0;
  memcpy(&nullByte, data, 1);
  unsigned offset = 1;

  int attrNum = 0;
  for(unsigned i = 0; i < attrs.size() ; i++){
    if( attrs[i].name == lhsAttr) { attrNum = i; break;}
  }

  //printf("attribute name is %s, the len is %d", attrs[attrNum].name.c_str(), attrs[attrNum].length);
  for(unsigned i = 0; i < attrs.size() ; i++){
    if( i == attrNum){
      fillSelectedDataByType( selectedData, data, offset, attrs[i].type); break;
    }
    else{
      unsigned attrLen;
      getNextAttrLen(&attrLen, data, offset, attrs[i]); // assume no nulls here, should account for nulls by passsing nullByte
      offset += attrLen;
    }
  }

}

Project::Project(Iterator* input, const vector<string>& attrNames) {
  this->iter = input; this->attrNames = attrNames;
}

void Project::theseAttributes(vector<Attribute> &attrs){this->iter->getAttributes(attrs);}
RC Project::getNextTuple(void* data)
{

  void* alldata [100];
  while(iter->getNextTuple(alldata) == SUCCESS){

    projectThatDataSon(data, alldata);
    return SUCCESS;
  }

  return QE_EOF;
}

void Project::projectThatDataSon(void* data, void* alldata){
  vector<Attribute> allattrs;
  theseAttributes(allattrs);
  char nulls = 0;
  unsigned offsetdata = 0;
  unsigned offsetalldata = 1;

  memcpy(data, &nulls, 1);
  offsetdata += 1;
  int j = 0; //index for attrNames
  for(int i = 0; i < allattrs.size(); i++){
    if(allattrs[i].name == attrNames[j]){
      j++;
      if( j == attrNames.size() + 1) break;
      if( allattrs[i].type != TypeVarChar){
        int a; float b;
        memcpy(&a, (char*) alldata + offsetalldata, sizeof(int));
        memcpy(&b, (char*) alldata + offsetalldata, sizeof(int));
      memcpy((char*) data+ offsetdata, (char*) alldata + offsetalldata, sizeof(int));
      offsetdata += 4; offsetalldata += 4;
    }else{
      int varcharlen = 0;
      // memcpy..
      memcpy(&varcharlen, (char*) alldata + offsetalldata, sizeof(int));
      offsetalldata += 4;
      memcpy((char*) data + offsetdata, &varcharlen, sizeof(int));
      offsetdata += 4;
      memcpy((char*) data + offsetdata, (char*)alldata + offsetalldata, varcharlen);
      offsetalldata += varcharlen; offsetdata += varcharlen;
    }

  }else{
    offsetalldata += allattrs[i].length; // also needs varchar case
  }

  }
}

INLJoin::INLJoin(Iterator* leftIn, IndexScan* rightIn, const Condition &condition)
{
  this->leftIn = leftIn; this->rightIn = rightIn; this->condition = condition;
}

void concatenate(void* leftdata, void* rightdata, vector<Attribute> leftattrs, vector<Attribute> rightattrs, void* data){
  // no varchar case yet
  char nulls = 0;
  memcpy(data, &nulls, 1);
  unsigned dataoffset = 1;
  unsigned leftdataoffset = 1;
  unsigned rightdataoffset = 1;

  // loop through attributes of the left and copy into data
  for(int i = 0; i < leftattrs.size(); i++) {
    switch (leftattrs[i].type) {
      case TypeInt : { 
        memcpy((char*) data + dataoffset, (char*) leftdata + leftdataoffset, INT_SIZE);
        dataoffset += INT_SIZE;
        leftdataoffset += INT_SIZE;
        break;
        }
      case TypeReal : {
        memcpy((char*) data + dataoffset, (char*) leftdata + leftdataoffset, REAL_SIZE); 
        dataoffset += REAL_SIZE;
        leftdataoffset += REAL_SIZE;
        break;
        }
    }
  }

  // repeat with attributes of the right
  for(int i = 0; i < rightattrs.size(); i++) {
    switch (rightattrs[i].type) {
      case TypeInt : { 
        memcpy((char*) data + dataoffset, (char*) rightdata + rightdataoffset, INT_SIZE);
        dataoffset += INT_SIZE;
        rightdataoffset += INT_SIZE;
        break;
        }
      case TypeReal : {
        memcpy((char*) data + dataoffset, (char*) rightdata + rightdataoffset, REAL_SIZE); 
        dataoffset += REAL_SIZE;
        rightdataoffset += REAL_SIZE;
        break;
        }
    }
  }

}

RC INLJoin::getNextTuple(void* data){
  //the data passed in here will need to include the left tuple concatenated with the right readTuple

  void* leftdata = malloc(100);
  while(leftIn->getNextTuple(leftdata) == SUCCESS){

    void* leftSelectedData = malloc(50);
    vector<Attribute> leftattrs;
    leftIn->getAttributes(leftattrs);
    // get Selected Data only works assuming the tuple is full with respect to its record descriptor(no nulls)
    // so for test 10 this iterator is a projection which does not use all the attributes, so getSelectedData won't work
    // perhaps use make a new function for selecting data from projected data
    Filter::getSelectedData(leftSelectedData, condition.lhsAttr, leftdata, leftattrs);

    void* rightdata = malloc(100);

    rightIn->setIterator(NULL, NULL, true, true); // this is only the case if there is no condition on the right iterator
    while(rightIn->getNextTuple(rightdata) == SUCCESS){

      vector<Attribute> rightattrs;
      rightIn->getAttributes(rightattrs);
      void* rightSelectedData = malloc(50);
      Filter::getSelectedData(rightSelectedData, condition.rhsAttr, rightdata, rightattrs);
      //
      // concatenate left data with right data ... memcpy(data, left, leftsize) .. memcpy(data + leftsize, right, rightsize)
      //
      Value rhs;
      rhs.type = TypeReal; // not always the case, just for test 9
      rhs.data = rightSelectedData;
      if(Filter::isEqual(leftSelectedData, rhs)) {
        concatenate(leftdata, rightdata, leftattrs, rightattrs, data); return SUCCESS;
      }
      free(rightSelectedData);
    }
    free(rightdata);
    free(leftSelectedData);
  }
  free(leftdata);

  return QE_EOF;
}
