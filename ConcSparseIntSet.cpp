/* Vaivaswatha N: GPL v3.0.
 * The sparse bit vector here uses a concurrent skip
 * list as the base container to handle data concurrently.
 * This implementation of concurrent skip lists
 * is based on the paper by Herlihy et al, titled
 * "A provably correct scalable concurrent skip list"
 */

#include "ConcSparseIntSet.h"

ConcSkipList::DeletedNodesList ConcSkipList::deletedNodes;

void ConcSkipList::initLSentinalLinks(void) 
{
    int layer;
    for (layer = 0; layer < MAX_HEIGHT; layer++)
	lSentinal.nexts[layer] = &rSentinal;
}

int ConcSkipList::findNode(int64_t key, Node const *preds[], Node const *succs[]) const
{
    int lFound = -1, layer;
    const Node *pred = &lSentinal, *cur;
    
    // traverse, from the top most linked list ...
    for (layer = MAX_HEIGHT-1; layer >= 0; layer--) {
	cur = pred->nexts[layer];
	// traverse at height "layer" as far as possible
	while (cur->key < key) {
	    pred = cur;
	    cur = pred->nexts[layer];
	}
	// below may be optimized as mentioned in the paper
	if (lFound == -1 && key == cur->key) {
	    lFound = layer;
	}
	// last node with a key less than "key" encountered at "layer"
	preds[layer] = pred;
	// the node that succeeds preds[layer] (at "layer")
	succs[layer] = cur;
    }
    return lFound;
}

// returns a random number in the range 0 to MAX_HEIGHT-1.
// probability of 1 is 1/2, 2 is 1/4, 3 is 1/8 and so on ...
int ConcSkipList::getRandomHeight(void) const
{
    // code taken from http://eternallyconfuzzled.com/tuts/datastructures/jsw_tut_skip.aspx
    // this is a very nice article on skip lists. a must read.
    // The idea to get a random height is simple. Ideally, each successive layer of the skip
    // list must be half the length, compared to the previous layer lenght (balanced search tree!).
    // So, the probability that a node has height h+1 is half the probability that it has height h.
    // Instead of calling rand() multiple times, use the individual bits inside a randomly generated
    // number. Each bit is 0/1 with a probability of 1/2.

    static int bits = 0, reset = 0;
    int h, found = 0, max = MAX_HEIGHT;
    int bitsLocal = bits, resetLocal = reset;
	
    for (h = 0; !found; h++) {
	if (resetLocal == 0) {
	    // TODO: Use random_r since random() is not thread safe.
	    bitsLocal = random();
	    resetLocal = sizeof ( int ) * CHAR_BIT;
	}
	     
	found = bitsLocal & 1;
	bitsLocal = bitsLocal >> 1;
	--(resetLocal);
    }

    // could lead to race conditions, but thats ok
    bits = bitsLocal;
    reset = resetLocal;
 
    // at this point we must have a number between 1..INF.
    // anything > max must be equally distributed among 
    // the possible heights 1..max.
    if ( h > max ) {
	// this is a trick I discovered (not in the link above) which
	// seems to provide a more accurate exponential distribution.
	h = (h % max) + 1;
    }

    // this function must return a number from 0..MAX_HEIGHT-1, hence
    // decrement h by one (the caller uses this to index array, hence).
    h--;
    assert(h >= 0 && h < MAX_HEIGHT);
    return h;
}

ConcSkipList::ConcSkipList() : lSentinal(MinInt, MAX_HEIGHT-1, 0) , rSentinal(MaxInt, MAX_HEIGHT-1, 0) 
{ 
    lSentinal.fullyLinked = true;
    rSentinal.fullyLinked = true;
    initLSentinalLinks();
}

// if pKey not already present 
//   insert pKey with a default 0 value.
//   return pointer to newly inserted Node.
// else
//   return pointer to existing Node with key pKey.
ConcSkipList::Node *ConcSkipList::insert_default(uint32_t pKey)
{
    // the internal container for key is bigger than what
    // is supported from outside. This is to accomodate MaxInt and MinInt
    int64_t key = (int64_t) pKey;
    int topLayer = getRandomHeight();
    Node *preds[MAX_HEIGHT], *succs[MAX_HEIGHT], *nodeFound;
    Node *pred, *succ, *prevPred, *newNode;
    int lFound, highestLocked, layer;
    bool valid;
    
    while (true) {
	lFound = findNode(key, (const Node **)preds, (const Node **)succs);
	if (lFound != -1) {
	    // Node found ... 
	    nodeFound = succs[lFound];
	    // but is it being deleted?
	    if (!nodeFound->marked) {
		// not being deleted, but is it being just inserted (linked)?
		while (!nodeFound->fullyLinked) {
		    // the node is being linked, wait for
		    // it to be fully inserted (linked).
		    // TODL: this looks very bad, maybe
		    // add some delay? how much? how?
		    ;
		}
		return nodeFound;
	    }
	    // The node is being deleted, try again till 
	    // it has been fully deleted. findNode() will
	    // stop returning "found" once its deleted fully.
	    // TODO: is a wait needed here too?
	    continue;
	}
	highestLocked = -1;
	prevPred = NULL;
	valid = true;
	for (layer = 0; layer <= topLayer && valid; layer++) {
	    pred = preds[layer];
	    succ = succs[layer];
	    // don't want to lock the same node multiple times ...
	    if (pred != prevPred) {
		pred->lock.lock();
		highestLocked = layer;
		prevPred = pred;
	    }
	    valid = !pred->marked && !succ->marked && 
		pred->nexts[layer] == succ;
	}
	if (!valid) {
	    prevPred = NULL;
	    // release locks and continue
	    for (layer = 0; layer <= highestLocked; layer++) {
		pred = preds[layer];
		// don't want to unlock the same node multiple times ...
		if (pred != prevPred) {
		    pred->lock.unlock();
		    prevPred = pred;
		}
	    }
	    continue;
	}
	
	// if we're here, it means we've got all the necessary locks. its now
	// safe to do the linked list operations for lists at all levels.
	newNode = new Node(key, topLayer, 0);
	for (layer = 0; layer <= topLayer; layer++)  {
	    newNode->nexts[layer] = succs[layer];
	    preds[layer]->nexts[layer] = newNode;
	}
	newNode->fullyLinked = true;
	prevPred = NULL;
	// release locks and return
	for (layer = 0; layer <= highestLocked && valid; layer++) {
	    pred = preds[layer];
	    // don't want to unlock the same node multiple times ...
	    if (pred != prevPred) {
		pred->lock.unlock();
		prevPred = pred;
	    }
	}
	return newNode;
    }
}

// not thread safe
ConcSkipList& ConcSkipList::operator= (const ConcSkipList &rhs)
{
    Node *prev[MAX_HEIGHT];

    for (int ii = 0; ii < MAX_HEIGHT; ii++)
	prev[ii] = &lSentinal;

    clearUnsafe();
    
    for (Node *rhsCur = rhs.lSentinal.nexts[0]; rhsCur != &rhs.rSentinal; 
	 rhsCur = rhsCur->nexts[0]) 
    {
	// wait until curNode is fullyLinked.
	// note that that i don't care much about marked (for deletion)
	// nodes. its always safe to iterate through marked nodes.
 	while (!rhsCur->fullyLinked) {
	    ;
	    // perhaps a better way to wait? see ConcSkipList::insert_default().
	}

	Node *newNode = new Node(*rhsCur);
	for (int layer = 0; layer <= rhsCur->topLayer; layer++) {
	    prev[layer]->nexts[layer] = newNode;
	    prev[layer] = newNode;
	}
    }

    // Finally finish linking the last node in each layer to rSentinal
    for (unsigned layer = 0; layer < MAX_HEIGHT; layer++) {
	prev[layer]->nexts[layer] = &rSentinal;
    }

    return (*this);
}


bool ConcSkipList::operator!= (const ConcSkipList &rhs) const
{
    return !((*this) == rhs);
}

bool ConcSkipList::operator== (const ConcSkipList &rhs) const
{
    Node *rhsCur, *lhsCur;

    for (rhsCur = rhs.lSentinal.nexts[0], lhsCur = lSentinal.nexts[0]; 
	 rhsCur != &rhs.rSentinal && lhsCur != &rSentinal; 
	 rhsCur = rhsCur->nexts[0], lhsCur = lhsCur->nexts[0]) 
    {
	// wait until curNode is fullyLinked.
	// note that that i don't care much about marked (for deletion)
	// nodes. its always safe to iterate through marked nodes.
	while (!rhsCur->fullyLinked || !lhsCur->fullyLinked) {
	    ;
	    // perhaps a better way to wait? see ConcSkipList::insert_default().
	}
	if (lhsCur->key != rhsCur->key ||
	    lhsCur->value != rhsCur->value)
	{
	    return false;
	}
    }

    if (lhsCur != &rSentinal ||
	rhsCur != &rhs.rSentinal)
    {
	return false;
    }

    return true;
}

bool ConcSkipList::contains(uint32_t pKey) const
{
    // the internal container for key is bigger than what
    // is supported from outside. This is to accomodate MaxInt and MinInt
    int64_t key = (int64_t) pKey;
    const Node *preds[MAX_HEIGHT], *succs[MAX_HEIGHT] ;
    int lFound = findNode (key, preds, succs);

    return (lFound != -1 &&
	    succs[lFound]->fullyLinked &&
	    !succs[lFound]->marked);
}

bool ConcSkipList::empty() const
{
    return lSentinal.nexts[0] == &rSentinal;
}

// this is the interface used for adding bits, by the covering
// sparse bit vector. this will atomically add "bit" to the node
// corresponding to pKey (with such a node being added newly if necessary).
// returns true if bit was newly set.
bool ConcSkipList::addKeyBit(uint32_t pKey, uint32_t bit) 
{
    Node *node;
    uint64_t value1, value2;

    assert(bit < wordSize);

    node = insert_default(pKey);
    assert(node != NULL);

    node->lock.lock();
    value1 = node->value;
    value2 = value1 | ((uint64_t)1 << bit);
    node->value = value2;
    node->lock.unlock();

    return (value1 != value2);
}

bool ConcSkipList::testKeyBit(uint32_t pKey, uint32_t bit) const
{
    int64_t key = (int64_t) pKey;
    const Node *preds[MAX_HEIGHT], *succs[MAX_HEIGHT] ;
    int lFound = findNode (key, preds, succs);

    assert(bit < wordSize);

    return (lFound != -1 &&
	    succs[lFound]->fullyLinked &&
	    !succs[lFound]->marked &&
	    (succs[lFound]->value & ((uint64_t)1 << bit)));

}

// not thread safe
void ConcSkipList::freeDeletedNodes(void)
{
    DeletedNodesList::iterator iter;

    // free elements that were deleted
    for (iter = deletedNodes.begin(); iter != deletedNodes.end(); iter++) {
	delete *iter;
    }
}

// not thread safe
void ConcSkipList::clearUnsafe(void) 
{
    Node *ptr1, *ptr2;
    
    // free elements on the list
    for (ptr1 = lSentinal.nexts[0]; ptr1 != &rSentinal; ptr1 = ptr2) {
	ptr2 = ptr1->nexts[0];
	delete ptr1;
    }

    initLSentinalLinks();
}

ConcSkipList::~ConcSkipList() 
{
    clearUnsafe();
}

ConcSkipList::ConcSkipListIterator::ConcSkipListIterator
(ConcSkipList &sl, ConcSkipList::Node *startFrom)
{
    // set this->sl
    this->sl = &sl;
    // set curNode
    if (startFrom == NULL) {
	this->curNode = sl.lSentinal.nexts[0];
    } else {
	// cannot start from lSentinal 
	// (doesn't make sense to do operator* then).
	assert(startFrom != &(sl.lSentinal));
	this->curNode = startFrom;
    }
    // set key/value
    if (curNode != &(sl.rSentinal)) {
	// curNode->key must hav a valid value.
	assert(curNode->key > MinInt && curNode->key < MaxInt);
	key = (uint32_t) curNode->key;
	value = (uint64_t) curNode->value;
    } else {
	// what does this mean?
	key = 0;
	value = 0;
    }
}
	
ConcSkipList::KeyValPair ConcSkipList::ConcSkipListIterator::operator* () const
{
    assert(curNode != &sl->lSentinal && curNode != &sl->rSentinal);
    return KeyValPair(key, value);
}

ConcSkipList::ConcSkipListIterator &ConcSkipList::ConcSkipListIterator::operator++ () 
{
    // cannot move past the end. also NULL is never possible.
    assert(curNode != NULL && curNode != &(sl->rSentinal));
    // this cannot happen as per above assert. just putting it
    // in for safety in non-debug code. is it a good thing?
    if (curNode == &sl->rSentinal)
	return *this;

    curNode = curNode->nexts[0];

    // wait until curNode is fullyLinked.
    // note that that i don't care much about marked (for deletion)
    // nodes. its always safe to iterate through marked nodes.
    while (!curNode->fullyLinked) {
	// perhaps a better way to wait? see ConcSkipList::insert_default().
	;
    }

    if (curNode != &(sl->rSentinal)) {
	// curNode->key must hav a valid value.
	assert(curNode->key > MinInt && curNode->key < MaxInt);
	key = (uint32_t) curNode->key;
	value = (uint64_t) curNode->value;
    } else {
	// what does this mean?
	key = 0;
	value = 0;
    }
    return *this;
}

bool ConcSkipList::ConcSkipListIterator::operator== (const ConcSkipList::ConcSkipListIterator &rhs) const
{
    return (sl == rhs.sl && curNode == rhs.curNode);
}

bool ConcSkipList::ConcSkipListIterator::operator!= (const ConcSkipList::ConcSkipListIterator &rhs)  const
{
    return !(*this == rhs);
}

ConcSkipList::ConcSkipListIterator ConcSkipList::begin()
{
    return iterator(*this);
}
ConcSkipList::ConcSkipListIterator ConcSkipList::end()
{
    return iterator(*this, &(this->rSentinal));
}

// Now start on the sparse bit vector using the concurrent skip list above.

static inline void get_base_off(uint32_t bit, uint32_t *base, uint32_t *off) 
{
    *base = bit / wordSize;
    *off = bit % wordSize;
}

static inline uint32_t get_bit_from_base_off(uint32_t base, uint32_t off)
{
    return ((base * wordSize) + off);
}

// return true if newly set
bool ConcSparseIntSet::test_and_set(uint32_t bit)
{
    uint32_t base, offset;
    
    get_base_off(bit, &base, &offset);
    return (this->sl.addKeyBit(base, offset));
}

void ConcSparseIntSet::set(uint32_t bit)
{
    uint32_t base, offset;
    
    get_base_off(bit, &base, &offset);
    this->sl.addKeyBit(base, offset);
}

bool ConcSparseIntSet::test(uint32_t bit) const
{
    uint32_t base, offset;

    get_base_off(bit, &base, &offset);
    return (this->sl.testKeyBit(base, offset));
}

bool ConcSparseIntSet::empty(void) const
{
    return sl.empty();
}

bool ConcSparseIntSet::operator== (const ConcSparseIntSet &rhs) const
{
    return sl == rhs.sl;
}

bool ConcSparseIntSet::operator!= (const ConcSparseIntSet &rhs) const
{
    return !((*this) == rhs);
}

ConcSparseIntSet& ConcSparseIntSet::operator= (const ConcSparseIntSet &rhs)
{
    sl = rhs.sl;
    return *this;
}


// returns the first set bit in "word", starting from "off".
// if there are none, it return wordSize.
uint32_t ConcSparseIntSet::ConcSparseIntSetIterator::firstSet(uint64_t val, uint32_t off) const
{
    // off == wordSize when iterator (caller) has reached end
    // we don't do anything in that case but just return the same.
    assert(off <= wordSize);
    uint64_t tmp = ((uint64_t) 1) << off;
    while (off < wordSize && (tmp & val) == 0) {
	off++;
	tmp = (tmp << 1);
    }
    return off;
}

ConcSparseIntSet::ConcSparseIntSetIterator::ConcSparseIntSetIterator
(ConcSkipList &sl, ConcSkipList::ConcSkipListIterator &sli) : sl(&sl), sli(sli)
{
    curOff = 0;

    if (sli == sl.end())
	return;

    // check if curOff bit is indeed set
    if ((*sli).second & ((uint64_t)1 << curOff)) {
	// yes it is, nothing more to do
	return;
    } else {
	// bit not set, look for the next set bit.
	++(*this);
    }
}

uint32_t ConcSparseIntSet::ConcSparseIntSetIterator::operator* () const
{
    uint32_t base, offset;
    // cannot indirect the iterator after it has reached its end.
    assert(sli != sl->end());

    base = (*sli).first;
    offset = curOff;

    return get_bit_from_base_off(base, offset);
}

ConcSparseIntSet::ConcSparseIntSetIterator &ConcSparseIntSet::ConcSparseIntSetIterator::operator++ ()
{
    assert(sli != sl->end());
    curOff = (curOff + 1);
    // if we've reached end, it'll be equal to wordSize, but never more
    assert (curOff <= wordSize); 
    curOff = firstSet((*sli).second, curOff);
    while (curOff == wordSize) {
	curOff = 0;
	// we need a loop because there are chances
	// of empty node, when it was just inserted
	// by another thread and it hasn't set a bit
	// yet. Hopefully this won't happen much.
	++sli;
	if (sli == sl->end()) {
	    curOff = 0; // makes it simpler to implement ==
	    return *this;
	}
	curOff = firstSet((*sli).second, curOff);
    }

    return *this;
}

bool ConcSparseIntSet::ConcSparseIntSetIterator::operator== (const ConcSparseIntSetIterator &rhs) const
{
    return (sl == rhs.sl && sli == rhs.sli && curOff == rhs.curOff);
}

bool ConcSparseIntSet::ConcSparseIntSetIterator::operator!= (const ConcSparseIntSetIterator &rhs) const
{
    return !(*this == rhs);
}

ConcSparseIntSet::ConcSparseIntSetIterator ConcSparseIntSet::begin()
{
    ConcSkipList::iterator sli = this->sl.begin();;
    return ConcSparseIntSetIterator(this->sl, sli);
}

ConcSparseIntSet::ConcSparseIntSetIterator ConcSparseIntSet::end()
{
    ConcSkipList::iterator sli = this->sl.end();;
    return ConcSparseIntSetIterator(this->sl, sli);
}

void ConcSparseIntSet::print(std::ostream &file)
{
    for (ConcSparseIntSet::iterator iter = begin(); iter != end(); ++iter) {
	file << (*iter) << " ";
    }
    file << std::endl;
}

// Everything below is just testing code
#ifdef TEST_CONC_SPARSE_INT_SET

#include <ctime>
#include <tbb/blocked_range.h>
#include <tbb/parallel_for.h>
#include <tbb/task_scheduler_init.h>
#include <tbb/concurrent_unordered_set.h>
typedef tbb::concurrent_unordered_set<int> RefSetType;

static const uint32_t size = 200000;
uint32_t input[size];
RefSetType ref;
ConcSkipList t1, t11;
ConcSparseIntSet t2, t21;

// for tbb's parallel_for
struct TestFuncKey {
    void operator() (const tbb::blocked_range<uint32_t> &range) const
    {
	uint32_t ii, val;

	for (ii = range.begin(); ii < range.end(); ii++) {
	    val = input[ii];
	    if (ref.find(val) != ref.end()) {
		// value already present, still try to insert
		t1.insert_default(val);
	    } else {
		// value not present
		ref.insert(val);
		t1.insert_default(val);
	    }
	}
    }
} testFuncKey;

struct TestFuncBit {
    void operator() (const tbb::blocked_range<uint32_t> &range) const
    {
	uint32_t ii, val;
	bool retVal;
	ConcSparseIntSet::iterator iter;

	for (ii = range.begin(); ii < range.end(); ii++) {
	    val = input[ii];
	    if (ref.find(val) != ref.end()) {
		// value already exists in the set.
		// See if the value can be reached through an iterator.
		// This is just to do some parallel testing for iterators.
		for (iter = t2.begin(); iter != t2.end(); ++iter) {
		    if (*iter == val)
			break;
		}
		assert(iter != t2.end());
	    } else {
		// value not present
		ref.insert(val);
		retVal = t2.test_and_set(val);
		assert(retVal == true);
	    }
	}
    }
} testFuncBit;

int main(void)
{
    uint32_t ii, val, retVal;
    time_t curTime;

    curTime = time(NULL);
    srandom(curTime);
    printf("seed=%lu\n", curTime);

    // PART1: Test only key insertion / contains. (ConcSkipList)

    // initial serial test.
    // insert elements randomly
    for (ii = 0; ii < 10000; ii++) {
	val = random() % 10000;
	if (ref.find(val) != ref.end()) {
	    // value already present, still try to insert
	    t1.insert_default(val);
	} else {
	    // value not present
	    ref.insert(val);
	    t1.insert_default(val);
	}
    }
    // verify inserted items
    for (RefSetType::iterator iter = ref.begin(); iter != ref.end(); iter++) {
	retVal = t1.contains(*iter);
	assert(retVal == true);
    }

    t11 = t1;
    assert(t11 == t1);

    // now a parallel test ...
    // fill some random input into an array (random() is not thread safe)
    for (ii = 0; ii < size; ii++) {
	input[ii] = random();
    }

    tbb::blocked_range<uint32_t> full_range(0, size, 5);
    tbb::task_scheduler_init init(4);

    // insert random elements, parallelly
    parallel_for(full_range, testFuncKey);

    // check if insertions happened correctly
    for (RefSetType::iterator iter = ref.begin(); iter != ref.end(); iter++) {
	retVal = t1.contains(*iter);
	assert(retVal == true);
    }
    // TODO: need to do some parallel testing for iterators too ...
    for (ConcSkipList::iterator iter = t1.begin(); iter != t1.end(); ++iter) {
	retVal = (ref.find((*iter).first) != ref.end());
	assert(retVal == true);
    }

    // PART2: test actual bit set/test. (ConcSparseIntSet)
    ref.clear();
    assert(t2.empty());

    // initial serial test.
    // insert elements randomly
    for (ii = 0; ii < 10000; ii++) {
	val = random() % 10000;
	if (ref.find(val) != ref.end()) {
	    // value already present, still try to insert
	    retVal = t2.test_and_set(val);
	    assert(retVal == false);
	} else {
	    // value not present
	    ref.insert(val);
	    retVal = t2.test_and_set(val);
	    assert(retVal == true);
	}
    }
    // verify inserted items
    for (RefSetType::iterator iter = ref.begin(); iter != ref.end(); iter++) {
	retVal = t2.test(*iter);
	assert(retVal == true);
    }

    // parallel now
    // insert random elements, parallelly
    parallel_for(full_range, testFuncBit);

    t21 = t2;
    assert(t21 == t2);
    unsigned tmp = random() % 29912; // random
    if (t21.test_and_set(tmp)) {
	ConcSparseIntSet t22 = t21, t23;
	assert(t21 != t2);
	t21 = t2;
	assert(t21 == t2 && t22 != t2 && t22 != t21);
	t23 = t21;
	assert(t23 == t21);
	if (t23.test_and_set(tmp + 1)) {
	    assert(t23 != t21 && t23 != t2 && t23 != t22);
	}
    }

    // check if insertions happened correctly
    for (RefSetType::iterator iter = ref.begin(); iter != ref.end(); iter++) {
	retVal = t2.test(*iter);
	assert(retVal == true);
    }

    for (ConcSparseIntSet::iterator iter = t2.begin(); iter != t2.end(); ++iter) {
	retVal = (ref.find(*iter) != ref.end());
	assert(retVal == true);
    }

    assert(!t2.empty());

    return 0;
}

#endif // TEST_CONC_SPARSE_INT_SET
