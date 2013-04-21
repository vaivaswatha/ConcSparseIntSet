/* Vaivaswatha N: GPL v3.0.
 * The sparse bit vector here uses a concurrent skip
 * list as the base container to handle data concurrently.
 * This implementation of concurrent skip lists
 * is based on the paper by Herlihy et al, titled
 * "A provably correct scalable concurrent skip list"
 */

#include "ConcSparseIntSet.h"

#include <cstdlib>
#include <cstdio>
#include <climits>
#include <cassert>

#include <tbb/concurrent_vector.h>
#include <tbb/combinable.h>
#include <tbb/spin_mutex.h>
typedef tbb::spin_mutex Lock;

#define MAX_HEIGHT 4
static const unsigned wordSize = sizeof(unsigned long) * 8;

// This class is just to ensure initialization of an int to 0.
class Int {
public:
    int val;
    Int() {
	val = 0;
    }
};
typedef tbb::combinable<Int> threadLocalInt;

class ConcSkipList {

    static const signed long MinInt = -1, MaxInt = LONG_MAX;
    class Node {
    public:
	signed long key;
	unsigned long value;
	
	int topLayer;
	
	Node *nexts[MAX_HEIGHT];
	bool marked;
	bool fullyLinked;
	Lock lock;

	Node(signed long key, int topLayer, unsigned long value) 
	{
	    this->key = key;
	    this->topLayer = topLayer;
	    this->value = value;
	}
    } lSentinal, rSentinal;

    typedef tbb::concurrent_vector<Node *> DeletedNodesList;

    DeletedNodesList deletedNodes;

    void init(void) {
	int layer;
	for (layer = 0; layer < MAX_HEIGHT; layer++)
	    lSentinal.nexts[layer] = &rSentinal;

	deletedNodes.clear();
    }

    int findNode(signed long key, Node *preds[], Node *succs[])
    {
	int lFound = -1, layer;
	Node *pred = &lSentinal, *cur;

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
    int getRandomHeight(void) 
    {
	// code taken from http://eternallyconfuzzled.com/tuts/datastructures/jsw_tut_skip.aspx
	// this is a very nice article on skip lists. a must read.
	// The idea to get a random height is simple. Ideally, each successive layer of the skip
	// list must be half the length, compared to the previous layer lenght (balanced search tree!).
	// So, the probability that a node has height h+1 is half the probability that it has height h.
	// Instead of calling rand() multiple times, use the individual bits inside a randomly generated
	// number. Each bit is 0/1 with a probability of 1/2.

	static threadLocalInt bits, reset;
	int h, found = 0, max = MAX_HEIGHT;
	
	for (h = 0; !found; h++) {
	    if (reset.local().val == 0) {
		// TODO: Use random_r since random() is not thread safe.
		bits.local().val = random();
		reset.local().val = sizeof ( int ) * CHAR_BIT;
	    }
	     
	    found = bits.local().val & 1;
	    bits.local().val = bits.local().val >> 1;
	    --(reset.local().val);
	}
 
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

public:
    ConcSkipList() : lSentinal(MinInt, MAX_HEIGHT-1, 0) , rSentinal(MaxInt, MAX_HEIGHT-1, 0) 
    { 
	init();
    }

    // if pKey not already present 
    //   insert pKey with a default 0 value.
    //   return pointer to newly inserted Node.
    // else
    //   return pointer to existing Node with key pKey.
    Node *insert_default(unsigned pKey)
    {
	signed long key = (signed long) pKey;
	int topLayer = getRandomHeight();
	Node *preds[MAX_HEIGHT], *succs[MAX_HEIGHT], *nodeFound;
	Node *pred, *succ, *prevPred, *newNode;
	int lFound, highestLocked, layer;
	bool valid;

	while (true) {
	    lFound = findNode(key, preds, succs);
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

    bool contains(unsigned pKey) {
	signed long key = (signed long) pKey;
	Node *preds[MAX_HEIGHT], *succs[MAX_HEIGHT] ;
	int lFound = findNode (key, preds, succs);

	return (lFound != -1 &&
		succs[lFound]->fullyLinked &&
		!succs[lFound]->marked);
    }

    // this is the interface used for adding bits, by the covering
    // sparse bit vector. this will atomically add "bit" to the node
    // corresponding to pKey (with such a node being added newly if necessary).
    // returns true if bit was newly set.
    bool add_key_bit(unsigned pKey, unsigned bit) {
	Node *node;
	unsigned long value1, value2;

	assert(bit < wordSize);

	node = insert_default(pKey);
	assert(node != NULL);

	node->lock.lock();
	value1 = node->value;
	value2 = value1 | (1 << bit);
	node->value = value2;
	node->lock.unlock();

	return (value1 != value2);
    }

    bool test_key_bit(unsigned pKey, unsigned bit) {
	signed long key = (signed long) pKey;
	Node *preds[MAX_HEIGHT], *succs[MAX_HEIGHT] ;
	int lFound = findNode (key, preds, succs);

	assert(bit < wordSize);

	return (lFound != -1 &&
		succs[lFound]->fullyLinked &&
		!succs[lFound]->marked &&
		(succs[lFound]->value | (1 << bit)));

    }

    // not thread safe
    void clear_unsafe(void) {
	Node *ptr1, *ptr2;
	DeletedNodesList::iterator iter;

	// free elements on the list
	for (ptr1 = lSentinal.nexts[0]; ptr1 != &rSentinal; ptr1 = ptr2) {
	    ptr2 = ptr1->nexts[0];
	    free(ptr1);
	}
	// free elements that were deleted
	for (iter = deletedNodes.begin(); iter != deletedNodes.end(); iter++) {
	    free(*iter);
	}

	init();
    }

    ~ConcSkipList() {
	clear_unsafe();
    }
};

// Now start on the sparse bit vector using the concurrent skip list above.

static inline void get_base_off(unsigned bit, unsigned *base, unsigned *off) {
    *base = bit / wordSize;
    *off = bit % wordSize;
}

ConcSparseIntSet::ConcSparseIntSet()
{
    sl = new ConcSkipList;
}

ConcSparseIntSet::~ConcSparseIntSet()
{
    delete sl;
}

bool ConcSparseIntSet::set(unsigned bit)
{
    unsigned base, offset;
    
    get_base_off(bit, &base, &offset);
    return (this->sl->add_key_bit(base, offset));
}

bool ConcSparseIntSet::test(unsigned bit)
{
    unsigned base, offset;

    get_base_off(bit, &base, &offset);
    return (this->sl->test_key_bit(base, offset));
}

// Uncomment below line to enable testing. Has a main() routine.
#define TEST_CONC_SPARSE_INT_SET

// Everything below is just testing code
#ifdef TEST_CONC_SPARSE_INT_SET

#include <tbb/blocked_range.h>
#include <tbb/parallel_for.h>
#include <tbb/task_scheduler_init.h>
#include <tbb/concurrent_unordered_set.h>
typedef tbb::concurrent_unordered_set<int> RefSetType;

static const unsigned size = 200000;
unsigned input[size];
RefSetType ref;
ConcSkipList t1;
ConcSparseIntSet t2;

// for tbb's parallel_for
struct TestFuncKey {
    void operator() (const tbb::blocked_range<unsigned> &range) const
    {
	unsigned ii, val;

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
    void operator() (const tbb::blocked_range<unsigned> &range) const
    {
	unsigned ii, val;
	bool retVal;

	for (ii = range.begin(); ii < range.end(); ii++) {
	    val = input[ii];
	    if (ref.find(val) != ref.end()) {
		// value already present, still try to insert
		retVal = t2.set(val);
		assert(retVal == false);
	    } else {
		// value not present
		ref.insert(val);
		retVal = t2.set(val);
		assert(retVal == true);
	    }
	}
    }
} testFuncBit;

int main(void)
{
    unsigned ii, val, retVal;

    srandom(time(NULL));

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

    // now a parallel test ...
    // fill some random input into an array (random() is not thread safe)
    for (ii = 0; ii < size; ii++) {
	input[ii] = random();
    }

    tbb::blocked_range<unsigned> full_range(0, size, 5);
    tbb::task_scheduler_init init(2);

    // insert random elements, parallelly
    parallel_for(full_range, testFuncKey);

    // check if insertions happened correctly
    for (RefSetType::iterator iter = ref.begin(); iter != ref.end(); iter++) {
	retVal = t1.contains(*iter);
	assert(retVal == true);
    }

    // PART2: test actual bit set/test. (ConcSparseIntSet)
    ref.clear();

    // initial serial test.
    // insert elements randomly
    for (ii = 0; ii < 10000; ii++) {
	val = random() % 10000;
	if (ref.find(val) != ref.end()) {
	    // value already present, still try to insert
	    retVal = t2.set(val);
	    assert(retVal == false);
	} else {
	    // value not present
	    ref.insert(val);
	    retVal = t2.set(val);
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

    // check if insertions happened correctly
    for (RefSetType::iterator iter = ref.begin(); iter != ref.end(); iter++) {
	retVal = t2.test(*iter);
	assert(retVal == true);
    }

    return 0;
}

#endif // TEST_CONC_SPARSE_INT_SET
