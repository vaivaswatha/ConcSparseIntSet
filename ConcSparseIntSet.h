/* Vaivaswatha N: GPL v3.0.  */

#ifndef _CONC_SPARSE_INT_SET
#define _CONC_SPARSE_INT_SET

#include <cstdlib>
#include <cstdio>
#include <climits>
#include <cassert>
#include <cstdint>
#include <utility>

#include <tbb/concurrent_vector.h>
#include <tbb/combinable.h>
#include <tbb/spin_mutex.h>
typedef tbb::spin_mutex Lock;

#define MAX_HEIGHT 4
// below is the wordSize for the value stored in each Node.
static const uint32_t wordSize = sizeof(uint64_t) * 8;

class ConcSkipList {
    // use a number just lesser/greater than the numbers
    // representable in uint32 (these are the open bounds).
    static const int64_t MinInt = -1, MaxInt = ((int64_t)UINT32_MAX+1);
    class Node {
    public:
	int64_t key;
	uint64_t value;
	
	int topLayer;
	
	Node *nexts[MAX_HEIGHT];
	bool marked;
	bool fullyLinked;
	Lock lock;

	Node(int64_t key, int topLayer, uint64_t value) 
	{
	    this->key = key;
	    this->topLayer = topLayer;
	    this->value = value;
	}
    } lSentinal, rSentinal;

    typedef tbb::concurrent_vector<Node *> DeletedNodesList;

    DeletedNodesList deletedNodes;
    void init(void);
    int findNode(int64_t key, Node *preds[], Node *succs[]);
    int getRandomHeight(void);
 public:
    ConcSkipList();
    // if pKey not already present 
    //   insert pKey with a default 0 value.
    //   return pointer to newly inserted Node.
    // else
    //   return pointer to existing Node with key pKey.
    Node *insert_default(uint32_t pKey);
    bool contains(uint32_t pKey);
    // this is the interface used for adding bits, by the covering
    // sparse bit vector. this will atomically add "bit" to the node
    // corresponding to pKey (with such a node being added newly if necessary).
    // returns true if bit was newly set.
    bool add_key_bit(uint32_t pKey, uint32_t bit);
    bool test_key_bit(uint32_t pKey, uint32_t bit);
    // not thread safe
    void clear_unsafe(void);
    ~ConcSkipList();

    typedef std::pair<uint32_t, uint64_t> KeyValPair;

    class ConcSkipListIterator {
	ConcSkipList *sl;
	Node *curNode;
	ConcSkipListIterator() {};
	
    public:
	uint32_t key;
	uint64_t value;

	// constructor for the iterator
	ConcSkipListIterator(ConcSkipList &sl, ConcSkipList::Node *startFrom = NULL);
	KeyValPair operator* ();
	ConcSkipListIterator &operator++ ();
	bool operator== (const ConcSkipListIterator &rhs);
	bool operator!= (const ConcSkipListIterator &rhs);
    };
    typedef ConcSkipListIterator iterator;
    
    iterator begin();
    iterator end();
};

// This skip list is designed to
// be suitable for use by a covering
// concurrent sparse bit vector class.
class ConcSparseIntSet {

    ConcSkipList *sl;

    // safe to define one privately to avoid wrong usage when using STL.
    ConcSparseIntSet(const ConcSparseIntSet &rhs) {};

 public:
    ConcSparseIntSet();
    ~ConcSparseIntSet();
    inline bool set(unsigned bit);
    inline bool test(unsigned bit);

    class ConcSparseIntSetIterator {
	ConcSkipList::iterator sli;
    };
    typedef ConcSparseIntSetIterator iterator;
    iterator begin();
    iterator end();
};

#endif // _CONC_SPARSE_INT_SET
