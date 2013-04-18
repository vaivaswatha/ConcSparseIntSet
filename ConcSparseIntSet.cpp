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

#include <tbb/combinable.h>
#include <tbb/spin_mutex.h>
typedef tbb::spin_mutex Lock;

#define MAX_HEIGHT 4

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

	Node(signed long key, int topLayer) 
	{
	    this->key = key;
	    this->topLayer = topLayer;
	}
    } lSentinal, rSentinal;

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
    ConcSkipList() : lSentinal(MinInt, MAX_HEIGHT-1) , rSentinal(MaxInt, MAX_HEIGHT-1) { }

    // the interface will have "key<int> and value<ulong>" so as to make it
    // fit for using as sparse bit vector. internally uses a long key.
    bool insert(int pKey, unsigned long value)
    {
	signed long key = (signed long) pKey;
	///////////////////// TODO //////////
    }
};


int main(void)
{

}
