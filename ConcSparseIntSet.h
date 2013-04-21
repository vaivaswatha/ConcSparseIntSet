/* Vaivaswatha N: GPL v3.0.  */

#ifndef _CONC_SPARSE_INT_SET
#define _CONC_SPARSE_INT_SET

class ConcSkipList;

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
};

#endif // _CONC_SPARSE_INT_SET
