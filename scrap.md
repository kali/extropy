
Invariants
==========
- same document
    - created_at, updated_at
    - scalar fields: "normalize" strings for search
    - array
        - count of elements in an array
        - count of filtered elements from an array
        - sum,min,max,avg of a numeric field of some subdocument in a array with filtering
        - first/last value (might be a special case of the previous one)
    - changelog
- 1 to N documents
    - simple fields denormalization (propagate some editable author fields on a picture document) by _id
    - same, based on another field
    - soft delete / cascade soft delete / delete / unlink
- N to 1 documents
    - same ideas than array, link by _id
    - same, but link differently
- N to N document
    - followers arrays and followee arrayes of users stay consistent, link by _id
    - same, not linked by _id
- floating aggregates (à la window_summable)

Simple invariant lifecycle
==========================
- initial (just configured, data state is undefined, proxy is noop)
- bootstrap
- run

ScalarFieldSameDocument
=======================

GenericSameDocumentArray 
========================
- computeOneIsLocal = false
- computeOne(id, optionalDoc)
    - generic: runs a configurable pipeline op computing the value
- fixOne(id, optionalDoc)
    - pull doc with array, run compute, findAndModify optimistic locking on the array
- run
    - proxy catch
        - full document updates, computeOneIsLocal: computeOne() and set on the fly
        - $set on full array, computeOneIsLocal: computeOne() and set counter on the fly
        - proxy, other updates: perform the update then trigger fixOne() async
- bootstrap
    - worker iterate over *all* documents in the collection and performs a fixOne
        - can be paralleled (sharding-like)
    - proxy run fixOne after each update

SameDocumentCount GenericSameDocumentArray
============================================
- override computeLocal: .count()
- override proxy:
    - $push without $each and without $slice: { counter: { $inc: 1 } }
    - $push with $each and without $slice: { counter: { $inc: n } }
    - $pushAll without $slice: { counter: { $inc: n } }

SameDocumentSum < GenericSameDocumentArray
==========================================
- override computeOneIsLocal = true
- override computeOne: map( [project] ).count()