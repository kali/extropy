
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
    - floating aggregates (à la window_summable)
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

Invariant lifecycle
===================
- initial (just configured, data state is undefined, proxy is noop)
- bootstrap
- run
- wait
- cleanup
