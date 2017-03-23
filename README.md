zebra
=====

```
Column-oriented binary format for immutable datasets
```

![](doc/zebra-cat.jpg)

Overview
--------

`zebra` is a typed data format for storing arbitrary combinations of
sums, products and arrays in compressed form. It achieves high
compression by decomposing this data in to a "struct of arrays"
representation on disk. `zebra` stores data in blocks so that files can
be streamed without loading the entire file in memory.

Mandatory Reviewers
-------------------

Given the current state of this library, its performance sensitivity,
and is criticality to ongoing production jobs that are still in a very
early/tentative state. There should be some mandatory review, for
production implications. Due to time and availability constraints this
is currently @jystic. So any reviews should hold off on merge until he
can sweep. Whilst this might be a bit more restrictive then we normally
are on most libraries, it is the safest option for the short term while
stability is low / churn is high, and it remains on critical path for
many production tasks.
