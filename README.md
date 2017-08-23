zebra
=====

```
Column-oriented binary format for immutable datasets
```

<img src="https://github.com/ambiata/zebra/raw/master/doc/zebra.jpg" width="350" align="right"/>

Overview
--------

`zebra` is a typed data format for storing arbitrary combinations of
sums, products and arrays in compressed form. It achieves high
compression by decomposing this data in to a "struct of arrays"
representation on disk. `zebra` stores data in blocks so that files can
be streamed without loading the entire file in memory.

Column-oriented
---------------

<img src="https://github.com/ambiata/zebra/raw/master/doc/row-vs-column.png" width="743" align="center"/>

_Image from the Dremel paper_

The "struct of arrays" representation used on disk is very similar to
that used by [Dremel](https://research.google.com/pubs/pub36632.html)
and [Parquet](https://parquet.apache.org/), and is known as column
oriented, as opposed to row or record oriented. This allows for higher
compression as similar data is grouped together. One notable difference
that Zebra has from Dremel and Parquet is that Zebra supports full sum
types (i.e. Rust-like enums) rather than just optional fields.

Compression
-----------

Zebra uses a number of techniques to achieve a relatively high
compression ratio, given the speed at which it can be encoded and
decoded.

When compressing integers, Zebra uses three techniques stacked on top of
each other. First, it uses a frame of reference encoding which offsets
each value in the column by the midpoint of the minimum and the maximum
so that big numbers because small numbers.

<img src="https://github.com/ambiata/zebra/raw/master/doc/frame-of-reference.png" width="767" align="center"/>

Zebra then uses zig-zag encoding to make all numbers positive, so that
small negative numbers can be encoded using a small number of bits.

<img src="https://github.com/ambiata/zebra/raw/master/doc/zig-zag-encoding.png" width="764" align="center"/>

Finally Zebra finds the maximum number of bits required to store each of
the numbers in the column. If that happens to be 6-bits as in the
example below, then 16 x 6-bit numbers can be packed in to 6 x 16-bit
numbers. All of these techniques are detailed and benchmarked in [Lemire
2012](https://arxiv.org/abs/1209.2137).

<img src="https://github.com/ambiata/zebra/raw/master/doc/bit-packing.png" width="773" align="center"/>

For strings, Zebra simply uses Snappy compression, however because data
is organised in columns, it achieves a higher than usual compression
ratio.

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
