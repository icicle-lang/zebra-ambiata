Zebra v2 plan
-------------

Zebra is currently specialised to icicle/blizzard but is pretty complete when
it comes to a compressed binary data store for the sort of data that Ambiata
wants to store.

Ambiata also has a need for an encrypted, compressed binary data store and
rather than build that from scratch it has been decided that we should at least
explore the possibility of adapting as much code as possible from what is now
Zebra.

The current Zebra has some features and capabilities that are Icicle/Blizzard
specific and not needed in the new format. Current Zebra also has some
limitations/omissions with regards to what is needed for the new format.
However, there is a large amount of code currently in Zebra that would be useful
for the new format.

The plan is to convert the current zebra into a multiple repo project which
will contain code common to new and old formats, another with the existing
Zebra format and a third with the new format. The aim of doing it this way
is:

* Minimal disruption to current work on Zebra/Icicle/Blizzard.
* Reuse of as much existing code as possible.
* Reduction in development time for the new format.
* Possibility of switching Icicle to the new format at some time.


The work-in-progress spec for the new format is as follows:

Properties on disk:

* Encrypted (optional?, maybe AES CTR)
* Random access.
* Compressed.
* Stored column major to exploit redundancy.
* Types (primitives and lists of primitives).
* Correctness
  * Sort keys (validated) stored with the data.
  * Integrity checks
* No need for modify in place.


Operational properties:

* Ability to introspect on file (baked in schema).
* Fast stats like count.

Run time properties:

* Performance
* Throughput.
* Target pulling out bigger chunks rather than single row.

Required tooling:

* Library
* Cli
* Tool to convert to/from Walrus.


Comments on Zebra and how it measures up in comparison to desired storage format:

* Zebra doesn't do encyption.
* Zebra does do compression (uses Snappy to compress arrays of bytes (strings),
  and a combination of delta encoding, zig-zag enconding and BP64 to compress
  integers (as described in Lemire12)).
* Zebra uses column major storage to exploit redundancy.
* Zebra supports primitives, arbitrarily nested product types and the
  maybe/option type. There is a plan for how to encode full sum types rather
  than making option a special case.
* Zebra is currently specialised to be sorted by entity/attribute/time/priority
  which are baked in concepts at the moment, but there are plans to generalise
  this to arbitrary schemas which can be sorted by any set of fields.
* Zebra files are immutable.
* Currently, the Zebra schema/meta-data isn't fully self-describing of the
  original input.
* Zebra facts allows for dumping the file as human readable facts, but the
  schema in the header is not enough to be able to do this in a roundtrippable
  way.
* Zebra provides a number of human readable summaries not limited to
      ```
      zebra cat --summary
      zebra cat --entity-details
      zebra facts
      ```


Things to research and possibly steal ideas from:
* Google's [column oriented data store](https://research.google.com/pubs/pub36632.html).
* [HDF5](https://support.hdfgroup.org/HDF5/) ([in Haskell](https://github.com/mokus0/hs-hdf5)).
* [NetCDF](]http://www.unidata.ucar.edu/software/netcdf/).
