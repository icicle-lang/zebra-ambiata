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


Things to research and possibly steal ideas from:
* Google's [column oriented data store](https://research.google.com/pubs/pub36632.html).
* [HDF5](https://support.hdfgroup.org/HDF5/) ([in Haskell](https://github.com/mokus0/hs-hdf5)).
