Zebra - Blizzard Shuffle Format
===============================

![](img/zebra-cat.jpg)

This repo will contain a library for reading and writing zebra files and
a command line tool for catting them.


## Format Description

```idris
file {
  header : header
  blocks : ? x block
}

header {
  "||ZEBRA||00000||" : 16 x u8

  --
  -- 'attr_schema_string' is a run of characters which describes the
  -- layout/format of the arrays, and the dimensionality:
  --
  --   b   - byte
  --   w   - word
  --   d   - double
  --   [?] - array
  --
  -- Example:
  --
  --   Maybe Int
  --
  -- Would be described as:
  --
  --   ww
  --
  -- Which means it is represented as:
  --
  --   is_some : word_array
  --   value   : word_array
  --
  -- Example:
  --
  --   struct Foo {
  --     foo0 : Maybe Int
  --     foo1 : String
  --     foo2 : List Bar
  --   }
  --
  --   struct Bar {
  --     bar0 : Bool
  --     bar1 : Maybe Int
  --   }
  --
  -- Would be described as:
  --
  --   ww[b][www]
  --
  -- Which means it is represented as:
  --
  --   foo0_is_some      : word_array
  --   foo0_value        : word_array
  --   foo1_length       : word_array
  --   foo1_bytes        : byte_array
  --   foo2_length       : word_array
  --   foo2_bar0         : word_array
  --   foo2_bar1_is_some : word_array
  --   foo2_bar1_value   : word_array
  --
  attr_count         : u32
  attr_name_length   : word_array attr_count
  attr_name_string   : byte_array
  attr_schema_length : word_array attr_count
  attr_schema_string : byte_array
}

block {
  -- invariant: block_size == sum of schema derived sizes

  block_size : u32

  --
  -- entity {
  --   hash    : word
  --   id      : string
  --   n_attrs : word
  -- }
  --
  entity_count      : u32
  entity_id_hash    : word_array entity_count
  entity_id_length  : word_array entity_count
  entity_id_string  : byte_array
  entity_attr_count : word_array entity_count

  --
  -- attr {
  --   id    : word
  --   count : word
  -- }
  --
  -- invariant: attr_count == sum entity_attr_count
  -- invariant: attr_ids are sorted for each entity
  --
  attr_count    : u32
  attr_id       : word_array attr_count
  attr_id_count : word_array attr_count

  --
  -- value {
  --   time         : word
  --   is_tombstone : word
  -- }
  --
  -- invariant: value_count == sum attr_id_count
  --
  value_count      : u32
  value_time_epoch : u64
  value_time_delta : word_array value_count
  value_tombstone  : word_array value_count

  --
  -- data {
  --   attr_id : word
  --   count   : word
  --   size    : word
  --   value   : array of ?
  -- }
  --
  -- 'data_value' contains flattened arrays of values, exactly how many
  -- arrays and what format is described by the schema in the header.
  --
  -- invariant: data_count == count of unique attr_ids
  -- invariant: data_id contains all ids referenced by attr_ids
  --
  data_count    : u32
  data_id       : word_array data_count
  data_id_count : word_array data_count
  data_size     : word_array data_count
  data_value    : ?
}

--
-- Byte arrays are expected to contain string data and will be
-- compressed using something like LZ4 or Snappy.
--
byte_array {
  size_compressed   : u32
  size_uncompressed : u32
  bytes             : size_compressed * u8
}

--
-- Word arrays pack sets of 64 integers in to BP64 encoded chunks. If
-- there isn't an exact multiple of 64 then the overflow integers are
-- stored after the chunks and encoded using VByte.
--
word_array n {
  size    : u32
  nbits   : (n `div` 64) x u8
  parts   : map bp64 nbits
  remains : (n `mod` 64) x vbyte
}

--
-- Integers compressed using bit packed words (BP64).
--
-- BP64 requires n x 64-bit words to store 64 n-bit words.
--
-- For more information, see Lemire et al. [1] where they introduce
-- SIMD-BP128. This is an identical scheme which works over 128 n-bit
-- words. It may be useful to implement their SIMD version if we find
-- this to be a bottleneck.
--
-- 1. Lemire D, Boytsov L.
--    Decoding billions of integers per second through vectorization.
--    http://arxiv.org/abs/1209.2137
--
bp64 n {
  values : n x u64
}

--
-- Thrift / protocol buffer style variable length integers.
--
-- Known as VByte encoding in the integer compression literature.
--
vbyte {
  bytes : ?
}
```

## Example

### Schema

```
ape   : Bool
bat   : Int
cobra : Double
dog   : String
eagle : List Int
fish  : List String
goat  : Goat
hawk  : List Hawk
ibis  : List (List Int)

struct Hawk {
  name   : String
  height : Int
  goats  : List Goat
}

struct Goat {
  name : String
  legs : Maybe Int
}
```

### Schema - Encoded

```
ape   : w
bat   : w
cobra : d
dog   : [b]
eagle : [w]
fish  : [[b]]
goat  : [b]ww
hawk  : [[b]w[[b]ww]]
ibis  : [[w]]
```

### Facts

```
E1|ape|true|2016-01-01
E1|ape|NA|2016-01-02
E1|ape|true|2016-01-03

E1|bat|123|2016-01-01
E1|bat|456|2016-02-01
E1|bat|NA|2016-03-01

E1|cobra|123.456|2016-01-01T00:00:00
E1|cobra|234.567|2016-01-01T00:01:00
E1|cobra|345.678|2016-01-01T00:02:00

E1|dog|maltese|2016-01-01
E1|dog|pug|2016-02-01

E1|eagle|[9,8,7,6]|2016-01-01
E1|eagle|[9,8,6]|2016-02-01
E1|eagle|[9,8]|2016-03-01

E1|fish|["haskell"]|2016-01-01
E1|fish|["haskell","rust"]|2016-02-01
E1|fish|["bash"]|2016-03-01
E1|fish|NA|2016-04-01

E1|goat|{"name": "Goaty McGoatface"}|2016-01-01
E1|goat|{"name": "Goaty McGoatface", "legs": 4}|2016-02-01

E1|hawk|[{"name": "Harold", height: 176, goats: []}]|2016-01-01
E1|hawk|[{"name": "Harold", height: 176, goats: []}, {"name": "Denise", height: 201, goats: [{"name": "Beryl", "legs": 3}]}]|2016-02-01

E1|ibis|[[1,0,0,0],[0,1,0,0],[0,0,1,0],[0,0,0,1]]|2016-01-01
E1|ibis|[[2,0,0,0],[0,2,0,0],[0,0,2,0],[0,0,0,2]]|2016-02-01

E2|bat|777|2016-02-01
E2|bat|666|2016-03-01
E2|bat|555|2016-04-01

E2|dog|jack russell|2016-01-01
E2|dog|pomeranian|2016-02-01
E2|dog|NA|2016-03-01
```

### Facts - Encoded

#### Header

```
   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1| Byte
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | identifier & version "||ZEBRA||00000||"                       ~ 4
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 8
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 12
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               | 16
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1| Byte
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | attr_count : u32 = 9                                          | 20
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | attr_name_length.size : u32 = 9                               | 24
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | attr_name_length.remains : 9 x u8 =                           ~ 28
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~   3,3,5,3,5,4,4,4,4                                           ~ 32
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~               | attr_name_string.size_compressed : u32 = 35   ~ 36
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~               | attr_name_string.size_uncompressed : u32 = 35 ~ 40
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~               | attr_name_string.bytes : 35 x u8 =            ~ 44
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                   "apebatcobradogeaglefishgoathawkibis"       ~ 48
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 52
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 56
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 60
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 64
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 68
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 72
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               | 76
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1| Byte
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | attr_schema_length.size : u32 = 9                             | 80
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | attr_schema_length.remains : 9 x u8 =                         ~ 84
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~   1,1,1,3,3,5,5,13,5                                          ~ 88
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~               | attr_schema_string.size_compressed : u32 = 37 ~ 92
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~               | attr_schema_string.size_uncompressed : u32 = 37 ~ 96
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~               | attr_schema_string.bytes : 37 x u8 =          ~ 100
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                   "wwd[b][w][[b]][b]ww[[b]w[[b]ww]][[w]]"     ~ 104
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 108
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 112
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 116
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 120
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 124
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 128
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 132
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                               | 134
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

#### Block

```
   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1| Byte
                                  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                                  | block_size : u32 = ?          | 136
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  |                               | 138
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  Some field names have been abbreviated to give us more space:
    - s/entity_id/eid
    - s/entity_attr_count/ea_cnt

   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1| Byte
                                  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                                  | entity_count : u32 = 2        ~ 140
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                               | eid_hash.size : u32 = 2       ~ 144
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                               | eid_hash.remains : 2 x u8 = 1,2 | 152
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | eid_length.size : u32 = 2     | eid_length.remains : 2 x u8 = 2,2 | 156
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | eid_string.compressed_size : u32 = 4                          | 160
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | eid_string.uncompressed_size : u32 = 4                        | 164
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | eid_string.bytes : 4 x u8 = "E1E2"                            | 168
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | ea_cnt.size : u32 = 2                                         | 172
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | ea_cnt.remains : 2 x u8 = 9,2 | 174
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1| Byte
                                  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                                  | attr_count : u32 = 11         ~ 176
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                               | attr_id.size : u32 = 11       ~ 180
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                               | attr_id.remains : 11 x u8 =   ~ 184
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                   0,1,2,3,4,5,6,7,8,1,2       ~ 188
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                               |               ~ 192
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~ attr_id_count.size : u32 = 11                 |               ~ 196
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~ attr_id_count.remains : 11 x u8 = 3,3,3,2,3,4,2,2,2,3,3       ~ 200
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 204
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                               | 206
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  For my sanity, I'm only going to provide an example of the 'hawk'
  attribute. So in reality we would have value_count = 30 below, but I'm
  only going to have value_count = 2.

  The magic number 13121654400 below is the number of seconds between
  2016-01-01 and 1600-03-01. E1's hawk attribute gives the following
  timestamps:

    +------------+------------------+---------+
    | Time       | Secs since epoch | Delta   |
    +------------+------------------+---------+
    | 2016-01-01 | 13121654400      | 0       |
    | 2016-02-01 | 13124332800      | 2678400 |
    +------------+------------------+---------+

  Some field names have been abbreviated to give us more space:
    - s/value_time_epoch/vepoch
    - s/value_time_delta/vdelta
    - s/value_tombston/vtomb

   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1| Byte
                                  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                                  | value_count : u32 = 2         ~ 208
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                               | vepoch : u64 =  13121654400   ~ 212
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 216
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                               |                               ~ 220
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~ vdelta.size : u32 = 5         |                               ~ 224
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~ vdelta.remains : 5 x u8 = 0, 2678400          |               ~ 228
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~ vtomb.size : u32 = 2                          | vtomb.remains ~ 232
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~: 2 x u8 = 0,0 | 233
  +-+-+-+-+-+-+-+-+

  Some field names have been abbreviated to give us more space:
    - s/data_id/did
    - s/data_id_count/dic
    - s/data_size/ds
    - s/remains/r

   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1| Byte
                  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                  | data_count : u32 = 1                          ~ 236
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~               | did.size : u32 = 1                            ~ 240
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~               | did.r : 1 x u8| dic.size : u32 = 1            ~ 244
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                               | dic.r : 1 x u8|               ~ 248
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | ds.size : u32 = 2             | ds.r : 2 x u8                 | 252
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  The schema for hawk is:

    hawk : List Hawk

    struct Hawk {
      name   : String
      height : Int
      goats  : List Goat
    }

    struct Goat {
      name : String
      legs : Maybe Int
    }

  Which is described as:

    [[b]w[[b]ww]]

  And encoded as:

    h_len        : word_array
    h_name_len   : word_array
    h_name_bs    : byte_array
    h_height     : word_array
    hg_len       : word_array
    hg_name_len  : word_array
    hg_name_bs   : byte_array
    hg_legs_some : word_array
    hg_legs_val  : word_array

  So, given the following data:

    E1|hawk|[{"name": "Harold", height: 176, goats: []}]|2016-01-01
    E1|hawk|[{"name": "Harold", height: 176, goats: []}, {"name": "Denise", height: 201, goats: [{"name": "Beryl", "legs": 3}]}]|2016-02-01

  We should get:

   0                   1                   2                   3
   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1| Byte
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | h_len.size : u32 = 2                                          | 256
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | h_len.r : 2 x u8 = 1,2        | h_name_len.size : u32 = 3     ~ 260
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                               | h_name_len.r : 3 x u8 = 6,6,6 ~ 264
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                               | h_name_bs.size_cmp : u32 = 18 ~ 268
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                               | h_name_bs.size_ucmp : u32 = 18 ~ 272
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                               | h_name_bs.bytes : 18 x u8 =   ~ 276
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~ "HaroldHaroldDenise"                                          ~ 280
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 284
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               ~ 288
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~                                                               | 292
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | h_height.size : u32 = 3                                       | 296
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | h_height.r : 3 x u8 = 176,176,201             |               ~ 300
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~ hg_len.size : u32 = 3                         |               ~ 304
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~ hg_len.r : 3 x u8 = 0,0,1                     |               ~ 308
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~ hg_name_len.size : u32 = 1                    | hg_name_len : 1 x u8 = 5 ~ 316
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~ hg_name_bs.size_cmp : u32 = 5                 |               ~ 320
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~ hg_name_bs.size_ucmp : u32 = 5                |               ~ 324
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~ hg_name_bs.bytes : 5 x u8 = "Beryl"                           | 328
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~ hg_legs_some.size : u32 = 1                                   | 332
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  | hg_legs_some.r| hg_legs_val.size : u32 = 1                    ~ 336
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  ~               | hg_legs_val.r | 338
  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

## Appendix

The thrift schema for Ivory facts is provided for reference.

```thrift
struct ThriftTombstone {}

// Unfortunately Thrift doesn't (yet) support recursive types, or this could be ThriftFactValue :(
union ThriftFactPrimitiveValue {
    1: string s;
    2: i32 i;
    3: i64 l;
    4: double d;
    5: bool b;
    7: i32 date;
}

struct ThriftFactStructSparse {
    1: map<string, ThriftFactPrimitiveValue> v;
}

union ThriftFactListValue {
    1: ThriftFactPrimitiveValue p;
    2: ThriftFactStructSparse s;
}

struct ThriftFactList {
    1: list<ThriftFactListValue> l;
}

union ThriftFactValue {
    1: string s;
    2: i32 i;
    3: i64 l;
    4: double d;
    5: bool b;
    6: ThriftTombstone t;
    7: ThriftFactStructSparse structSparse;
    8: ThriftFactList lst;
    9: i32 date;
}

struct ThriftFact {
    1: string entity;
    2: string attribute;
    3: ThriftFactValue value;
    4: optional i32 seconds;
}
```
