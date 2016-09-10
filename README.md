pg\_pb3\_ld
===========

Introduction
------------

This project is still a work in progress -- use at your own risk!

How to build
------------

The decoder plugin resides under the *decoder* subdirectory.  In order to build
it first make sure PGXS is installed, then enter the subdirectory and run
`make` followed by `make install`.

Setting up
----------

Before you can use logical decoding, you must set
[wal\_level](https://www.postgresql.org/docs/current/static/runtime-config-wal.html#GUC-WAL-LEVEL)
to logical and
[max\_replication\_slots](https://www.postgresql.org/docs/current/static/runtime-config-replication.html#GUC-MAX-REPLICATION-SLOTS)
to at least 1.  Now you can create a replication slot:

```
SELECT * FROM pg_create_logical_replication_slot('my_application', 'pg_pb3_ld');
```

and connect to it.  The replication level `START_REPLICATION` command can
specify a list of options which control various details of the decoding output.
The options supported by the plugin are:

##### enable\_begin\_messages (*bool*)

If enabled, a *BeginTransaction* message is sent at the beginning of each
decoded transaction.

The default is *false*.

##### enable\_commit\_messages (*bool*)

If enabled, a *CommitTransaction* message is sent at the end of each decoded
transaction.

The default is *true*.

##### type\_oids\_mode (*enum*)

Controls how the `type_oids` field in *FieldSetDescription* messages is written.

There are three supported modes:

  1. In `disabled` mode the `type_oids` protocol fields are never present.
  2. In `omit_nulls` mode, the `type_oids` protocol fields contain only the
  type oids of non-NULL fields.
  3. In `full` mode the full list of type oids is always provided.

The default is *disabled*.

##### binary\_oid\_ranges (*oid range list*)

A comma-separated list of oid ranges to decode as binary values.  The minimum
and the maximum of a range should be separated by HYPHEN-MINUS (-).  Both the
minimum and the maximum of a range are inclusive, i.e. all ranges are closed.
A single value can be specified by omitting the maximum value and the
HYPHEN-MINUS character.  The ranges should appear in the list ordered by their
minimum value, and no two ranges should overlap.

The default is the empty list, meaning that all values are sent to the client
in text format.

Examples:

  1. `1-9999` sends all values of built-in types over in binary
  2. `17,20-21,23` sends bytea, int2, int4 and int8 values over in binary

##### formats\_mode (*enum*)

Controls how the `formats` field in *FieldSetDescription* messages is written.

There are three supported modes:

  1. In `disabled` mode the `formats` protocol fields are never present.
  2. In `libpq` mode the `formats` fields contain one byte per field in the
  set.  A zero byte ('\x00') means the value is in text format, and a one
  ('\x01') means the value is in binary.  As a special case, if this field is
  omitted, all values are in text format.
  3. The `full` mode works exactly the same way as `libpq` mode, except the
  field is never omitted.

The default is *disabled*.

##### enable\_table\_oids (*bool*)

If enabled, each *TableDescription* message includes the oid of the target
table in the *table_oid* field.

The default is *false*.

