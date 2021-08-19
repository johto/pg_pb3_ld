#ifndef PG_PROTO3_LD_H
#define PG_PROTO3_LD_H

#include "postgres.h"

#include "access/htup_details.h"
#include "lib/stringinfo.h"
#include "replication/output_plugin.h"

#define NUM_MAX_COLUMNS (MaxHeapAttributeNumber + 1)

struct PB3LD_Private;

/* utils.c */

typedef struct {
	Oid min;
	Oid max;
} PB3LD_Oid_Range;

extern PB3LD_Oid_Range *pb3ld_parse_binary_oid_ranges(const char *input);
extern void pb3ld_wire_message_begin(struct PB3LD_Private *privdata, int32 msgtype);
extern void pb3ld_wire_message_end(struct PB3LD_Private *privdata, int32 msgtype);
extern bool pb3ld_should_flush_message_buffer(struct PB3LD_Private *privdata);
extern void pb3ld_flush_message_buffer(struct PB3LD_Private *privdata, StringInfo out);

/* fsd.c */

typedef struct {
	const struct PB3LD_Private *privdata;

	int num_columns;

	const char *names[NUM_MAX_COLUMNS];
	const char *values[NUM_MAX_COLUMNS];
	int value_lengths[NUM_MAX_COLUMNS];
	Oid type_oids[NUM_MAX_COLUMNS];
	bool nulls[NUM_MAX_COLUMNS];
	bool binary_formats[NUM_MAX_COLUMNS];
} PB3LD_FieldSetDescription;

extern void fsd_init(PB3LD_FieldSetDescription *fsd, const struct PB3LD_Private *privdata);
extern void fsd_reset(PB3LD_FieldSetDescription *fsd);
extern void fsd_populate_from_tuple(PB3LD_FieldSetDescription *fds,
									Relation relation,
									ReorderBufferTupleBuf *tuple);
extern void fsd_populate_via_index(PB3LD_FieldSetDescription *fds,
								   Relation relation,
								   ReorderBufferTupleBuf *tuple,
								   Oid rd_replidindex);
extern void fsd_serialize(PB3LD_FieldSetDescription *fsd, int32 field_number, StringInfo out);

/* pg_pb3_ld.c */

typedef enum {
	PB3LD_FSD_TYPE_OIDS_DISABLED,
	PB3LD_FSD_TYPE_OIDS_OMIT_NULLS,
	PB3LD_FSD_TYPE_OIDS_FULL,
} PB3LD_FSD_Type_Oids_Mode;

typedef enum {
	PB3LD_FSD_FORMATS_DISABLED,
	PB3LD_FSD_FORMATS_OMIT_NULLS,
	PB3LD_FSD_FORMATS_FULL,
} PB3LD_FSD_Formats_Mode;

typedef struct {
	char *schema_name;
	char *table_name;
	Oid table_oid;
} PB3LD_TableDescription;

typedef struct PB3LD_Private
{
	/*
	 * A memory context for the change callback to use.  This is reset after
	 * every change to avoid leaking memory used by type output functions etc.
	 */
	MemoryContext change_context;

	/* Pre-allocated memory for the change code to work with. */
	PB3LD_FieldSetDescription change_fsd;

	int32	protocol_version;

	bool	sent_message_this_transaction;
	int		wire_message_target_size;

	/*
	 * A memory context for header_buf and message_buf.  Only used for
	 * tracking usage, and not actually reset.
	 */
	MemoryContext buf_context;
	StringInfo header_buf;
	StringInfo message_buf;

	bool	begin_messages_enabled;
	bool	commit_messages_enabled;

	bool	repl_identity_required;

	PB3LD_FSD_Type_Oids_Mode type_oids_mode;
	PB3LD_Oid_Range *binary_oid_ranges;
	PB3LD_FSD_Formats_Mode formats_mode;

	bool	table_oids_enabled;
} PB3LD_Private;

/* protobuf.c */

extern void pb3_append_int32(StringInfo s, int32 val);

extern void pb3_append_wmsg_header(StringInfo s, int32 msgtype);

extern void pb3_append_varint_kv(StringInfo s, int32 field_number, int32 val);

extern void pb3_append_oid_kv(StringInfo s, int32 field_number, Oid oid);

extern void pb3_append_enum_kv(StringInfo s, int32 field_number, int32 value);

extern void pb3_append_string_kv(StringInfo s, int32 field_number, const char *str);

extern void pb3_append_bytes_kv(StringInfo s, int32 field_number, const char *bytes, int len);

extern void pb3_append_varint_key(StringInfo s, int32 field_number);
extern void pb3_append_varlen_key(StringInfo s, int32 field_number);

#endif
