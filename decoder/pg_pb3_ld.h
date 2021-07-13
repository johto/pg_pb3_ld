#ifndef PG_PROTO3_LD_H
#define PG_PROTO3_LD_H

#include "postgres.h"

#include "lib/stringinfo.h"

/* utils.c */

typedef struct {
	Oid min;
	Oid max;
} PB3LD_Oid_Range;

extern PB3LD_Oid_Range *pb3ld_parse_binary_oid_ranges(const char *input);

/* pg_pb3_ld.c */

typedef enum {
	PB3LD_FSD_TYPE_OIDS_DISABLED,
	PB3LD_FSD_TYPE_OIDS_OMIT_NULLS,
	PB3LD_FSD_TYPE_OIDS_FULL,
} PB3LD_FSD_Type_Oids_Mode;

typedef enum {
	PB3LD_FSD_FORMATS_DISABLED,
	PB3LD_FSD_FORMATS_LIBPQ,
	PB3LD_FSD_FORMATS_OMIT_NULLS,
	PB3LD_FSD_FORMATS_FULL,
} PB3LD_FSD_Formats_Mode;

typedef struct
{
	MemoryContext context;

	int32	protocol_version;

	bool	begin_messages_enabled;
	bool	commit_messages_enabled;

	bool	repl_identity_required;

	bool	type_oids_mode;
	PB3LD_Oid_Range *binary_oid_ranges;
	PB3LD_FSD_Formats_Mode formats_mode;

	bool	table_oids_enabled;
} PB3LD_Private;

/* pb3.c */

extern void pb3_append_int32(StringInfo s, int32 val);

extern void pb3_append_wmsg_header(StringInfo s, int32 msgtype);

extern void pb3_append_varint_key(StringInfo s, int32 field_number);
extern void pb3_append_varint_kv(StringInfo s, int32 field_number, int32 val);

extern void pb3_append_oid_kv(StringInfo s, int32 field_number, Oid oid);

extern void pb3_append_enum_kv(StringInfo s, int32 field_number, int32 value);

extern void pb3_append_string_kv(StringInfo s, int32 field_number, const char *str);

extern void pb3_append_bytes_kv(StringInfo s, int32 field_number, const char *bytes, int len);

extern void pb3_append_varlen_key(StringInfo s, int32 field_number);

extern void pb3_fix_reserved_length(StringInfo s, int reserved_start, int reserved_len);

#endif
