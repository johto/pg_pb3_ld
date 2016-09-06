#include "postgres.h"

#include "lib/stringinfo.h"

#include "pg_pb3_ld.h"

static int pb3_varint_size(int val);

void
pb3_append_int32(StringInfo s, int32 val)
{
	Assert(val >= 0);

	while (val > 127)
	{
		appendStringInfoCharMacro(s, (char) (0x80 | ((uint8) val & 0x7F)));
		val >>= 7;
	}
	appendStringInfoCharMacro(s, (char) ((uint8) val));
}

static void
pb3_append_uint32(StringInfo s, uint32 val)
{
	while (val > 127)
	{
		appendStringInfoCharMacro(s, (char) (0x80 | ((uint8) val & 0x7F)));
		val >>= 7;
	}
	appendStringInfoCharMacro(s, (char) ((uint8) val));
}

void
pb3_append_wmsg_header(StringInfo s, int32 msgtype)
{
	pb3_append_enum_kv(s, 1, msgtype);
}

void
pb3_append_varint_key(StringInfo s, int32 field_number)
{
	pb3_append_int32(s, (((int32) field_number) << 3) | 0);
}

void
pb3_append_varint_kv(StringInfo s, int32 field_number, int32 val)
{
	pb3_append_int32(s, (((int32) field_number) << 3) | 0);
	pb3_append_int32(s, val);
}

void
pb3_append_oid_kv(StringInfo s, int32 field_number, Oid oid)
{
	pb3_append_int32(s, (((int32) field_number) << 3) | 0);
	pb3_append_uint32(s, (uint32) oid);
}

void
pb3_append_enum_kv(StringInfo s, int32 field_number, int32 value)
{
	pb3_append_int32(s, (((int32) field_number) << 3) | 0);
	pb3_append_int32(s, value);
}

void
pb3_append_string_kv(StringInfo s, int32 field_number, const char *str)
{
	pb3_append_bytes_kv(s, field_number, str, strlen(str));
}

void
pb3_append_bytes_kv(StringInfo s, int32 field_number, const char *bytes, int len)
{
	Assert(len == 0 || bytes != NULL);

	pb3_append_int32(s, (((int32) field_number) << 3) | 2);
	pb3_append_int32(s, (int32) len);
	if (bytes != NULL)
		appendBinaryStringInfo(s, bytes, len);
}

void
pb3_append_varlen_key(StringInfo s, int32 field_number)
{
	pb3_append_int32(s, (((int32) field_number) << 3) | 2);
}

static int
pb3_varint_size(int val)
{
	int n = 1;

	for (;;)
	{
		Assert(val >= 0);

		val >>= 7;
		if (val == 0)
			return n;
		n++;
	}
}

void
pb3_fix_reserved_length(StringInfo s, int reserved_start, int reserved_len)
{
	char *reserved = s->data + reserved_start;
	int msg_len = s->len - reserved_start - reserved_len;
	int header_len = pb3_varint_size(msg_len);
	int off = header_len - reserved_len;

	if (off != 0)
	{
		if (off > 0)
		{
			enlargeStringInfo(s, off);
			/* enlargeStringInfo might have repalloc'd */
			reserved = s->data + reserved_start;
		}
		memmove(reserved + header_len, reserved + reserved_len, msg_len);

		s->len += off;
	}

	while (msg_len > 127)
	{
		*reserved++ = (char) (0x80 | ((uint8) msg_len & 0x7F));
		msg_len >>= 7;
	}
	*reserved++ = (char) ((uint8) msg_len);
}
