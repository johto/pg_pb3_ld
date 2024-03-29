#include <limits.h>

#include "postgres.h"

#include "utils/int8.h"

#include "pg_pb3_ld.h"

/* WireMessageHeader */
#define PB3LD_WHDR_TYPES	1
#define PB3LD_WHDR_OFFSETS	2

static Oid
pb3ld_parse_binary_oid_value(const char *value)
{
	int64 bigint;

	(void) scanint8(value, false, &bigint);
	if (bigint < 0)
		elog(ERROR, "oids can't be negative");
	else if (bigint == 0)
		elog(ERROR, "oid can't be InvalidOid (0)");
	else if (bigint > OID_MAX)
		elog(ERROR, "oids can't be larger than OID_MAX (%u)", OID_MAX);
	return (Oid) bigint;
}

static void
pb3ld_range_parse_error_callback(void *arg)
{
	errcontext("while parsing binary_oid_ranges range \"%s\"", (const char *) arg);
}

static void
pb3ld_parse_binary_oid_range(char *value, PB3LD_Oid_Range *range)
{
	ErrorContextCallback sqlerrcontext;
	char *hyphen;

	sqlerrcontext.callback = pb3ld_range_parse_error_callback;
	sqlerrcontext.arg = (void *) value;
	sqlerrcontext.previous = error_context_stack;
	error_context_stack = &sqlerrcontext;

	hyphen = strchr(value, '-');
	if (hyphen != NULL)
	{
		const char *unaltered = (const char *) pstrdup(value);

		sqlerrcontext.arg = (void *) unaltered;

		*hyphen = '\0';

		range->min = pb3ld_parse_binary_oid_value(value);
		range->max = pb3ld_parse_binary_oid_value(hyphen + 1);
		if (range->max < range->min)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("the upper bound of a range can't be lower than its lower bound in binary_oid_ranges")));
	}
	else
	{
		range->min = pb3ld_parse_binary_oid_value(value);
		range->max = range->min;
	}

	error_context_stack = sqlerrcontext.previous;
}

/*
 * pb3ld_parse_binary_oid_ranges parses a comma-separated list of oid ranges
 * into privdata->binary_oid_ranges.  privdata->num_binary_oid_ranges is set to
 * the number of ranges parsed.  An exception is raised on invalid input.
 */
PB3LD_Oid_Range *
pb3ld_parse_binary_oid_ranges(const char *input)
{
	int num_alloc;
	const char *nextp;
	PB3LD_Oid_Range *ranges;

	while (isspace((unsigned char) *input))
		input++;

	if (*input == '\0')
		return NULL;

	num_alloc = 1;
	nextp = input;
	for (;;) {
		nextp = strchr(nextp, ',');
		if (nextp == NULL)
			break;

		num_alloc++;

		nextp++;
		while (isspace((unsigned char) *nextp))
			nextp++;

		if (*nextp == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid input syntax for binary_oid_ranges")));
	}

	ranges = (PB3LD_Oid_Range *) palloc(sizeof(PB3LD_Oid_Range) * num_alloc + 1);
	ranges[num_alloc].min = InvalidOid;
	ranges[num_alloc].max = InvalidOid;

	nextp = input;
	for (int rangeno = 0; ; rangeno++)
	{
		const char *end;
		char *value;

		if (rangeno >= num_alloc)
			elog(ERROR, "internal error: rangeno %d >= num_alloc %d", rangeno, num_alloc);

		end = strchr(nextp, ',');
		if (end == NULL)
		{
			if (rangeno != num_alloc - 1)
				elog(ERROR, "internal error: rangeno %d != num_alloc - 1 %d", rangeno, num_alloc - 1);

			value = pstrdup(nextp);
		}
		else
		{
			Size len = (Size) (end - nextp);
			if (len == 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid input syntax for binary_oid_ranges")));

			value = pnstrdup(nextp, len);
		}

		pb3ld_parse_binary_oid_range(value, ranges + rangeno);
		if (rangeno > 0)
		{
			const PB3LD_Oid_Range previous = ranges[rangeno - 1];
			const PB3LD_Oid_Range current = ranges[rangeno];

			if (previous.max >= current.min)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("binary_oid_ranges range %u - %u overlaps with or precedes range %u - %u",
								previous.min, previous.max,
								current.min, current.max)));
		}

		if (end == NULL)
			break;
		nextp = end + 1;
	}

	return ranges;
}

void
pb3ld_wire_message_begin(PB3LD_Private *privdata, int32 msgtype)
{
	pb3_append_enum_kv(privdata->header_buf, PB3LD_WHDR_TYPES, msgtype);
	pb3_append_varint_kv(privdata->header_buf, PB3LD_WHDR_OFFSETS, privdata->message_buf->len);
}

void
pb3ld_wire_message_end(PB3LD_Private *privdata, int32 msgtype)
{
}

bool
pb3ld_should_flush_message_buffer(PB3LD_Private *privdata)
{
	return privdata->message_buf->len > privdata->wire_message_target_size;
}

void
pb3ld_flush_message_buffer(PB3LD_Private *privdata, StringInfo out)
{
	const int desired_alloc_len = privdata->wire_message_target_size * 2;

	Assert(privdata->message_buf->len > 0);
	Assert(privdata->header_buf->len > 0);

	pb3_append_int32(out, privdata->header_buf->len);
	appendBinaryStringInfo(out, privdata->header_buf->data, privdata->header_buf->len);
	appendBinaryStringInfo(out, privdata->message_buf->data, privdata->message_buf->len);

	privdata->sent_message_this_transaction = true;

	/*
	 * If we needed more memory than expected to process this message, release
	 * it now.
	 */
	if (privdata->message_buf->maxlen > desired_alloc_len)
	{
		MemoryContext oldcxt;

		pfree(privdata->message_buf->data);

		oldcxt = MemoryContextSwitchTo(privdata->buf_context);

		privdata->message_buf->data = palloc(desired_alloc_len);
		privdata->message_buf->maxlen = desired_alloc_len;

		MemoryContextSwitchTo(oldcxt);
	}

	resetStringInfo(privdata->header_buf);
	resetStringInfo(privdata->message_buf);
}
