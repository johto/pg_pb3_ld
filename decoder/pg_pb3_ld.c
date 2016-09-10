#include "postgres.h"

#include "access/genam.h"
#include "access/sysattr.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "nodes/parsenodes.h"
#include "replication/output_plugin.h"
#include "replication/logical.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "pg_pb3_ld.h"

#define PB3LD_WMSG_BEGIN	0
#define PB3LD_WMSG_COMMIT	1
#define PB3LD_WMSG_INSERT	2
#define PB3LD_WMSG_UPDATE	3
#define PB3LD_WMSG_DELETE	4

/* InsertDescription */
#define PB3LD_INS_TABLE_DESC	1
#define PB3LD_INS_NEW_VALUES	3

/* UpdateDescription */
#define PB3LD_UPD_TABLE_DESC	1
#define PB3LD_UPD_KEY_FIELDS	3
#define PB3LD_UPD_NEW_VALUES	5

/* DeleteDescription */
#define PB3LD_DEL_TABLE_DESC	1
#define PB3LD_DEL_KEY_FIELDS	3

/* FieldSetDescription */
#define PB3LD_FSD_NAMES			2
#define PB3LD_FSD_VALUES		3
#define PB3LD_FSD_TYPE_OIDS		4
#define PB3LD_FSD_NULLS			5
#define PB3LD_FSD_FORMATS		6

/* TableDescription */
#define PB3LD_TD_SCHEMANAME		1
#define PB3LD_TD_TABLENAME		2
#define PB3LD_TD_TABLEOID		3

#define EXTERNAL_ONDISK_OK		true
#define EXTERNAL_ONDISK_NOTOK	false

PG_MODULE_MAGIC;

typedef struct
{
	const PB3LD_Private *privdata;

	Relation relation;
	StringInfoData nulls;
	StringInfoData formats;
} PB3LD_FieldSetDescription;

extern void _PG_init(void);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

static void pb3ld_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
						  bool is_init);
static void pb3ld_shutdown(LogicalDecodingContext *ctx);
static void pb3ld_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void pb3ld_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
							 XLogRecPtr commit_lsn);
static void pb3ld_fds_init(const PB3LD_Private *privdata,
						   PB3LD_FieldSetDescription *fds,
						   Relation relation);
static void pb3ld_fds_reset(PB3LD_FieldSetDescription *fds);
static void pb3ld_fds_append_null(PB3LD_FieldSetDescription *fds, bool isnull);
static void pb3ld_fds_write_nulls(PB3LD_FieldSetDescription *fds, StringInfo out);
static void pb3ld_fds_append_format(PB3LD_FieldSetDescription *fds, bool isnull, int format);
static void pb3ld_fds_write_formats(PB3LD_FieldSetDescription *fds, StringInfo out);
static bool pb3ld_fds_type_binary(const PB3LD_FieldSetDescription *fds, Oid typid);
static void pb3ld_fds_attribute(PB3LD_FieldSetDescription *fds,
								StringInfo s,
								const char *attname,
								Oid typid,
								Datum valdatum,
								bool isnull,
								bool external_ondisk_ok);
static void pb3ld_write_FieldSetDescription(PB3LD_FieldSetDescription *fds,
											const int reserved_len,
											StringInfo out,
											ReorderBufferTupleBuf *tuple,
											Oid rd_replidindex);
static void pb3ld_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
						 Relation relation, ReorderBufferChange *change);

void
_PG_init(void)
{
}

void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = pb3ld_startup;
	cb->shutdown_cb = pb3ld_shutdown;
	cb->begin_cb = pb3ld_begin_txn;
	cb->commit_cb = pb3ld_commit_txn;
	cb->change_cb = pb3ld_change;
}

static void
pb3ld_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
			  bool is_init)
{
	ListCell   *option;
	PB3LD_Private *privdata;

	(void) is_init;

	privdata = palloc(sizeof(PB3LD_Private));
	privdata->context = AllocSetContextCreate(ctx->context,
											  "PB3LD memory context",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);

	ctx->output_plugin_private = privdata;

	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

	privdata->begin_messages_enabled = false;
	privdata->commit_messages_enabled = true;

	privdata->repl_identity_required = true;

	privdata->type_oids_mode = PB3LD_FSD_TYPE_OIDS_DISABLED;
	privdata->binary_oid_ranges = NULL;
	privdata->num_binary_oid_ranges = 0;
	privdata->formats_mode = PB3LD_FSD_FORMATS_DISABLED;

	privdata->table_oids_enabled = false;

	foreach(option, ctx->output_plugin_options)
	{
		DefElem *elem = lfirst(option);

		Assert(elem->arg == NULL || IsA(elem->arg, String));

		if (strcmp(elem->defname, "enable_begin_messages") == 0)
		{
			if (elem->arg == NULL)
				privdata->begin_messages_enabled = true;
			else if (!parse_bool(strVal(elem->arg), &privdata->begin_messages_enabled))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "enable_commit_messages") == 0)
		{
			if (elem->arg == NULL)
				privdata->commit_messages_enabled = true;
			else if (!parse_bool(strVal(elem->arg), &privdata->commit_messages_enabled))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "type_oids_mode") == 0)
		{
			char *mode;

			if (elem->arg == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("formats_mode requires an argument")));
			mode = strVal(elem->arg);
			if (strcmp(mode, "disabled") == 0)
				privdata->type_oids_mode = PB3LD_FSD_TYPE_OIDS_DISABLED;
			else if (strcmp(mode, "omit_nulls") == 0)
				privdata->type_oids_mode = PB3LD_FSD_TYPE_OIDS_OMIT_NULLS;
			else if (strcmp(mode, "full") == 0)
				privdata->type_oids_mode = PB3LD_FSD_TYPE_OIDS_FULL;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("\"%s\" is not a valid value for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "binary_oid_ranges") == 0)
		{
			if (elem->arg == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("binary_oid_ranges requires an argument")));
			pb3ld_parse_binary_oid_ranges(privdata, strVal(elem->arg));
		}
		else if (strcmp(elem->defname, "formats_mode") == 0)
		{
			char *mode;

			if (elem->arg == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("formats_mode requires an argument")));
			mode = strVal(elem->arg);
			if (strcmp(mode, "disabled") == 0)
				privdata->formats_mode = PB3LD_FSD_FORMATS_DISABLED;
			else if (strcmp(mode, "libpq") == 0)
				privdata->formats_mode = PB3LD_FSD_FORMATS_LIBPQ;
			else if (strcmp(mode, "omit_nulls") == 0)
				privdata->formats_mode = PB3LD_FSD_FORMATS_OMIT_NULLS;
			else if (strcmp(mode, "full") == 0)
				privdata->formats_mode = PB3LD_FSD_FORMATS_FULL;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("\"%s\" is not a valid value for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));

		}
		else if (strcmp(elem->defname, "enable_table_oids") == 0)
		{
			if (elem->arg == NULL)
				privdata->table_oids_enabled = true;
			else if (!parse_bool(strVal(elem->arg), &privdata->table_oids_enabled))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								strVal(elem->arg), elem->defname)));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" = \"%s\" is not supported",
							elem->defname,
							elem->arg ? strVal(elem->arg) : "(null)")));
		}
	}
}

static void
pb3ld_shutdown(LogicalDecodingContext *ctx)
{
	PB3LD_Private *privdata = ctx->output_plugin_private;

	MemoryContextDelete(privdata->context);
}


/* BEGIN callback */
static void
pb3ld_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	PB3LD_Private *privdata = ctx->output_plugin_private;

	if (!privdata->begin_messages_enabled)
		return;

	OutputPluginPrepareWrite(ctx, true);
	pb3_append_wmsg_header(ctx->out, PB3LD_WMSG_BEGIN);
	OutputPluginWrite(ctx, true);
}

/* COMMIT callback */
static void
pb3ld_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					 XLogRecPtr commit_lsn)
{
	PB3LD_Private *privdata = ctx->output_plugin_private;

	if (!privdata->commit_messages_enabled)
		return;

	OutputPluginPrepareWrite(ctx, true);
	pb3_append_wmsg_header(ctx->out, PB3LD_WMSG_COMMIT);
	OutputPluginWrite(ctx, true);
}

static void
pb3ld_write_TableDescription(const PB3LD_Private *privdata, StringInfo out, Relation relation)
{
	const int reserved_len = 1;
	int reserved_start;
	Form_pg_class class_form;

	reserved_start = out->len;
	appendStringInfoSpaces(out, reserved_len);

	pb3_append_string_kv(out, PB3LD_TD_SCHEMANAME,
						   get_namespace_name(get_rel_namespace(RelationGetRelid(relation))));
	class_form = RelationGetForm(relation);
	pb3_append_string_kv(out, PB3LD_TD_TABLENAME,
						   NameStr(class_form->relname));

	if (privdata->table_oids_enabled)
		pb3_append_oid_kv(out, PB3LD_TD_TABLEOID, RelationGetRelid(relation));

	pb3_fix_reserved_length(out, reserved_start, reserved_len);
}

static void
pb3ld_fds_attribute(PB3LD_FieldSetDescription *fds,
					StringInfo s,
					const char *attname,
					Oid typid,
					Datum valdatum,
					bool isnull,
					bool external_ondisk_ok)
{
	if (isnull)
	{
		pb3ld_fds_append_null(fds, true);
		/* don't care about the format */
		pb3ld_fds_append_format(fds, true, 0);
		pb3_append_bytes_kv(s, PB3LD_FSD_VALUES, NULL, 0);
	}
	else
	{
		bool binary_output;
		Oid typoutput;
		bool typisvarlena;
		char *valuedata;
		int valuelen;

		binary_output = pb3ld_fds_type_binary(fds, typid);

		if (binary_output)
			getTypeBinaryOutputInfo(typid, &typoutput, &typisvarlena);
		else
			getTypeOutputInfo(typid, &typoutput, &typisvarlena);

		if (typisvarlena && VARATT_IS_EXTERNAL_ONDISK(valdatum))
		{
			if (external_ondisk_ok)
			{
				/*
				 * TOASTed datum whose value did not change.  The value itself
				 * is not written to WAL in this case, and in the real database
				 * it might have been VACUUMed away.  We don't really have any
				 * options other than to omit the column.
				 */
				return;
			}
			else
			{
				/* shouldn't happen */
				elog(ERROR, "attname %s of relation %u is VARATT_EXTERNAL_ONDISK",
						attname, RelationGetRelid(fds->relation));
			}
		}

		pb3ld_fds_append_null(fds, false);

		if (typisvarlena)
			valdatum = PointerGetDatum(PG_DETOAST_DATUM(valdatum));

		if (binary_output)
		{
			pb3ld_fds_append_format(fds, false, 1);
			bytea *val = OidSendFunctionCall(typoutput, valdatum);
			valuedata = VARDATA(val);
			valuelen = VARSIZE(val) - VARHDRSZ;
		}
		else
		{
			pb3ld_fds_append_format(fds, false, 0);
			valuedata = OidOutputFunctionCall(typoutput, valdatum);
			valuelen = strlen(valuedata);
		}

		pb3_append_bytes_kv(s, PB3LD_FSD_VALUES, valuedata, valuelen);
	}

	pb3_append_string_kv(s, PB3LD_FSD_NAMES, attname);

	switch (fds->privdata->type_oids_mode)
	{
		case PB3LD_FSD_TYPE_OIDS_DISABLED:
			break;
		case PB3LD_FSD_TYPE_OIDS_OMIT_NULLS:
			if (!isnull)
				pb3_append_oid_kv(s, PB3LD_FSD_TYPE_OIDS, typid);
			break;
		case PB3LD_FSD_TYPE_OIDS_FULL:
			pb3_append_oid_kv(s, PB3LD_FSD_TYPE_OIDS, typid);
			break;
		default:
			elog(ERROR, "unexpected type_oids_mode %d", (int) fds->privdata->type_oids_mode);
	}
}

static void
pb3ld_fds_init(const PB3LD_Private *privdata, PB3LD_FieldSetDescription *fds, Relation relation)
{
	memset(fds, 0, sizeof(PB3LD_FieldSetDescription));

	fds->privdata = privdata;

	fds->relation = relation;
	initStringInfo(&fds->nulls);
	if (privdata->formats_mode != PB3LD_FSD_FORMATS_DISABLED)
		initStringInfo(&fds->formats);
}

static void
pb3ld_fds_reset(PB3LD_FieldSetDescription *fds)
{
	fds->nulls.len = 0;
	if (fds->privdata->formats_mode != PB3LD_FSD_FORMATS_DISABLED)
		fds->formats.len = 0;
}

static void
pb3ld_fds_append_null(PB3LD_FieldSetDescription *fds, bool isnull)
{
	if (isnull)
		appendStringInfoChar(&fds->nulls, '\001');
	else
		appendStringInfoChar(&fds->nulls, '\000');
}

static void
pb3ld_fds_write_nulls(PB3LD_FieldSetDescription *fds, StringInfo out)
{
	pb3_append_bytes_kv(out, PB3LD_FSD_NULLS, fds->nulls.data, fds->nulls.len);
}

static void
pb3ld_fds_append_format(PB3LD_FieldSetDescription *fds, bool isnull, int format)
{
	switch (fds->privdata->formats_mode)
	{
		case PB3LD_FSD_FORMATS_DISABLED:
			return;
		case PB3LD_FSD_FORMATS_OMIT_NULLS:
			if (!isnull)
				appendStringInfoChar(&fds->formats, format);
			break;
		case PB3LD_FSD_FORMATS_LIBPQ:
			/* fallthrough; handled in pb3ld_fds_write_formats */
		case PB3LD_FSD_FORMATS_FULL:
			appendStringInfoChar(&fds->formats, format);
			break;
		default:
			elog(ERROR, "unexpected formats_mode %d", (int) fds->privdata->formats_mode);
	}
}

static void
pb3ld_fds_write_formats(PB3LD_FieldSetDescription *fds, StringInfo out)
{
	int i;

	switch (fds->privdata->formats_mode)
	{
		case PB3LD_FSD_FORMATS_DISABLED:
			return;
		case PB3LD_FSD_FORMATS_LIBPQ:
			for (i = 0;;i++)
			{
				if (i == fds->formats.len)
				{
					/* only text values; omit the "formats" field */
					return;
				}

				if (fds->formats.data[i] != 0)
					break;
			}
			pb3_append_bytes_kv(out, PB3LD_FSD_FORMATS, fds->formats.data, fds->formats.len);
			break;
		case PB3LD_FSD_FORMATS_OMIT_NULLS:
			/* fallthrough; handled in pb3ld_fds_attribute */
		case PB3LD_FSD_FORMATS_FULL:
			pb3_append_bytes_kv(out, PB3LD_FSD_FORMATS, fds->formats.data, fds->formats.len);
			break;
		default:
			elog(ERROR, "unexpected formats_mode %d", (int) fds->privdata->formats_mode);
	}
}

static bool
pb3ld_fds_type_binary(const PB3LD_FieldSetDescription *fds, Oid typid)
{
	Oid *r = fds->privdata->binary_oid_ranges;
	int i;

	if (r == NULL)
		return false;

	for (i = 0; i < fds->privdata->num_binary_oid_ranges; ++i)
	{
		if (typid < r[i*2])
			return false;
		else if (typid >= r[i*2] && typid <= r[i*2+1])
			return true;
	}
	return false;
}

/*
 * pb3ld_write_FieldSetDescription writes out a FieldSetDescription into the
 * output buffer "out".  The caller should have written the preceding varlen
 * key already.  "fds" should point to an initialized
 * PB3LD_FieldSetDescription, and must be reset before reuse.  If
 * rd_replidindex is a valid oid, the index with that oid is looked up, and
 * only the attributes that are part of that index are written out.
 *
 * A FieldSetDescription is an embedded message, so we must precede it with a
 * variable encoded length.  However, since we can't easily know the length of
 * the encoded message before we've written it out in its entirety, we reserve
 * some space and hope we got it right, or memmove accordingly if we didn't.
 * In most cases we reserve two bytes for the length.  That means that any
 * message with a length between 128 and 16383 bytes (inclusive) can be encoded
 * without having to memmove the data around.  However for the key fields in an
 * UPDATE or a DELETE we only reserve a single byte; the reasoning being that
 * most of the time we can fit everything in fewer than 128 bytes.
 */
static void
pb3ld_write_FieldSetDescription(PB3LD_FieldSetDescription *fds,
								const int reserved_len,
								StringInfo out,
								ReorderBufferTupleBuf *tuple,
								Oid rd_replidindex)
{
	TupleDesc tupdesc;
	HeapTuple htup;
	int natt;
	int reserved_start;

	reserved_start = out->len;
	appendStringInfoSpaces(out, reserved_len);

	htup = &tuple->tuple;
	tupdesc = RelationGetDescr(fds->relation);

	if (OidIsValid(rd_replidindex))
	{
		Relation indexrel;

		indexrel = index_open(rd_replidindex, AccessShareLock);
		for (natt = 0; natt < indexrel->rd_index->indnatts; natt++)
		{
			int					relattr = indexrel->rd_index->indkey.values[natt];
			Form_pg_attribute	attr;
			Datum				valdatum;
			bool				isnull;
			Oid					typid;

			attr = tupdesc->attrs[relattr - 1];

			if (attr->attisdropped || attr->attnum < 0)
				elog(ERROR, "attribute %d of index %u is dropped or a system column", natt, rd_replidindex);

			typid = attr->atttypid;
			valdatum = heap_getattr(htup, relattr, tupdesc, &isnull);
			pb3ld_fds_attribute(fds, out, NameStr(attr->attname),
								typid, valdatum, isnull, EXTERNAL_ONDISK_NOTOK);
		}
		index_close(indexrel, NoLock);
	}
	else
	{
		for (natt = 0; natt < tupdesc->natts; natt++)
		{
			Form_pg_attribute	attr;
			Datum				valdatum;
			bool				isnull;
			Oid					typid;

			attr = tupdesc->attrs[natt];

			if (attr->attisdropped || attr->attnum < 0)
				continue;

			typid = attr->atttypid;
			valdatum = heap_getattr(htup, natt + 1, tupdesc, &isnull);
			pb3ld_fds_attribute(fds, out, NameStr(attr->attname),
								typid, valdatum, isnull, EXTERNAL_ONDISK_OK);
		}
	}

	pb3ld_fds_write_nulls(fds, out);
	pb3ld_fds_write_formats(fds, out);

	pb3_fix_reserved_length(out, reserved_start, reserved_len);
}

static void
pb3ld_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
			 Relation relation, ReorderBufferChange *change)
{
	PB3LD_Private *privdata = ctx->output_plugin_private;
	char relreplident = relation->rd_rel->relreplident;
	Oid rd_replidindex = InvalidOid;
	PB3LD_FieldSetDescription fds;
	MemoryContext old;

	if (relreplident == REPLICA_IDENTITY_NOTHING)
	{
		/*
		 * System catalog and/or whatnot; don't replicate.
		 */
		return;
	}
	else if (relreplident == REPLICA_IDENTITY_DEFAULT)
	{
		if (change->action == REORDER_BUFFER_CHANGE_UPDATE ||
			change->action == REORDER_BUFFER_CHANGE_DELETE)
		{
			RelationGetIndexList(relation);
			rd_replidindex = relation->rd_replidindex;
			/* TODO */
			if (privdata->repl_identity_required && !OidIsValid(rd_replidindex))
			{
				Form_pg_class class_form;

				class_form = RelationGetForm(relation);
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("foo bar %s sucks", NameStr(class_form->relname))));
			}
		}
	}
	else if (relreplident != REPLICA_IDENTITY_FULL)
	{
		/* TODO: REPLICA_IDENTITY_INDEX */
		elog(ERROR, "unexpected replica identity %d", relreplident);
	}

	old = MemoryContextSwitchTo(privdata->context);

	OutputPluginPrepareWrite(ctx, true);

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			pb3_append_wmsg_header(ctx->out, PB3LD_WMSG_INSERT);

			pb3_append_varlen_key(ctx->out, PB3LD_INS_TABLE_DESC);
			pb3ld_write_TableDescription(privdata, ctx->out, relation);

			Assert(change->data.tp.newtuple != NULL);

			pb3ld_fds_init(privdata, &fds, relation);

			pb3_append_varlen_key(ctx->out, PB3LD_INS_NEW_VALUES);
			pb3ld_write_FieldSetDescription(&fds, 2, ctx->out, change->data.tp.newtuple, InvalidOid);

			if (change->data.tp.oldtuple != NULL)
				elog(ERROR, "oldtuple is not NULL in INSERT");
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			pb3_append_wmsg_header(ctx->out, PB3LD_WMSG_UPDATE);

			pb3_append_varlen_key(ctx->out, PB3LD_UPD_TABLE_DESC);
			pb3ld_write_TableDescription(privdata, ctx->out, relation);

			Assert(change->data.tp.newtuple != NULL);

			pb3ld_fds_init(privdata, &fds, relation);

			pb3_append_varlen_key(ctx->out, PB3LD_UPD_NEW_VALUES);
			pb3ld_write_FieldSetDescription(&fds, 2, ctx->out, change->data.tp.newtuple, InvalidOid);

			if (change->data.tp.oldtuple != NULL || OidIsValid(rd_replidindex))
			{
				ReorderBufferTupleBuf *keytuple;

				if (change->data.tp.oldtuple != NULL)
					keytuple = change->data.tp.oldtuple;
				else
					keytuple = change->data.tp.newtuple;

				pb3ld_fds_reset(&fds);

				pb3_append_varlen_key(ctx->out, PB3LD_UPD_KEY_FIELDS);
				pb3ld_write_FieldSetDescription(&fds, 2, ctx->out, keytuple, rd_replidindex);
			}
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			pb3_append_wmsg_header(ctx->out, PB3LD_WMSG_DELETE);

			pb3_append_varlen_key(ctx->out, PB3LD_DEL_TABLE_DESC);
			pb3ld_write_TableDescription(privdata, ctx->out, relation);

			if (change->data.tp.newtuple != NULL)
				elog(ERROR, "newtuple is not NULL in DELETE");

			if (change->data.tp.oldtuple != NULL)
			{
				pb3ld_fds_init(privdata, &fds, relation);

				pb3_append_varlen_key(ctx->out, PB3LD_DEL_KEY_FIELDS);
				pb3ld_write_FieldSetDescription(&fds, 1, ctx->out, change->data.tp.oldtuple, rd_replidindex);
			}
			break;
		default:
			elog(ERROR, "unexpected change action %d", change->action);
			break;
	}

	OutputPluginWrite(ctx, true);

	MemoryContextSwitchTo(old);
	MemoryContextReset(privdata->context);
}
