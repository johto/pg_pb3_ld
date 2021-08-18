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

/* TableDescription */
#define PB3LD_TD_SCHEMANAME		1
#define PB3LD_TD_TABLENAME		2
#define PB3LD_TD_TABLEOID		3

PG_MODULE_MAGIC;

extern void _PG_init(void);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

static void pb3ld_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
						  bool is_init);
static void pb3ld_shutdown(LogicalDecodingContext *ctx);
static void pb3ld_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void pb3ld_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
							 XLogRecPtr commit_lsn);
static void pb3ld_write_TableDescription(const PB3LD_Private *privdata,
										 StringInfo out,
										 Relation relation);
static void pb3ld_write_field_set_attribute(PB3LD_FieldSetDescription *fds,
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

	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

	privdata = palloc(sizeof(PB3LD_Private));
	privdata->change_context = AllocSetContextCreate(ctx->context,
													 "PB3LD change memory context",
													 ALLOCSET_DEFAULT_MINSIZE,
													 ALLOCSET_DEFAULT_INITSIZE,
													 ALLOCSET_DEFAULT_MAXSIZE);
	fsd_init(&privdata->change_fsd_new, privdata);
	fsd_init(&privdata->change_fsd_key, privdata);

	ctx->output_plugin_private = privdata;

	privdata->sent_message_this_transaction = false;
	/* TODO: this could be configurable */
	privdata->wire_message_target_size = 4 * 1024 * 1024;
	privdata->buf_context = AllocSetContextCreate(ctx->context,
												  "PB3LD internal buffer memory context",
												  ALLOCSET_DEFAULT_MINSIZE,
												  ALLOCSET_DEFAULT_INITSIZE,
												  ALLOCSET_DEFAULT_MAXSIZE);
	privdata->header_buf = makeStringInfo();
	privdata->message_buf = makeStringInfo();

	privdata->begin_messages_enabled = false;
	privdata->commit_messages_enabled = true;

	privdata->repl_identity_required = true;

	privdata->type_oids_mode = PB3LD_FSD_TYPE_OIDS_DISABLED;
	privdata->binary_oid_ranges = NULL;
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
			privdata->binary_oid_ranges = pb3ld_parse_binary_oid_ranges(strVal(elem->arg));
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

	enlargeStringInfo(privdata->message_buf, 2 * privdata->wire_message_target_size);
}

static void
pb3ld_shutdown(LogicalDecodingContext *ctx)
{
	PB3LD_Private *privdata = ctx->output_plugin_private;

	MemoryContextDelete(privdata->change_context);
	MemoryContextDelete(privdata->buf_context);
}


/* BEGIN callback */
static void
pb3ld_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	PB3LD_Private *privdata = ctx->output_plugin_private;

	Assert(privdata->header_buf->len == 0);
	Assert(privdata->message_buf->len == 0);

	privdata->sent_message_this_transaction = false;

	if (privdata->begin_messages_enabled)
	{
		pb3ld_wire_message_begin(privdata, PB3LD_WMSG_BEGIN);
		pb3ld_wire_message_end(privdata, PB3LD_WMSG_BEGIN);
	}
}

/* COMMIT callback */
static void
pb3ld_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					 XLogRecPtr commit_lsn)
{
	PB3LD_Private *privdata = ctx->output_plugin_private;

	if (!privdata->sent_message_this_transaction && privdata->header_buf->len == 0)
	{
		/* ignore transactions with no decoded changes */
		return;
	}

	if (privdata->commit_messages_enabled)
	{
		pb3ld_wire_message_begin(privdata, PB3LD_WMSG_COMMIT);
		pb3ld_wire_message_end(privdata, PB3LD_WMSG_COMMIT);
	}

	if (privdata->header_buf->len > 0)
	{
		OutputPluginPrepareWrite(ctx, true);
		pb3ld_flush_message_buffer(privdata, ctx->out);
		OutputPluginWrite(ctx, true);
	}
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
pb3ld_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
			 Relation relation, ReorderBufferChange *change)
{
	PB3LD_Private *privdata = ctx->output_plugin_private;
	char relreplident = relation->rd_rel->relreplident;
	Oid rd_replidindex = InvalidOid;
	MemoryContext oldcxt;

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

	oldcxt = MemoryContextSwitchTo(privdata->change_context);

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			Assert(change->data.tp.newtuple != NULL);

			fsd_reset(&privdata->change_fsd_new);
			fsd_populate_from_tuple(&privdata->change_fsd_new, relation, change->data.tp.newtuple);

			pb3ld_wire_message_begin(privdata, PB3LD_WMSG_INSERT);
			pb3_append_varlen_key(privdata->message_buf, PB3LD_INS_TABLE_DESC);
			pb3ld_write_TableDescription(privdata, privdata->message_buf, relation);
			fsd_serialize(&privdata->change_fsd_new, PB3LD_INS_NEW_VALUES, privdata->message_buf);
			pb3ld_wire_message_end(privdata, PB3LD_WMSG_INSERT);

			if (change->data.tp.oldtuple != NULL)
				elog(ERROR, "oldtuple is not NULL in INSERT");
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			Assert(change->data.tp.newtuple != NULL);

			if (change->data.tp.oldtuple != NULL || OidIsValid(rd_replidindex))
			{
				ReorderBufferTupleBuf *keytuple;

				if (change->data.tp.oldtuple != NULL)
					keytuple = change->data.tp.oldtuple;
				else
					keytuple = change->data.tp.newtuple;
			}
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			if (change->data.tp.newtuple != NULL)
				elog(ERROR, "newtuple is not NULL in DELETE");

			if (change->data.tp.oldtuple != NULL)
			{
			}

			break;
		default:
			elog(ERROR, "unexpected change action %d", change->action);
			break;
	}

	if (pb3ld_should_flush_message_buffer(privdata))
	{
		OutputPluginPrepareWrite(ctx, true);
		pb3ld_flush_message_buffer(privdata, ctx->out);
		OutputPluginWrite(ctx, true);
	}

	MemoryContextSwitchTo(oldcxt);
	MemoryContextReset(privdata->change_context);
}
