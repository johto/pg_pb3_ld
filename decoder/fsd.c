#include "postgres.h"

#include "access/genam.h"
#include "lib/stringinfo.h"
#include "replication/output_plugin.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#include "pg_pb3_ld.h"

/* FieldSetDescription */
#define PB3LD_FSD_NAMES			2
#define PB3LD_FSD_VALUES		3
#define PB3LD_FSD_TYPE_OIDS		4
#define PB3LD_FSD_NULLS			5
#define PB3LD_FSD_FORMATS		6

#define EXTERNAL_ONDISK_OK		true
#define EXTERNAL_ONDISK_NOTOK	false


static void fsd_add_attribute(PB3LD_FieldSetDescription *fsd,
							  Relation relation,
							  const char *attname,
							  Oid typid,
							  Datum valdatum,
							  bool isnull,
							  bool external_ondisk_ok);

static bool fsd_should_output_binary_for_type(const PB3LD_FieldSetDescription *fsd, Oid typid);

void
fsd_init(PB3LD_FieldSetDescription *fsd, const PB3LD_Private *privdata)
{
	fsd->privdata = privdata;
	fsd_reset(fsd);
}

void
fsd_reset(PB3LD_FieldSetDescription *fsd)
{
	const int next = 0;

	fsd->num_columns = 0;

	fsd->names[next] = NULL;
	fsd->values[next] = NULL;
	fsd->type_oids[next] = InvalidOid;
}

void
fsd_populate_from_tuple(PB3LD_FieldSetDescription *fsd,
						Relation relation,
						ReorderBufferTupleBuf *tuple)
{
	TupleDesc tupdesc;
	HeapTuple htup;
	int natt;

	htup = &tuple->tuple;
	tupdesc = RelationGetDescr(relation);

	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		Form_pg_attribute attr;
		Datum valdatum;
		bool isnull;
		Oid typid;

		attr = TupleDescAttr(tupdesc, natt);
		if (attr->attisdropped || attr->attnum < 0)
			continue;

		typid = attr->atttypid;
		valdatum = heap_getattr(htup, natt + 1, tupdesc, &isnull);
		fsd_add_attribute(fsd, relation, NameStr(attr->attname), typid,
						  valdatum, isnull, EXTERNAL_ONDISK_OK);
	}
}

void
fsd_populate_via_index(PB3LD_FieldSetDescription *fsd,
					   Relation relation,
					   ReorderBufferTupleBuf *tuple,
					   Oid rd_replidindex)
{
	TupleDesc tupdesc;
	HeapTuple htup;
	Relation indexrel;
	int natt;

	htup = &tuple->tuple;
	tupdesc = RelationGetDescr(relation);
	indexrel = index_open(rd_replidindex, AccessShareLock);
	for (natt = 0; natt < indexrel->rd_index->indnatts; natt++)
	{
		int relattr = indexrel->rd_index->indkey.values[natt];
		Form_pg_attribute attr;
		Datum valdatum;
		bool isnull;
		Oid typid;

		attr = TupleDescAttr(tupdesc, relattr - 1);
		if (attr->attisdropped || attr->attnum < 0)
			elog(ERROR, "attribute %d of index %u is dropped or a system column", natt, rd_replidindex);

		typid = attr->atttypid;
		valdatum = heap_getattr(htup, relattr, tupdesc, &isnull);
		fsd_add_attribute(fsd, relation, NameStr(attr->attname), typid, valdatum, isnull, EXTERNAL_ONDISK_NOTOK);
	}
	index_close(indexrel, NoLock);
}

static void
fsd_add_attribute(PB3LD_FieldSetDescription *fsd,
				  Relation relation,
				  const char *attname,
				  Oid typid,
				  Datum valdatum,
				  bool isnull,
				  bool external_ondisk_ok)
{
	int current, next;

	current = fsd->num_columns;
	if (current >= NUM_MAX_COLUMNS)
	{
		elog(ERROR, "attname %s of relation %u exceeds maximum number of columns %d",
			 attname, RelationGetRelid(relation), NUM_MAX_COLUMNS);
	}

	next = current + 1;
	fsd->names[next] = NULL;
	fsd->values[next] = NULL;
	fsd->type_oids[next] = InvalidOid;

	if (isnull)
	{
		fsd->names[current] = attname;
		fsd->values[current] = "";
		fsd->value_lengths[current] = 0;
		fsd->type_oids[current] = typid;
		fsd->nulls[current] = true;
		fsd->binary_formats[current] = false;
		fsd->num_columns++;
	}
	else
	{
		bool binary_output;
		Oid typoutput;
		bool typisvarlena;
		char *valuedata;
		int valuelen;

		binary_output = fsd_should_output_binary_for_type(fsd, typid);

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
					 attname, RelationGetRelid(relation));
			}
		}

		if (typisvarlena)
			valdatum = PointerGetDatum(PG_DETOAST_DATUM(valdatum));

		if (binary_output)
		{
			bytea *val;

			val = OidSendFunctionCall(typoutput, valdatum);
			valuedata = VARDATA(val);
			valuelen = VARSIZE(val) - VARHDRSZ;
		}
		else
		{
			valuedata = OidOutputFunctionCall(typoutput, valdatum);
			valuelen = strlen(valuedata);
		}

		fsd->names[current] = attname;
		fsd->values[current] = valuedata;
		fsd->value_lengths[current] = valuelen;
		fsd->type_oids[current] = typid;
		fsd->nulls[current] = false;
		fsd->binary_formats[current] = binary_output;
		fsd->num_columns++;
	}
}

static bool
fsd_should_output_binary_for_type(const PB3LD_FieldSetDescription *fsd, Oid typid)
{
	PB3LD_Oid_Range *r = fsd->privdata->binary_oid_ranges;
	if (r == NULL)
		return false;

	while (r->min != InvalidOid)
	{
		if (typid < r->min)
			return false;
		else if (typid <= r->max)
			return true;
		r++;
	}
	return false;
}

void
fsd_serialize(PB3LD_FieldSetDescription *fsd, int32 field_number, StringInfo out)
{
	const PB3LD_Private* privdata = fsd->privdata;
	StringInfoData tmpbuf;
	StringInfoData formatsbuf;
	int i;

	initStringInfo(&tmpbuf);

	for (i = 0; i < fsd->num_columns; i++)
	{
		Assert(fsd->names[i] != NULL);
		Assert(fsd->values[i] != NULL);

		pb3_append_string_kv(&tmpbuf, PB3LD_FSD_NAMES, fsd->names[i]);

		if (fsd->nulls[i])
		{
			Assert(fsd->value_lengths[i] == 0);
			Assert(*fsd->values[i] == '\x00');

			pb3_append_bytes_kv(&tmpbuf, PB3LD_FSD_VALUES, fsd->values[i], 0);

			if (privdata->type_oids_mode == PB3LD_FSD_TYPE_OIDS_FULL)
				pb3_append_oid_kv(&tmpbuf, PB3LD_FSD_TYPE_OIDS, fsd->type_oids[i]);
		}
		else
		{

			pb3_append_bytes_kv(&tmpbuf, PB3LD_FSD_VALUES,
								fsd->values[i], fsd->value_lengths[i]);

			if (privdata->type_oids_mode != PB3LD_FSD_TYPE_OIDS_DISABLED)
				pb3_append_oid_kv(&tmpbuf, PB3LD_FSD_TYPE_OIDS, fsd->type_oids[i]);
		}
	}

	pb3_append_varlen_key(&tmpbuf, PB3LD_FSD_NULLS);
	pb3_append_int32(&tmpbuf, (int32) fsd->num_columns);
	for (i = 0; i < fsd->num_columns; i++)
	{
		if (fsd->nulls[i])
			appendStringInfoChar(&tmpbuf, '\001');
		else
			appendStringInfoChar(&tmpbuf, '\000');
	}

	if (privdata->formats_mode != PB3LD_FSD_FORMATS_DISABLED)
	{
		initStringInfo(&formatsbuf);

		for (i = 0; i < fsd->num_columns; i++)
		{
			if (privdata->formats_mode == PB3LD_FSD_FORMATS_OMIT_NULLS && fsd->nulls[i])
				continue;
			if (fsd->binary_formats[i])
				appendStringInfoChar(&formatsbuf, '\001');
			else
				appendStringInfoChar(&formatsbuf, '\000');
		}

		pb3_append_varlen_key(&tmpbuf, PB3LD_FSD_FORMATS);
		pb3_append_int32(&tmpbuf, (int32) formatsbuf.len);
		appendBinaryStringInfo(&tmpbuf, formatsbuf.data, formatsbuf.len);
	}

	pb3_append_varlen_key(privdata->message_buf, field_number);
	pb3_append_int32(privdata->message_buf, (int32) tmpbuf.len);
	appendBinaryStringInfo(privdata->message_buf, tmpbuf.data, tmpbuf.len);
	/* clear, since tmpbuf might be large */
	pfree(tmpbuf.data);
}
