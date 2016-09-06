#include <limits.h>

#include "postgres.h"

#include "utils/int8.h"

#include "pg_pb3_ld.h"

/*
 * pb3ld_parse_binary_oid_ranges parses a comma-separated list of oid ranges
 * into privdata->binary_oid_ranges.  privdata->num_binary_oid_ranges is set to
 * the number of ranges parsed.  An exception is raised on invalid input.
 *
 * XXX This code sucks.  Please fix.
 */
void
pb3ld_parse_binary_oid_ranges(PB3LD_Private *privdata, const char *input)
{
	int num_ranges = 0;
	int num_alloc = 512;
	Oid *ranges = (Oid *) palloc(sizeof(Oid) * 2 * num_alloc);
	char *nextp = pstrdup(input);

	while (isspace((unsigned char) *nextp))
		nextp++;

	if (*nextp == '\0')
	{
		privdata->binary_oid_ranges = NULL;
		privdata->num_binary_oid_ranges = 0;
	}

	for (;;)
	{
		int i;

		for (i = 0; i < 2; i++)
		{
			char *end;
			int64 bigint;

			end = strpbrk(nextp, ",-");
			if (end != NULL)
			{
				if (i == 1 && *end == '-')
					elog(ERROR, "syntax error");
				*end = '\0';
			}

			(void) scanint8(nextp, false, &bigint);
			if (bigint < 0)
				elog(ERROR, "oids can't be negative");
			else if (bigint > OID_MAX)
				elog(ERROR, "oids can't be more than %u", OID_MAX);
			ranges[num_ranges * 2 + i] = (Oid) bigint;

			if (end != NULL)
				nextp = end + 1;
			else
			{
				if (i == 0)
					ranges[num_ranges * 2 + 1] = ranges[num_ranges * 2];

				num_ranges++;
				goto out;
			}
		}

		num_ranges++;
		if (num_ranges >= num_alloc)
			elog(ERROR, "fixme");
	}

out:
	/* TODO: sanity check the ranges */

	privdata->binary_oid_ranges = ranges;
	privdata->num_binary_oid_ranges = num_ranges;
}

