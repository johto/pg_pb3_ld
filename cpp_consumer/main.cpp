#include <cstdio>
#include <cstdlib>

#include <libpq-fe.h>

#include <stdexcept>
#include <exception>

#include "cpp_consumer.h"

int
main(int argc, char* argv[])
{
	CppConsumer::ReplicationSlotOptions slot_options;
	const char * const keywords[] = {"dbname","replication",NULL};
	const char * const values[] = {"marko","database",NULL};

	PGconn *conn;

	conn = PQconnectdbParams(keywords, values, 0);
	if (!conn)
	{
		fprintf(stderr, "out of memory\n");
		exit(1);
	}

	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "could not connect: %s", PQerrorMessage(conn));
		exit(1);
	}

	CppConsumer::Consumer consumer(conn);
	if (!consumer.StartStreaming("pg_pb3_ld", "0/0", slot_options))
	{
		fprintf(stderr, "%s\n", consumer.LastError().c_str());
		exit(1);
	}

	for (;;)
	{
		CppConsumer::Change change;

		if (!consumer.ReadChange(&change))
		{
			fprintf(stderr, "%s\n", consumer.LastError().c_str());
			exit(1);
		}
	}

	return 0;
}
