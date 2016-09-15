#ifndef PG_PB3_LD_CPP_CONSUMER_H
#define PG_PB3_LD_CPP_CONSUMER_H

#include <libpq-fe.h>

#include <string>
#include <cstdarg>

namespace CppConsumer {

struct ReplicationSlotOptions {
	ReplicationSlotOptions() :
		// These should match the defaults of the decoder.  It's a bit
		// unfortunate that there's no optional in the C++ standard yet, since
		// that would be ideal here.
		enable_begin_messages(false),
		enable_commit_messages(true),
		type_oids_mode("disabled"),
		binary_oid_ranges(""),
		formats_mode("disabled"),
		enable_table_oids(false)
	{
	}

	std::string WireFormat() const;

	bool enable_begin_messages;
	bool enable_commit_messages;
	std::string type_oids_mode;
	std::string binary_oid_ranges;
	std::string formats_mode;
	bool enable_table_oids;
};

class Change {
public:

private:
};

class Consumer {
public:
	Consumer(PGconn *conn);
	~Consumer();

	bool StartStreaming(const std::string slot_name,
						const std::string start_lsn,
						const ReplicationSlotOptions options);
	bool ReadChange(Change *change);
	std::string LastError() const;

private:
	void SetError(const std::string error);
	void SetError(const char *error);
	void SetErrorf(const char *str, ...);

private:
	PGconn *conn_;
	bool streaming_;
	std::string error_;
};

};

#endif
