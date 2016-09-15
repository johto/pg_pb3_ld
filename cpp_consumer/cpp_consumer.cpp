#include "cpp_consumer.h"

#include <cstdarg>
#include <cstdlib>
#include <stdexcept>
#include <exception>

#include <libpq-fe.h>

using namespace CppConsumer;

std::string
ReplicationSlotOptions::WireFormat() const
{
	std::string fmt;
	int noptions = 0;

	auto maybe_comma = [&fmt, &noptions]() {
		if (noptions > 0)
			fmt.append(", ");
		++noptions;
	};

	auto fmt_string = [maybe_comma, &fmt](const char *name, const std::string val) {
		maybe_comma();
		fmt.append(name);
		fmt.append(" '");
		fmt.append(val);
		fmt.append("'");
	};

	auto fmt_bool = [fmt_string](const char *name, bool val) {
		if (val)
			fmt_string(name, "true");
		else
			fmt_string(name, "false");
	};

	fmt_bool("enable_begin_messages", this->enable_begin_messages);
	fmt_bool("enable_commit_messages", this->enable_commit_messages);
	fmt_string("type_oids_mode", this->type_oids_mode);
	fmt_string("binary_oid_ranges", this->binary_oid_ranges);
	fmt_string("formats_mode", this->formats_mode);
	fmt_bool("enable_table_oids", this->enable_table_oids);

	return fmt;
}

Consumer::Consumer(PGconn *conn) :
	conn_(conn),
	streaming_(false),
	error_("(no error)")
{
	if (PQstatus(conn) != CONNECTION_OK)
		throw std::logic_error("unexpected connection status");
}

Consumer::~Consumer()
{
	PQfinish(conn_);
	conn_ = NULL;
}

bool
Consumer::StartStreaming(const std::string slot_name, const std::string start_lsn, const ReplicationSlotOptions options)
{
	auto query = std::string("START_REPLICATION SLOT ");
	query.append(slot_name);
	query.append(" LOGICAL ");
	query.append(start_lsn);
	query.append("(");
	query.append(options.WireFormat());
	query.append(")");

	PGresult *res = PQexec(conn_, query.c_str());
	if (!res)
	{
		SetError("could not execute query: out of memory");
		return false;
	}
	if (PQresultStatus(res) != PGRES_COPY_BOTH)
	{
		SetErrorf("could not create replication slot: %s",
				  PQresultErrorMessage(res));
		PQclear(res);
		return false;
	}

	streaming_ = true;
	return true;
}

bool
Consumer::ReadChange(Change *change)
{
	int ret;
	char *buffer;

	if (!streaming_)
		throw std::logic_error("ReadChange called but the consumer is not streaming; call StartStreaming first");

	ret = PQgetCopyData(conn_, &buffer, 0);
	if (ret > 0)
	{
		return true;
	}
	else if (ret == -2)
	{
		streaming_ = false;
		SetError(PQerrorMessage(conn_));
		return false;
	}
	else if (ret == -1)
	{
		/* TODO */
		abort();
	}
	else
	{
		SetError("unexpected zero return value from PQgetCopyData");
		return false;
	}
}

std::string
Consumer::LastError() const
{
	return error_;
}

void
Consumer::SetError(const std::string error)
{
	error_ = error;

	// Strip trailing whitespace
	auto lnw = error_.find_last_not_of("\r\n\t ");
	if (lnw == std::string::npos)
		error_ = "";
	else
		error_ = error_.substr(0, lnw + 1);
}

void
Consumer::SetError(const char *error)
{
	return SetError(std::string(error));
}

void
Consumer::SetErrorf(const char *str, ...)
{
	char formatted[2048];
	va_list args;

	va_start(args, str);
	(void) vsnprintf(formatted, sizeof(formatted), str, args);
	va_end(args);

	SetError(std::string(formatted));
}
