package test

import (
	"context"
	"github.com/jackc/pgx/v4"
	proto "github.com/golang/protobuf/proto"
	"strings"
	"testing"
)

var replicationSlotName string = "pgpb3ldtest"
var outputPluginName string = "pg_pb3_ld"

var tenk1FieldNames = []string{
	"unique1", "unique2", "two", "four",
	"ten", "twenty", "hundred", "thousand",
	"twothousand", "fivethous", "tenthous",
	"odd", "even", "stringu1", "stringu2", "string4",
}
var tenk1FieldTypeOids = []uint32{23,23,23,23,23,23,23,23,23,23,23,23,23,19,19,25}
var tenk1TableDescriptionNoOid = &TableDescription{
	SchemaName: "public",
	TableName: "tenk1",
}

var tblIdentityFullFieldNames = []string{"f1","f2"}
var tblIdentityFullDescription = &TableDescription{
	SchemaName: "public",
	TableName: "tbl_identity_full",
}

func testSetup(t *testing.T) *pgx.Conn {
	conninfo := strings.Join([]string{
		"sslmode=disable",
		// required for predictability
		"synchronous_commit=on",
	}, " ")

	dbh, err := pgx.Connect(context.Background(), conninfo)
	if err != nil {
		t.Fatal(err)
	}
	var isSuperUser string
	err = dbh.QueryRow(context.Background(), "SHOW is_superuser").Scan(&isSuperUser)
	if err != nil {
		_ = dbh.Close(context.Background())
		t.Fatal(err)
	}
	if isSuperUser != "on" {
		_ = dbh.Close(context.Background())
		t.Fatalf("not a superuser (got %q; expected \"on\")", isSuperUser)
	}

	_, err = dbh.Exec(context.Background(), `
DROP TABLE IF EXISTS tenk1;
CREATE TABLE tenk1 (
    unique1     int4,
    unique2     int4,
    two         int4,
    four        int4,
    ten         int4,
    twenty      int4,
    hundred     int4,
    thousand    int4,
    twothousand int4,
    fivethous   int4,
    tenthous    int4,
    odd         int4,
    even        int4,
    stringu1    name,
    stringu2    name,
    string4     text,

	PRIMARY KEY (unique1),
	UNIQUE (unique2)
);
DROP TABLE IF EXISTS tbl_identity_nothing;
CREATE TABLE tbl_identity_nothing (
	f1 int4
);
ALTER TABLE tbl_identity_nothing REPLICA IDENTITY NOTHING;
DROP TABLE IF EXISTS tbl_identity_full;
CREATE TABLE tbl_identity_full (
	f1 int4,
	f2 text
);
ALTER TABLE tbl_identity_full REPLICA IDENTITY FULL;
`)
	if err != nil {
		_ = dbh.Close(context.Background())
		t.Fatal(err)
	}

	_, err = dbh.Exec(context.Background(), `SELECT pg_create_logical_replication_slot($1, $2)`, replicationSlotName, outputPluginName)
	if err != nil {
		_, err = dbh.Exec(context.Background(), "SELECT pg_drop_replication_slot($1)", replicationSlotName)
		if err != nil {
			_ = dbh.Close(context.Background())
			t.Fatal(err)
		}
		_, err = dbh.Exec(context.Background(), `SELECT pg_create_logical_replication_slot($1, $2)`, replicationSlotName, outputPluginName)
		if err != nil {
			_ = dbh.Close(context.Background())
			t.Fatal(err)
		}
	}

	return dbh
}

func createStringValues(numValues int, vals ...string) [][]byte {
	ret := make([][]byte, numValues)
	for i := 0; i < numValues; i++ {
		if len(vals) > i {
			ret[i] = []byte(vals[i])
		} else {
			ret[i] = []byte{}
		}
	}
	return ret
}

func createNulls(options []string, vals ...int) []byte {
	var bm []byte

	_ = options
	if true {
		currentByte := byte(0)
		for _, l := range vals {
			for i := 0; i < l; i++ {
				bm = append(bm, currentByte)
			}
			currentByte = 1 - currentByte
		}
	}
	return bm
}

func createFormats(options []string, vals ...int) []byte {
	var bm []byte

	_ = options
	if true {
		currentFormat := byte(0)
		for _, l := range vals {
			for i := 0; i < l; i++ {
				bm = append(bm, currentFormat)
			}
			currentFormat = 1 - currentFormat
		}
	}
	return bm
}


func testTeardown(t *testing.T, dbh *pgx.Conn) {
	_, _ = dbh.Exec(context.Background(), "SELECT pg_drop_replication_slot($1)", replicationSlotName)
	_ = dbh.Close(context.Background())
}

func runTest(t *testing.T, dbh *pgx.Conn, sql string, options []string, expectedMessages []proto.Message) {
	if options == nil {
		options = []string{}
	}

	_, err := dbh.Exec(context.Background(), sql)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := dbh.Query(context.Background(), `SELECT data FROM pg_logical_slot_get_binary_changes($1, NULL, NULL, VARIADIC $2)`,
		replicationSlotName,
		options,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	numExpectedMessages := len(expectedMessages)
	messageNum := 1
	for rows.Next() {
		var data []byte

		err = rows.Scan(&data)
		if err != nil {
			t.Fatal(err)
		}

		if len(data) < 3 {
			t.Fatalf("unexpected data %+#v length %d", data, len(data))
		}
		header_len := int32(0)
		for i := 0; ; i++ {
			if i > 6 || i >= len(data) {
				t.Fatalf("could not parse wire message header %+#v", data)
			}
			header_len = int32(data[i] & 0x7F);
			if (data[i] & 0x7F) == data[i] {
				data = data[i + 1:]
				break
			}
		}

		wireMsg := &WireMessageHeader{}
		err = proto.Unmarshal(data[:header_len], wireMsg)
		if err != nil {
			t.Fatal(err)
		}
		data = data[header_len:]

		if len(wireMsg.Types) != len(wireMsg.Offsets) {
			t.Fatalf(
				"invalid wireMsg: len(Types) %d != len(Offsets) %d",
				len(wireMsg.Types),
				len(wireMsg.Offsets),
			)
		}

		for i, typ := range wireMsg.Types {
			var receivedTextFormat string
			var msg proto.Message

			offset := wireMsg.Offsets[i]
			if offset > int32(len(data)) {
				t.Fatalf(
					"invalid wireMsg: offset %d > len(data) %d",
					offset,
					len(data),
				)
			}
			msgData := data[offset:]
			if i + 1 < len(wireMsg.Offsets) {
				nextOffset := wireMsg.Offsets[i + 1]
				msgLen := nextOffset - offset
				msgData = msgData[:msgLen]
			}

			switch typ {
				case WireMessageType_WMSG_BEGIN:
					begin := &BeginTransaction{}
					err = proto.Unmarshal(msgData, begin)
					if err != nil {
						t.Fatal(err)
					}
					receivedTextFormat = proto.MarshalTextString(begin)
					msg = begin
				case WireMessageType_WMSG_COMMIT:
					commit := &CommitTransaction{}
					err = proto.Unmarshal(msgData, commit)
					if err != nil {
						t.Fatal(err)
					}
					receivedTextFormat = proto.MarshalTextString(commit)
					msg = commit
				case WireMessageType_WMSG_INSERT:
					ins := &InsertDescription{}
					err = proto.Unmarshal(msgData, ins)
					if err != nil {
						t.Fatal(err)
					}
					receivedTextFormat = proto.MarshalTextString(ins)
					msg = ins
				case WireMessageType_WMSG_UPDATE:
					upd := &UpdateDescription{}
					err = proto.Unmarshal(msgData, upd)
					if err != nil {
						t.Fatal(err)
					}
					receivedTextFormat = proto.MarshalTextString(upd)
					msg = upd
				case WireMessageType_WMSG_DELETE:
					del := &DeleteDescription{}
					err = proto.Unmarshal(msgData, del)
					if err != nil {
						t.Fatal(err)
					}
					receivedTextFormat = proto.MarshalTextString(del)
					msg = del
				default:
					t.Fatalf("unknown wire message type %+#v", typ)
			}
			if len(expectedMessages) == 0 {
				t.Fatalf("found message %+#v after the last expected message", msg)
			}

			if !proto.Equal(msg, expectedMessages[0]) {
				t.Logf("message number %d does not match:\n    %T:%+v\n\n  is not equal to\n\n    %T:%+v",
						 messageNum, msg, msg, expectedMessages[0], expectedMessages[0])
				t.Logf("received message was: %s", receivedTextFormat)
				t.FailNow()
			}

			expectedMessages = expectedMessages[1:]
			messageNum++
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}

	if len(expectedMessages) > 0 {
		t.Fatalf("only found %d out of %d expected messages",
				 numExpectedMessages - len(expectedMessages),
				 numExpectedMessages)
	}
}
