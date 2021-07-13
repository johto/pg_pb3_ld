package test

import (
	"context"
	"github.com/jackc/pgx/v4"
	proto "github.com/golang/protobuf/proto"
	"reflect"
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

func runTest(t *testing.T, dbh *pgx.Conn, sql string, options []string, expectedMessages []interface{}) {
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

		// N.B: Empty messages (i.e. nothing after the header) are perfectly
		// valid for e.g. BeginTransaction.
		if len(data) < 2 {
			t.Fatalf("unexpected data %+#v length %d", data, len(data))
		}

		wireMsg := &WireMessageHeader{}
		err = proto.Unmarshal(data[:2], wireMsg)
		if err != nil {
			t.Fatal(err)
		}
		data = data[2:]

		var receivedTextFormat string

		var msg interface{}
		switch wireMsg.Typ {
			case WireMessageType_WMSG_BEGIN:
				begin := &BeginTransaction{}
				err = proto.Unmarshal(data, begin)
				if err != nil {
					t.Fatal(err)
				}
				receivedTextFormat = proto.MarshalTextString(begin)
				msg = begin
			case WireMessageType_WMSG_COMMIT:
				commit := &CommitTransaction{}
				err = proto.Unmarshal(data, commit)
				if err != nil {
					t.Fatal(err)
				}
				receivedTextFormat = proto.MarshalTextString(commit)
				msg = commit
			case WireMessageType_WMSG_INSERT:
				ins := &InsertDescription{}
				err = proto.Unmarshal(data, ins)
				if err != nil {
					t.Fatal(err)
				}
				receivedTextFormat = proto.MarshalTextString(ins)
				msg = ins
			case WireMessageType_WMSG_UPDATE:
				upd := &UpdateDescription{}
				err = proto.Unmarshal(data, upd)
				if err != nil {
					t.Fatal(err)
				}
				receivedTextFormat = proto.MarshalTextString(upd)
				msg = upd
			case WireMessageType_WMSG_DELETE:
				del := &DeleteDescription{}
				err = proto.Unmarshal(data, del)
				if err != nil {
					t.Fatal(err)
				}
				receivedTextFormat = proto.MarshalTextString(del)
				msg = del
			default:
				t.Fatalf("unknown wire message type %+#v", wireMsg.Typ)
		}

		if len(expectedMessages) == 0 {
			t.Fatalf("found message %+#v after the last expected message", msg)
		}

		if !reflect.DeepEqual(msg, expectedMessages[0]) {
			t.Logf("message number %d does not match:\n    %T:%+v\n\n  is not equal to\n\n    %T:%+v",
					 messageNum, msg, msg, expectedMessages[0], expectedMessages[0])
			t.Logf("received message was: %s", receivedTextFormat)
			t.FailNow()
		}

		expectedMessages = expectedMessages[1:]
		messageNum++
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
