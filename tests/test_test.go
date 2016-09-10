package test

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	proto "github.com/golang/protobuf/proto"
	"reflect"
	"strings"
	"testing"
	"os"
	"time"
)

var _ = fmt.Fprintf
var _ = os.Args
var _ = time.Second

var replicationSlotName string = "pgpb3ldtest"
var outputPluginName string = "pg_pb3_ld"

func testSetup(t *testing.T) *sql.DB {
	conninfo := strings.Join([]string{
		"sslmode=disable",
		// required for predictability
		"synchronous_commit=on",
	}, " ")

	dbh, err := sql.Open("postgres", conninfo)
	if err != nil {
		t.Fatal(err)
	}
	err = dbh.Ping()
	if err != nil {
		t.Fatal(err)
	}
	var isSuperUser string
	err = dbh.QueryRow("SHOW is_superuser").Scan(&isSuperUser)
	if err != nil {
		_ = dbh.Close()
		t.Fatal(err)
	}
	if isSuperUser != "on" {
		_ = dbh.Close()
		t.Fatalf("not a superuser (got %q; expected \"on\")", isSuperUser)
	}

	dbh.SetMaxIdleConns(1)
	dbh.SetMaxOpenConns(1)

	_, err = dbh.Exec(`
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
		_ = dbh.Close()
		t.Fatal(err)
	}

	_, err = dbh.Exec(`SELECT pg_create_logical_replication_slot($1, $2)`, replicationSlotName, outputPluginName)
	if err != nil {
		pge, ok := err.(*pq.Error)
		if !ok {
			_ = dbh.Close()
			t.Fatal(err)
		}
		if pge.Code.Name() != "duplicate_object" {
			t.Fatal(pge)
		}
		_, err = dbh.Exec("SELECT pg_drop_replication_slot($1)", replicationSlotName)
		if err != nil {
			_ = dbh.Close()
			t.Fatal(err)
		}
		_, err = dbh.Exec(`SELECT pg_create_logical_replication_slot($1, $2)`, replicationSlotName, outputPluginName)
		if err != nil {
			_ = dbh.Close()
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


func testTeardown(t *testing.T, dbh *sql.DB) {
	_, _ = dbh.Exec("SELECT pg_drop_replication_slot($1)", replicationSlotName)
	_ = dbh.Close()
}

func runTest(t *testing.T, dbh *sql.DB, sql string, options []string, expectedMessages []interface{}) {
	if options == nil {
		options = []string{}
	}

	_, err := dbh.Exec(sql)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := dbh.Query(`SELECT data FROM pg_logical_slot_get_binary_changes($1, NULL, NULL, VARIADIC $2)`,
		replicationSlotName,
		pq.Array(options),
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
			t.Logf("message number %d does not match: %T:%+v != %T:%+v",
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

func TestBasic(t *testing.T) {
	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	sql := `
BEGIN;
INSERT INTO tenk1(unique1) VALUES (1);
UPDATE tenk1 SET unique2 = -20;
DELETE FROM tenk1;
COMMIT;
`

	options := []string{}

	var expected []interface{}
	expected = append(expected,
		&InsertDescription{
			Table: tenk1TableDescriptionNoOid,
			NewValues: &FieldSetDescription{
				Names: tenk1FieldNames,
				Values: createStringValues(16, "1"),
				Nulls: createNulls(options,1,15),
			},
		},
	)
	expected = append(expected,
		&UpdateDescription{
			Table: tenk1TableDescriptionNoOid,
			NewValues: &FieldSetDescription{
				Names: tenk1FieldNames,
				Values: createStringValues(16, "1", "-20"),
				Nulls: createNulls(options,2,14),
			},
			KeyFields: &FieldSetDescription{
				Names: []string{"unique1"},
				Values: createStringValues(1, "1"),
				Nulls: createNulls(options,1),
			},
		},
	)
	expected = append(expected,
		&DeleteDescription{
			Table: tenk1TableDescriptionNoOid,
			KeyFields: &FieldSetDescription{
				Names: []string{"unique1"},
				Values: createStringValues(1, "1"),
				Nulls: createNulls(options,1),
			},
		},
	)
	expected = append(expected, &CommitTransaction{})
	runTest(t, dbh, sql, options, expected)
}

func TestBeginCommit(t *testing.T) {
	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	sql := `
BEGIN;
-- not decoded
INSERT INTO tbl_identity_nothing DEFAULT VALUES;
COMMIT;
`

	options := []string{
		"enable_begin_messages",	"on",
		"enable_commit_messages",	"on",
	}

	var expected []interface{}
	expected = append(expected, &BeginTransaction{})
	expected = append(expected, &CommitTransaction{})
	runTest(t, dbh, sql, options, expected)
}

// This is supposed to test pb3_fix_reserved_length.  It's relying quite a lot
// on the exact structure of the message, but I'm a bit too lazy to build any
// better tests for this right now.
func TestMessageLengthCornerCases(t *testing.T) {
	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	testMassive := (os.Getenv("TestMassiveEmbeddedMessage") == "PLEASE")

	sql := `
BEGIN;
-- 127 bytes
INSERT INTO tbl_identity_full (f1, f2) VALUES (1, repeat('j', 110));
-- 128 bytes
INSERT INTO tbl_identity_full (f1, f2) VALUES (1, repeat('j', 111));
-- 129 bytes
INSERT INTO tbl_identity_full (f1, f2) VALUES (1, repeat('j', 112));
-- 16383 bytes
INSERT INTO tbl_identity_full (f1, f2) VALUES (1, repeat('j', 16365));
-- 16384 bytes
INSERT INTO tbl_identity_full (f1, f2) VALUES (1, repeat('j', 16366));
-- 16385 bytes
INSERT INTO tbl_identity_full (f1, f2) VALUES (1, repeat('j', 16367));
-- 2097151 bytes
INSERT INTO tbl_identity_full (f1, f2) VALUES (1, repeat('j', 2097132));
-- 2097152 bytes
INSERT INTO tbl_identity_full (f1, f2) VALUES (1, repeat('j', 2097133));
-- 2097153 bytes
INSERT INTO tbl_identity_full (f1, f2) VALUES (1, repeat('j', 2097134));
COMMIT;
`
	if testMassive {
		sql += `
BEGIN;
-- 268435455 bytes
INSERT INTO tbl_identity_full (f1, f2) VALUES (1, repeat('j', 268435435));
COMMIT;
BEGIN;
-- 268435456 bytes
INSERT INTO tbl_identity_full (f1, f2) VALUES (1, repeat('j', 268435436));
COMMIT;
BEGIN;
-- 268435457 bytes
INSERT INTO tbl_identity_full (f1, f2) VALUES (1, repeat('j', 268435437));
COMMIT;
`
	}

	options := []string{
		"enable_begin_messages",	"off",
		"enable_commit_messages",	"off",
	}

	trl := func(repeat_length int) *InsertDescription {
		return &InsertDescription{
			Table: tblIdentityFullDescription,
			NewValues: &FieldSetDescription{
				Names: tblIdentityFullFieldNames,
				Values: createStringValues(2, "1", strings.Repeat("j", repeat_length)),
				Nulls: createNulls(options,2),
			},
		}
	}

	var expected []interface{}
	expected = append(expected, trl(110))
	expected = append(expected, trl(111))
	expected = append(expected, trl(112))
	expected = append(expected, trl(16365))
	expected = append(expected, trl(16366))
	expected = append(expected, trl(16367))
	expected = append(expected, trl(2097132))
	expected = append(expected, trl(2097133))
	expected = append(expected, trl(2097134))
	if testMassive {
		expected = append(expected, trl(268435435))
		expected = append(expected, trl(268435436))
		expected = append(expected, trl(268435437))
	}

	runTest(t, dbh, sql, options, expected)
}

func TestTableOids(t *testing.T) {
	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	tdCopy := *tenk1TableDescriptionNoOid
	err := dbh.QueryRow(`SELECT 'tenk1'::regclass::oid`).Scan(&tdCopy.TableOid)
	if err != nil {
		t.Fatal(err)
	}

	sql := `
BEGIN;
INSERT INTO tenk1(unique1) VALUES (1);
UPDATE tenk1 SET unique2 = -20;
DELETE FROM tenk1;
COMMIT;
`

	options := []string{"enable_table_oids","on"}

	var expected []interface{}
	expected = append(expected,
		&InsertDescription{
			Table: &tdCopy,
			NewValues: &FieldSetDescription{
				Names: tenk1FieldNames,
				Values: createStringValues(16, "1"),
				Nulls: createNulls(options,1,15),
			},
		},
	)
	expected = append(expected,
		&UpdateDescription{
			Table: &tdCopy,
			NewValues: &FieldSetDescription{
				Names: tenk1FieldNames,
				Values: createStringValues(16, "1", "-20"),
				Nulls: createNulls(options,2,14),
			},
			KeyFields: &FieldSetDescription{
				Names: []string{"unique1"},
				Values: createStringValues(1, "1"),
				Nulls: createNulls(options,1),
			},
		},
	)
	expected = append(expected,
		&DeleteDescription{
			Table: &tdCopy,
			KeyFields: &FieldSetDescription{
				Names: []string{"unique1"},
				Values: createStringValues(1, "1"),
				Nulls: createNulls(options,1),
			},
		},
	)
	expected = append(expected, &CommitTransaction{})
	runTest(t, dbh, sql, options, expected)
}

func TestTableVarattExternalOndisk(t *testing.T) {
	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	sql := `
BEGIN;
ALTER TABLE tenk1 ALTER COLUMN string4 SET STORAGE EXTERNAL;
INSERT INTO tenk1(unique1, unique2, string4)
SELECT 1, 10, repeat('j', 9001);
UPDATE tenk1 SET unique2 = 20;
DELETE FROM tenk1;
COMMIT;
`

	options := []string{
		"type_oids_mode","full",
		"formats_mode","full",
	}

	var expected []interface{}
	expected = append(expected,
		&InsertDescription{
			Table: tenk1TableDescriptionNoOid,
			NewValues: &FieldSetDescription{
				Names: tenk1FieldNames,
				Values: createStringValues(16, "1", "10",
					"", "", "", "",
					"", "", "", "",
					"", "", "", "",
					"",
					strings.Repeat("j", 9001),
				),
				TypeOids: tenk1FieldTypeOids,
				Nulls: createNulls(options,2,13,1),
				Formats: createFormats(options, 16),
			},
		},
	)
	expected = append(expected,
		&UpdateDescription{
			Table: tenk1TableDescriptionNoOid,
			NewValues: &FieldSetDescription{
				Names: tenk1FieldNames[:15],
				Values: createStringValues(15, "1", "20"),
				TypeOids: tenk1FieldTypeOids[:15],
				Nulls: createNulls(options,2,13),
				Formats: createFormats(options, 15),
			},
			KeyFields: &FieldSetDescription{
				Names: []string{"unique1"},
				Values: createStringValues(1, "1"),
				TypeOids: tenk1FieldTypeOids[:1],
				Nulls: createNulls(options,1),
				Formats: createFormats(options, 1),
			},
		},
	)
	expected = append(expected,
		&DeleteDescription{
			Table: tenk1TableDescriptionNoOid,
			KeyFields: &FieldSetDescription{
				Names: []string{"unique1"},
				Values: createStringValues(1, "1"),
				TypeOids: tenk1FieldTypeOids[:1],
				Nulls: createNulls(options,1),
				Formats: createFormats(options, 1),
			},
		},
	)
	expected = append(expected, &CommitTransaction{})
	runTest(t, dbh, sql, options, expected)
}

func TestBinaryField(t *testing.T) {
	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	sql := `
INSERT INTO tenk1(unique1) VALUES (1);
`

	options := []string{
		"enable_commit_messages","no",
		"binary_oid_ranges","0-1,2-400,401-4000",
	}

	var expected []interface{}
	expected = append(expected,
		&InsertDescription{
			Table: tenk1TableDescriptionNoOid,
			NewValues: &FieldSetDescription{
				Names: tenk1FieldNames,
				Values: createStringValues(16, "\x00\x00\x00\x01"),
				Nulls: createNulls(options,1,15),
			},
		},
	)
	runTest(t, dbh, sql, options, expected)
}

func TestVarlenBinaryField(t *testing.T) {
	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	sql := `
INSERT INTO tenk1(unique1, string4) VALUES (2, 'foobarbaz');
`

	options := []string{
		"enable_commit_messages","no",
		"binary_oid_ranges","25",
	}

	var expected []interface{}
	expected = append(expected,
		&InsertDescription{
			Table: tenk1TableDescriptionNoOid,
			NewValues: &FieldSetDescription{
				Names: tenk1FieldNames,
				Values: createStringValues(16,
					"2", "", "", "",
					"", "", "", "",
					"", "", "", "",
					"", "", "", "foobarbaz",
				),
				Nulls: createNulls(options,1,14,1),
			},
		},
	)
	runTest(t, dbh, sql, options, expected)
}

func TestLargeEmbeddedMessage(t *testing.T) {
	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	sql := `INSERT INTO tenk1(unique1, string4) SELECT 1, repeat('j', 16384)`

	options := []string{}

	var expected []interface{}
	expected = append(expected,
		&InsertDescription{
			Table: tenk1TableDescriptionNoOid,
			NewValues: &FieldSetDescription{
				Names: tenk1FieldNames,
				Values: createStringValues(16, "1",
					"", "", "", "",
					"", "", "", "",
					"", "", "", "",
					"", "",
					strings.Repeat("j", 16384),
				),
				Nulls: createNulls(options,1,14,1),
			},
		},
	)
	expected = append(expected, &CommitTransaction{})
	runTest(t, dbh, sql, options, expected)
}
