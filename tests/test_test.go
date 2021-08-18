package test

import (
	"context"
	proto "github.com/golang/protobuf/proto"
	"strings"
	"testing"
	"os"
)

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

	var expected []proto.Message
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

	var expected []proto.Message
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

	var expected []proto.Message
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
	err := dbh.QueryRow(context.Background(), `SELECT 'tenk1'::regclass::oid`).Scan(&tdCopy.TableOid)
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

	var expected []proto.Message
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

	var expected []proto.Message
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
		"binary_oid_ranges","1,2-2,3-400,401-4000",
	}

	var expected []proto.Message
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

	var expected []proto.Message
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

	var expected []proto.Message
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
