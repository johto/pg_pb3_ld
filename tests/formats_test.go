package test

import (
	proto "github.com/golang/protobuf/proto"
	"testing"
)

func TestFormatsFullMixed(t *testing.T) {
	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	sql := `
INSERT INTO tenk1(unique1) VALUES (1);
`

	options := []string{
		"enable_commit_messages","no",
		"binary_oid_ranges","2-400,401-4000",
		"formats_mode","full",
	}

	var expected []proto.Message
	expected = append(expected,
		&InsertDescription{
			Table: tenk1TableDescriptionNoOid,
			NewValues: &FieldSetDescription{
				Names: tenk1FieldNames,
				Values: createStringValues(16, "\x00\x00\x00\x01"),
				Nulls: createNulls(options, 1, 15),
				Formats: createFormats(options, 0, 1, 15),
			},
		},
	)
	runTest(t, dbh, sql, options, expected)
}

func TestFormatsFullAllText(t *testing.T) {
	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	sql := `
INSERT INTO tenk1(unique1) VALUES (1);
`

	options := []string{
		"enable_commit_messages","no",
		"formats_mode","full",
	}

	var expected []proto.Message
	expected = append(expected,
		&InsertDescription{
			Table: tenk1TableDescriptionNoOid,
			NewValues: &FieldSetDescription{
				Names: tenk1FieldNames,
				Values: createStringValues(16, "1"),
				Nulls: createNulls(options, 1, 15),
				Formats: createFormats(options, 16),
			},
		},
	)
	runTest(t, dbh, sql, options, expected)
}

func TestFormatsFullAllBinary(t *testing.T) {
	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	sql := `
INSERT INTO tbl_identity_full(f1,f2) VALUES (1,'2');
`

	options := []string{
		"enable_commit_messages","no",
		"binary_oid_ranges","23,25",
		"formats_mode","omit_nulls",
	}

	var expected []proto.Message
	expected = append(expected,
		&InsertDescription{
			Table: tblIdentityFullDescription,
			NewValues: &FieldSetDescription{
				Names: tblIdentityFullFieldNames,
				Values: createStringValues(2, "\x00\x00\x00\x01", "2"),
				Nulls: createNulls(options, 2),
				Formats: createFormats(options, 0, 2),
			},
		},
	)
	runTest(t, dbh, sql, options, expected)
}

func TestFormatsOmitNullsMixed(t *testing.T) {
	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	sql := `
INSERT INTO tenk1(unique1) VALUES (1);
`

	options := []string{
		"enable_commit_messages","no",
		"binary_oid_ranges","2-400,401-4000",
		"formats_mode","omit_nulls",
	}

	var expected []proto.Message
	expected = append(expected,
		&InsertDescription{
			Table: tenk1TableDescriptionNoOid,
			NewValues: &FieldSetDescription{
				Names: tenk1FieldNames,
				Values: createStringValues(16, "\x00\x00\x00\x01"),
				Nulls: createNulls(options,1,15),
				Formats: createFormats(options,0,1),
			},
		},
	)
	runTest(t, dbh, sql, options, expected)
}

func TestFormatsOmitNullsAllText(t *testing.T) {
	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	sql := `
INSERT INTO tenk1(unique1) VALUES (1);
`

	options := []string{
		"enable_commit_messages","no",
		"formats_mode","omit_nulls",
	}

	var expected []proto.Message
	expected = append(expected,
		&InsertDescription{
			Table: tenk1TableDescriptionNoOid,
			NewValues: &FieldSetDescription{
				Names: tenk1FieldNames,
				Values: createStringValues(16, "1"),
				Nulls: createNulls(options,1,15),
				Formats: createFormats(options, 1),
			},
		},
	)
	runTest(t, dbh, sql, options, expected)
}

func TestFormatsOmitNullsAllBinary(t *testing.T) {
	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	sql := `
INSERT INTO tbl_identity_full(f1,f2) VALUES (1,'2');
`

	options := []string{
		"enable_commit_messages","no",
		"binary_oid_ranges","23,25",
		"formats_mode","omit_nulls",
	}

	var expected []proto.Message
	expected = append(expected,
		&InsertDescription{
			Table: tblIdentityFullDescription,
			NewValues: &FieldSetDescription{
				Names: tblIdentityFullFieldNames,
				Values: createStringValues(2, "\x00\x00\x00\x01", "2"),
				Nulls: createNulls(options, 2),
				Formats: createFormats(options, 0, 2),
			},
		},
	)
	runTest(t, dbh, sql, options, expected)
}
