package test

import (
	"testing"
)


func TestTypeOidsOmitNulls(t *testing.T) {
	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	sql := `
BEGIN;
INSERT INTO tenk1(unique1) VALUES (1);
UPDATE tenk1 SET unique2 = -20;
DELETE FROM tenk1;
COMMIT;
`

	options := []string{
		"type_oids_mode","omit_nulls",
		"enable_commit_messages","no",
	}

	var expected []interface{}
	expected = append(expected,
		&InsertDescription{
			Table: tenk1TableDescriptionNoOid,
			NewValues: &FieldSetDescription{
				Names: tenk1FieldNames,
				Values: createStringValues(16, "1"),
				TypeOids: tenk1FieldTypeOids[:1],
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
				TypeOids: tenk1FieldTypeOids[:2],
				Nulls: createNulls(options,2,14),
			},
			KeyFields: &FieldSetDescription{
				Names: []string{"unique1"},
				Values: createStringValues(1, "1"),
				TypeOids: tenk1FieldTypeOids[:1],
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
				TypeOids: tenk1FieldTypeOids[:1],
				Nulls: createNulls(options,1),
			},
		},
	)
	runTest(t, dbh, sql, options, expected)
}

func TestTypeOidsFull(t *testing.T) {
	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	sql := `
BEGIN;
INSERT INTO tenk1(unique1) VALUES (1);
UPDATE tenk1 SET unique2 = -20;
DELETE FROM tenk1;
COMMIT;
`

	options := []string{
		"type_oids_mode","full",
		"enable_commit_messages","no",
	}

	var expected []interface{}
	expected = append(expected,
		&InsertDescription{
			Table: tenk1TableDescriptionNoOid,
			NewValues: &FieldSetDescription{
				Names: tenk1FieldNames,
				Values: createStringValues(16, "1"),
				TypeOids: tenk1FieldTypeOids,
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
				TypeOids: tenk1FieldTypeOids,
				Nulls: createNulls(options,2,14),
			},
			KeyFields: &FieldSetDescription{
				Names: []string{"unique1"},
				Values: createStringValues(1, "1"),
				TypeOids: tenk1FieldTypeOids[:1],
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
				TypeOids: tenk1FieldTypeOids[:1],
				Nulls: createNulls(options,1),
			},
		},
	)
	runTest(t, dbh, sql, options, expected)
}
