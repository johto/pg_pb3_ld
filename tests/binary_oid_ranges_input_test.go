package test

import (
	"context"
	"strings"
	"testing"
)

func TestBinaryOidRangesInput(t *testing.T) {
	tests := []struct{
		input string
		expect_failure bool
		expect_error string
	}{
		{"foo", true, "invalid input syntax for integer"},
		{"0", true, "oid can't be InvalidOid"},
		{"0-0", true, "oid can't be InvalidOid"},
		{"0-1", true, "oid can't be InvalidOid"},
		{"-1", true, "invalid input syntax for integer"},
		{"1", false, ""},
		{"4294967295", false, ""},
		{"4294967296", true, "oids can't be larger than OID_MAX"},
		{"1,", true, "invalid input syntax for binary_oid_ranges"},
		{"1-", true, "invalid input syntax for integer"},
		{"1-,", true, "invalid input syntax for binary_oid_ranges"},
		{"1,2", false, ""},
		{"2-1", true, "the upper bound of a range can't be lower than its lower bound"},
		{"1,1-2", true, "overlaps with range"},
		{"1-3,2-4", true, "overlaps with range"},
		{"1,2,3,4,5,6,7,8,9,10", false, ""},
		{"1-2,3,4-5", false, ""},
	}

	dbh := testSetup(t)
	defer testTeardown(t, dbh)

	for _, test := range tests {
		options := []string{
			"binary_oid_ranges", test.input,
		}

		if test.expect_failure && test.expect_error == "" {
			panic(test.input)
		}

		_, err := dbh.Exec(
			context.Background(),
			`SELECT pg_logical_slot_get_binary_changes($1, NULL, 1, VARIADIC $2)`,
			replicationSlotName,
			options,
		)
		if err != nil {
			if !test.expect_failure {
				t.Errorf("test %q failed unexpectedly: %s", test.input, err)
				continue
			}
			if strings.Index(err.Error(), test.expect_error) == -1 {
				t.Errorf("test %q failed with an unexpected error: %s (expected to contain %q)", test.input, err, test.expect_error)
				continue
			}
		} else {
			if test.expect_failure {
				t.Errorf("test %q succeeded unexpectedly", test.input)
				continue
			}
		}
	}
}
