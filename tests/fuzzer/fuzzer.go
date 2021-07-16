package main

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	proto "github.com/golang/protobuf/proto"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	SQL_INTEGER = iota
	//SQL_TEXT
	//SQL_BYTEA
)

type SQLType int

func (t SQLType) String() string {
	switch t {
		case SQL_INTEGER:
			return "int4"
		default:
			panic(t)
	}
}

type TestSchema struct {
	TableName string
	NumColumns int
	ColumnNames []string
	ColumnTypes []SQLType
}

type TestCase struct {
}

var replicationSlotName string = "pgpb3ldtest"
var outputPluginName string = "pg_pb3_ld"

func generateSQLType() SQLType {
	v := rand.Intn(1)
	if v == 0 {
		return SQL_INTEGER
	} else {
		panic(v)
	}
}

func generateSQLValue(t SQLType) []byte {
	if t == SQL_INTEGER {
		r := rand.Int63n(4294967296)
		return []byte(strconv.Itoa(int(r - 2147483648)))
	} else {
		panic(t)
	}
}

func generateSQLIdentifier() string {
	alphabet := []byte("abcdefghijklmnopqrstuvwxyz_ABCDEFGHIJKLMNOPQRSTUVWXYZ")

	var length int
	for {
		length = int(rand.NormFloat64() * 10 + 5)
		if length > 0 && length <= 63 {
			break
		}
	}
	value := ""
	for i := 0; i < length; i++ {
		value += string(alphabet[rand.Intn(len(alphabet))])
	}
	return value
}

func generateTestSchema() TestSchema {
	schema := TestSchema{}

	for {
		schema.NumColumns = int(rand.NormFloat64() * 10 + 5)
		if schema.NumColumns > 0 && schema.NumColumns < 512 {
			break
		}
	}

	schema.TableName = generateSQLIdentifier()
	schema.ColumnNames = make([]string, schema.NumColumns)
	schema.ColumnTypes = make([]SQLType, schema.NumColumns)
	columns := make(map[string]struct{})
	for i := 0; i < schema.NumColumns; i++ {
		for {
			schema.ColumnNames[i] = generateSQLIdentifier()
			_, exists := columns[schema.ColumnNames[i]]
			if !exists {
				columns[schema.ColumnNames[i]] = struct{}{}
				break
			}
		}
		schema.ColumnTypes[i] = generateSQLType()
	}

	return schema
}

func (s *TestSchema) SetupSQL() string {
	sql := ""
	sql += fmt.Sprintf(`DROP TABLE IF EXISTS "%s";`, s.TableName)
	sql += "\n\n"
	sql += fmt.Sprintf(`CREATE TABLE "%s" (`, s.TableName)
	sql += "\n"
	for i, colname := range s.ColumnNames {
		coltype := s.ColumnTypes[i]
		if i > 0 {
			sql += ",\n"
		}
		sql += fmt.Sprintf(`    "%s" %s`, colname, coltype.String())
	}
	sql += "\n);"
	return sql
}

func (s *TestSchema) TeardownSQL() string {
	return fmt.Sprintf(`DROP TABLE "%s;"`, s.TableName);
}

type DecodedMessage struct {
	LSN pglogrepl.LSN

	// Only one of the following fields will be set.
	Err error
	Message proto.Message
}

type Fuzzer struct {
	dbh *pgx.Conn
	conninfo []string

	replConn *pgconn.PgConn
	replCancel context.CancelFunc
	replMessageChan chan *DecodedMessage
}

func NewFuzzer(conninfo []string) *Fuzzer {
	dbh, err := pgx.Connect(context.Background(), strings.Join(conninfo, " "))
	if err != nil {
		log.Fatal(err)
	}
	var isSuperUser string
	err = dbh.QueryRow(context.Background(), "SHOW is_superuser").Scan(&isSuperUser)
	if err != nil {
		panic(err)
	}
	if isSuperUser != "on" {
		panic(fmt.Sprintf("not a superuser (got %q; expected \"on\")", isSuperUser))
	}

	fuzzer := &Fuzzer{
		dbh: dbh,
		replConn: nil,
		conninfo: conninfo,
	}

	fuzzer.createReplicationSlot()

	return fuzzer
}

func (f *Fuzzer) createReplicationSlot() {
	_, err := f.dbh.Exec(context.Background(), `SELECT pg_create_logical_replication_slot($1, $2)`, replicationSlotName, outputPluginName)
	if err != nil {
		pge, ok := err.(*pgconn.PgError)
		if !ok {
			panic(err)
		}
		if pge.Code != "42710" {
			panic(err)
		}
		_, err = f.dbh.Exec(context.Background(), "SELECT pg_drop_replication_slot($1)", replicationSlotName)
		if err != nil {
			panic(err)
		}
		_, err = f.dbh.Exec(context.Background(), `SELECT pg_create_logical_replication_slot($1, $2)`, replicationSlotName, outputPluginName)
		if err != nil {
			panic(err)
		}
	}
}

func (f *Fuzzer) openReplicationConnection() {
	if f.replConn != nil {
		panic("uh oh")
	}

	options := []string{
	}

	replConnInfo := append(f.conninfo, "replication=database")
	replConn, err := pgconn.Connect(context.Background(), strings.Join(replConnInfo, " "))
	if err != nil {
		panic(err)
	}
	sysident, err := pglogrepl.IdentifySystem(context.Background(), replConn)
	if err != nil {
		panic(err)
	}
	err = pglogrepl.StartReplication(
		context.Background(),
		replConn,
		replicationSlotName,
		sysident.XLogPos,
		pglogrepl.StartReplicationOptions{
			PluginArgs: options,
		},
	)
	if err != nil {
		panic(err)
	}
	f.replConn = replConn

	ctx, cancel := context.WithCancel(context.Background())
	f.replCancel = cancel
	f.replMessageChan = make(chan *DecodedMessage, 1)
	go f.backgroundReceiveLogicalDecodingMessages(ctx)
}

func (f *Fuzzer) closeReplicationConnection() {
	if f.replConn != nil {
		f.shutdownLogicalReceiver()
		_ = f.replConn.Close(context.Background())
		f.replConn = nil
		f.replCancel()
		f.replCancel = nil
		f.replMessageChan = nil
	}
}

func (f *Fuzzer) MainLoop() {
	for {
		log.Printf("MainLoop")
		schema := generateTestSchema()
		err := f.testMain(schema)
		if err != nil {
			f.closeReplicationConnection()
			time.Sleep(5 * time.Second)
		}
		time.Sleep(time.Second)
	}
}

func (f *Fuzzer) testMain(schema TestSchema) error {
	sql := schema.SetupSQL()
	defer func() {
		_, _ = f.dbh.Exec(context.Background(), schema.TeardownSQL())
	}()
	err := testSetup(f.dbh, sql)
	if err != nil {
		f.logFuzzError("setup", err, sql)
		return err
	}

	if f.replConn == nil {
		f.openReplicationConnection()
	}

	err = f.runTests(schema)
	if err != nil {
		f.logFuzzError("run", err, sql)
		return err
	}

	return nil
}

func (f *Fuzzer) runTests(schema TestSchema) error {
	var minimumLSN pglogrepl.LSN
	err := f.dbh.QueryRow(context.Background(), "SELECT pg_current_xlog_location()").Scan(&minimumLSN)
	if err != nil {
		panic(err)
	}

	for {
		decodedMessage := <-f.replMessageChan
		if decodedMessage.Err != nil {
			return decodedMessage.Err
		}
		if decodedMessage.LSN < minimumLSN {
			continue
		}
		msg := decodedMessage.Message
		fmt.Printf("MESSAGE %#+v\n", msg)
	}

	return nil
}

func (f *Fuzzer) shutdownLogicalReceiver() {
	f.replCancel()
	for {
		msg, ok := <-f.replMessageChan
		if !ok {
			panic(ok)
		}

		if msg == nil {
			break
		}
	}

	// At this point the channel should be closed.  Make sure it is, and then
	// clean up.
	select {
		case _, ok := <-f.replMessageChan:
			if ok {
				panic(ok)
			}
		case <-time.After(time.Second):
			panic("timeout")
	}
}

func (f *Fuzzer) backgroundReceiveLogicalDecodingMessages(ctx context.Context) {
	// No message should take more than a minute to decode, or something's wrong.
	messageTimeout := time.Minute
	messageDeadline := time.Now().Add(messageTimeout)
	sendStatusUpdate := false
	clientLSN, err := pglogrepl.ParseLSN("0/0")
	if err != nil {
		panic(err)
	}
	for {
		if sendStatusUpdate {
			// We intentionally don't use "ctx" here, since this should be a
			// really short call.
			commDeadline := time.Now().Add(5 * time.Second)
			commCtx, cancel := context.WithDeadline(context.Background(), commDeadline)
			err := pglogrepl.SendStandbyStatusUpdate(
				commCtx,
				f.replConn,
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: clientLSN,
					WALFlushPosition: clientLSN,
					WALApplyPosition: clientLSN,
					ClientTime: time.Now(),
					ReplyRequested: false,
				},
			)
			cancel()
			if err != nil {
				panic(err)
			}
			sendStatusUpdate = false
		}

		commCtx, cancel := context.WithDeadline(ctx, messageDeadline)
		msg, err := f.replConn.ReceiveMessage(commCtx)
		cancel()
		if err != nil && pgconn.Timeout(err) {
			// If the parent context was not canceled, something's wrong and
			// it's better to panic.  Otherwise we shut down cleanly.  It's not
			// a biggie if the replication connection was left in a bad state,
			// since the tester will restart it on test failures.
			if ctx.Err() == nil {
				panic(err)
			}

			f.replMessageChan <- nil
			close(f.replMessageChan)

			return
		} else if err != nil {
			panic(err)
		}

		var copyData *pgproto3.CopyData
		switch msg := msg.(type) {
			case *pgproto3.CopyData:
				copyData = msg
			case *pgproto3.ErrorResponse:
				panic(fmt.Sprintf("%#+v", msg))
			default:
				panic(fmt.Sprintf("%#+v", msg))
		}

		if copyData.Data[0] == pglogrepl.PrimaryKeepaliveMessageByteID {
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				panic(err)
			}
			sendStatusUpdate = pkm.ReplyRequested
		} else if copyData.Data[0] == pglogrepl.XLogDataByteID {
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				panic(err)
			}
			if xld.WALStart >= clientLSN {
				fmt.Printf("%q %q\n", xld.WALStart, clientLSN)

				decodedMessage, err := f.parseWireMessage(xld.WALData)
				if err != nil {
					f.replMessageChan <- &DecodedMessage{
						LSN: xld.WALStart,
						Err: err,
					}
				} else {
					f.replMessageChan <- &DecodedMessage{
						LSN: xld.WALStart,
						Message: decodedMessage,
					}
				}

				messageDeadline = time.Now().Add(messageTimeout)
				clientLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData)) + 1
			}
		}
	}
}

func (f *Fuzzer) parseWireMessage(data []byte) (proto.Message, error) {
	// N.B: Empty messages (i.e. nothing after the header) are perfectly
	// valid for e.g. BeginTransaction.
	if len(data) < 2 {
		return nil, fmt.Errorf("unexpected wire message %+#v length %d", data, len(data))
	}

	wireMsg := &WireMessageHeader{}
	err := proto.Unmarshal(data[:2], wireMsg)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal WireMessageHeader: %s", err)
	}
	data = data[2:]

	var receivedTextFormat string
	_ = receivedTextFormat

	var msg proto.Message
	switch wireMsg.Typ {
		case WireMessageType_WMSG_BEGIN:
			begin := &BeginTransaction{}
			err = proto.Unmarshal(data, begin)
			if err != nil {
				return nil, fmt.Errorf("could not unmarshal BeginTransaction: %s", err)
			}
			receivedTextFormat = proto.MarshalTextString(begin)
			msg = begin
		case WireMessageType_WMSG_COMMIT:
			commit := &CommitTransaction{}
			err = proto.Unmarshal(data, commit)
			if err != nil {
				return nil, fmt.Errorf("could not unmarshal CommitTransaction: %s", err)
			}
			receivedTextFormat = proto.MarshalTextString(commit)
			msg = commit
		case WireMessageType_WMSG_INSERT:
			ins := &InsertDescription{}
			err = proto.Unmarshal(data, ins)
			if err != nil {
				return nil, fmt.Errorf("could not unmarshal InsertDescription: %s", err)
			}
			receivedTextFormat = proto.MarshalTextString(ins)
			msg = ins
		case WireMessageType_WMSG_UPDATE:
			upd := &UpdateDescription{}
			err = proto.Unmarshal(data, upd)
			if err != nil {
				return nil, fmt.Errorf("could not unmarshal UpdateDescription: %s", err)
			}
			receivedTextFormat = proto.MarshalTextString(upd)
			msg = upd
		case WireMessageType_WMSG_DELETE:
			del := &DeleteDescription{}
			err = proto.Unmarshal(data, del)
			if err != nil {
				return nil, fmt.Errorf("could not unmarshal DeleteDescription: %s", err)
			}
			receivedTextFormat = proto.MarshalTextString(del)
			msg = del
		default:
			return nil, fmt.Errorf("unknown wire message type %+#v", wireMsg.Typ)
	}

	return msg, nil
}

func (f *Fuzzer) logFuzzError(prefix string, fuzzErr error, datas ...string) {
	datas = append(datas, fuzzErr.Error())
	data := []byte(strings.Join(datas, "\n\n------\n\n") + "\n")
	filename := prefix + time.Now().Format("20060102150405.999") + ".log"
	err := os.WriteFile(filepath.Join("errors", filename), data, 0644)
	if err != nil {
		panic(err)
	}
	log.Printf("%s failure: %s", prefix, fuzzErr)
}

func main() {
	var seed int64
	err := binary.Read(crand.Reader, binary.BigEndian, &seed)
	if err != nil {
		panic(err)
	}
	rand.Seed(seed)

	conninfo := []string{
		"sslmode=disable",
		// required for predictability
		"synchronous_commit=on",
	}
	fuzzer := NewFuzzer(conninfo)
	fuzzer.MainLoop()
_ = proto.Unmarshal
}

func testSetup(dbh *pgx.Conn, sql string) error {
	_, err := dbh.Exec(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
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

/*
func runTests(dbh *pgx.Conn, schema TestSchema) error {
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

		var msg proto.Message
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
				ins.Padding = nil
				msg = ins
			case WireMessageType_WMSG_UPDATE:
				upd := &UpdateDescription{}
				err = proto.Unmarshal(data, upd)
				if err != nil {
					t.Fatal(err)
				}
				receivedTextFormat = proto.MarshalTextString(upd)
				upd.Padding = nil
				msg = upd
			case WireMessageType_WMSG_DELETE:
				del := &DeleteDescription{}
				err = proto.Unmarshal(data, del)
				if err != nil {
					t.Fatal(err)
				}
				receivedTextFormat = proto.MarshalTextString(del)
				del.Padding = nil
				msg = del
			default:
				t.Fatalf("unknown wire message type %+#v", wireMsg.Typ)
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
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}

	if len(expectedMessages) > 0 {
		t.Fatalf("only found %d out of %d expected messages",
				 numExpectedMessages - len(expectedMessages),
				 numExpectedMessages)
	}
}
*/
