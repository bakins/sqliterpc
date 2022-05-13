package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bakins/sqliterpc"
)

type Driver struct {
	transport http.RoundTripper
}

func NewDriver(transport http.RoundTripper) *Driver {
	if transport == nil {
		transport = http.DefaultTransport
	}

	d := Driver{
		transport: transport,
	}

	return &d
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	u, err := url.Parse(name)
	if err != nil {
		return nil, err
	}

	// TODO: params as options?

	if u.Scheme == "" {
		return nil, errors.New("scheme must be set in url")
	}

	c := connector{
		driver:  d,
		baseURL: u.String(),
	}
	return &c, nil
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	c, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}

	return c.Connect(context.Background())
}

type connector struct {
	driver  *Driver
	baseURL string
}

func (c *connector) Driver() driver.Driver {
	return c.driver
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	transport := c.driver.transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	connection := connection{
		client: sqliterpc.NewDatabaseServiceProtobufClient(c.baseURL, &http.Client{Transport: transport}),
	}

	return &connection, nil
}

type connection struct {
	client sqliterpc.DatabaseService
}

func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

var ErrConnectionClosed = errors.New("conenction closed")

func (c *connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.client == nil {
		return nil, ErrConnectionClosed
	}

	s := statement{
		connection: c,
		query:      query,
	}

	return &s, nil
}

func (c *connection) Close() error {
	c.client = nil
	return nil
}

func (c *connection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

var ErrTransactionsUnsupported = errors.New("transactions are not supported")

func (c *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, ErrTransactionsUnsupported
}

type statement struct {
	connection *connection
	query      string
}

func (s *statement) Close() error {
	s.connection = nil
	return nil
}

func (s *statement) NumInput() int {
	return -1
}

var (
	ErrStatementClosed  = errors.New("statement is closed")
	ErrExecUnsupported  = errors.New("exec is unsupported - use ExecContext")
	ErrQueryUnsupported = errors.New("query is unsupported - use QueryContext")
)

func (s *statement) Exec(args []driver.Value) (driver.Result, error) {
	return nil, ErrExecUnsupported
}

func (s *statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.connection == nil {
		return nil, ErrStatementClosed
	}

	values, err := namedValueToParameters(args)
	if err != nil {
		return nil, err
	}

	req := sqliterpc.ExecRequest{
		Sql:        s.query,
		Parameters: values,
	}

	resp, err := s.connection.client.Exec(ctx, &req)
	if err != nil {
		return nil, err
	}

	e := execResult{
		ExecResponse: resp,
	}

	return &e, nil
}

type execResult struct {
	*sqliterpc.ExecResponse
}

func (e *execResult) LastInsertId() (int64, error) {
	return e.ExecResponse.LastInsertId, nil
}

func (e *execResult) RowsAffected() (int64, error) {
	return e.ExecResponse.RowsAffected, nil
}

// see https://pkg.go.dev/database/sql/driver@go1.18.2#Value
func namedValueToParameters(args []driver.NamedValue) ([]*sqliterpc.Value, error) {
	values := make([]*sqliterpc.Value, len(args))

	for _, arg := range args {
		n := arg.Ordinal - 1
		if n > len(args) || n < 0 {
			return nil, fmt.Errorf("invalid ordinal in value: %d", arg.Ordinal)
		}
		switch t := arg.Value.(type) {
		case nil:
			v := sqliterpc.Value{
				Kind: &sqliterpc.Value_NullValue{
					NullValue: &sqliterpc.NullValue{
						Valid: false,
					},
				},
			}
			values[n] = &v

		case int64:
			v := sqliterpc.Value{
				Kind: &sqliterpc.Value_IntegerValue{
					IntegerValue: &sqliterpc.IntergerValue{
						Value: t,
						Valid: true,
					},
				},
			}
			values[n] = &v

		case bool:
			v := sqliterpc.Value{
				Kind: &sqliterpc.Value_BoolValue{
					BoolValue: &sqliterpc.BoolValue{
						Value: t,
						Valid: true,
					},
				},
			}
			values[n] = &v

		case []byte:
			v := sqliterpc.Value{
				Kind: &sqliterpc.Value_BlobValue{
					BlobValue: &sqliterpc.BlobValue{
						Value: t,
						Valid: true,
					},
				},
			}
			values[n] = &v

		case string:
			v := sqliterpc.Value{
				Kind: &sqliterpc.Value_TextValue{
					TextValue: &sqliterpc.TextValue{
						Value: t,
						Valid: true,
					},
				},
			}
			values[n] = &v

		case time.Time:
			v := sqliterpc.Value{
				Kind: &sqliterpc.Value_TimeValue{
					TimeValue: &sqliterpc.TimeValue{
						Value: timestamppb.New(t),
						Valid: true,
					},
				},
			}
			values[n] = &v

		default:
			return nil, fmt.Errorf("unsupported type %T", t)
		}
	}

	return values, nil
}

func (s *statement) Query(args []driver.Value) (driver.Rows, error) {
	return nil, ErrQueryUnsupported
}

func (s *statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.connection == nil {
		return nil, ErrStatementClosed
	}

	values, err := namedValueToParameters(args)
	if err != nil {
		return nil, err
	}

	req := sqliterpc.QueryRequest{
		Sql:        s.query,
		Parameters: values,
	}

	resp, err := s.connection.client.Query(ctx, &req)
	if err != nil {
		return nil, err
	}
	_ = resp

	r := rows{
		response: resp,
		current:  0,
	}

	return &r, nil
}

// assumes only access by one goroutin
type rows struct {
	response *sqliterpc.QueryResponse
	current  int
}

var ErrRowsClosed = errors.New("rows closed")

func (r *rows) Columns() []string {
	if r.response == nil {
		return nil
	}

	out := make([]string, len(r.response.Columns))

	for i, column := range r.response.Columns {
		out[i] = column.Name
	}

	return out
}

// TODO: implement RowsColumnTypeDatabaseTypeName and RowsColumnTypeScanType and ColumnTypeNullable

func (r *rows) Close() error {
	r.response = nil

	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.response == nil {
		return ErrRowsClosed
	}

	if r.current >= len(r.response.Rows) {
		return io.EOF
	}

	row := r.response.Rows[r.current].Values

	// does this matter?
	// if len(dest) < len(row.Values) {
	//	return fmt.Errorf("not enough receivers for values: %d < %d", len(dest), len(row.Values))
	//}

	if len(dest) > len(row) {
		return fmt.Errorf("not enough values for receivers: %d < %d", len(row), len(dest))
	}

	for i := range dest {
		switch r.response.Columns[i].Type {
		case sqliterpc.TypeCode_TYPE_CODE_INTEGER:
			v := row[i].GetIntegerValue()
			if v.GetValid() {
				dest[i] = v.GetValue()
			} else {
				dest[i] = nil
			}

		case sqliterpc.TypeCode_TYPE_CODE_TEXT:
			v := row[i].GetTextValue()
			if v.GetValid() {
				dest[i] = v.GetValue()
			} else {
				dest[i] = nil
			}

		case sqliterpc.TypeCode_TYPE_CODE_BLOB:
			v := row[i].GetBlobValue()
			if v.GetValid() {
				dest[i] = v.GetValue()
			} else {
				dest[i] = nil
			}

		case sqliterpc.TypeCode_TYPE_CODE_REAL:
			v := row[i].GetRealValue()
			if v.GetValid() {
				dest[i] = v.GetValue()
			} else {
				dest[i] = nil
			}

		case sqliterpc.TypeCode_TYPE_CODE_NUMERIC:
			v := row[i].GetNumericValue()
			if v.GetValid() {
				dest[i] = v.GetValue()
			} else {
				dest[i] = nil
			}

		case sqliterpc.TypeCode_TYPE_CODE_BOOL:
			v := row[i].GetNumericValue()
			if v.GetValid() {
				dest[i] = v.GetValue()
			} else {
				dest[i] = nil
			}

		case sqliterpc.TypeCode_TYPE_CODE_TIME:
			v := row[i].GetTimeValue()
			if v.GetValid() {
				dest[i] = v.GetValue().AsTime
			} else {
				dest[i] = nil
			}

		case sqliterpc.TypeCode_TYPE_CODE_NULL:
			dest[i] = nil

		default:
			// should never happen, but just in case
			return fmt.Errorf("unsupported column type %q for %q", r.response.Columns[i].Type, r.response.Columns[i].Name)
		}
	}

	r.current++

	return nil
}
