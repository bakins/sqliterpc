package server

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/twitchtv/twirp"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bakins/sqliterpc"
)

type DatabaseServer struct {
	db *sql.DB
}

var _ sqliterpc.DatabaseService = &DatabaseServer{}

func New(dsn string) (*DatabaseServer, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(-1)
	db.SetMaxIdleConns(1)
	// TODO: is this correct? I think we just need to lock on execs
	db.SetMaxOpenConns(1)

	s := DatabaseServer{
		db: db,
	}

	return &s, nil
}

func (s *DatabaseServer) Close() error {
	return s.db.Close()
}

func (s *DatabaseServer) Exec(ctx context.Context, req *sqliterpc.ExecRequest) (*sqliterpc.ExecResponse, error) {
	parameters, err := valuesToParams(req.Parameters)
	if err != nil {
		// TODO: properly wrap the errors - bad sql should return invalidargument, etc
		twerr := twirp.InternalError(err.Error())
		return nil, twerr
	}

	// TODO: prepared statement cache?

	result, err := s.db.ExecContext(ctx, req.Sql, parameters...)
	if err != nil {
		// TODO: properly wrap the errors - bad sql should return invalidargument, etc
		// see https://pkg.go.dev/github.com/mattn/go-sqlite3#ErrNo
		twerr := twirp.InternalError(err.Error())
		return nil, twerr
	}

	last, _ := result.LastInsertId()
	affected, _ := result.RowsAffected()

	resp := sqliterpc.ExecResponse{
		LastInsertId: last,
		RowsAffected: affected,
	}

	return &resp, nil
}

func valuesToParams(values []*sqliterpc.Value) ([]interface{}, error) {
	parameters := make([]interface{}, len(values))

	for i, val := range values {
		switch parameter := val.Kind.(type) {
		case *sqliterpc.Value_IntegerValue:
			if parameter.IntegerValue.Valid {
				parameters[i] = parameter.IntegerValue.Value
			} else {
				parameters[i] = nil
			}
		case *sqliterpc.Value_TextValue:
			if parameter.TextValue.Valid {
				parameters[i] = parameter.TextValue.Value
			} else {
				parameters[i] = nil
			}

		case *sqliterpc.Value_BlobValue:
			if parameter.BlobValue.Valid {
				parameters[i] = parameter.BlobValue.Value
			} else {
				parameters[i] = nil
			}

		case *sqliterpc.Value_RealValue:
			if parameter.RealValue.Valid {
				parameters[i] = parameter.RealValue.Value
			} else {
				parameters[i] = nil
			}

		case *sqliterpc.Value_NumericValue:
			if parameter.NumericValue.Valid {
				parameters[i] = parameter.NumericValue.Value
			} else {
				parameters[i] = nil
			}

		case *sqliterpc.Value_BoolValue:
			if parameter.BoolValue.Valid {
				parameters[i] = parameter.BoolValue.Value
			} else {
				parameters[i] = nil
			}
		case *sqliterpc.Value_TimeValue:
			if parameter.TimeValue.Valid {
				parameters[i] = parameter.TimeValue.Value.AsTime()
			} else {
				parameters[i] = nil
			}

		case *sqliterpc.Value_NullValue:
			parameters[i] = nil
		default:
			return nil, fmt.Errorf("unsupported type %T", parameter)
		}
	}
	return parameters, nil
}

func (s *DatabaseServer) Query(ctx context.Context, req *sqliterpc.QueryRequest) (*sqliterpc.QueryResponse, error) {
	parameters, err := valuesToParams(req.Parameters)
	if err != nil {
		// TODO: properly wrap the errors - bad sql should return invalidargument, etc
		twerr := twirp.InternalError(err.Error())
		return nil, twerr
	}
	// TODO: prepared statement cache?

	rows, err := s.db.QueryContext(ctx, req.Sql, parameters...)
	if err != nil {
		// TODO: properly wrap the errors - bad sql should return invalidargument, etc
		twerr := twirp.InternalError(err.Error())
		return nil, twerr
	}

	defer rows.Close()

	// TODO: how to handle large responses without streaming?
	// use paging?

	// can we cache column metadata? select * may be an issues.
	// also, if DDL changes while cached.

	types, err := rows.ColumnTypes()
	if err != nil {
		// TODO: properly wrap the errors
		twerr := twirp.InternalError(err.Error())
		return nil, twerr
	}

	resp := sqliterpc.QueryResponse{
		Types: make([]sqliterpc.TypeCode, len(types)),
	}

	for i, t := range types {
		code := databaseTypeConvSqlite(t.DatabaseTypeName())
		if code == sqliterpc.TypeCode_TYPE_CODE_NULL {
			// can you even declare a column as type NULL?
			twerr := twirp.InternalErrorf("unable to handle column type %q", t.DatabaseTypeName())
			return nil, twerr
		}
		resp.Types[i] = code
	}

	// avert your eyes! this is clunky and needs some refactoring
	for rows.Next() {
		scanTarget := make([]interface{}, len(types))

		// see https://github.com/mattn/go-sqlite3/blob/2df077b74c66723d9b44d01c8db88e74191bdd0e/sqlite3_type.go#L58
		for i, t := range resp.Types {
			switch t {
			case sqliterpc.TypeCode_TYPE_CODE_INTEGER:
				scanTarget[i] = &sql.NullInt64{}
			case sqliterpc.TypeCode_TYPE_CODE_TEXT:
				scanTarget[i] = &sql.NullString{}
			case sqliterpc.TypeCode_TYPE_CODE_BLOB:
				scanTarget[i] = &nullBytes{}
			case sqliterpc.TypeCode_TYPE_CODE_REAL:
				scanTarget[i] = &sql.NullFloat64{}
			case sqliterpc.TypeCode_TYPE_CODE_NUMERIC:
				scanTarget[i] = &sql.NullFloat64{}
			case sqliterpc.TypeCode_TYPE_CODE_BOOL:
				scanTarget[i] = &sql.NullBool{}
			case sqliterpc.TypeCode_TYPE_CODE_TIME:
				scanTarget[i] = &sql.NullTime{}
			default:
				// should never get here, but just in case
				twerr := twirp.InternalErrorf("unable to handle column type %q", t.String())
				return nil, twerr
			}
		}

		if err := rows.Scan(scanTarget...); err != nil {
			// TODO: properly wrap the errors
			twerr := twirp.InternalError(err.Error())
			return nil, twerr
		}

		row := sqliterpc.ListValue{
			Values: make([]*sqliterpc.Value, len(types)),
		}

		for i, t := range resp.Types {
			switch t {
			case sqliterpc.TypeCode_TYPE_CODE_INTEGER:
				s := scanTarget[i].(*sql.NullInt64)
				row.Values[i] = &sqliterpc.Value{
					Kind: &sqliterpc.Value_IntegerValue{
						IntegerValue: &sqliterpc.IntergerValue{
							Value: s.Int64,
							Valid: s.Valid,
						},
					},
				}

			case sqliterpc.TypeCode_TYPE_CODE_TEXT:
				s := scanTarget[i].(*sql.NullString)
				row.Values[i] = &sqliterpc.Value{
					Kind: &sqliterpc.Value_TextValue{
						TextValue: &sqliterpc.TextValue{
							Value: s.String,
							Valid: s.Valid,
						},
					},
				}

			case sqliterpc.TypeCode_TYPE_CODE_BLOB:
				s := scanTarget[i].(*nullBytes)
				row.Values[i] = &sqliterpc.Value{
					Kind: &sqliterpc.Value_BlobValue{
						BlobValue: &sqliterpc.BlobValue{
							Value: s.Value,
							Valid: s.Valid,
						},
					},
				}

			case sqliterpc.TypeCode_TYPE_CODE_REAL:
				s := scanTarget[i].(*sql.NullFloat64)
				row.Values[i] = &sqliterpc.Value{
					Kind: &sqliterpc.Value_RealValue{
						RealValue: &sqliterpc.RealValue{
							Value: s.Float64,
							Valid: s.Valid,
						},
					},
				}

			case sqliterpc.TypeCode_TYPE_CODE_NUMERIC:
				s := scanTarget[i].(*sql.NullFloat64)
				row.Values[i] = &sqliterpc.Value{
					Kind: &sqliterpc.Value_NumericValue{
						NumericValue: &sqliterpc.NumericValue{
							Value: s.Float64,
							Valid: s.Valid,
						},
					},
				}

			case sqliterpc.TypeCode_TYPE_CODE_BOOL:
				s := scanTarget[i].(*sql.NullBool)
				row.Values[i] = &sqliterpc.Value{
					Kind: &sqliterpc.Value_BoolValue{
						BoolValue: &sqliterpc.BoolValue{
							Value: s.Bool,
							Valid: s.Valid,
						},
					},
				}

			case sqliterpc.TypeCode_TYPE_CODE_TIME:
				s := scanTarget[i].(*sql.NullTime)

				v := sqliterpc.TimeValue{
					Valid: s.Valid,
				}

				if s.Valid {
					v.Value = timestamppb.New(s.Time)
				}

				row.Values[i] = &sqliterpc.Value{
					Kind: &sqliterpc.Value_TimeValue{
						TimeValue: &v,
					},
				}

			default:
				// should never get here, but just in case
				twerr := twirp.InternalErrorf("unable to handle column type %q", t.String())
				return nil, twerr
			}
		}

		resp.Rows = append(resp.Rows, &row)

	}

	return &resp, nil
}

// based on https://github.com/mattn/go-sqlite3/blob/2df077b74c66723d9b44d01c8db88e74191bdd0e/sqlite3_type.go#L80
func databaseTypeConvSqlite(t string) sqliterpc.TypeCode {
	if strings.Contains(t, "INT") {
		return sqliterpc.TypeCode_TYPE_CODE_INTEGER
	}
	if t == "CLOB" || t == "TEXT" ||
		strings.Contains(t, "CHAR") {
		return sqliterpc.TypeCode_TYPE_CODE_TEXT
	}
	if t == "BLOB" {
		return sqliterpc.TypeCode_TYPE_CODE_BLOB
	}
	if t == "REAL" || t == "FLOAT" ||
		strings.Contains(t, "DOUBLE") {
		return sqliterpc.TypeCode_TYPE_CODE_REAL
	}
	if t == "DATE" || t == "DATETIME" ||
		t == "TIMESTAMP" {
		return sqliterpc.TypeCode_TYPE_CODE_TIME
	}
	if t == "NUMERIC" ||
		strings.Contains(t, "DECIMAL") {
		return sqliterpc.TypeCode_TYPE_CODE_NUMERIC
	}
	if t == "BOOLEAN" || t == "BOOL" {
		return sqliterpc.TypeCode_TYPE_CODE_BOOL
	}

	return sqliterpc.TypeCode_TYPE_CODE_NULL
}

type nullBytes struct {
	Value []byte
	Valid bool
}

func (n *nullBytes) Scan(value interface{}) error {
	if value == nil {
		n.Value, n.Valid = nil, false
		return nil
	}

	n.Valid = true

	val, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("cannot convert %T to []bytes", value)
	}

	n.Value = val

	return nil
}
