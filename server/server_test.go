package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bakins/sqliterpc"
	"github.com/bakins/sqliterpc/server"
)

func TestSimple(t *testing.T) {
	s, err := server.New("file::memory:?cache=shared&_journal_mode=WAL")
	require.NoError(t, err)

	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	_, err = s.Exec(
		ctx,
		&sqliterpc.ExecRequest{
			Sql: `create table testing (
				intCol INTEGER,
				textCol TEXT,
				blobCol BLOB,
				realCol REAL,
				numericCol NUMERIC,
				boolCol BOOLEAN,
				timeCol TIMESTAMP
			)`,
		},
	)

	require.NoError(t, err)

	testTime := time.Now()

	_, err = s.Exec(
		ctx,
		&sqliterpc.ExecRequest{
			Sql: `insert into testing (intCol, textCol, blobCol, realCol, numericCol, boolCol, timeCol) values(?, ?, ?, ?, ?, ?, ?)`,
			Parameters: []*sqliterpc.Value{
				{
					Kind: &sqliterpc.Value_IntegerValue{
						IntegerValue: &sqliterpc.IntergerValue{
							Value: 10,
							Valid: true,
						},
					},
				},
				{
					Kind: &sqliterpc.Value_TextValue{
						TextValue: &sqliterpc.TextValue{
							Value: "text-value",
							Valid: true,
						},
					},
				},
				{
					Kind: &sqliterpc.Value_BlobValue{
						BlobValue: &sqliterpc.BlobValue{
							Value: []byte("blob-value"),
							Valid: true,
						},
					},
				},
				{
					Kind: &sqliterpc.Value_RealValue{
						RealValue: &sqliterpc.RealValue{
							Value: 101.0,
							Valid: true,
						},
					},
				},
				{
					Kind: &sqliterpc.Value_NumericValue{
						NumericValue: &sqliterpc.NumericValue{
							Value: 1024.0,
							Valid: true,
						},
					},
				},
				{
					Kind: &sqliterpc.Value_BoolValue{
						BoolValue: &sqliterpc.BoolValue{
							Value: true,
							Valid: true,
						},
					},
				},
				{
					Kind: &sqliterpc.Value_TimeValue{
						TimeValue: &sqliterpc.TimeValue{
							Value: timestamppb.New(testTime),
							Valid: true,
						},
					},
				},
			},
		},
	)
	require.NoError(t, err)

	// insert using type specific null
	_, err = s.Exec(
		ctx,
		&sqliterpc.ExecRequest{
			Sql: `insert into testing (intCol, textCol, blobCol, realCol, numericCol, boolCol, timeCol) values(?, ?, ?, ?, ?, ?, ?)`,
			Parameters: []*sqliterpc.Value{
				{
					Kind: &sqliterpc.Value_IntegerValue{
						IntegerValue: &sqliterpc.IntergerValue{
							Valid: false,
						},
					},
				},
				{
					Kind: &sqliterpc.Value_TextValue{
						TextValue: &sqliterpc.TextValue{
							Valid: false,
						},
					},
				},
				{
					Kind: &sqliterpc.Value_BlobValue{
						BlobValue: &sqliterpc.BlobValue{
							Valid: false,
						},
					},
				},
				{
					Kind: &sqliterpc.Value_RealValue{
						RealValue: &sqliterpc.RealValue{
							Valid: false,
						},
					},
				},
				{
					Kind: &sqliterpc.Value_NumericValue{
						NumericValue: &sqliterpc.NumericValue{
							Valid: false,
						},
					},
				},
				{
					Kind: &sqliterpc.Value_BoolValue{
						BoolValue: &sqliterpc.BoolValue{
							Valid: false,
						},
					},
				},
				{
					Kind: &sqliterpc.Value_TimeValue{
						TimeValue: &sqliterpc.TimeValue{
							Valid: false,
						},
					},
				},
			},
		},
	)
	require.NoError(t, err)

	//	// insert using type general null
	_, err = s.Exec(
		ctx,
		&sqliterpc.ExecRequest{
			Sql: `insert into testing (intCol, textCol, blobCol, realCol, numericCol, boolCol, timeCol) values(?, ?, ?, ?, ?, ?, ?)`,
			Parameters: []*sqliterpc.Value{
				{
					Kind: &sqliterpc.Value_NullValue{},
				},
				{
					Kind: &sqliterpc.Value_NullValue{},
				},
				{
					Kind: &sqliterpc.Value_NullValue{},
				},
				{
					Kind: &sqliterpc.Value_NullValue{},
				},
				{
					Kind: &sqliterpc.Value_NullValue{},
				},
				{
					Kind: &sqliterpc.Value_NullValue{},
				},
				{
					Kind: &sqliterpc.Value_NullValue{},
				},
			},
		},
	)
	require.NoError(t, err)

	resp, err := s.Query(
		ctx,
		&sqliterpc.QueryRequest{
			Sql: "select * from testing",
		},
	)

	require.NoError(t, err)
	require.Len(t, resp.Rows, 3)

	require.Len(t, resp.Rows[0].Values, 7)

	// Note: we use direct struct access (ie, .Valid) rather than the helpers
	// (ie GetValid) to ensure the correct type was set
	require.True(t, resp.Rows[0].Values[0].GetIntegerValue().Valid)
	require.Equal(t, int64(10), resp.Rows[0].Values[0].GetIntegerValue().Value)

	require.True(t, resp.Rows[0].Values[1].GetTextValue().Valid)
	require.Equal(t, "text-value", resp.Rows[0].Values[1].GetTextValue().Value)

	require.True(t, resp.Rows[0].Values[2].GetBlobValue().Valid)
	require.Equal(t, []byte("blob-value"), resp.Rows[0].Values[2].GetBlobValue().Value)

	require.True(t, resp.Rows[0].Values[3].GetRealValue().Valid)
	require.Equal(t, 101.0, resp.Rows[0].Values[3].GetRealValue().Value)

	require.True(t, resp.Rows[0].Values[4].GetNumericValue().Valid)
	require.Equal(t, 1024.0, resp.Rows[0].Values[4].GetNumericValue().Value)

	require.True(t, resp.Rows[0].Values[5].GetBoolValue().Valid)
	require.Equal(t, true, resp.Rows[0].Values[5].GetBoolValue().Value)

	require.True(t, resp.Rows[0].Values[6].GetTimeValue().Valid)
	require.Equal(t, testTime.YearDay(), resp.Rows[0].Values[6].GetTimeValue().Value.AsTime().YearDay())

	require.Len(t, resp.Rows[1].Values, 7)

	require.False(t, resp.Rows[1].Values[0].GetIntegerValue().Valid)

	require.False(t, resp.Rows[1].Values[1].GetTextValue().Valid)

	require.False(t, resp.Rows[1].Values[2].GetBlobValue().Valid)

	require.False(t, resp.Rows[1].Values[3].GetRealValue().Valid)

	require.False(t, resp.Rows[1].Values[4].GetNumericValue().Valid)

	require.False(t, resp.Rows[1].Values[5].GetBoolValue().Valid)

	require.False(t, resp.Rows[1].Values[6].GetTimeValue().Valid)

	require.Len(t, resp.Rows[1].Values, 7)

	require.False(t, resp.Rows[1].Values[0].GetIntegerValue().Valid)

	require.False(t, resp.Rows[1].Values[1].GetTextValue().Valid)

	// we should get back type specific NULL even when inserting general NULL
	require.Len(t, resp.Rows[2].Values, 7)
	require.False(t, resp.Rows[2].Values[2].GetBlobValue().Valid)

	require.False(t, resp.Rows[2].Values[3].GetRealValue().Valid)

	require.False(t, resp.Rows[2].Values[4].GetNumericValue().Valid)

	require.False(t, resp.Rows[2].Values[5].GetBoolValue().Valid)

	require.False(t, resp.Rows[2].Values[6].GetTimeValue().Valid)
}
