package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
				numericCol NUMERIC
			)`,
		},
	)

	require.NoError(t, err)

	_, err = s.Exec(
		ctx,
		&sqliterpc.ExecRequest{
			Sql: `insert into testing (intCol, textCol, blobCol, realCol,numericCol) values(?, ?, ?, ?, ?)`,
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
			},
		},
	)
	require.NoError(t, err)
	/*
		_, err = s.Exec(
			ctx,
			&sqliterpc.ExecRequest{
				Sql: `insert into testing (intCol) values(?)`,
				Parameters: []*sqliterpc.Value{
					{
						Kind: &sqliterpc.Value_IntegerValue{
							IntegerValue: &sqliterpc.IntergerValue{
								Valid: false,
							},
						},
					},
				},
			},
		)
		require.NoError(t, err)

		_, err = s.Exec(
			ctx,
			&sqliterpc.ExecRequest{
				Sql: `insert into testing (intCol) values(?)`,
				Parameters: []*sqliterpc.Value{
					{
						Kind: &sqliterpc.Value_NullValue{},
					},
				},
			},
		)
		require.NoError(t, err)

	*/
	resp, err := s.Query(
		ctx,
		&sqliterpc.QueryRequest{
			Sql: "select * from testing",
		},
	)

	require.NoError(t, err)
	require.Len(t, resp.Rows, 1)

	require.Len(t, resp.Rows[0].Values, 5)

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

	// require.False(t, resp.Rows[1].Values[0].GetIntegerValue().GetValid())
	// require.False(t, resp.Rows[2].Values[0].GetIntegerValue().GetValid())
}
