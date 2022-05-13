package driver_test

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bakins/sqliterpc"
	"github.com/bakins/sqliterpc/driver"
	"github.com/bakins/sqliterpc/server"
)

func TestDriver(t *testing.T) {
	file := "testing.db"
	defer os.Remove(file)

	s, err := server.New(file)
	require.NoError(t, err)

	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	_, err = s.Exec(
		ctx,
		&sqliterpc.ExecRequest{
			Sql: `DROP TABLE IF EXISTS testing`,
		},
	)
	require.NoError(t, err)

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

	svr := httptest.NewServer(sqliterpc.NewDatabaseServiceServer(s))
	defer svr.Close()

	connector, err := driver.NewDriver(nil).OpenConnector(svr.URL)
	require.NoError(t, err)

	db := sql.OpenDB(connector)

	for i := 0; i < 100; i++ {
		res, err := db.ExecContext(ctx, `insert into testing (intCol) values (?)`, i)
		require.NoError(t, err)

		affectedRows, err := res.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), affectedRows)
	}

	rows, err := db.QueryContext(ctx, `select intCol from testing`)
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		var val int64
		err := rows.Scan(&val)
		require.NoError(t, err)

		require.Equal(t, int64(count), val)

		count++
	}

	require.Equal(t, 100, count)
}
