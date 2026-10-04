// Package migrations embeds the SQL migrations (goose), so the migrate command and the tests use the same files.
package migrations

import "embed"

// FS holds every migration, applied in file-name order.
//
//go:embed *.sql
var FS embed.FS

// Version is the latest migration. The API reports ready only once the database has reached it, so traffic never hits a schema the code does not expect.
const Version = 1
