// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"testing"

	"github.com/matryer/is"
)

func TestSource_Teardown(t *testing.T) {
	is := is.New(t)
	con := NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func TestSourceConfig_Validate_SnapshotMode(t *testing.T) {
	tests := []struct {
		name         string
		snapshotMode string
		wantErr      bool
	}{
		{
			name:         "default mode initial",
			snapshotMode: "initial",
			wantErr:      false,
		},
		{
			name:         "snapshot only mode",
			snapshotMode: "initial_only",
			wantErr:      false,
		},
		{
			name:         "invalid mode",
			snapshotMode: "invalid_mode",
			wantErr:      true,
		},
		{
			name:         "empty mode",
			snapshotMode: "",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			cfg := SourceConfig{
				Config: Config{
					DSN: "user:password@tcp(localhost:3306)/testdb",
				},
				Tables:       []string{"*"},
				SnapshotMode: tt.snapshotMode,
			}

			err := cfg.Validate(context.Background())
			if tt.wantErr {
				is.True(err != nil)
			} else {
				is.NoErr(err)
			}
		})
	}
}
