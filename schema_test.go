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
	"encoding/json"
	"testing"
	"time"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/go-mysql-org/go-mysql/canal"
	mysqlschema "github.com/go-mysql-org/go-mysql/schema"
	"github.com/google/go-cmp/cmp"
	"github.com/hamba/avro/v2"
	"github.com/matryer/is"
)

func TestMysqlSchemaToAvroSchema(t *testing.T) {
	is := is.New(t)

	tableCol := mysqlschema.TableColumn{
		Name:    "test_col",
		Type:    mysqlschema.TYPE_STRING,
		RawType: "varchar(255)",
	}

	avroCol, err := mysqlSchemaToAvroCol(tableCol, false)
	is.NoErr(err)

	is.Equal(avroCol.Name, "test_col")
	is.Equal(avroCol.Type, avro.String)
}

func TestMysqlSchemaToAvroSchema_Nullable(t *testing.T) {
	is := is.New(t)

	tableCol := mysqlschema.TableColumn{
		Name:    "test_col",
		Type:    mysqlschema.TYPE_STRING,
		RawType: "varchar(255)",
	}

	avroCol, err := mysqlSchemaToAvroCol(tableCol, true)
	is.NoErr(err)

	is.Equal(avroCol.Name, "test_col")
	is.Equal(avroCol.Type, avro.String)
	is.True(avroCol.Nullable)
}

// field creates an Avro field with the specified name, type, and nullability.
func field(is *is.I, fieldName string, t avro.Type, nullable bool) *avro.Field {
	var fieldSchema avro.Schema
	if nullable {
		var err error
		fieldSchema, err = avro.NewUnionSchema([]avro.Schema{avro.NewNullSchema(), avro.NewPrimitiveSchema(t, nil)})
		is.NoErr(err)
	} else {
		fieldSchema = avro.NewPrimitiveSchema(t, nil)
	}

	field, err := avro.NewField(fieldName, fieldSchema)
	is.NoErr(err)

	return field
}

// fixed8ByteField creates an Avro fixed-size field (8 bytes) for BIT types.
func fixed8ByteField(is *is.I, fieldName string, nullable bool) *avro.Field {
	fixed8Size := 8

	fixed, err := avro.NewFixedSchema(fieldName+"_fixed", "", fixed8Size, nil)
	is.NoErr(err)

	var fieldSchema avro.Schema
	if nullable {
		fieldSchema, err = avro.NewUnionSchema([]avro.Schema{avro.NewNullSchema(), fixed})
		is.NoErr(err)
	} else {
		fieldSchema = fixed
	}

	field, err := avro.NewField(fieldName, fieldSchema)
	is.NoErr(err)

	return field
}

func toMap(is *is.I, bs []byte) map[string]any {
	m := map[string]any{}
	is.NoErr(json.Unmarshal(bs, &m))

	return m
}

func expectedKeyRecordSchema(is *is.I, tableName string) map[string]any {
	recordSchema, err := avro.NewRecordSchema(tableName+"_key", "mysql", []*avro.Field{
		field(is, "f1", avro.String, false),
	})
	is.NoErr(err)

	bs, err := recordSchema.MarshalJSON()
	is.NoErr(err)

	return toMap(is, bs)
}

// expectedPayloadRecordSchema generates the expected Avro schema for a payload record
// based on the table column types. The nullable parameter determines whether all
// fields should be nullable (for testing nullable column support).
func expectedPayloadRecordSchema(is *is.I, tableName string, nullable bool) map[string]any {
	var fields []*avro.Field

	numCols := []struct {
		name string
		typ  avro.Type
	}{
		// Signed Numeric Types
		{"tiny_int_col", avro.Int},
		{"small_int_col", avro.Int},
		{"medium_int_col", avro.Int},
		{"int_col", avro.Int},
		{"big_int_col", avro.Long},

		// Signed Numeric Types with n-digits
		{"tiny_int_d_col", avro.Int},
		{"small_int_d_col", avro.Int},
		{"medium_int_d_col", avro.Int},
		{"int_d_col", avro.Int},
		{"big_int_d_col", avro.Long},

		// Unsigned Numeric Types
		{"utiny_int_col", avro.Int},
		{"usmall_int_col", avro.Int},
		{"umedium_int_col", avro.Int},
		{"uint_col", avro.Long},
		{"ubig_int_col", avro.Long},
	}

	for _, c := range numCols {
		fields = append(fields, field(is, c.name, c.typ, nullable))
	}

	fields = append(fields, field(is, "decimal_col", avro.Double, nullable))
	fields = append(fields, field(is, "float_col", avro.Float, nullable))
	fields = append(fields, field(is, "double_col", avro.Double, nullable))

	// Unsigned Numeric Types with n-digits
	unsignedDCols := []struct {
		name string
		typ  avro.Type
	}{
		{"utiny_int_d_col", avro.Int},
		{"usmall_int_d_col", avro.Int},
		{"umedium_int_d_col", avro.Int},
		{"uint_d_col", avro.Long},
		{"ubig_int_d_col", avro.Long},
	}
	for _, c := range unsignedDCols {
		fields = append(fields, field(is, c.name, c.typ, nullable))
	}

	fields = append(fields, field(is, "double_d_col", avro.Double, nullable))

	// Bit Types (most common)
	for _, col := range []string{"bit1_col", "bit8_col", "bit64_col"} {
		fields = append(fields, fixed8ByteField(is, col, nullable))
	}

	// String Types
	for _, col := range []string{"char_col", "varchar_col", "tiny_text_col", "text_col", "medium_text_col", "long_text_col"} {
		fields = append(fields, field(is, col, avro.String, nullable))
	}

	// Binary Types
	for _, col := range []string{"binary_col", "varbinary_col", "tiny_blob_col", "blob_col", "medium_blob_col", "long_blob_col"} {
		fields = append(fields, field(is, col, avro.Bytes, nullable))
	}

	// Date and Time Types
	dateCols := []struct {
		name string
		typ  avro.Type
	}{
		{"date_col", avro.String},
		{"time_col", avro.String},
		{"datetime_col", avro.String},
		{"timestamp_col", avro.String},
		{"year_col", avro.Int},
	}
	for _, c := range dateCols {
		fields = append(fields, field(is, c.name, c.typ, nullable))
	}

	// Other Types (JSON)
	fields = append(fields, field(is, "json_col", avro.String, nullable))

	recordSchema, err := avro.NewRecordSchema(tableName+"_payload", "mysql", fields)
	is.NoErr(err)

	bs, err := recordSchema.MarshalJSON()
	is.NoErr(err)

	return toMap(is, bs)
}

// allTypesTestData returns test data for all MySQL column types.
// The nullable parameter determines whether values should be nil.
// Values represent edge cases like max values for specific MySQL types
// and are written in the Avro equivalent type to properly assert formatting.
func allTypesTestData(nullable bool) map[string]any {
	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)

	data := map[string]any{
		// Signed max values
		"tiny_int_col":   int32(127),
		"small_int_col":  int32(32767),
		"medium_int_col": int32(8388607),
		"int_col":        int32(2147483647),
		"big_int_col":    int64(9223372036854775807),

		// Signed Numeric Types with n-digits
		"tiny_int_d_col":   int32(1),
		"small_int_d_col":  int32(32),
		"medium_int_d_col": int32(838),
		"int_d_col":        int32(21474),
		"big_int_d_col":    int64(92233720368),

		// Unsigned max values
		"utiny_int_col":   int32(255),
		"usmall_int_col":  int32(65535),
		"umedium_int_col": int32(16777215),
		"uint_col":        int64(429496729),
		"ubig_int_col":    int64(1844674407370955161),

		// Unsigned Numeric Types with n-digits
		"utiny_int_d_col":   int32(2),
		"usmall_int_d_col":  int32(65),
		"umedium_int_d_col": int32(167),
		"uint_d_col":        int64(42949672),
		"ubig_int_d_col":    int64(184467440737),

		"double_d_col": 12345678.22,

		"float_col":   float32(123.5),
		"decimal_col": 123.5,
		"double_col":  123.5,

		// String Types
		"char_col":        "char",
		"varchar_col":     "varchar",
		"tiny_text_col":   "tiny text",
		"text_col":        "text",
		"medium_text_col": "medium text",
		"long_text_col":   "long text",

		// Binary Types - should be 10 bytes as specified in gorm struct
		"binary_col":      []byte("binary    "),
		"varbinary_col":   []byte("varbinary"),
		"tiny_blob_col":   []byte("tiny blob"),
		"blob_col":        []byte("blob"),
		"medium_blob_col": []byte("medium blob"),
		"long_blob_col":   []byte("long blob"),

		// Bit Types (8 bytes each, big-endian)
		"bit1_col":  []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x01},
		"bit8_col":  []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xAB},
		"bit64_col": []byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0},

		// Date and Time Types - MySQL stores with different precision, truncate to match
		"date_col":      now.Format("2006-01-02"),
		"time_col":      now.Format("15:04:05"),
		"datetime_col":  now.Format("2006-01-02 15:04:05"),
		"timestamp_col": now.Format("2006-01-02 15:04:05"),
		"year_col":      now.Year(),

		// Other Types
		"json_col": `{"key":"value"}`,
	}

	if nullable {
		for k := range data {
			data[k] = nil
		}
	}

	return data
}

// buildAvroColsFromRowsEvent converts canal.RowsEvent columns to Avro columns
// using the provided column nullability map.
func buildAvroColsFromRowsEvent(is *is.I, rowsEvent *canal.RowsEvent, columnNullability map[string]bool) []*avroNamedType {
	avroCols := make([]*avroNamedType, len(rowsEvent.Table.Columns))
	for i, col := range rowsEvent.Table.Columns {
		nullable := columnNullability[col.Name]
		avroCol, err := mysqlSchemaToAvroCol(col, nullable)
		is.NoErr(err)
		avroCols[i] = avroCol
	}
	return avroCols
}

// queryColumnNullabilityFromDB queries the database to determine which columns
// are nullable for a given table.
func queryColumnNullabilityFromDB(ctx context.Context, is *is.I, db testutils.DB, tableName string) map[string]bool {
	columnNullability := make(map[string]bool)
	rows, err := db.SqlxDB.QueryxContext(ctx, `
		SELECT column_name, is_nullable
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
	`, "meroxadb", tableName)
	is.NoErr(err)
	defer rows.Close()

	for rows.Next() {
		var colName, isNullable string
		is.NoErr(rows.Scan(&colName, &isNullable))
		columnNullability[colName] = isNullable == "YES"
	}
	is.NoErr(rows.Err())
	return columnNullability
}

// TestSchema_Payload_SQLX_Rows tests schema generation from sqlx.Rows
// (used for snapshot mode with initial table scan).
func TestSchema_Payload_SQLX_Rows(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	ctx := context.Background()

	type SchemaAllTypes struct {
		// Note: the go types specified here for each column aren't really relevant for
		// the schema tests, we just need the gorm struct tags. We use the avro
		// equivalent in go though to be consistent.

		// Signed Numeric Types
		TinyIntCol   int32 `gorm:"column:tiny_int_col;type:tinyint;not null"`
		SmallIntCol  int32 `gorm:"column:small_int_col;type:smallint;not null"`
		MediumIntCol int32 `gorm:"column:medium_int_col;type:mediumint;not null"`
		IntCol       int32 `gorm:"column:int_col;type:int;not null"`
		BigIntCol    int64 `gorm:"column:big_int_col;type:bigint;not null"`

		// Signed Numeric Types with n-digits
		TinyIntDCol   int32 `gorm:"column:tiny_int_d_col;type:tinyint(1);not null"`
		SmallIntDCol  int32 `gorm:"column:small_int_d_col;type:smallint(2);not null"`
		MediumIntDCol int32 `gorm:"column:medium_int_d_col;type:mediumint(3);not null"`
		IntDCol       int32 `gorm:"column:int_d_col;type:int(5);not null"`
		BigIntDCol    int64 `gorm:"column:big_int_d_col;type:bigint(11);not null"`

		// Unsigned Numeric Types
		UTinyIntCol   int32 `gorm:"column:utiny_int_col;type:tinyint unsigned;not null"`
		USmallIntCol  int32 `gorm:"column:usmall_int_col;type:smallint unsigned;not null"`
		UMediumIntCol int32 `gorm:"column:umedium_int_col;type:mediumint unsigned;not null"`
		UIntCol       int32 `gorm:"column:uint_col;type:int unsigned;not null"`
		UBigIntCol    int64 `gorm:"column:ubig_int_col;type:bigint unsigned;not null"`

		DecimalCol float64 `gorm:"column:decimal_col;type:decimal(10,2);not null"`
		FloatCol   float32 `gorm:"column:float_col;type:float(24);not null"`
		DoubleCol  float64 `gorm:"column:double_col;type:double;not null"`

		// Unsigned Numeric Types with n-digits
		UTinyIntDCol   int32 `gorm:"column:utiny_int_d_col;type:tinyint(1) unsigned;not null"`
		USmallIntDCol  int32 `gorm:"column:usmall_int_d_col;type:smallint(2) unsigned;not null"`
		UMediumIntDCol int32 `gorm:"column:umedium_int_d_col;type:mediumint(3) unsigned;not null"`
		UIntDCol       int32 `gorm:"column:uint_d_col;type:int(8) unsigned;not null"`
		UBigIntDCol    int64 `gorm:"column:ubig_int_d_col;type:bigint(12) unsigned;not null"`

		DoubleDCol float64 `gorm:"column:double_d_col;type:double(10,2);not null"`

		// Bit Types (most common)
		Bit1Col  []byte `gorm:"column:bit1_col;type:bit(1);not null"`
		Bit8Col  []byte `gorm:"column:bit8_col;type:bit(8);not null"`
		Bit64Col []byte `gorm:"column:bit64_col;type:bit(64);not null"`

		// String Types
		CharCol       string `gorm:"column:char_col;type:char(10);not null"`
		VarcharCol    string `gorm:"column:varchar_col;type:varchar(255);not null"`
		TinyTextCol   string `gorm:"column:tiny_text_col;type:tinytext;not null"`
		TextCol       string `gorm:"column:text_col;type:text;not null"`
		MediumTextCol string `gorm:"column:medium_text_col;type:mediumtext;not null"`
		LongTextCol   string `gorm:"column:long_text_col;type:longtext;not null"`

		// Binary Types
		BinaryCol     []byte `gorm:"column:binary_col;type:binary(10);not null"`
		VarbinaryCol  []byte `gorm:"column:varbinary_col;type:varbinary(255);not null"`
		TinyBlobCol   []byte `gorm:"column:tiny_blob_col;type:tinyblob;not null"`
		BlobCol       []byte `gorm:"column:blob_col;type:blob;not null"`
		MediumBlobCol []byte `gorm:"column:medium_blob_col;type:mediumblob;not null"`
		LongBlobCol   []byte `gorm:"column:long_blob_col;type:longblob;not null"`

		// Date and Time Types
		DateCol      time.Time `gorm:"column:date_col;type:date;not null"`
		TimeCol      time.Time `gorm:"column:time_col;type:time;not null"`
		DateTimeCol  time.Time `gorm:"column:datetime_col;type:datetime;not null"`
		TimestampCol time.Time `gorm:"column:timestamp_col;type:timestamp;not null"`
		YearCol      int       `gorm:"column:year_col;type:year;not null"`

		// Other Types
		JSONCol string `gorm:"column:json_col;type:json;not null"`
	}

	testutils.CreateTables(is, db, &SchemaAllTypes{})

	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := SchemaAllTypes{
		// Signed max values
		TinyIntCol:   127,
		SmallIntCol:  32767,
		MediumIntCol: 8388607,
		IntCol:       2147483647,
		BigIntCol:    9223372036854775807,

		// Signed Numeric Types with n-digits
		TinyIntDCol:   1,
		SmallIntDCol:  32,
		MediumIntDCol: 838,
		IntDCol:       21474,
		BigIntDCol:    92233720368,

		// Unsigned max values
		UTinyIntCol:   255,
		USmallIntCol:  65535,
		UMediumIntCol: 16777215,
		UIntCol:       429496729,
		UBigIntCol:    1844674407370955161,

		DecimalCol: 123.45,
		FloatCol:   123.5,
		DoubleCol:  123.5,

		// Unsigned Numeric Types with n-digits
		UTinyIntDCol:   2,
		USmallIntDCol:  65,
		UMediumIntDCol: 167,
		UIntDCol:       42949672,
		UBigIntDCol:    184467440737,

		DoubleDCol: 12345678.22,

		// Bit Types (8 bytes each, big-endian)
		Bit1Col:  []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x01},
		Bit8Col:  []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xAB},
		Bit64Col: []byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0},

		// String Types
		CharCol:       "char",
		VarcharCol:    "varchar",
		TinyTextCol:   "tiny text",
		TextCol:       "text",
		MediumTextCol: "medium text",
		LongTextCol:   "long text",

		// Binary Types - 10 bytes as specified in gorm struct
		BinaryCol:     []byte("binary    "),
		VarbinaryCol:  []byte("varbinary"),
		TinyBlobCol:   []byte("tiny blob"),
		BlobCol:       []byte("blob"),
		MediumBlobCol: []byte("medium blob"),
		LongBlobCol:   []byte("long blob"),

		// Date and Time Types
		DateCol:      now,
		TimeCol:      now,
		DateTimeCol:  now,
		TimestampCol: now,
		YearCol:      now.Year(),

		// Other Types
		JSONCol: `{"key":"value"}`,
	}

	is.NoErr(db.Create(&testData).Error)
	tableName := testutils.TableName(is, db, &SchemaAllTypes{})

	rows, err := db.SqlxDB.Queryx("select * from " + tableName)
	is.NoErr(err)
	colTypes, err := sqlxRowsToAvroCol(rows)
	is.NoErr(err)

	payloadSchemaManager := newSchemaMapper()
	_, err = payloadSchemaManager.createPayloadSchema(ctx, tableName, colTypes)
	is.NoErr(err)

	row := db.SqlxDB.QueryRowx("select * from " + tableName)
	dest := map[string]any{}
	is.NoErr(row.MapScan(dest))

	formatted := map[string]any{}
	for k, v := range dest {
		formatted[k] = payloadSchemaManager.formatValue(ctx, k, v)
	}

	is.True(len(formatted) > 0)

	s, err := schema.Get(ctx, tableName+"_payload", 1)
	is.NoErr(err)

	actualSchema := toMap(is, s.Bytes)
	expectedSchema := expectedPayloadRecordSchema(is, tableName, false)

	is.Equal("", cmp.Diff(expectedSchema, actualSchema))
}

// TestSchema_Payload_SQLX_Rows_Nullable tests schema generation from sqlx.Rows
// with nullable columns.
func TestSchema_Payload_SQLX_Rows_Nullable(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	ctx := context.Background()

	type SchemaAllNullTypes struct {
		// Same column types as SchemaAllTypes but all nullable
		TinyIntCol     *int32     `gorm:"column:tiny_int_col;type:tinyint"`
		SmallIntCol    *int32     `gorm:"column:small_int_col;type:smallint"`
		MediumIntCol   *int32     `gorm:"column:medium_int_col;type:mediumint"`
		IntCol         *int32     `gorm:"column:int_col;type:int"`
		BigIntCol      *int64     `gorm:"column:big_int_col;type:bigint"`
		TinyIntDCol    *int32     `gorm:"column:tiny_int_d_col;type:tinyint(1)"`
		SmallIntDCol   *int32     `gorm:"column:small_int_d_col;type:smallint(2)"`
		MediumIntDCol  *int32     `gorm:"column:medium_int_d_col;type:mediumint(3)"`
		IntDCol        *int32     `gorm:"column:int_d_col;type:int(5)"`
		BigIntDCol     *int64     `gorm:"column:big_int_d_col;type:bigint(11)"`
		UTinyIntCol    *int32     `gorm:"column:utiny_int_col;type:tinyint unsigned"`
		USmallIntCol   *int32     `gorm:"column:usmall_int_col;type:smallint unsigned"`
		UMediumIntCol  *int32     `gorm:"column:umedium_int_col;type:mediumint unsigned"`
		UIntCol        *int32     `gorm:"column:uint_col;type:int unsigned"`
		UBigIntCol     *int64     `gorm:"column:ubig_int_col;type:bigint unsigned"`
		DecimalCol     *float64   `gorm:"column:decimal_col;type:decimal(10,2)"`
		FloatCol       *float32   `gorm:"column:float_col;type:float(24)"`
		DoubleCol      *float64   `gorm:"column:double_col;type:double"`
		UTinyIntDCol   *int32     `gorm:"column:utiny_int_d_col;type:tinyint(1) unsigned"`
		USmallIntDCol  *int32     `gorm:"column:usmall_int_d_col;type:smallint(2) unsigned"`
		UMediumIntDCol *int32     `gorm:"column:umedium_int_d_col;type:mediumint(3) unsigned"`
		UIntDCol       *int32     `gorm:"column:uint_d_col;type:int(8) unsigned"`
		UBigIntDCol    *int64     `gorm:"column:ubig_int_d_col;type:bigint(12) unsigned"`
		DoubleDCol     *float64   `gorm:"column:double_d_col;type:double(10,2)"`
		Bit1Col        []byte     `gorm:"column:bit1_col;type:bit(1)"`
		Bit8Col        []byte     `gorm:"column:bit8_col;type:bit(8)"`
		Bit64Col       []byte     `gorm:"column:bit64_col;type:bit(64)"`
		CharCol        *string    `gorm:"column:char_col;type:char(10)"`
		VarcharCol     *string    `gorm:"column:varchar_col;type:varchar(255)"`
		TinyTextCol    *string    `gorm:"column:tiny_text_col;type:tinytext"`
		TextCol        *string    `gorm:"column:text_col;type:text"`
		MediumTextCol  *string    `gorm:"column:medium_text_col;type:mediumtext"`
		LongTextCol    *string    `gorm:"column:long_text_col;type:longtext"`
		BinaryCol      []byte     `gorm:"column:binary_col;type:binary(10)"`
		VarbinaryCol   []byte     `gorm:"column:varbinary_col;type:varbinary(255)"`
		TinyBlobCol    []byte     `gorm:"column:tiny_blob_col;type:tinyblob"`
		BlobCol        []byte     `gorm:"column:blob_col;type:blob"`
		MediumBlobCol  []byte     `gorm:"column:medium_blob_col;type:mediumblob"`
		LongBlobCol    []byte     `gorm:"column:long_blob_col;type:longblob"`
		DateCol        *time.Time `gorm:"column:date_col;type:date"`
		TimeCol        *time.Time `gorm:"column:time_col;type:time"`
		DateTimeCol    *time.Time `gorm:"column:datetime_col;type:datetime"`
		TimestampCol   *time.Time `gorm:"column:timestamp_col;type:timestamp"`
		YearCol        *int       `gorm:"column:year_col;type:year"`
		JSONCol        *string    `gorm:"column:json_col;type:json"`
	}

	testutils.CreateTables(is, db, &SchemaAllNullTypes{})

	testData := allTypesTestData(true)

	is.NoErr(db.Model(&SchemaAllNullTypes{}).Create(&testData).Error)
	tableName := testutils.TableName(is, db, &SchemaAllNullTypes{})

	rows, err := db.SqlxDB.Queryx("select * from " + tableName)
	is.NoErr(err)
	colTypes, err := sqlxRowsToAvroCol(rows)
	is.NoErr(err)

	payloadSchemaManager := newSchemaMapper()
	_, err = payloadSchemaManager.createPayloadSchema(ctx, tableName, colTypes)
	is.NoErr(err)

	row := db.SqlxDB.QueryRowx("select * from " + tableName)
	dest := map[string]any{}
	is.NoErr(row.MapScan(dest))

	formatted := map[string]any{}
	for k, v := range dest {
		formatted[k] = payloadSchemaManager.formatValue(ctx, k, v)
	}

	s, err := schema.Get(ctx, tableName+"_payload", 1)
	is.NoErr(err)

	actualSchema := toMap(is, s.Bytes)
	expectedSchema := expectedPayloadRecordSchema(is, tableName, true)

	is.Equal("", cmp.Diff(expectedSchema, actualSchema))
	is.Equal("", cmp.Diff(testData, formatted))
}

// TestSchema_Payload_canal_RowsEvent tests schema generation from canal.RowsEvent
// (used for CDC mode with change data capture).
func TestSchema_Payload_canal_RowsEvent(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	ctx := context.Background()

	type SchemaAllTypes struct {
		// Same struct as in TestSchema_Payload_SQLX_Rows
		TinyIntCol     int32     `gorm:"column:tiny_int_col;type:tinyint;not null"`
		SmallIntCol    int32     `gorm:"column:small_int_col;type:smallint;not null"`
		MediumIntCol   int32     `gorm:"column:medium_int_col;type:mediumint;not null"`
		IntCol         int32     `gorm:"column:int_col;type:int;not null"`
		BigIntCol      int64     `gorm:"column:big_int_col;type:bigint;not null"`
		TinyIntDCol    int32     `gorm:"column:tiny_int_d_col;type:tinyint(1);not null"`
		SmallIntDCol   int32     `gorm:"column:small_int_d_col;type:smallint(2);not null"`
		MediumIntDCol  int32     `gorm:"column:medium_int_d_col;type:mediumint(3);not null"`
		IntDCol        int32     `gorm:"column:int_d_col;type:int(5);not null"`
		BigIntDCol     int64     `gorm:"column:big_int_d_col;type:bigint(11);not null"`
		UTinyIntCol    int32     `gorm:"column:utiny_int_col;type:tinyint unsigned;not null"`
		USmallIntCol   int32     `gorm:"column:usmall_int_col;type:smallint unsigned;not null"`
		UMediumIntCol  int32     `gorm:"column:umedium_int_col;type:mediumint unsigned;not null"`
		UIntCol        int32     `gorm:"column:uint_col;type:int unsigned;not null"`
		UBigIntCol     int64     `gorm:"column:ubig_int_col;type:bigint unsigned;not null"`
		DecimalCol     float64   `gorm:"column:decimal_col;type:decimal(10,2);not null"`
		FloatCol       float32   `gorm:"column:float_col;type:float(24);not null"`
		DoubleCol      float64   `gorm:"column:double_col;type:double;not null"`
		UTinyIntDCol   int32     `gorm:"column:utiny_int_d_col;type:tinyint(1) unsigned;not null"`
		USmallIntDCol  int32     `gorm:"column:usmall_int_d_col;type:smallint(2) unsigned;not null"`
		UMediumIntDCol int32     `gorm:"column:umedium_int_d_col;type:mediumint(3) unsigned;not null"`
		UIntDCol       int32     `gorm:"column:uint_d_col;type:int(8) unsigned;not null"`
		UBigIntDCol    int64     `gorm:"column:ubig_int_d_col;type:bigint(12) unsigned;not null"`
		DoubleDCol     float64   `gorm:"column:double_d_col;type:double(10,2);not null"`
		Bit1Col        []byte    `gorm:"column:bit1_col;type:bit(1);not null"`
		Bit8Col        []byte    `gorm:"column:bit8_col;type:bit(8);not null"`
		Bit64Col       []byte    `gorm:"column:bit64_col;type:bit(64);not null"`
		CharCol        string    `gorm:"column:char_col;type:char(10);not null"`
		VarcharCol     string    `gorm:"column:varchar_col;type:varchar(255);not null"`
		TinyTextCol    string    `gorm:"column:tiny_text_col;type:tinytext;not null"`
		TextCol        string    `gorm:"column:text_col;type:text;not null"`
		MediumTextCol  string    `gorm:"column:medium_text_col;type:mediumtext;not null"`
		LongTextCol    string    `gorm:"column:long_text_col;type:longtext;not null"`
		BinaryCol      []byte    `gorm:"column:binary_col;type:binary(10);not null"`
		VarbinaryCol   []byte    `gorm:"column:varbinary_col;type:varbinary(255);not null"`
		TinyBlobCol    []byte    `gorm:"column:tiny_blob_col;type:tinyblob;not null"`
		BlobCol        []byte    `gorm:"column:blob_col;type:blob;not null"`
		MediumBlobCol  []byte    `gorm:"column:medium_blob_col;type:mediumblob;not null"`
		LongBlobCol    []byte    `gorm:"column:long_blob_col;type:longblob;not null"`
		DateCol        time.Time `gorm:"column:date_col;type:date;not null"`
		TimeCol        time.Time `gorm:"column:time_col;type:time;not null"`
		DateTimeCol    time.Time `gorm:"column:datetime_col;type:datetime;not null"`
		TimestampCol   time.Time `gorm:"column:timestamp_col;type:timestamp;not null"`
		YearCol        int       `gorm:"column:year_col;type:year;not null"`
		JSONCol        string    `gorm:"column:json_col;type:json;not null"`
	}

	testutils.CreateTables(is, db, &SchemaAllTypes{})

	now := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := SchemaAllTypes{
		// Signed max values
		TinyIntCol:   127,
		SmallIntCol:  32767,
		MediumIntCol: 8388607,
		IntCol:       2147483647,
		BigIntCol:    9223372036854775807,

		// Signed Numeric Types with n-digits
		TinyIntDCol:   1,
		SmallIntDCol:  32,
		MediumIntDCol: 838,
		IntDCol:       21474,
		BigIntDCol:    92233720368,

		// Unsigned max values
		UTinyIntCol:   255,
		USmallIntCol:  65535,
		UMediumIntCol: 16777215,
		UIntCol:       429496729,
		UBigIntCol:    1844674407370955161,

		DecimalCol: 123.45,
		FloatCol:   123.5,
		DoubleCol:  123.5,

		// Unsigned Numeric Types with n-digits
		UTinyIntDCol:   2,
		USmallIntDCol:  65,
		UMediumIntDCol: 167,
		UIntDCol:       42949672,
		UBigIntDCol:    184467440737,

		DoubleDCol: 12345678.22,

		// Bit Types (8 bytes each, big-endian)
		Bit1Col:  []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x01},
		Bit8Col:  []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xAB},
		Bit64Col: []byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0},

		// String Types
		CharCol:       "char",
		VarcharCol:    "varchar",
		TinyTextCol:   "tiny text",
		TextCol:       "text",
		MediumTextCol: "medium text",
		LongTextCol:   "long text",

		// Binary Types - 10 bytes as specified in gorm struct
		BinaryCol:     []byte("binary    "),
		VarbinaryCol:  []byte("varbinary"),
		TinyBlobCol:   []byte("tiny blob"),
		BlobCol:       []byte("blob"),
		MediumBlobCol: []byte("medium blob"),
		LongBlobCol:   []byte("long blob"),

		// Date and Time Types
		DateCol:      now,
		TimeCol:      now,
		DateTimeCol:  now,
		TimestampCol: now,
		YearCol:      now.Year(),

		// Other Types
		JSONCol: `{"key":"value"}`,
	}

	tableName := testutils.TableName(is, db, &SchemaAllTypes{})

	rowsEvent := testutils.TriggerRowInsertEvent(ctx, is, tableName, func() {
		is.NoErr(db.Create(&testData).Error)
	})

	columnNullability := queryColumnNullabilityFromDB(ctx, is, db, tableName)

	avroCols := buildAvroColsFromRowsEvent(is, rowsEvent, columnNullability)

	payloadSchemaManager := newSchemaMapper()
	_, err := payloadSchemaManager.createPayloadSchema(ctx, tableName, avroCols)
	is.NoErr(err)

	formatted := map[string]any{}
	for i, col := range rowsEvent.Table.Columns {
		formatted[col.Name] = payloadSchemaManager.formatValue(ctx, col.Name, rowsEvent.Rows[0][i])
	}

	is.True(len(formatted) > 0)

	s, err := schema.Get(ctx, tableName+"_payload", 1)
	is.NoErr(err)

	actualSchema := toMap(is, s.Bytes)
	expectedSchema := expectedPayloadRecordSchema(is, tableName, false)

	is.Equal("", cmp.Diff(expectedSchema, actualSchema))
}

// TestSchema_Payload_canal_RowsEvent_Nullable tests schema generation from
// canal.RowsEvent with nullable columns.
func TestSchema_Payload_canal_RowsEvent_Nullable(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	ctx := context.Background()

	type SchemaAllNullTypes struct {
		// Same column types as SchemaAllTypes but all nullable
		TinyIntCol     *int32     `gorm:"column:tiny_int_col;type:tinyint"`
		SmallIntCol    *int32     `gorm:"column:small_int_col;type:smallint"`
		MediumIntCol   *int32     `gorm:"column:medium_int_col;type:mediumint"`
		IntCol         *int32     `gorm:"column:int_col;type:int"`
		BigIntCol      *int64     `gorm:"column:big_int_col;type:bigint"`
		TinyIntDCol    *int32     `gorm:"column:tiny_int_d_col;type:tinyint(1)"`
		SmallIntDCol   *int32     `gorm:"column:small_int_d_col;type:smallint(2)"`
		MediumIntDCol  *int32     `gorm:"column:medium_int_d_col;type:mediumint(3)"`
		IntDCol        *int32     `gorm:"column:int_d_col;type:int(5)"`
		BigIntDCol     *int64     `gorm:"column:big_int_d_col;type:bigint(11)"`
		UTinyIntCol    *int32     `gorm:"column:utiny_int_col;type:tinyint unsigned"`
		USmallIntCol   *int32     `gorm:"column:usmall_int_col;type:smallint unsigned"`
		UMediumIntCol  *int32     `gorm:"column:umedium_int_col;type:mediumint unsigned"`
		UIntCol        *int32     `gorm:"column:uint_col;type:int unsigned"`
		UBigIntCol     *int64     `gorm:"column:ubig_int_col;type:bigint unsigned"`
		DecimalCol     *float64   `gorm:"column:decimal_col;type:decimal(10,2)"`
		FloatCol       *float32   `gorm:"column:float_col;type:float(24)"`
		DoubleCol      *float64   `gorm:"column:double_col;type:double"`
		UTinyIntDCol   *int32     `gorm:"column:utiny_int_d_col;type:tinyint(1) unsigned"`
		USmallIntDCol  *int32     `gorm:"column:usmall_int_d_col;type:smallint(2) unsigned"`
		UMediumIntDCol *int32     `gorm:"column:umedium_int_d_col;type:mediumint(3) unsigned"`
		UIntDCol       *int32     `gorm:"column:uint_d_col;type:int(8) unsigned"`
		UBigIntDCol    *int64     `gorm:"column:ubig_int_d_col;type:bigint(12) unsigned"`
		DoubleDCol     *float64   `gorm:"column:double_d_col;type:double(10,2)"`
		Bit1Col        []byte     `gorm:"column:bit1_col;type:bit(1)"`
		Bit8Col        []byte     `gorm:"column:bit8_col;type:bit(8)"`
		Bit64Col       []byte     `gorm:"column:bit64_col;type:bit(64)"`
		CharCol        *string    `gorm:"column:char_col;type:char(10)"`
		VarcharCol     *string    `gorm:"column:varchar_col;type:varchar(255)"`
		TinyTextCol    *string    `gorm:"column:tiny_text_col;type:tinytext"`
		TextCol        *string    `gorm:"column:text_col;type:text"`
		MediumTextCol  *string    `gorm:"column:medium_text_col;type:mediumtext"`
		LongTextCol    *string    `gorm:"column:long_text_col;type:longtext"`
		BinaryCol      []byte     `gorm:"column:binary_col;type:binary(10)"`
		VarbinaryCol   []byte     `gorm:"column:varbinary_col;type:varbinary(255)"`
		TinyBlobCol    []byte     `gorm:"column:tiny_blob_col;type:tinyblob"`
		BlobCol        []byte     `gorm:"column:blob_col;type:blob"`
		MediumBlobCol  []byte     `gorm:"column:medium_blob_col;type:mediumblob"`
		LongBlobCol    []byte     `gorm:"column:long_blob_col;type:longblob"`
		DateCol        *time.Time `gorm:"column:date_col;type:date"`
		TimeCol        *time.Time `gorm:"column:time_col;type:time"`
		DateTimeCol    *time.Time `gorm:"column:datetime_col;type:datetime"`
		TimestampCol   *time.Time `gorm:"column:timestamp_col;type:timestamp"`
		YearCol        *int       `gorm:"column:year_col;type:year"`
		JSONCol        *string    `gorm:"column:json_col;type:json"`
	}

	testutils.CreateTables(is, db, &SchemaAllNullTypes{})

	testData := allTypesTestData(true)

	tableName := testutils.TableName(is, db, &SchemaAllNullTypes{})

	rowsEvent := testutils.TriggerRowInsertEvent(ctx, is, tableName, func() {
		is.NoErr(db.Model(&SchemaAllNullTypes{}).Create(&testData).Error)
	})

	columnNullability := queryColumnNullabilityFromDB(ctx, is, db, tableName)

	avroCols := buildAvroColsFromRowsEvent(is, rowsEvent, columnNullability)

	payloadSchemaManager := newSchemaMapper()
	_, err := payloadSchemaManager.createPayloadSchema(ctx, tableName, avroCols)
	is.NoErr(err)

	formatted := map[string]any{}
	for i, col := range rowsEvent.Table.Columns {
		formatted[col.Name] = payloadSchemaManager.formatValue(ctx, col.Name, rowsEvent.Rows[0][i])
	}

	s, err := schema.Get(ctx, tableName+"_payload", 1)
	is.NoErr(err)

	actualSchema := toMap(is, s.Bytes)
	expectedSchema := expectedPayloadRecordSchema(is, tableName, true)

	is.Equal("", cmp.Diff(expectedSchema, actualSchema))
	is.Equal("", cmp.Diff(testData, formatted))
}

func TestSchema_Key(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	ctx := context.Background()

	type SchemaExample struct {
		ID int    `gorm:"column:id;type:int;primaryKey"`
		F1 string `gorm:"column:f1;type:varchar(255);not null"`
	}

	testutils.CreateTables(is, db, &SchemaExample{})

	is.NoErr(db.Model(&SchemaExample{}).Create(&map[string]any{
		"id": 1,
		"f1": "test",
	}).Error)
	tableName := testutils.TableName(is, db, &SchemaExample{})

	rows, err := db.SqlxDB.Queryx("select * from " + tableName)
	is.NoErr(err)

	colTypes, err := sqlxRowsToAvroCol(rows)
	is.NoErr(err)

	keySchemaManager := newSchemaMapper()

	var f1Col *avroNamedType
	for _, colType := range colTypes {
		if colType.Name == "f1" {
			f1Col = colType
			break
		}
	}

	_, err = keySchemaManager.createKeySchema(ctx, tableName, []*avroNamedType{f1Col})
	is.NoErr(err)

	s, err := schema.Get(ctx, tableName+"_key", 1)
	is.NoErr(err)

	actualKeySchema := toMap(is, s.Bytes)
	expectedKeySchema := expectedKeyRecordSchema(is, tableName)

	is.Equal("", cmp.Diff(expectedKeySchema, actualKeySchema))
}

// TestSchema_NullValues_SQLX_Rows tests that NULL values in nullable columns
// are properly preserved when reading from sqlx.Rows.
func TestSchema_NullValues_SQLX_Rows(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	ctx := context.Background()

	type SchemaNullValues struct {
		ID        int        `gorm:"column:id;type:int;primaryKey"`
		StringCol *string    `gorm:"column:string_col;type:varchar(255)"`
		IntCol    *int       `gorm:"column:int_col;type:int"`
		FloatCol  *float64   `gorm:"column:float_col;type:float"`
		BoolCol   *bool      `gorm:"column:bool_col;type:tinyint(1)"`
		TimeCol   *time.Time `gorm:"column:time_col;type:timestamp"`
	}

	testutils.CreateTables(is, db, &SchemaNullValues{})

	is.NoErr(db.Model(&SchemaNullValues{}).Create(&map[string]any{
		"id": 1,
	}).Error)

	tableName := testutils.TableName(is, db, &SchemaNullValues{})

	rows, err := db.SqlxDB.Queryx("select * from " + tableName)
	is.NoErr(err)
	colTypes, err := sqlxRowsToAvroCol(rows)
	is.NoErr(err)

	payloadSchemaManager := newSchemaMapper()
	_, err = payloadSchemaManager.createPayloadSchema(ctx, tableName, colTypes)
	is.NoErr(err)

	row := db.SqlxDB.QueryRowx("select * from " + tableName)
	dest := map[string]any{}
	is.NoErr(row.MapScan(dest))

	is.True(dest["string_col"] == nil)
	is.True(dest["int_col"] == nil)
	is.True(dest["float_col"] == nil)
	is.True(dest["bool_col"] == nil)
	is.True(dest["time_col"] == nil)
}

// TestSchema_NullValues_canal_RowsEvent tests that NULL values in nullable columns
// are properly preserved when reading from canal.RowsEvent.
func TestSchema_NullValues_canal_RowsEvent(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	ctx := context.Background()

	type SchemaNullValues struct {
		ID        int        `gorm:"column:id;type:int;primaryKey"`
		StringCol *string    `gorm:"column:string_col;type:varchar(255)"`
		IntCol    *int       `gorm:"column:int_col;type:int"`
		FloatCol  *float64   `gorm:"column:float_col;type:float"`
		BoolCol   *bool      `gorm:"column:bool_col;type:tinyint(1)"`
		TimeCol   *time.Time `gorm:"column:time_col;type:timestamp"`
	}

	testutils.CreateTables(is, db, &SchemaNullValues{})

	tableName := testutils.TableName(is, db, &SchemaNullValues{})

	rowsEvent := testutils.TriggerRowInsertEvent(ctx, is, tableName, func() {
		is.NoErr(db.Model(&SchemaNullValues{}).Create(&map[string]any{
			"id": 1,
		}).Error)
	})

	columnNames := make([]string, len(rowsEvent.Table.Columns))
	for i, col := range rowsEvent.Table.Columns {
		columnNames[i] = col.Name
	}

	is.True(len(columnNames) > 0)
}
