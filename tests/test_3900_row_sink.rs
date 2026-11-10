// -----------------------------------------------------------------------------
// Copyright (c) 2026, Oracle and/or its affiliates.
//
// This software is dual-licensed to you under the Universal Permissive License
// (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
// 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
// either license.
//
// If you elect to accept the software under the Apache License, Version 2.0,
// the following applies:
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// test_3900_row_sink.rs
//
// Tests fetching rows into a row sink (Cursor::fetch_into), which must give
// the values that fetching rows gives.
//
// Added by Tnsor Labs, 2026-11-10.
// -----------------------------------------------------------------------------

mod common;

use oracledb::{OracleNumber, OracleTimestamp, RowSink, WireValue};
use rstest::*;

use common::conn;

/// A value as both paths can show it.
#[derive(Clone, Debug, PartialEq)]
enum Value {
    Null,
    Text(String),
    Raw(Vec<u8>),
    Double(f64),
    Float(f32),
    Boolean(bool),
}

/// Collects rows of values, as a columnar consumer would: each column on
/// its own, with repeats taken from the column's previous value.
#[derive(Default)]
struct Collector {
    columns: Vec<Vec<Value>>,
    rows: usize,
    /// Rows the sink was told to discard, in total.
    discarded: usize,
}

impl Collector {
    fn new(columns: usize) -> Self {
        Self { columns: vec![Vec::new(); columns], ..Self::default() }
    }

    fn push_row(&mut self, row: Vec<Value>) {
        for (column, value) in self.columns.iter_mut().zip(row) {
            column.push(value);
        }
        self.rows += 1;
    }

    fn rows(&self) -> Vec<Vec<Value>> {
        (0..self.rows)
            .map(|i| self.columns.iter().map(|c| c[i].clone()).collect())
            .collect()
    }
}

fn national_text(bytes: &[u8]) -> String {
    let units: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|pair| u16::from_be_bytes([pair[0], pair[1]]))
        .collect();
    String::from_utf16(&units).unwrap()
}

impl RowSink for Collector {
    fn append(
        &mut self,
        column: usize,
        value: WireValue<'_>,
    ) -> Result<(), oracledb::Error> {
        let value = match value {
            WireValue::Null => Value::Null,
            WireValue::Number(bytes) => {
                Value::Text(WireValue::number(bytes).to_string())
            }
            WireValue::Text(bytes) => {
                Value::Text(String::from_utf8(bytes.to_vec()).unwrap())
            }
            WireValue::NationalText(bytes) => {
                Value::Text(national_text(bytes))
            }
            WireValue::Raw(bytes) => Value::Raw(bytes.to_vec()),
            WireValue::Timestamp(bytes) => {
                Value::Text(WireValue::timestamp(bytes).to_string())
            }
            WireValue::BinaryDouble(v) => Value::Double(v),
            WireValue::BinaryFloat(v) => Value::Float(v),
            WireValue::Boolean(v) => Value::Boolean(v),
            WireValue::IntervalDS(bytes) => {
                Value::Text(WireValue::interval_ds(bytes).to_string())
            }
            WireValue::IntervalYM(bytes) => {
                Value::Text(WireValue::interval_ym(bytes).to_string())
            }
            WireValue::Rowid(text) => Value::Text(text.to_string()),
            WireValue::Json(bytes) => {
                Value::Text(canonical_json(&WireValue::json(bytes)?))
            }
            WireValue::Vector(bytes) => {
                Value::Text(format!("{:?}", WireValue::vector(bytes)?))
            }
        };
        self.columns[column].push(value);
        Ok(())
    }

    fn repeat(&mut self, column: usize) -> Result<(), oracledb::Error> {
        let last = self.columns[column].last().cloned().unwrap();
        self.columns[column].push(last);
        Ok(())
    }

    fn end_row(&mut self) -> Result<(), oracledb::Error> {
        self.rows += 1;
        Ok(())
    }

    fn discard(&mut self, rows: usize) -> Result<(), oracledb::Error> {
        self.rows -= rows;
        self.discarded += rows;
        for column in &mut self.columns {
            column.truncate(self.rows);
        }
        Ok(())
    }
}

/// JSON as text with object keys sorted, so that two values compare equal
/// whatever order their objects' keys come in.
fn canonical_json(value: &oracledb::JsonValue) -> String {
    use oracledb::JsonValue;
    match value {
        JsonValue::JsonObject(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let fields: Vec<String> = keys
                .into_iter()
                .map(|k| format!("{k:?}:{}", canonical_json(&map[k])))
                .collect();
            format!("{{{}}}", fields.join(","))
        }
        JsonValue::JsonArray(items) => {
            let items: Vec<String> =
                items.iter().map(canonical_json).collect();
            format!("[{}]", items.join(","))
        }
        other => format!("{other:?}"),
    }
}

/// A value of a row read the usual way, by the column's type.
fn row_value(
    row: &oracledb::Row,
    index: usize,
    db_type: &oracledb::DbType,
) -> Result<Value, oracledb::Error> {
    let name = db_type.to_string();
    let value = if name.contains("NUMBER") || name.contains("INTEGER") {
        row.get::<Option<OracleNumber>>(index)?
            .map(|v| Value::Text(v.to_string()))
    } else if name.contains("DATE") || name.contains("TIMESTAMP") {
        row.get::<Option<OracleTimestamp>>(index)?
            .map(|v| Value::Text(v.to_string()))
    } else if name.contains("RAW") || name.contains("BLOB") {
        row.get::<Option<Vec<u8>>>(index)?.map(Value::Raw)
    } else if name.contains("BINARY_DOUBLE") {
        row.get::<Option<f64>>(index)?.map(Value::Double)
    } else if name.contains("BINARY_FLOAT") {
        row.get::<Option<f32>>(index)?.map(Value::Float)
    } else if name.contains("BOOLEAN") {
        row.get::<Option<bool>>(index)?.map(Value::Boolean)
    } else if name.contains("JSON") {
        row.get::<Option<oracledb::JsonValue>>(index)?
            .map(|v| Value::Text(canonical_json(&v)))
    } else if name.contains("VECTOR") {
        row.get::<Option<oracledb::Vector>>(index)?
            .map(|v| Value::Text(format!("{v:?}")))
    } else if name.contains("INTERVAL_DS") {
        row.get::<Option<oracledb::OracleIntervalDS>>(index)?
            .map(|v| Value::Text(v.to_string()))
    } else if name.contains("INTERVAL_YM") {
        row.get::<Option<oracledb::OracleIntervalYM>>(index)?
            .map(|v| Value::Text(v.to_string()))
    } else {
        row.get::<Option<String>>(index)?.map(Value::Text)
    };
    Ok(value.unwrap_or(Value::Null))
}

/// Reads the query through `next()`, and again through `fetch_into()`, and
/// checks both give the same rows. Returns the rows and the collector.
fn compare(
    conn: &oracledb::Connection,
    sql: &str,
    fetch_array_size: u32,
) -> Result<(Vec<Vec<Value>>, Collector), oracledb::Error> {
    let cursor = conn
        .statement(sql)?
        .fetch_array_size(fetch_array_size)
        .build()?
        .query(&[])?;
    let types: Vec<_> = cursor.columns().iter().map(|c| c.db_type()).collect();
    let mut expected = Vec::new();
    for row in cursor {
        let row = row?;
        let values = (0..types.len())
            .map(|i| row_value(&row, i, types[i]))
            .collect::<Result<Vec<_>, _>>()?;
        expected.push(values);
    }

    let mut cursor = conn
        .statement(sql)?
        .fetch_array_size(fetch_array_size)
        .build()?
        .query(&[])?;
    assert!(cursor.columns().iter().all(oracledb::row_sink_supports));
    let mut sink = Collector::new(types.len());
    // The rows that came with the execute, then the sink.
    while cursor.buffered_rows() > 0 {
        let row = cursor.next().unwrap()?;
        let values = (0..types.len())
            .map(|i| row_value(&row, i, types[i]))
            .collect::<Result<Vec<_>, _>>()?;
        sink.push_row(values);
    }
    while cursor.fetch_into(&mut sink)? > 0 {}
    assert_eq!(cursor.fetch_into(&mut sink)?, 0);
    assert_eq!(sink.rows(), expected);
    Ok((expected, sink))
}

const ALL_TYPES: &str = r#"
    select
        level as id,
        case when mod(level, 3) = 0 then null else level * 1.25 end as num,
        cast(-level as number(9)) as neg,
        case when mod(level, 4) = 0 then null
            else 'row ' || level end as vc,
        cast('fixed' as char(8)) as ch,
        cast(n'nät ' || level as nvarchar2(20)) as nvc,
        hextoraw('0A0B' || to_char(mod(level, 256), 'FM0X')) as rw,
        date '2025-01-01' + mod(level, 40) as dt,
        timestamp '2025-01-01 10:00:00.123456' + numtodsinterval(level, 'SECOND')
            as ts,
        cast(level / 8 as binary_double) as bd,
        cast(level / 4 as binary_float) as bf,
        mod(level, 2) = 0 as flag,
        case when mod(level, 6) = 0 then null
            else numtodsinterval(level * 1.5, 'SECOND') end as ids,
        numtoyminterval(mod(level, 30) - 15, 'MONTH') as iym,
        case when mod(level, 5) = 0 then null
            else to_clob('clob ' || level) end as cl,
        to_nclob('nclob ' || level) as ncl,
        to_blob(hextoraw('FF' || to_char(mod(level, 256), 'FM0X'))) as bl
    from dual connect by level <= 1000
    order by id
"#;

#[rstest]
fn test_3900(conn: oracledb::Connection) -> Result<(), oracledb::Error> {
    // Every supported type (LOBs are fetched as LONG and LONG RAW), NULLs,
    // and several fetches.
    let (rows, _) = compare(&conn, ALL_TYPES, 100)?;
    assert_eq!(rows.len(), 1000);
    assert_eq!(rows[2][1], Value::Null);
    assert_eq!(rows[0][5], Value::Text("nät 1".to_string()));
    Ok(())
}

#[rstest]
fn test_3901(conn: oracledb::Connection) -> Result<(), oracledb::Error> {
    // Columns equal to the previous row's are not sent again: repeats,
    // including in the first row of a fetch.
    let (rows, _) = compare(
        &conn,
        "select 'same' as s, 42 as n, case when level <= 500 then null \
         else 'late' end as l, level as id \
         from dual connect by level <= 1000 order by id",
        7,
    )?;
    assert!(rows.iter().all(|r| r[0] == Value::Text("same".to_string())));
    assert_eq!(rows[499][2], Value::Null);
    assert_eq!(rows[500][2], Value::Text("late".to_string()));
    Ok(())
}

#[rstest]
fn test_3902(conn: oracledb::Connection) -> Result<(), oracledb::Error> {
    // A fetch larger than a packet, read as its packets arrive: the
    // response is read again from its start when more of it arrives, and
    // the sink is told to discard what it had.
    conn.test_set_end_of_response(false)?;
    let (rows, sink) = compare(
        &conn,
        "select level as id, rpad('x', 200, 'y') || level as wide \
         from dual connect by level <= 4000 order by id",
        10000,
    )?;
    assert_eq!(rows.len(), 4000);
    assert!(sink.discarded > 0, "no response was read again");
    Ok(())
}

#[rstest]
fn test_3903(conn: oracledb::Connection) -> Result<(), oracledb::Error> {
    // No rows, and rows that all came with the execute.
    let (rows, _) = compare(&conn, "select 1 as n from dual where 1 = 0", 50)?;
    assert!(rows.is_empty());
    let (rows, _) = compare(&conn, "select 1 as n from dual", 50)?;
    assert_eq!(rows.len(), 1);
    Ok(())
}

#[rstest]
fn test_3904(conn: oracledb::Connection) -> Result<(), oracledb::Error> {
    // Rows are buffered: fetch_into refuses until they are read.
    let mut cursor = conn
        .statement("select level from dual connect by level <= 10")?
        .prefetch_rows(5)
        .build()?
        .query(&[])?;
    let mut sink = Collector::new(1);
    if cursor.buffered_rows() > 0 {
        assert!(cursor.fetch_into(&mut sink).is_err());
    }
    Ok(())
}

#[rstest]
fn test_3905(conn: oracledb::Connection) -> Result<(), oracledb::Error> {
    // Physical ROWIDs of a heap table, logical ones (UROWID) of an
    // index-organized table, and NULL ROWIDs.
    let _heap = common::create_table(
        &conn,
        "test_3905_heap",
        "id number, name varchar2(30)",
    )?;
    let _iot = common::create_table_with_options(
        &conn,
        "test_3905_iot",
        "id number primary key, name varchar2(30)",
        "organization index",
    )?;
    for table in ["test_3905_heap", "test_3905_iot"] {
        conn.execute(
            &format!(
                "insert into {table} select level, 'name ' || level \
                 from dual connect by level <= 500"
            ),
            &[],
        )?;
    }
    conn.commit()?;
    let (rows, _) = compare(
        &conn,
        "select rowid as rid, id, case when mod(id, 7) = 0 then null \
         else rowid end as maybe from test_3905_heap order by id",
        64,
    )?;
    assert_eq!(rows.len(), 500);
    assert!(matches!(&rows[0][0], Value::Text(t) if t.len() == 18));
    assert_eq!(rows[6][2], Value::Null);
    let (rows, _) = compare(
        &conn,
        "select rowid as rid, id from test_3905_iot order by id",
        64,
    )?;
    assert_eq!(rows.len(), 500);
    assert!(matches!(&rows[0][0], Value::Text(t) if t.starts_with('*')));
    Ok(())
}

#[rstest]
fn test_3906(conn: oracledb::Connection) -> Result<(), oracledb::Error> {
    // JSON and VECTOR values, which arrive as value-based LOBs, with NULLs.
    if common::skip_unless_native_json_supported(&conn)
        || common::skip_unless_vectors_supported(&conn)
    {
        return Ok(());
    }
    let (rows, _) = compare(
        &conn,
        "select level as id, \
         case when mod(level, 5) = 0 then null else \
             json('{\"n\":' || level || ',\"tags\":[\"a\",\"b\"],\"x\":1.5}') end as doc, \
         case when mod(level, 7) = 0 then null else \
             to_vector('[' || level || ',2.5,-1]', 3, float32) end as vec, \
         to_vector('[' || level || ',1]', 2, float64) as vec64 \
         from dual connect by level <= 700 order by id",
        64,
    )?;
    assert_eq!(rows.len(), 700);
    assert_eq!(rows[4][1], Value::Null);
    assert_eq!(rows[6][2], Value::Null);
    assert!(matches!(&rows[0][1], Value::Text(t) if t.contains("tags")));
    Ok(())
}
