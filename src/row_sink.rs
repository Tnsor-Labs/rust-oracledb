//-----------------------------------------------------------------------------
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
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// row_sink.rs
//
// Fetching rows into a sink that receives each value as it is decoded, so
// that columnar consumers (such as Arrow builders) avoid building a row.
//
// Added by Tnsor Labs, 2026-11-10.
//-----------------------------------------------------------------------------

use crate::constants;
use crate::error::Error;
use crate::metadata::Metadata;
use crate::ora_type::OracleIntervalDS;
use crate::ora_type::OracleIntervalYM;
use crate::ora_type::OracleNumber;
use crate::ora_type::OracleTimestamp;
use crate::read_buffer::FromBuf;

/// A column value as it arrives from the database, borrowed from the network
/// buffer.
#[derive(Debug)]
pub enum WireValue<'a> {
    Null,
    /// An Oracle NUMBER or BINARY_INTEGER; see [`WireValue::number`].
    Number(&'a [u8]),
    /// CHAR, VARCHAR2, or LONG text in the database character set (UTF-8).
    Text(&'a [u8]),
    /// NCHAR or NVARCHAR2 text in the national character set (UTF-16BE).
    NationalText(&'a [u8]),
    /// RAW or LONG RAW bytes.
    Raw(&'a [u8]),
    /// DATE or TIMESTAMP (WITH TIME ZONE, WITH LOCAL TIME ZONE); see
    /// [`WireValue::timestamp`].
    Timestamp(&'a [u8]),
    BinaryDouble(f64),
    BinaryFloat(f32),
    Boolean(bool),
    /// INTERVAL DAY TO SECOND; see [`WireValue::interval_ds`].
    IntervalDS(&'a [u8]),
    /// INTERVAL YEAR TO MONTH; see [`WireValue::interval_ym`].
    IntervalYM(&'a [u8]),
    /// ROWID or UROWID, as its text.
    Rowid(&'a str),
}

impl WireValue<'_> {
    /// Decodes Oracle NUMBER bytes.
    pub fn number(bytes: &[u8]) -> OracleNumber {
        OracleNumber::from_buf(bytes)
    }

    /// Decodes Oracle DATE or TIMESTAMP bytes.
    pub fn timestamp(bytes: &[u8]) -> OracleTimestamp {
        OracleTimestamp::from_buf(bytes)
    }

    /// Decodes Oracle INTERVAL DAY TO SECOND bytes.
    pub fn interval_ds(bytes: &[u8]) -> OracleIntervalDS {
        OracleIntervalDS::from_buf(bytes)
    }

    /// Decodes Oracle INTERVAL YEAR TO MONTH bytes.
    pub fn interval_ym(bytes: &[u8]) -> OracleIntervalYM {
        OracleIntervalYM::from_buf(bytes)
    }
}

/// Receives the values of fetched rows, column by column, row by row.
pub trait RowSink {
    /// Appends the value of `column` in the current row.
    fn append(
        &mut self,
        column: usize,
        value: WireValue<'_>,
    ) -> Result<(), Error>;

    /// Appends, for `column` in the current row, the value it had in the row
    /// before, which the database did not send again. The row before may
    /// have been read from the cursor as a [`crate::Row`].
    fn repeat(&mut self, column: usize) -> Result<(), Error>;

    /// Ends the current row: every column has had a value.
    fn end_row(&mut self) -> Result<(), Error>;

    /// Removes the last `rows` rows ended with [`RowSink::end_row`], and any
    /// values of a row not ended. Called when a response must be read again
    /// from its start, because more of it arrived.
    fn discard(&mut self, rows: usize) -> Result<(), Error>;
}

/// Whether values of the column can be fetched into a [`RowSink`].
pub fn supports(metadata: &Metadata) -> bool {
    if metadata.is_array() {
        return false;
    }
    matches!(
        metadata.db_type().ora_type_num,
        constants::ORA_TYPE_NUM_BINARY_DOUBLE
            | constants::ORA_TYPE_NUM_BINARY_FLOAT
            | constants::ORA_TYPE_NUM_BOOLEAN
            | constants::ORA_TYPE_NUM_DATE
            | constants::ORA_TYPE_NUM_TIMESTAMP
            | constants::ORA_TYPE_NUM_TIMESTAMP_LTZ
            | constants::ORA_TYPE_NUM_TIMESTAMP_TZ
            | constants::ORA_TYPE_NUM_BINARY_INTEGER
            | constants::ORA_TYPE_NUM_NUMBER
            | constants::ORA_TYPE_NUM_CHAR
            | constants::ORA_TYPE_NUM_LONG
            | constants::ORA_TYPE_NUM_VARCHAR
            | constants::ORA_TYPE_NUM_RAW
            | constants::ORA_TYPE_NUM_LONG_RAW
            | constants::ORA_TYPE_NUM_INTERVAL_DS
            | constants::ORA_TYPE_NUM_INTERVAL_YM
            | constants::ORA_TYPE_NUM_ROWID
            | constants::ORA_TYPE_NUM_UROWID
    )
}
