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
// fetch.rs
//
// Defines the structure used for sending and receiving the fetch message.
// -----------------------------------------------------------------------------

use crate::client::Client;
use crate::constants;
use crate::error::Error;
use crate::messages::Message;
use crate::response::Response;
use crate::row::DbRow;
use crate::row_sink::RowSink;
use crate::statement::CachedStatement;
use crate::write_buffer::WriteBuffer;

pub struct FetchMessage<'statement, 'sink> {
    statement: &'statement mut CachedStatement,
    last_row: Option<DbRow>,
    sink: Option<&'sink mut dyn RowSink>,
}

impl FetchMessage<'_, '_> {
    pub fn new<'statement>(
        statement: &'statement mut CachedStatement,
        last_row: Option<DbRow>,
    ) -> FetchMessage<'statement, 'static> {
        FetchMessage { statement, last_row, sink: None }
    }

    /// A fetch whose rows go into `sink` (see [`RowSink`]).
    pub fn with_sink<'statement, 'sink>(
        statement: &'statement mut CachedStatement,
        sink: &'sink mut dyn RowSink,
    ) -> FetchMessage<'statement, 'sink> {
        FetchMessage { statement, last_row: None, sink: Some(sink) }
    }
}

impl Message for FetchMessage<'_, '_> {
    /// Deserializes a TTC row data message.
    fn deserialize_row_data(
        &mut self,
        client: &Client,
        resp: &mut Response,
    ) -> Result<(), Error> {
        match self.sink.as_deref_mut() {
            Some(sink) => resp.deserialize_row_into(self.statement, sink),
            None => resp.deserialize_row_data(client, self.statement, true),
        }
    }

    fn post_deserialize(
        &mut self,
        client: &mut Client,
        resp: &mut Response,
    ) -> Result<(), Error> {
        resp.check_for_statement_error(client, self.statement)
    }

    fn restart_deserialize(&mut self, resp: &Response) -> Result<(), Error> {
        match self.sink.as_deref_mut() {
            Some(sink) => sink.discard(resp.sink_rows()),
            None => Ok(()),
        }
    }

    fn pre_deserialize(&mut self, _client: &mut Client, resp: &mut Response) {
        resp.set_prev_fetch_last_row(self.last_row.take());
        resp.set_num_columns(self.statement.out_metadata().len());
    }

    fn serialize(&self, client: &Client, buf: &mut WriteBuffer) {
        buf.write_function_header(client, constants::TTC_RPC_FETCH);
        buf.write_ub2(self.statement.cursor_id());
        buf.write_ub4(self.statement.options().fetch_array_size());
    }
}
