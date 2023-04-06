use core::result::Result;
use dashmap::DashMap;
use ibv::connection::conn::{connect, Conn};
use log::{debug, error};
use std::{io::IoSlice, sync::Arc};

use crate::rpc::{
    callback::CallbackPool,
    protocol::{ResponseHeader, RESPONSE_HEADER_SIZE},
};
pub struct Client {
    connections: DashMap<String, Arc<Conn>>,
    pool: Arc<CallbackPool>,
}

impl Client {
    pub fn new() -> Self {
        let mut pool = CallbackPool::new();
        pool.init();
        let pool = Arc::new(pool);
        Client {
            connections: DashMap::new(),
            pool,
        }
    }

    pub fn close(&self) {
        self.pool.free();
    }

    pub async fn add_connection(&self, addr: &str) {
        let conn = Arc::new(connect(addr).await.unwrap());
        debug!("connect to {} success", addr);
        let conn1 = conn.clone();
        self.connections.insert(addr.to_string(), conn1);
        tokio::spawn(parse_response(conn, self.pool.clone()));
    }

    pub fn get_connection(&self, addr: &str) -> Option<Arc<Conn>> {
        self.connections.get(addr).map(|conn| conn.value().clone())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn call_remote(
        &self,
        server_address: &str,
        operation_type: u32,
        req_flags: u32,
        path: &str,
        send_meta_data: &[u8],
        send_data: &[u8],
        status: &mut i32,
        rsp_flags: &mut u32,
        recv_meta_data_length: &mut usize,
        recv_data_length: &mut usize,
        recv_meta_data: &mut [u8],
        recv_data: &mut [u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (batch, id) = self
            .pool
            .register_callback(recv_meta_data, recv_data)
            .await?;
        debug!(
            "call_remote on {:?}, batch {}, id: {}",
            server_address, batch, id
        );
        // send request to remote
        self.send_request(
            server_address,
            batch,
            id,
            operation_type,
            req_flags,
            path,
            send_meta_data,
            send_data,
        )
        .await?;

        let (s, f, meta_data_length, data_length) = self.pool.wait_for_callback(id).await?;
        debug!(
            "call_remote success, id: {}, status: {}, flags: {}, meta_data_length: {}, data_length: {}",
            id, s, f, meta_data_length, data_length
        );
        *status = s;
        *rsp_flags = f;
        *recv_meta_data_length = meta_data_length;
        *recv_data_length = data_length;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn send_request(
        &self,
        addr: &str,
        batch: u32,
        id: u32,
        operation_type: u32,
        req_flags: u32,
        path: &str,
        send_meta_data: &[u8],
        send_data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = self.get_connection(addr).unwrap();
        let mut request = Vec::new();
        let total_length = path.len() + send_meta_data.len() + send_data.len();
        request.extend_from_slice(&batch.to_le_bytes());
        request.extend_from_slice(&id.to_le_bytes());
        request.extend_from_slice(&operation_type.to_le_bytes());
        request.extend_from_slice(&req_flags.to_le_bytes());
        request.extend_from_slice(&(total_length as u32).to_le_bytes());
        request.extend_from_slice(&(path.len() as u32).to_le_bytes());
        request.extend_from_slice(&(send_meta_data.len() as u32).to_le_bytes());
        request.extend_from_slice(&(send_data.len() as u32).to_le_bytes());
        request.extend_from_slice(path.as_bytes());
        let request = &[
            IoSlice::new(&request),
            IoSlice::new(send_meta_data),
            IoSlice::new(send_data),
        ];
        debug!("send_request: {:?}", request);
        conn.send_msg(request).await?;
        Ok(())
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn parse_response(conn: Arc<Conn>, pool: Arc<CallbackPool>) {
    loop {
        let response = conn.recv_msg().await.unwrap();
        debug!("parse_response: recv response: {:?}", response);
        // parse response
        let header = parse_response_header(response);

        let batch = header.batch;
        let id = header.id;

        if pool.lock_if_not_timeout(batch, id).is_err() {
            debug!("parse_response: lock timeout");
            continue;
        }
        debug!("parse_response: lock success");
        let meta_data_result = {
            match pool.get_meta_data_ref(id, header.meta_data_length as usize) {
                Ok(meta_data_result) => Ok(meta_data_result),
                Err(_) => Err("meta_data_result error"),
            }
        };
        let data_result = {
            match pool.get_data_ref(id, header.data_length as usize) {
                Ok(data_result) => Ok(data_result),
                Err(_) => Err("data_result error"),
            }
        };

        if let (Ok(data), Ok(meta_data)) = (data_result, meta_data_result) {
            parse_response_body(response, meta_data, data);
            conn.release(response).await;
            if let Err(e) = pool
                .response(
                    id,
                    header.status,
                    header.flags,
                    header.meta_data_length as usize,
                    header.data_length as usize,
                )
                .await
            {
                error!("Error writing response back: {}", e);
                break;
            };
        } else {
            error!("Error getting data or meta_data");
            break;
        }
        // todo: realease the buf in response
    }
}

pub fn parse_response_header(response: &[u8]) -> ResponseHeader {
    let header = &response[0..RESPONSE_HEADER_SIZE];
    let batch = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let id = u32::from_le_bytes(header[4..8].try_into().unwrap());
    let status = i32::from_le_bytes(header[8..12].try_into().unwrap());
    let flags = u32::from_le_bytes(header[12..16].try_into().unwrap());
    let total_length = u32::from_le_bytes(header[16..20].try_into().unwrap());
    let meta_data_length = u32::from_le_bytes(header[20..24].try_into().unwrap());
    let data_length = u32::from_le_bytes(header[24..28].try_into().unwrap());
    // debug!(
    //     "received response_header batch: {}, id: {}, status: {}, flags: {}, total_length: {}, meta_data_length: {}, data_length: {}",
    //     batch, id, status, flags, total_length, meta_data_length, data_length
    // );
    ResponseHeader {
        batch,
        id,
        status,
        flags,
        total_length,
        meta_data_length,
        data_length,
    }
}

pub fn parse_response_body(response: &[u8], meta_data: &mut [u8], data: &mut [u8]) {
    let meta_data_length = meta_data.len();
    let data_length = data.len();
    debug!(
        "waiting for response_meta_data, length: {}",
        meta_data_length
    );
    // copy response to meta_data
    meta_data
        .copy_from_slice(&response[RESPONSE_HEADER_SIZE..RESPONSE_HEADER_SIZE + meta_data_length]);
    debug!("received reponse_meta_data, meta_data: {:?}", meta_data);
    // copy response to data
    data.copy_from_slice(
        &response[RESPONSE_HEADER_SIZE + meta_data_length
            ..RESPONSE_HEADER_SIZE + meta_data_length + data_length],
    );
    debug!("received reponse_data, data: {:?}", data);
}
