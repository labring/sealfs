use super::heart::Heart;

use crate::{common::request::OperationType, rpc::server::Handler};
use async_trait::async_trait;

#[derive(Default)]
pub struct ManagerService {
    pub heart: Heart,
}

#[async_trait]
impl Handler for ManagerService {
    async fn dispatch(
        &self,
        operation_type: u32,
        _flags: u32,
        path: Vec<u8>,
        data: Vec<u8>,
        _metadata: Vec<u8>,
    ) -> anyhow::Result<(i32, u32, Vec<u8>, Vec<u8>)> {
        let r#type = OperationType::try_from(operation_type).unwrap();
        match r#type {
            OperationType::SendHeart => {
                let address = String::from_utf8(path).unwrap();
                let lifetime_length = u32::from_le_bytes(data[0..4].try_into().unwrap());
                let lifetime =
                    String::from_utf8(data[4..4 + lifetime_length as usize].to_vec()).unwrap();
                self.heart.register_server(address, lifetime).await;
                Ok((0, 0, Vec::new(), Vec::new()))
            }
            OperationType::GetMetadata => {
                let mut vec: Vec<u8> = vec![];
                self.heart.instances.iter().for_each(|instance| {
                    let key = instance.key();
                    vec.extend(key.as_bytes().iter());
                });
                Ok((0, 0, vec, Vec::new()))
            }
            _ => todo!(),
        }
    }
}
