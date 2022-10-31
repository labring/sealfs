// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use log::debug;
use tonic::{Request, Response, Status};

use fsbase::remote_fs_server::{RemoteFs, RemoteFsServer};
use fsbase::{
    CreateDirectoryRequest, CreateDirectoryResponse, CreateFileRequest, CreateFileResponse,
    DeleteDirectoryRequest, DeleteDirectoryResponse, DeleteFileRequest, DeleteFileResponse,
    GetFileAttributesRequest, GetFileAttributesResponse, OpenFileRequest, OpenFileResponse,
    ReadDirectoryRequest, ReadDirectoryResponse, ReadFileRequest, ReadFileResponse,
    WriteFileRequest, WriteFileResponse,
};

pub mod fsbase {
    tonic::include_proto!("fsbase");
}

pub fn new_fs_service(service: FsService) -> RemoteFsServer<FsService> {
    RemoteFsServer::new(service)
}

#[derive(Default)]
pub struct FsService {}

#[tonic::async_trait]
impl RemoteFs for FsService {
    async fn create_file(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<CreateFileResponse>, Status> {
        debug!("Got a request from {:?}", request.remote_addr());
        let response = CreateFileResponse { status: 0 };
        Ok(Response::new(response))
    }

    async fn create_directory(
        &self,
        request: Request<CreateDirectoryRequest>,
    ) -> Result<Response<CreateDirectoryResponse>, Status> {
        debug!("Got a request from {:?}", request.remote_addr());
        let response = CreateDirectoryResponse { status: 0 };
        Ok(Response::new(response))
    }

    async fn get_file_attributes(
        &self,
        request: Request<GetFileAttributesRequest>,
    ) -> Result<Response<GetFileAttributesResponse>, Status> {
        debug!("Got a request from {:?}", request.remote_addr());
        let response = GetFileAttributesResponse {
            status: 0,
            mode: 0,
            size: 0,
        };
        Ok(Response::new(response))
    }

    async fn read_directory(
        &self,
        request: Request<ReadDirectoryRequest>,
    ) -> Result<Response<ReadDirectoryResponse>, Status> {
        debug!("Got a request from {:?}", request.remote_addr());
        let entries = vec![0];
        let response = ReadDirectoryResponse { status: 0, entries };
        Ok(Response::new(response))
    }

    async fn open_file(
        &self,
        request: Request<OpenFileRequest>,
    ) -> Result<Response<OpenFileResponse>, Status> {
        debug!("Got a request from {:?}", request.remote_addr());
        let response = OpenFileResponse { status: 0 };
        Ok(Response::new(response))
    }

    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<ReadFileResponse>, Status> {
        debug!("Got a request from {:?}", request.remote_addr());
        let data = vec![0];
        let response = ReadFileResponse {
            status: 0,
            size: 0,
            data,
        };
        Ok(Response::new(response))
    }

    async fn write_file(
        &self,
        request: Request<WriteFileRequest>,
    ) -> Result<Response<WriteFileResponse>, Status> {
        debug!("Got a request from {:?}", request.remote_addr());
        let response = WriteFileResponse { status: 0, size: 0 };
        Ok(Response::new(response))
    }

    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<DeleteFileResponse>, Status> {
        debug!("Got a request from {:?}", request.remote_addr());
        let response = DeleteFileResponse { status: 0 };
        Ok(Response::new(response))
    }

    async fn delete_directory(
        &self,
        request: Request<DeleteDirectoryRequest>,
    ) -> Result<Response<DeleteDirectoryResponse>, Status> {
        debug!("Got a request from {:?}", request.remote_addr());
        let response = DeleteDirectoryResponse { status: 0 };
        Ok(Response::new(response))
    }
}
