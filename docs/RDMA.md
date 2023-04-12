## Communication Principle of RDMA in sealfs

GitHub Repository: https://github.com/mond77/ibv.git

### Connection Establishment

The endpoint device addresses and RecvBuffer memory addresses are exchanged via TCP.

RemoteBufManager is the allocator of the remote RecvBuffer.

Both SendBuffer and RecvBuffer are memory regions that have been registered with `ibv_reg_mr`.

### send_msg() Method

`fn send_msg(&self, msg: &[IoSlice<'_>]) -> io::Result<()> `

The main process of the send_msg() method includes:

1. Allocation of SendBuffer
2. Locking
3. Allocation of RemoteBuf
4. Issuing of WR (work request)
5. Unlocking

Each request/response corresponds to a write_with_imm operation, which generates a WC (work completion) on both the local and remote ends. write_with_imm consumes one RQE (Receive Queue Element) on the remote end. The WC type of the remote end is write_with_imm, and the WC type of the local end is write.

### recv_msg() Method

`fn recv_msg(&self) -> io::Result<&[u8]> `

The main process of the recv_msg() method includes:

1. A task in the polling background that polls a CQ (Completion Queue) notifies the task blocked in recv_msg() when a type write_with_imm WC is received.
2. The task then reads the data of this write.
3. The returned & [u8] points to the data located on the RecvBuffer.

### Memory Management

#### SendBuffer

SendBuffer is linearly allocated, and when released, it is marked with AtomicBool, greatly reducing the complexity of memory management. A release_task is used to maintain the linear release order of each allocated memory block and determine whether it is released or not using AtomicBool.

#### RecvBuffer

RecvBuffer is linearly allocated and released.

