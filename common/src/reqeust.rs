// request
// | id | type | flags | total_length | filename_length | filename | meta_data_length | meta_data | data_length | data |
// | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 1~4kB | 4Byte | 0~ | 4Byte | 0~ |
pub const REQUEST_HEADER_SIZE: usize = 16;

pub struct RequestHeader {
    pub id: u32,
    pub r#type: OperationType,
    pub flags: u32,
    pub total_length: u32,
}

pub enum OperationType {
    CreateFile = 1,
    CreateDir = 2,
    GetFileAttr = 3,
    ReadDir = 4,
    OpenFile = 5,
    ReadFile = 6,
    WriteFile = 7,
}

impl TryFrom<u32> for OperationType {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(OperationType::CreateFile),
            2 => Ok(OperationType::CreateDir),
            3 => Ok(OperationType::GetFileAttr),
            4 => Ok(OperationType::ReadDir),
            5 => Ok(OperationType::OpenFile),
            6 => Ok(OperationType::ReadFile),
            7 => Ok(OperationType::WriteFile),
            _ => panic!("Unkown value: {}", value),
        }
    }
}
