use crate::{storage_engine::StorageEngine, EngineError};

pub struct DistributedEngine<Storage: StorageEngine> {
    pub local_storage: Storage,
}

impl<Storage> DistributedEngine<Storage>
where
    Storage: StorageEngine,
{
    pub fn new(local_storage: Storage) -> Self {
        Self { local_storage }
    }

    pub async fn create_dir(&self, path: String) -> Result<(), EngineError> {
        // a temporary implementation
        self.local_storage.create_directory(path)?;
        Ok(())
    }

    pub async fn delete_dir(&self, path: String) -> Result<(), EngineError> {
        // a temporary implementation
        self.local_storage.delete_directory(path)?;
        Ok(())
    }

    pub async fn read_dir(&self, path: String) -> Result<Vec<String>, EngineError> {
        // a temporary implementation
        self.local_storage.read_directory(path)?;
        Ok(Vec::new())
    }

    pub async fn create_file(&self, path: String) -> Result<(), EngineError> {
        // a temporary implementation
        self.local_storage.create_file(path)?;
        Ok(())
    }

    pub async fn delete_file(&self, path: String) -> Result<(), EngineError> {
        // a temporary implementation
        self.local_storage.delete_file(path)?;
        Ok(())
    }

    pub async fn read_file(&self, path: String) -> Result<Vec<u8>, EngineError> {
        // a temporary implementation
        self.local_storage.read_file(path)?;
        Ok(Vec::new())
    }

    pub async fn write_file(&self, path: String, data: &[u8]) -> Result<(), EngineError> {
        // a temporary implementation
        self.local_storage.write_file(path, data)?;
        Ok(())
    }

    pub async fn get_file_attr(&self, path: String) -> Result<Option<Vec<String>>, EngineError> {
        // a temporary implementation
        self.local_storage.read_directory(path)?;
        Ok(None)
    }
}
