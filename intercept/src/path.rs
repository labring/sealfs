lazy_static::lazy_static! {
    pub static ref CURRENT_DIR: String = std::env::current_dir()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    pub static ref MOUNT_POINT: String = {
        let mut value =
            std::env::var("SEALFS_MOUNT_POINT").unwrap_or_else(|_| "/mnt/fs".to_string());
        if value.ends_with('/') {
            value.pop();
        }
        value
    };
    pub static ref VOLUME_NAME: String = {
        std::env::var("SEALFS_VOLUME_NAME").unwrap_or_else(|_| "sealfs".to_string())
    };
}

fn get_realpath(path: &str) -> Option<String> {
    // An absolute pathname
    let path = if path.starts_with('/') {
        path.to_string()
    } else {
        let mut cwd = CURRENT_DIR.to_string();
        cwd.push('/');
        cwd.push_str(path);
        cwd
    };
    let mut start = 0;
    let mut end;
    let path_bytes = path.as_bytes();
    let mut result = String::new();
    while start < path_bytes.len() {
        while start < path_bytes.len() && path_bytes[start] == b'/' {
            start += 1;
        }
        end = start;
        while end < path_bytes.len() && path_bytes[end] != b'/' {
            end += 1;
        }
        let len = end - start;
        if len == 0 {
            break;
        } else if len == 1 && path_bytes[start] == b'.' {
            /* nothing */
        } else if len == 2 && path_bytes[start] == b'.' && path_bytes[start + 1] == b'.' {
            while result.len() > 0 && !result.ends_with('/') {
                result.pop();
            }
            if result.len() == 0 {
                return None;
            }
            result.pop();
        } else {
            result.push('/');
            result.push_str(&String::from_utf8(path_bytes[start..end].to_vec()).unwrap());
        }
        start = end;
    }
    Some(result)
}

pub fn get_absolutepath(dir_path: &str, file_path: &str) -> Result<String, i32> {
    // An absolute pathname
    if file_path.starts_with('/') {
        match get_realpath(file_path) {
            Some(value) => return Ok(value),
            None => return Err(0),
        }
    }

    // By file descriptor
    if file_path.is_empty() {
        match get_realpath(dir_path) {
            Some(value) => return Ok(value),
            None => return Err(0),
        }
    }

    // By file descriptor
    match get_realpath(&(dir_path.to_string() + "/" + &file_path)) {
        Some(value) => Ok(value),
        None => Err(0),
    }
}

pub fn get_remotepath(path: &str) -> Option<String> {
    if path.starts_with(MOUNT_POINT.as_str()) {
        let mut remotepath = VOLUME_NAME.clone();
        remotepath.push_str(&path[MOUNT_POINT.len()..]);
        if remotepath.len() > 1 && remotepath.ends_with('/') {
            remotepath.pop();
        }
        return Some(remotepath);
    }
    None
}
