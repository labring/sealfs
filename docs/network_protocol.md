
v1

| id | type | flags | total_length | filename_length | filename | meta_data_length | meta_data | data_length | data |
| 8Byte | 8Byte | 8Byte | 8Byte | 8Byte | 1~4kB | 8Byte | 0~ | 8Byte | 0~ |

Read twice. Fisrt for 'id', 'type', 'meta_data', 'total_data_length', and second for filename and request data.