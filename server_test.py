
import unittest
import socket
import struct
import yaml
import os


class op_type():
    CreateFile = 1
    CreateDir = 2
    GetFileAttr = 3
    ReadDir = 4
    OpenFile = 5
    ReadFile = 6
    WriteFile = 7
    DeleteFile = 8
    DeleteDir = 9


def make_body(filepath):
    utf8_filepath = bytes(filepath, encoding='utf-8')
    filelen = struct.pack("I", len(utf8_filepath))
    return filelen + utf8_filepath


def make_header(type, body_len):
    id = 0
    flag = 0
    total_length = body_len + 16
    header = struct.pack("4I", id, type, flag, total_length)
    return header


def get_status(client):
    response = client.recv(1024)
    _, status, _, _ = struct.unpack("4I", response)
    return status


def send_request(index, type, filepath):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(address[index])
    body = make_body(filepath)
    header = make_header(type, len(body))
    request = header + body
    client.send(request)
    return client


# hash
# /                     3347450360 % 3 = 2
# /t1.txt               4220407925 % 3 = 2
# /test1/               3344789521 % 3 = 1
# /test1/t1.txt         1630769907 % 3 = 0
# /test1/test_dir/      2589669281 % 3 = 2
dic = {
    '/': 2,
    '/t1.txt': 2,
    '/test1/': 1,
    '/test1/t1.txt': 0,
    '/test1/test_dir/': 2,
}
address = [('127.0.0.1', 8080), ('127.0.0.1', 8081), ('127.0.0.1', 8082)]


class TestDistributed_engine(unittest.TestCase):
    def test_file(self):
        path = '/t1.txt'
        client = send_request(dic[path], op_type.CreateFile, path)
        self.assertEqual(0, get_status(client))

        client = send_request(dic[path], op_type.CreateFile, path)
        self.assertNotEqual(0, get_status(client))

        client = send_request(dic[path], op_type.DeleteFile, path)
        self.assertEqual(0, get_status(client))

    def test_dir1(self):
        path = '/test1/'
        client = send_request(dic[path], op_type.CreateDir, path)
        self.assertEqual(0, get_status(client))

        client = send_request(dic[path], op_type.CreateDir, path)
        self.assertNotEqual(0, get_status(client))

        client = send_request(dic[path], op_type.DeleteDir, path)
        self.assertEqual(0, get_status(client))

        path = '/test1/test_dir/'
        client = send_request(dic[path], op_type.CreateDir, path)
        self.assertNotEqual(0, get_status(client))

    def test_dir2(self):
        path = '/test1/'
        client = send_request(dic[path], op_type.CreateDir, path)
        self.assertEqual(0, get_status(client))

        path = '/test1/test_dir/'
        client = send_request(dic[path], op_type.CreateDir, path)
        self.assertEqual(0, get_status(client))

        path = '/test1/'
        client = send_request(dic[path], op_type.DeleteDir, path)
        self.assertNotEqual(0, get_status(client))

        path = '/test1/test_dir/'
        client = send_request(dic[path], op_type.DeleteDir, path)
        self.assertEqual(0, get_status(client))

        path = '/test1/'
        client = send_request(dic[path], op_type.DeleteDir, path)
        self.assertEqual(0, get_status(client))

    def test_all(self):
        path = '/test1/t1.txt'
        client = send_request(dic[path], op_type.CreateFile, path)
        self.assertNotEqual(0, get_status(client))

        path = '/test1/'
        client = send_request(dic[path], op_type.CreateDir, path)
        self.assertEqual(0, get_status(client))

        path = '/test1/t1.txt'
        client = send_request(dic[path], op_type.CreateFile, path)
        self.assertEqual(0, get_status(client))

        path = '/test1/test_dir/'
        client = send_request(dic[path], op_type.CreateDir, path)
        self.assertEqual(0, get_status(client))

        path = '/test1/'
        client = send_request(dic[path], op_type.DeleteDir, path)
        self.assertNotEqual(0, get_status(client))

        path = '/test1/t1.txt'
        client = send_request(dic[path], op_type.DeleteFile, path)
        self.assertEqual(0, get_status(client))

        path = '/test1/'
        client = send_request(dic[path], op_type.DeleteDir, path)
        self.assertNotEqual(0, get_status(client))

        path = '/test1/test_dir/'
        client = send_request(dic[path], op_type.DeleteDir, path)
        self.assertEqual(0, get_status(client))

        path = '/test1/'
        client = send_request(dic[path], op_type.DeleteDir, path)
        self.assertEqual(0, get_status(client))

# path = '/t1.txt'
# client = send_request(dic[path], op_type.DeleteFile, path)
# print(get_status(client))
# client.close()

# path = '/test1/t1.txt'
# client = send_request(dic[path], op_type.DeleteFile, path)
# path = '/test1/test_dir/'
# client = send_request(dic[path], op_type.DeleteDir, path)
# path = '/test1/'
# client = send_request(dic[path], op_type.DeleteDir, path)
# path = '/t1.txt'
# client = send_request(dic[path], op_type.DeleteFile, path)


path = '/test1/t1.txt'
client = send_request(dic[path], op_type.DeleteFile, path)

path = '/test1/test_dir/'
client = send_request(dic[path], op_type.DeleteDir, path)

path = '/test1/'
client = send_request(dic[path], op_type.DeleteDir, path)

unittest.main(warnings='ignore')
