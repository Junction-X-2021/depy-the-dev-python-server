import json
import socket


def init_server():
    server_socket = socket.socket()
    server_socket.bind(('127.0.0.1', 8080))
    server_socket.listen(10)
    while True:
        connection, address = server_socket.accept()
        while True:
            received_data = connection.recv(1024).decode()
            try:
                received_data = json.loads(received_data)
                # 송신 데이터 (수신된 json 에 따라 다르게 처리)
                if 'hostname' in received_data.keys() and 'stage' in received_data.keys():
                    send_data = {"score": [{"hostname": "host1", "stage": 99}]}
                    connection.send(json.dumps(send_data).encode())
            except json.decoder.JSONDecodeError:
                continue


if __name__ == '__main__':
    init_server()
