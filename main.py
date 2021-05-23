import asyncore
import pickle
import socket
import pymysql

score_outgoing = []
room_outgoing = {}
buffer_size = 512

client = pymysql.connect(
    user="jaram", password="Qoswlfdlsnrn", database="game",
    host="game-server.cfxyvd6mgspo.ap-northeast-2.rds.amazonaws.com", port=3306)


class MainServer(asyncore.dispatcher):
    def __init__(self, port):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.bind(('', port))
        self.listen(10)

    def handle_accept(self) -> None:
        connection, address = self.accept()
        SecondServer(connection)


class SecondServer(asyncore.dispatcher_with_send):
    def handle_read(self) -> None:
        received_data = self.recv(buffer_size)
        if received_data:
            received_data = dict(pickle.loads(received_data))
            if 'hostname' in received_data.keys() and 'stage' in received_data.keys():
                score_outgoing.append(self.socket)
                self.send_score(received_data)
            elif 'room_id' in received_data.keys() and 'last' in received_data.keys():
                status = -1
                if received_data['last'] in room_outgoing.keys() and self.socket in room_outgoing[received_data['last']]:
                    room_outgoing[received_data['last']].remove(self.socket)
                if received_data['room_id'] in room_outgoing.keys():
                    if len(room_outgoing[received_data['room_id']]) == 1:
                        room_outgoing[received_data['room_id']].append(self.socket)
                        status = "go"
                    elif len(room_outgoing[received_data['room_id']]) == 0:
                        room_outgoing[received_data['room_id']].append(self.socket)
                        status = "waiting"
                    else:
                        status = "full"
                elif received_data['room_id'] not in room_outgoing.keys():
                    room_outgoing[received_data['room_id']] = [self.socket]
                    status = "waiting"
                self.send_room_info(received_data['room_id'], status)
        else:
            self.close()

    @staticmethod
    def send_room_info(room_id, status_code):
        send_data = {"room_id": room_id, "status": status_code}
        remove = []
        for i in room_outgoing[room_id]:
            try:
                i.send(pickle.dumps(send_data))
            except Exception:
                remove.append(i)
                continue
            for r in remove:
                room_outgoing[room_id].remove(r)

    @staticmethod
    def send_score(data: dict):
        cursor = client.cursor()
        cursor.execute("insert into game.score(hostname, score) values (%s, %s)", (data['hostname'], data['stage']))
        cursor.execute("select hostname, score from game.score order by score desc")

        send_data = []
        for hostname, score in cursor.fetchall():
            send_data.append({"hostname": hostname, "stage": score})
        remove = []
        for i in score_outgoing:
            try:
                i.send(pickle.dumps(send_data))
            except Exception:
                remove.append(i)
                continue
            for r in remove:
                score_outgoing.remove(r)


if __name__ == '__main__':
    MainServer(8080)
    asyncore.loop()
    client.close()
