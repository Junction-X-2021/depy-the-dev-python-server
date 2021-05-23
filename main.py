import asyncore
import pickle
import socket
import pymysql

score_outgoing = []
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
        ScoreServer(connection)


class ScoreServer(asyncore.dispatcher_with_send):
    def handle_read(self) -> None:
        received_data = self.recv(buffer_size)
        if received_data:
            received_data = dict(pickle.loads(received_data))
            if 'hostname' in received_data.keys() and 'stage' in received_data.keys():
                score_outgoing.append(self.socket)
                self.send_score(received_data)
        else:
            self.close()

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
