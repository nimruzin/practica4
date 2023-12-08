from flask import Flask, request, jsonify
import socket
import random
from datetime import datetime



class Statistics():
    def __init__(self):
        self.app = Flask(__name__)
        self.statistics = []
        self.server_ip = '127.0.0.1'  # IP-адрес сервера
        self.server_port = 6379  # Порт сервера

    def report(self):
        self.data = request.get_json()
        self.key_order = self.data.get("Dimensions", [])  # Получение списка ключей из data
        self.sorted_statistics = sorted(self.statistics, key=lambda x: [x.get(key) for key in self.key_order])

        return jsonify(self.sorted_statistics)


    def get_statistics(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.server_ip, self.server_port))

        # Получение статистики из тела запроса
        self.dimensions = request.get_json()

        self.time = datetime.now().time()

        for i in self.statistics:
            if i['URL'] == self.dimensions["URL"]:
                self.new_time_interval = i['TimeInterval'][:-5] + str(self.time)[:5]
                i['TimeInterval'] = self.new_time_interval
                i['Count'] += 1
                return jsonify({"message": "Statistics updated."})
        
        self.ID = random.randint(1, 99999)
        self.dimensions['Count'] = 1
        self.new_time_interval = self.dimensions['TimeInterval'] + "-" + str(self.time)[:5]
        self.dimensions['TimeInterval'] = self.new_time_interval
        self.dimensions['ID'] = self.ID
        self.statistics.append(self.dimensions)

        self.zapros = "HSET" + " " + str(self.dimensions['ID']) + " " + self.dimensions['TimeInterval'] + ";" + self.dimensions['SourceIP'] + ";" + self.dimensions['URL']
        self.sock.send(self.zapros.encode())  # Отправить запрос
        self.response = self.sock.recv(1024).decode()  # Получить ответ от сервера
        print(self.response)
        self.sock.close()

        print(self.statistics)

        return jsonify({"message": "Statistics added."})

    def run(self):
        self.app.route('/report', methods=['POST'])(self.report)
        self.app.route('/', methods=['POST'])(self.get_statistics)
        self.app.run(host="127.0.0.1", port=81)

S = Statistics()
if __name__ == '__main__':
    S.run()