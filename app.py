import requests
from flask import Flask, redirect, request
from datetime import datetime
import string
import random
from waitress import serve
import socket

app = Flask(__name__)
links = {}

# Установить IP-адрес и порт сервера
server_ip = '127.0.0.1'  # IP-адрес сервера
server_port = 6379  # Порт сервера


# Генерация случайного кода для сокращенной ссылки
def generate_short_link_code():
    characters = string.ascii_letters + string.digits
    code = ''.join(random.choice(characters) for _ in range(random.randint(3, 10)))
    return code

# Обработка запроса на сокращение ссылки
@app.route('/', methods=['POST'])
def shorten_link():
    # Подключиться к серверу
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((server_ip, server_port))

    long_link = request.form.get('link')
    short_link_code = generate_short_link_code()
    short_link = request.host_url + short_link_code
    links[short_link_code] = long_link

    clink = "hget" + " " + long_link
    sock.send(clink.encode())  # Отправить запрос
    response = sock.recv(1024).decode()  # Получить ответ от сервера
    print(response)

    if response == "Key not found":
        clink = "hset" + " " + long_link + " " + short_link
        sock.send(clink.encode())  # Отправить запрос
        response = sock.recv(1024).decode()  # Получить ответ от сервера
        print(response)
        sock.close()
        return short_link
    else:
        return "the link has already been generated"

def zapros(URL, SourceIP, TimeInterval):
    param = {'URL': URL, 'SourceIP': SourceIP, 'TimeInterval': TimeInterval}
    requests.post('http://127.0.0.1:81', json=param)
    print(param)

# Обработка запроса на переход по сокращенной ссылке
@app.route('/<short_link_code>', methods=['GET'])
def redirect_to_long_link(short_link_code):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((server_ip, server_port))
    long_link = links.get(short_link_code)
    time = datetime.now().time()
    zapros(long_link + "(" + str(short_link_code) + ")", str(request.remote_addr), str(time)[:5])
    if long_link:
        clink = "hget" + " " + long_link
        sock.send(clink.encode())  # Отправить запрос
        response = sock.recv(1024).decode()  # Получить ответ от сервера
        print(response)
        sock.close()
        return redirect(long_link)
    else:
        return 'Ссылка не найдена'

if __name__ == '__main__':
    app.run(host="127.0.0.1", port=80)
