from flask import Flask, render_template, send_from_directory, request
from flask_socketio import SocketIO, emit
from flask_cors import CORS  # Импорт CORS
import uuid
from collections import defaultdict

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'

# Включите CORS для Flask-маршрутов
CORS(app)

# Настройте Socket.IO
socketio = SocketIO(
    app,
    cors_allowed_origins="*",  # Разрешить все источники для разработки
    logger=True,                # Включить логи для отладки
    engineio_logger=True,       # Логи Engine.IO
    async_mode='threading'      # Режим работы
)

operators = [
    {"id": str(uuid.uuid4()), "name": "Опер_сейлс_2", "occupied_by": None, "os_type": None, "queue": []},
    {"id": str(uuid.uuid4()), "name": "Опер_сервис", "occupied_by": None, "os_type": None, "queue": []},
    {"id": str(uuid.uuid4()), "name": "Опер_сейлс_5", "occupied_by": None, "os_type": None, "queue": []},
    {"id": str(uuid.uuid4()), "name": "Супервизор", "occupied_by": None, "os_type": None, "queue": []},
    {"id": str(uuid.uuid4()), "name": "Оператор2", "occupied_by": None, "os_type": None, "queue": []},
    {"id": str(uuid.uuid4()), "name": "Оператор3", "occupied_by": None, "os_type": None, "queue": []}
]

user_connections = defaultdict(set)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/static/<path:path>')
def send_static(path):
    return send_from_directory('static', path)

def process_queue(operator):
    if not operator['occupied_by'] and operator['queue']:
        next_user = operator['queue'].pop(0)
        operator['occupied_by'] = next_user['username']
        operator['os_type'] = next_user['os_type']
        emit('operators_update', operators, broadcast=True)
        
        # Важно: отправляем уведомление в комнату пользователя
        for username, sids in user_connections.items():
            if username == next_user['username']:
                for sid in sids:
                    emit('queue_advanced', {
                        'operator_name': operator['name'],
                        'username': next_user['username']
                    }, room=sid)
                break

@socketio.on('connect')
def handle_connect():
    print(f"Клиент подключился: {request.sid}")
    emit('operators_update', operators)

@socketio.on('set_username')
def handle_set_username(username):
    print(f"Пользователь установил имя: {username}")
    user_connections[username].add(request.sid)
    emit('user_registered', {'username': username}, room=request.sid)
    emit('operators_update', operators, broadcast=True)

@socketio.on('occupy_operator')
def handle_occupy(data):
    print(f"Занятие оператора: {data}")
    operator = next((op for op in operators if op['id'] == data['operator_id']), None)
    if operator and not operator['occupied_by']:
        operator['occupied_by'] = data['username']
        operator['os_type'] = data['os_type']
        emit('operators_update', operators, broadcast=True)

@socketio.on('join_queue')
def handle_join_queue(data):
    print(f"Вход в очередь: {data}")
    operator = next((op for op in operators if op['id'] == data['operator_id']), None)
    if operator and data['username'] not in [u['username'] for u in operator['queue']]:
        operator['queue'].append({
            'username': data['username'],
            'os_type': data['os_type']
        })
        emit('operators_update', operators, broadcast=True)

@socketio.on('release_operator')
def handle_release(data):
    print(f"Освобождение оператора: {data}")
    operator = next((op for op in operators if op['id'] == data['operator_id']), None)
    if operator and operator['occupied_by'] == data['username']:
        operator['occupied_by'] = None
        operator['os_type'] = None
        process_queue(operator)
        emit('operators_update', operators, broadcast=True)

@socketio.on('disconnect')
def handle_disconnect():
    print(f"Клиент отключился: {request.sid}")
    for username, sids in user_connections.items():
        if request.sid in sids:
            sids.remove(request.sid)
            if not sids:
                del user_connections[username]
                for op in operators:
                    if op['occupied_by'] == username:
                        op['occupied_by'] = None
                        op['os_type'] = None
                        process_queue(op)
                    op['queue'] = [u for u in op['queue'] if u['username'] != username]
                emit('operators_update', operators, broadcast=True)
            break

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)