from flask import Flask, render_template, send_from_directory, request
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import uuid
from collections import defaultdict
import os

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')

# Включите CORS для Flask-маршрутов
CORS(app, resources={r"/*": {"origins": "*"}})

# Настройте Socket.IO
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    logger=True,
    engineio_logger=True,
    async_mode='threading',
    ping_timeout=60,
    ping_interval=25,
    always_connect=True
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
    # Не отправляем operators_update сразу, ждем регистрации
    emit('connected', {'status': 'connected', 'sid': request.sid}, room=request.sid)

@socketio.on('set_username')
def handle_set_username(username):
    print(f"Пользователь установил имя: {username}")
    
    if not username or len(username.strip()) == 0:
        emit('registration_error', {'error': 'Имя не может быть пустым'}, room=request.sid)
        return
    
    # Сохраняем связь SID → username
    user_connections[username].add(request.sid)
    
    # Отправляем подтверждение
    emit('user_registered', {
        'username': username,
        'message': 'Регистрация успешна'
    }, room=request.sid)
    
    # Отправляем обновленный список операторов
    emit('operators_update', operators, room=request.sid)
    
    print(f"Пользователь {username} зарегистрирован с SID: {request.sid}")

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

@socketio.on('ping')
def handle_ping():
    emit('pong', room=request.sid)

if __name__ == '__main__':
    import os
    port = int(os.environ.get("PORT", 5000))
    socketio.run(
        app,
        host='0.0.0.0',
        port=port,
        debug=False,
        allow_unsafe_werkzeug=True
    )


