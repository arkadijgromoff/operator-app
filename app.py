from flask import Flask, render_template, send_from_directory, request
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import uuid
import os
import threading
import time
from datetime import datetime
import sqlite3
import json
from contextlib import contextmanager

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

# Контекстный менеджер для работы с БД
@contextmanager
def get_db_connection():
    conn = sqlite3.connect('operators.db', check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_database():
    """Инициализация базы данных"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Таблица операторов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS operators (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                occupied_by TEXT,
                os_type TEXT,
                queue TEXT DEFAULT '[]'
            )
        ''')
        
        # Таблица пользователей (для сохранения сессии)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'online'
            )
        ''')
        
        # Проверяем, есть ли операторы в базе
        cursor.execute("SELECT COUNT(*) as count FROM operators")
        result = cursor.fetchone()
        
        if result['count'] == 0:
            # Добавляем стандартных операторов
            operators = [
                ("Опер_сейлс_2", str(uuid.uuid4())),
                ("Опер_сервис", str(uuid.uuid4())),
                ("Опер_сейлс_5", str(uuid.uuid4())),
                ("Супервизор", str(uuid.uuid4())),
                ("Оператор2", str(uuid.uuid4())),
                ("Оператор3", str(uuid.uuid4()))
            ]
            
            for name, op_id in operators:
                cursor.execute(
                    "INSERT INTO operators (id, name, occupied_by, os_type, queue) VALUES (?, ?, NULL, NULL, '[]')",
                    (op_id, name)
                )
        
        conn.commit()
        print(f"[{datetime.now()}] ✅ База данных инициализирована")

def get_operators_from_db():
    """Получение списка операторов из БД"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM operators")
        operators = []
        
        for row in cursor.fetchall():
            operators.append({
                'id': row['id'],
                'name': row['name'],
                'occupied_by': row['occupied_by'],
                'os_type': row['os_type'],
                'queue': json.loads(row['queue'])
            })
        
        return operators

def update_operator_in_db(operator_id, occupied_by=None, os_type=None, queue=None):
    """Обновление оператора в БД"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        if queue is not None:
            cursor.execute(
                "UPDATE operators SET queue = ? WHERE id = ?",
                (json.dumps(queue), operator_id)
            )
        
        # Обновляем занятость оператора
        if occupied_by is not None:
            cursor.execute(
                "UPDATE operators SET occupied_by = ?, os_type = ? WHERE id = ?",
                (occupied_by, os_type, operator_id)
            )
        elif occupied_by is None and os_type is None:
            # Явное освобождение оператора
            cursor.execute(
                "UPDATE operators SET occupied_by = NULL, os_type = NULL WHERE id = ?",
                (operator_id,)
            )
        
        conn.commit()

def add_user_to_db(username):
    """Добавление/обновление пользователя в БД"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO users (username, last_seen, status) VALUES (?, CURRENT_TIMESTAMP, 'online')",
            (username,)
        )
        conn.commit()

def update_user_last_seen(username):
    """Обновление времени последней активности пользователя"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE users SET last_seen = CURRENT_TIMESTAMP WHERE username = ?",
            (username,)
        )
        conn.commit()

def cleanup_inactive_users():
    """Очистка неактивных пользователей (больше 30 минут неактивности)"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Находим неактивных пользователей
        cursor.execute(
            "SELECT username FROM users WHERE last_seen < datetime('now', '-30 minutes')"
        )
        inactive_users = [row['username'] for row in cursor.fetchall()]
        
        for username in inactive_users:
            # Освобождаем операторов, занятых неактивными пользователями
            cursor.execute(
                "UPDATE operators SET occupied_by = NULL, os_type = NULL WHERE occupied_by = ?",
                (username,)
            )
            
            # Удаляем из очередей
            cursor.execute("SELECT * FROM operators")
            operators = cursor.fetchall()
            
            for operator in operators:
                queue = json.loads(operator['queue'])
                new_queue = [user for user in queue if user['username'] != username]
                
                if len(new_queue) != len(queue):
                    cursor.execute(
                        "UPDATE operators SET queue = ? WHERE id = ?",
                        (json.dumps(new_queue), operator['id'])
                    )
        
        # Удаляем неактивных пользователей
        cursor.execute(
            "DELETE FROM users WHERE last_seen < datetime('now', '-30 minutes')"
        )
        
        conn.commit()
        
        if inactive_users:
            print(f"[{datetime.now()}] 🧹 Удалены неактивные пользователи: {inactive_users}")

# Функция для поддержания активности сервера и очистки БД
def background_tasks():
    """Фоновые задачи для поддержания сервера"""
    while True:
        try:
            time.sleep(300)  # Каждые 5 минут
            
            # 1. Отправка keep-alive сигнала
            print(f"[{datetime.now()}] 🔄 Отправка keep-alive сигнала...")
            socketio.emit('server_keepalive', {
                'timestamp': datetime.now().isoformat(),
                'message': 'Server is alive',
                'connected_clients': len(socketio.server.manager.rooms.get('/', {}))
            })
            
            # 2. Очистка неактивных пользователей
            cleanup_inactive_users()
            
        except Exception as e:
            print(f"[{datetime.now()}] Ошибка в фоновых задачах: {e}")

# Инициализация БД при старте
init_database()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/static/<path:path>')
def send_static(path):
    return send_from_directory('static', path)

@app.route('/health')
def health_check():
    """Эндпоинт для проверки здоровья сервера"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM operators")
        operators_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM users")
        users_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM users WHERE status = 'online'")
        online_users = cursor.fetchone()['count']
    
    return {
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'connected_clients': len(socketio.server.manager.rooms.get('/', {})),
        'operators_count': operators_count,
        'users_count': users_count,
        'online_users': online_users,
        'server': 'operator-app-yn89.onrender.com'
    }, 200

@app.route('/ping')
def ping():
    """Простой эндпоинт для пинга сервера"""
    return {'status': 'pong', 'timestamp': datetime.now().isoformat()}, 200

def process_queue(operator_id):
    """Обработка очереди для оператора"""
    operators = get_operators_from_db()
    operator = next((op for op in operators if op['id'] == operator_id), None)
    
    if operator and not operator['occupied_by'] and operator['queue']:
        next_user = operator['queue'].pop(0)
        
        # Обновляем оператора в БД
        update_operator_in_db(
            operator_id,
            occupied_by=next_user['username'],
            os_type=next_user['os_type'],
            queue=operator['queue']
        )
        
        # Получаем обновленный список операторов
        updated_operators = get_operators_from_db()
        
        # Отправляем обновление всем клиентам
        socketio.emit('operators_update', updated_operators, broadcast=True)
        
        # Отправляем уведомление пользователю, который вышел из очереди
        socketio.emit('queue_advanced', {
            'operator_name': operator['name'],
            'username': next_user['username'],
            'message': f'Оператор "{operator["name"]}" свободен!'
        })
        
        print(f"[{datetime.now()}] 🔔 Отправлено уведомление для {next_user['username']} о продвижении в очереди")
        return True
    
    return False

@socketio.on('connect')
def handle_connect():
    print(f"[{datetime.now()}] 🔗 Клиент подключился: {request.sid}")

@socketio.on('set_username')
def handle_set_username(data):
    """Установка имени пользователя с проверкой существующей сессии"""
    username = data.get('username')
    print(f"[{datetime.now()}] 📝 Пользователь пытается установить имя: {username}")
    
    if not username or len(username.strip()) == 0:
        emit('registration_error', {'error': 'Имя не может быть пустым'}, room=request.sid)
        return
    
    # Добавляем/обновляем пользователя в БД
    add_user_to_db(username)
    
    # Проверяем, есть ли у пользователя активные сессии
    operators = get_operators_from_db()
    user_operators = [op for op in operators if op['occupied_by'] == username]
    user_queues = []
    
    for op in operators:
        for user in op['queue']:
            if user['username'] == username:
                user_queues.append({
                    'operator_name': op['name'],
                    'position': op['queue'].index(user) + 1,
                    'operator_id': op['id']
                })
    
    # Отправляем подтверждение с информацией о текущем состоянии
    emit('user_registered', {
        'username': username,
        'message': 'Регистрация успешна',
        'restored_data': {
            'occupied_operators': [op['name'] for op in user_operators],
            'queues': user_queues
        }
    }, room=request.sid)
    
    # Отправляем обновленный список операторов
    emit('operators_update', operators, room=request.sid)
    
    print(f"[{datetime.now()}] ✅ Пользователь {username} зарегистрирован. Восстановлено: {len(user_operators)} операторов, {len(user_queues)} очередей")

@socketio.on('occupy_operator')
def handle_occupy(data):
    print(f"[{datetime.now()}] 🎯 Занятие оператора: {data}")
    
    # Проверяем, свободен ли оператор
    operators = get_operators_from_db()
    operator = next((op for op in operators if op['id'] == data['operator_id']), None)
    
    if not operator:
        emit('operation_error', {'error': 'Оператор не найден'}, room=request.sid)
        return
    
    if operator['occupied_by']:
        emit('operation_error', {'error': 'Оператор уже занят'}, room=request.sid)
        return
    
    # Проверяем, не стоит ли пользователь уже в очереди к этому оператору
    if any(user['username'] == data['username'] for user in operator['queue']):
        # Удаляем пользователя из очереди
        new_queue = [user for user in operator['queue'] if user['username'] != data['username']]
        update_operator_in_db(data['operator_id'], queue=new_queue)
    
    # Обновляем оператора в БД
    update_operator_in_db(
        data['operator_id'],
        occupied_by=data['username'],
        os_type=data['os_type']
    )
    
    # Обновляем пользователя
    update_user_last_seen(data['username'])
    
    # Отправляем обновление всем
    emit('operators_update', get_operators_from_db(), broadcast=True)

@socketio.on('join_queue')
def handle_join_queue(data):
    print(f"[{datetime.now()}] 📝 Вход в очередь: {data}")
    
    operators = get_operators_from_db()
    operator = next((op for op in operators if op['id'] == data['operator_id']), None)
    
    if not operator:
        emit('operation_error', {'error': 'Оператор не найден'}, room=request.sid)
        return
    
    # Проверяем, не занят ли уже оператор этим пользователем
    if operator['occupied_by'] == data['username']:
        emit('operation_error', {'error': 'Вы уже заняли этого оператора'}, room=request.sid)
        return
    
    # Проверяем, не стоит ли уже пользователь в очереди
    if data['username'] not in [u['username'] for u in operator['queue']]:
        # Добавляем пользователя в очередь
        new_queue = operator['queue'].copy()
        new_queue.append({
            'username': data['username'],
            'os_type': data['os_type']
        })
        
        # Обновляем оператора в БД
        update_operator_in_db(data['operator_id'], queue=new_queue)
        
        # Обновляем пользователя
        update_user_last_seen(data['username'])
        
        # Отправляем обновление всем
        emit('operators_update', get_operators_from_db(), broadcast=True)

@socketio.on('release_operator')
def handle_release(data):
    print(f"[{datetime.now()}] 🗑️ Освобождение оператора: {data}")
    
    # Проверяем, что оператор действительно занят этим пользователем
    operators = get_operators_from_db()
    operator = next((op for op in operators if op['id'] == data['operator_id']), None)
    
    if not operator:
        emit('operation_error', {'error': 'Оператор не найден'}, room=request.sid)
        return
    
    if operator['occupied_by'] != data['username']:
        emit('operation_error', {'error': 'Этот оператор занят другим пользователем'}, room=request.sid)
        return
    
    # Освобождаем оператора в БД
    update_operator_in_db(
        data['operator_id'],
        occupied_by=None,
        os_type=None
    )
    
    # Обновляем пользователя
    update_user_last_seen(data['username'])
    
    # Обрабатываем очередь
    queue_processed = process_queue(data['operator_id'])
    
    # Если очередь не обработалась (не было очереди), отправляем обновление
    if not queue_processed:
        emit('operators_update', get_operators_from_db(), broadcast=True)

@socketio.on('heartbeat')
def handle_heartbeat(data):
    """Обработка heartbeat для поддержания сессии"""
    username = data.get('username')
    if username:
        update_user_last_seen(username)
        emit('heartbeat_response', {'status': 'ok'}, room=request.sid)

@socketio.on('disconnect')
def handle_disconnect():
    print(f"[{datetime.now()}] 🔌 Клиент отключился: {request.sid}")
    # Не очищаем данные пользователя - они сохраняются в БД

@socketio.on('server_ping')
def handle_server_ping():
    """Обработчик для тестового пинга от клиентов"""
    print(f"[{datetime.now()}] 📡 Получен server_ping от {request.sid}")
    emit('server_pong', {
        'timestamp': datetime.now().isoformat(),
        'message': 'Server is alive and responding'
    }, room=request.sid)

@socketio.on('operation_error')
def handle_operation_error(data):
    """Обработчик ошибок операций"""
    print(f"[{datetime.now()}] ❌ Ошибка операции: {data}")

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    
    # Запускаем фоновый поток для keep-alive и очистки
    background_thread = threading.Thread(target=background_tasks, daemon=True)
    background_thread.start()
    
    print(f"[{datetime.now()}] 🚀 Сервер запускается на порту {port}")
    print(f"[{datetime.now()}] ⚡ SQLite база данных подключена")
    print(f"[{datetime.now()}] 🔄 Фоновые задачи активированы")
    
    socketio.run(
        app,
        host='0.0.0.0',
        port=port,
        debug=False,
        allow_unsafe_werkzeug=True
    )
