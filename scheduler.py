# scheduler.py
import time
import threading
from queue import Queue
from flask import Flask, request, jsonify

app = Flask(__name__)
task_queue = Queue()
tasks = {}
workers = []
LOCK = threading.Lock()

class Task:
    def __init__(self, task_id, func, args):
        self.task_id = task_id
        self.func = func
        self.args = args
        self.status = 'pending'

def worker():
    while True:
        task = task_queue.get()
        if task is None:
            break
        with LOCK:
            task.status = 'running'
        try:
            task.func(*task.args)
            with LOCK:
                task.status = 'completed'
        except Exception as e:
            with LOCK:
                task.status = f'failed: {e}'
        task_queue.task_done()

@app.route('/schedule', methods=['POST'])
def schedule_task():
    data = request.json
    task_id = data.get('task_id')
    func = globals().get(data.get('func'))
    args = data.get('args', [])
    if not func:
        return jsonify({'error': 'Function not found'}), 400
    task = Task(task_id, func, args)
    with LOCK:
        tasks[task_id] = task
    task_queue.put(task)
    return jsonify({'status': 'task scheduled'}), 200

@app.route('/status/<task_id>', methods=['GET'])
def task_status(task_id):
    with LOCK:
        task = tasks.get(task_id)
        if not task:
            return jsonify({'error': 'Task not found'}), 404
        return jsonify({'status': task.status}), 200

def start_workers(num_workers=4):
    for _ in range(num_workers):
        t = threading.Thread(target=worker)
        t.daemon = True
        t.start()
        workers.append(t)

if __name__ == '__main__':
    start_workers()
    app.run(host='0.0.0.0', port=5000)
