import requests
from flask import Flask, jsonify

app = Flask(__name__)

user_service_host = 'user'

@app.route('/')
def index():
    return "Hello world"

def get_user():
    r = requests.get(f'http://{user_service_host}:5000/user/profile')
    return r.json()

@app.route('/todo')
def get_todo():
    user = get_user()
    r = requests.get("https://jsonplaceholder.typicode.com/todos")
    return jsonify(r.json())

@app.route('/todo/<id>')
def get_todo_by_id(id):
    user = get_user()
    r = requests.get(f"https://jsonplaceholder.typicode.com/todos/{id}")
    return jsonify(r.json())


if __name__ == '__main__':
    app.run( host='0.0.0.0', port=8080)

