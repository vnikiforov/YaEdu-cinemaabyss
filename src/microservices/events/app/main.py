import os
import requests
import random
from flask import Flask, request, Response

__app = Flask(__name__)

@__app.route('/health')
def health_check():
    return "Events service is Online"

if __name__ == '__main__':
    print("Запуск микросервера тестирования брокера Kafka")
    
    __app.run(host='0.0.0.0', port=os.environ["PORT"], debug=True, threaded=True)