import os
import requests
import random
from flask import Flask, request, Response, jsonify

CINE_ABYS_PROXY_NAME = "CineAbysProxy/1.0"
PROXING_REQUEST_TIMEOUT = 30

__app = Flask(__name__)

mvs_requests_percentag = 0 # Процент запросов, напралямых на сервис movies - по умолчанию НИЧЕГО!

if os.getenv("GRADUAL_MIGRATION"):
    migartion_level = os.getenv("GRADUAL_MIGRATION") or ''
    
    if migartion_level.lower() == "true" :
        mvs_requests_percentag = int(os.environ["MOVIES_MIGRATION_PERCENT"])

SERVER_CONFIG = {
    os.environ["MOVIES_SERVICE_URL"] : mvs_requests_percentag,    
    os.environ["MONOLITH_URL"] : 100 - mvs_requests_percentag 
}

# Преобразуем конфигурацию в удобный формат для выбора
TARGET_SERVERS = list(SERVER_CONFIG.keys())
SERVER_WEIGHTS = list(SERVER_CONFIG.values())

# Возвращат сервер согласно заданному процентному соотношению
def get_target_server():        
    return random.choices(TARGET_SERVERS, weights=SERVER_WEIGHTS, k=1)[0]

def modify_request_headers(headers, target_server):
    # Модификация заголовков запроса
    new_headers = {}
    for key, value in headers:
        if key.lower() not in ['host', 'accept-encoding']:
            new_headers[key] = value

    new_headers['User-Agent'] = CINE_ABYS_PROXY_NAME  
    new_headers['X-Proxy-Target'] = target_server

    return new_headers

@__app.route('/health')
def health_check():
    return jsonify({"status": True})

@__app.route('/api', defaults={'path': ''})
@__app.route('/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'])
def proxy(path):
    try:
        str_path = path or ''
        
        match str_path.lower():
            case('movies'): # Проксируется между монолитом и мс пока только обработка запросов к фильмам
                # Выбираем целевой сервер по процентному соотношению
                target_server = get_target_server()                
            case _:
                target_server = os.environ["MONOLITH_URL"]

        target_url = f"{target_server}/api/{path}"
        
        print (f"Proxying {request.method} to: {target_url} ({SERVER_CONFIG[target_server]}% traffic)")
        
        # Подготавливаем запрос
        headers = modify_request_headers(request.headers, target_server)
        
        # Отправляем запрос
        response = requests.request(
            method          = request.method,
            url             = target_url,
            headers         = headers,
            params          = request.args,
            data            = request.get_data(),
            cookies         = request.cookies,
            allow_redirects = False,
            timeout         = PROXING_REQUEST_TIMEOUT
        )
        
        # Создаем ответ
        excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        response_headers = [
            (name, value) for name, value in response.raw.headers.items()
            if name.lower() not in excluded_headers
        ]
        
        # Добавляем информацию о балансировке
        response_headers.append(('X-Load-Balancer-Target', target_server))
        response_headers.append(('X-Traffic-Percentage', str(SERVER_CONFIG[target_server])))
        
        proxy_response = Response(response.content, response.status_code, response_headers)
        return proxy_response
        
    except requests.exceptions.Timeout:
        return "Target server timeout", 504
    except requests.exceptions.ConnectionError:
        return "Cannot connect to target server", 502
    except Exception as e:        
        return f"Internal proxy error: {str(e)}", 500

@__app.after_request
def after_request(response):
    # Добавляем заголовки с информацией о балансировке
    response.headers['X-Load-Balancer-Type'] = 'percentage-based'
    response.headers['X-Available-Servers'] = ', '.join(TARGET_SERVERS)
    return response

if __name__ == '__main__':
    print("Запуск прокси с балансировкой по процентному соотношению")
    print("\nКонфигурация серверов:")
    total_percentage = sum(SERVER_CONFIG.values())
    for server, percentage in SERVER_CONFIG.items():
        print(f"  {server} -> {percentage}% трафика")
        
    if total_percentage != 100:
        print("⚠️  Внимание: сумма процентов не равна 100%!")
    
    __app.run(host='0.0.0.0', port=os.environ["PORT"], debug=True, threaded=True)