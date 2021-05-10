from kafka import KafkaProducer
from kafka import KafkaConsumer
from getmac import get_mac_address as gma
import json
import psutil
import time
import threading
import wget
import socket
import distro
import installer
import subprocess
import socket
import os
import sqlite3


mac = gma()
connection = sqlite3.connect('tasks.db')
cursor = connection.cursor()
cursor.execute("""CREATE TABLE IF NOT EXISTS
results(task TEXT, topic TEXT )""")

connection.commit()
connection.close()


BOOTSTRAP_SERVERS = ['172.16.12.56:9092']
t_kafka = threading.Thread()
producer = None
global config_data

def register_kafka_listener(topic, listener):
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=BOOTSTRAP_SERVERS,
                             auto_offset_reset='earliest',
                             enable_auto_commit=True
                             )
    def poll():
        

        consumer.poll(timeout_ms=5000)
        for msg in consumer:
            listener(msg)
    t_kafka = threading.Thread()
    t_kafka._target = poll
    t_kafka.daemon = True
    t_kafka.start()

def kafka_config_listener(data):
    global config_data
    config_data = json.loads(data.value.decode('utf-8'))
    request_result = {
        'mac' : gma(),
        'sequence_number' : config_data['sequence_number'],
        'result_code' : 0,
        'message' : ''
    }

    if 'fileDownload' in config_data:
        print(config_data['location'] + ' ' + config_data['fileDownload'] + ' ' + config_data['type'])
        try:
            if os.path.exists(config_data['path'] + config_data['fileDownload']):
                os.remove(config_data['path'] + config_data['fileDownload'])

            wget.download(config_data['location'] + config_data['fileDownload'], out= config_data['path'])

            if config_data['type'] == 'script':
                if installer.run_script().returncode == 0:
                    request_result['message'] = 'script successfully executed'
                else:
                    request_result['message'] = 'unable to execute script'
            else:
                request_result['message'] = 'file successfully uploaded'
        except Exception as e:
            request_result['message'] = 'unable to download file'
            request_result['result_code'] = 1111
            print(e)

        try:
            producer.send('REQUEST_RESULT', json.dumps(request_result).encode('utf-8'))
            result = produce.get(timeout=20)
        except Exception as e:
            connection = sqlite3.connect('tasks.db')
            cursor = connection.cursor()
            cursor.execute('INSERT INTO results VALUES (?, ?)', ("{}".format(request_result), 'REQUEST_RESULT'))
            connection.commit()
            connection.close()
            print(e)

        

    else:
        print(config_data)

def kafka_management_listener(data):
    data = json.loads(data.value.decode('utf-8'))
    print(data)
    request_result = {
    'mac' : gma(),
    'sequence_number' : data['sequence_number'],
    'result_code' : 0,
    'version' : '',
    'latest_version': '',
    'message' : ''
    }

    if data['action'] == 'install':
        if(installer.is_package_installed(data['app'])):
            request_result['result_code'] = 1000
        else:
            result = installer.install_package(data['app'], data['version'])
            request_result['result_code'] = result.returncode

            if result.returncode == 0:              
                request_result['version'], request_result['latest_version'] = installer.get_package_versions(data['app'])

    elif data['action'] == 'remove':
        if(not installer.is_package_installed(data['app'])):
            request_result['result_code'] = 1000
        else:
            result = installer.uninstall_package(data['app'])
            request_result['result_code'] = result.returncode

    elif data['action'] == 'update':
        if(not installer.is_package_installed(data['app'])):
            request_result['result_code'] = 1000
        else:    
            result = installer.update_package(data['app'], data['version'])
            request_result['result_code'] = result.returncode
            if result.returncode == 0: 
                request_result['version'], request_result['latest_version'] = installer.get_package_versions(data['app'])
    elif data['action'] == 'update_all':
        result = installer.update_all()    
        request_result['result_code'] = result.returncode
        send_device_info()
    try:
        producer.send('REQUEST_RESULT', json.dumps(request_result).encode('utf-8'))
        result = produce.get(timeout=20)
    except Exception as e:
        connection = sqlite3.connect('tasks.db')
        cursor = connection.cursor()
        cursor.execute('INSERT INTO results VALUES (?, ?)', ("{}".format(request_result), 'REQUEST_RESULT'))
        connection.commit()
        connection.close()
        print(e)

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
        register_kafka_listener(gma().replace(':', '') + '_CONFIG', kafka_config_listener)
        register_kafka_listener(gma().replace(':', '') + '_MANAGEMENT', kafka_management_listener)
        break
    except Exception as e:
        print(e)
        time.sleep(10)
        pass

def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 80))
    return s.getsockname()[0]

ip_address = get_ip()



device_info = {
    'name': socket.gethostname(),
    'ip' : ip_address,
    'mac' : gma(),
    'distribution' : distro.name(),
    'version' : distro.version(),
}
producer.send('DEVICE_INFO', json.dumps(device_info).encode('utf-8'))

def send_device_info():
    device_info = {
        'name': socket.gethostname(),
        'ip' : ip_address,
        'mac' : gma(),
        'distribution' : distro.name(),
        'version' : distro.version(),
        'packages' : installer.get_manualy_installed_packages()
    }
    try:
        producer.send(f'{mac}_DEVICE_INFO'.replace(':',''), json.dumps(device_info).encode('utf-8'))
    except Exception as e:
        connection = sqlite3.connect('tasks.db')
        cursor = connection.cursor()
        cursor.execute('INSERT INTO results VALUES (?, ?)', ("{}".format(device_info), f'{mac}_DEVICE_INFO'.replace(':','')))
        connection.commit()
        connection.close()
        print(e)
        
def loop_device_info():
    while(True):
        time.sleep(2)
        installer.update()
        send_device_info()
        time.sleep(600)

t_device_info = threading.Thread()
t_device_info._target = loop_device_info
t_device_info.daemon = True
t_device_info.start()

def send_alive_info():
    while(True):
        
        alive = {
            'alive' : True,
            'mac' : mac
        }
        try:
            producer.send(f'{mac}_DEVICE_INFO'.replace(':',''), json.dumps(alive).encode('utf-8'))
            time.sleep(5)
        except Exception as e:
            print(e)

t_send_alive_info = threading.Thread()
t_send_alive_info._target = send_alive_info
t_send_alive_info.daemon = True
t_send_alive_info.start()


while(True):
    hdd = psutil.disk_usage('/')
    monitor = {    
        'name' : distro.name(),
        'ip' : ip_address,
	    'mac' : mac,
        'time' : time.strftime('%H:%M:%S', time.localtime()),
        'cpu_usage' : round(psutil.cpu_percent(), 2),
        'ram_usage' : round(psutil.virtual_memory().percent, 2),
        #'cpu_freq' : psutil.cpu_freq(), #freq, min, max
        #'cpu_stats' : psutil.cpu_stats(), #? interrupts, soft_interrupts, syscalls
        'disk_space': round(hdd.total / (2**30), 2),
        'used_disk_space' : round(hdd.used / (2**30), 2)
        #'disk partitions' : psutil.disk_partitions()
    
    }
    try:
        produce = producer.send('MONITORING', json.dumps(monitor).encode('utf-8'))
        result = produce.get(timeout=20)
        connection = sqlite3.connect('tasks.db')
        cursor = connection.cursor()
        for row in cursor.execute("SELECT * FROM results"):
            print(row)
            producer.send(row[1], json.dumps(eval(row[0])).encode('utf-8'))
        cursor.execute("delete from results")
        connection.commit()
        connection.close()
        time.sleep(5)
    except Exception as e:
        connection = sqlite3.connect('tasks.db')
        cursor = connection.cursor()
        cursor.execute('INSERT INTO results VALUES (?, ?)', ("{}".format(monitor), 'MONITORING'))
        connection.commit()
        connection.close()
        print(e)
