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

#print(installer.is_package_installed("python"))

#print(installer.get_manualy_installed_packages())

#print(subprocess.run(["apt", "list"]).returncode)

BOOTSTRAP_SERVERS = ['172.16.12.56:9092']
producer = KafkaProducer(bootstrap_servers=['172.16.12.56:9092'])
t_kafka = threading.Thread()

device_info = {
    "name": socket.gethostname(),
    "mac" : gma(),
    "distribution" : distro.name(),
    "version" : distro.version(),
    "packages" : installer.get_manualy_installed_packages()
}


request_result = {
    "mac" : gma(),
    "sequence_number" : -1,
    "result_code" : 0,
    "result" : "error",
    "message" : ""
}

producer.send('DEVICE_INFO', json.dumps(device_info).encode('utf-8'))

global config_data

def register_kafka_listener(topic, listener):

    def poll():
        consumer = KafkaConsumer(topic, bootstrap_servers=BOOTSTRAP_SERVERS)

        consumer.poll(timeout_ms=5000)
        for msg in consumer:
            listener(msg)
    t_kafka = threading.Thread()
    t_kafka._target = poll
    t_kafka.daemon = True
    t_kafka.start()

def kafka_config_listener(data):
    global config_data
    config_data = json.loads(data.value.decode("utf-8"))

    if 'fileDownload' in config_data:
        print('http://172.16.12.56/api/uploads/' + config_data['fileDownload'])
        wget.download('http://172.16.12.56:5000/api/uploads/' + config_data['fileDownload'])
    else:
        print(config_data)

def kafka_management_listener(data):
    data = json.loads(data.value.decode("utf-8"))
    request_result['sequence_number'] = data['sequence_number']

    if data['action'] == 'install':
        if(installer.is_package_installed(data['app'])):
            request_result['result'] = 'success'
            request_result['result_code'] = 1000
            request_result['message'] = 'Already installed'
        else:
            result = installer.install_package(data['app'])
            request_result['result_code'] = result.returncode
            if result.returncode == 0:
                request_result['result'] = 'success'                  
                request_result['message'] = 'Successfully installed'
            else:
                request_result['result'] = 'error'


    elif data['action'] == 'remove':
        result = installer.uninstall_package(data['app'])
        request_result['result_code'] = result.returncode
        if result.returncode == 0:
            request_result['result'] = 'success'                
            request_result['message'] = 'Successfully uninstalled'
        else:
            request_result['result'] = 'error'

    #request_result['message'] = result.stdout
    producer.send('REQUEST_RESULT', json.dumps(request_result).encode('utf-8'))
    result = produce.get(timeout=60)
    

register_kafka_listener('CONFIG', kafka_config_listener)
register_kafka_listener(gma().replace(':', '') + '_MANAGEMENT', kafka_management_listener)


while(True):
    hdd = psutil.disk_usage('/')
    monitor = {    
        "name" : distro.name(),
	    "mac" : gma(),
        "time" : time.strftime("%H:%M:%S", time.localtime()),
        "cpu_usage" : round(psutil.cpu_percent(), 2),
        "ram_usage" : round(psutil.virtual_memory().percent, 2),
        "cpu_freq" : psutil.cpu_freq(), #freq, min, max
        "cpu_stats" : psutil.cpu_stats(), #? interrupts, soft_interrupts, syscalls
        "disk_space": round(hdd.total / (2**30), 2),
        "used_disk_space" : round(hdd.used / (2**30), 2)
        #"disk partitions" : psutil.disk_partitions()
    
    }
    produce = producer.send('MONITORING', json.dumps(monitor).encode('utf-8'))
    result = produce.get(timeout=60)
    time.sleep(5)

