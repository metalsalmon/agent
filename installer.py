import re
import subprocess

PACKAGE_MANAGERS = {
    # 'command': 'test if package ? exists'-commnad
    'dpkg': 'dpkg -s ?',
    'brew': 'brew ls ?'
    # just add new package managers here
}

def find_package_manager():
    for pm in PACKAGE_MANAGERS.keys():
        if subprocess.call(['which', pm], stdout=subprocess.PIPE, stderr=subprocess.PIPE) == 0:
            return pm
    raise OSError('Unable to find package manager.')

def is_package_installed(name):
    return subprocess.call(PACKAGE_MANAGERS[find_package_manager()].replace('?', name)+' > /dev/null 2>&1', shell=True) == 0

def detect_package_version(name):
    if not is_package_installed(name):
        raise OSError('Unable to find package.')
    try:
        x = subprocess.check_output(name+' --version > /dev/null 2>&1', shell=True)
    except subprocess.CalledProcessError:
        pass
    else:
        a = re.findall('\\d+\\.\\d+\\.\\d+', x)
        if len(a) > 0:
            return a[0]
        b = re.findall('\\d+\\.\\d+', x)
        if len(b) > 0:
            return b[0]
def get_manualy_installed_packages():
    package_list = []
    package = {}
    x =  subprocess.check_output(['apt', 'list', '--manual-installed'])
    for line in x.splitlines():
        line = str(line, 'utf-8')
        if 'INSTALLED' in line.upper():
            package = {
                'package': re.search('(.*)/', line).group(1),
                'version': re.search(' (.[^ ]*) ', line).group(1),
                'latest_version' : re.search(' (.[^ ]*) ', line).group(1)
            }
            package_list.append(package)

        elif 'UPGRADABLE' in line.upper():
            package = {
                'package': re.search('(.*)/', line).group(1),
                'version': re.search('(?<=upgradable from: )(.*)(?=])', line).group(1), 
                'latest_version': re.search(' (.[^ ]*) ', line).group(1)
            }
            package_list.append(package)   

    return package_list

def install_package(name, version):
    if version == 'latest':
        return subprocess.run(['sudo', 'apt', 'install', '-y', name], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    else:
        print(version)
        return subprocess.run(['sudo', 'apt', 'install', '-y', name+'='+version], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def uninstall_package(name):
    return subprocess.run(['sudo', 'apt', 'purge', '-y', name], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def update_package(name, version):
    if version == 'latest':
        subprocess.run(['sudo', 'apt', 'update', '-y'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        return subprocess.run(['sudo', 'apt', 'install', '--only-upgrade' , '-y', name], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    else:
        return subprocess.run(['sudo', 'apt-get', 'install', '-y', name+'='+version], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def update():
    subprocess.run(['sudo', 'apt', 'update', '-y'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def get_package_versions(name):
    result = subprocess.check_output(['apt-cache', 'policy', name], text=True)
    return re.search('[\n\r].*Installed:\s*([^\n\r]*)', result).group(1), re.search('[\n\r].*Candidate:\s*([^\n\r]*)', result).group(1)
