import re
import subprocess

PACKAGE_MANAGERS = {
    # "command": "test if package ? exists"-commnad
    "dpkg": "dpkg -s ?",
    "brew": "brew ls ?"
    # just add new package managers here
}

def find_package_manager():
    for pm in PACKAGE_MANAGERS.keys():
        if subprocess.call(["which", pm], stdout=subprocess.PIPE, stderr=subprocess.PIPE) == 0:
            return pm
    raise OSError("Unable to find package manager.")

def is_package_installed(name):
    return subprocess.call(PACKAGE_MANAGERS[find_package_manager()].replace("?", name)+" > /dev/null 2>&1", shell=True) == 0

def detect_package_version(name):
    if not is_package_installed(name):
        raise OSError("Unable to find package.")
    try:
        x = subprocess.check_output(name+" --version > /dev/null 2>&1", shell=True)
    except subprocess.CalledProcessError:
        pass
    else:
        a = re.findall("\\d+\\.\\d+\\.\\d+", x)
        if len(a) > 0:
            return a[0]
        b = re.findall("\\d+\\.\\d+", x)
        if len(b) > 0:
            return b[0]
def get_manualy_installed_packages():
    package_list = []
    x =  subprocess.check_output(["apt", "list", "--manual-installed"])
    for line in x.splitlines():
        line = str(line, 'utf-8')
        if not "INSTALLED" in line.upper():
            continue
        else:
            package = {
                "package": re.search('(.*)/', line).group(1),
                "version": re.search(' (.[^ ]*) ', line).group(1) 
            }
            package_list.append(package)
    
    return package_list

#print(is_package_installed("python"))

