import subprocess
import time

# ===============  
UTIL_VERSION = 10
# ===============

YELLOW = "\033[93m"
CYAN = "\033[96m"
OFF = "\033[00m"

subprocess.run("cls", shell=True)

def command(stmt):
    print(f"{YELLOW}{stmt}{OFF}")
    result = subprocess.run(stmt, capture_output=True, text=True, encoding="utf-8")
    print(result.stdout, end="")
    # time.sleep(2)

for i in range(1, UTIL_VERSION+1):
    command(f'orpheusplus run -i "SELECT * FROM VTABLE test_merge OF VERSION {i}"')
