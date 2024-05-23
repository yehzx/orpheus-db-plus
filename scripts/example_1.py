import subprocess
import time

YELLOW = "\033[93m"
CYAN = "\033[96m"
OFF = "\033[00m"

subprocess.run("cls", shell=True)

def command(stmt):
    print(f"{YELLOW}{stmt}{OFF}")
    result = subprocess.run(stmt, capture_output=True, text=True, encoding="utf-8")
    print(result.stdout, end="")
    time.sleep(2)
    

command("orpheusplus ls")
command("orpheusplus init -n new_table -s ./examples/sample_schema.csv")
command("orpheusplus insert -n new_table -d ./examples/data_1.csv")
command("orpheusplus commit -n new_table -m version_1")
command('orpheusplus run -i "SELECT * FROM VTABLE new_table"')
command("orpheusplus insert -n new_table -d ./examples/data_2.csv")
command("orpheusplus commit -n new_table -m version_2")
command('orpheusplus run -i "SELECT * FROM VTABLE new_table"')
command("orpheusplus checkout -n new_table -v 1")
command('orpheusplus run -i "SELECT * FROM VTABLE new_table"')
command("orpheusplus ls")
command("orpheusplus log -n new_table")
command("orpheusplus drop -n new_table --all -y")
