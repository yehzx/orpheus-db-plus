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
    # time.sleep(2)
    

command("orpheusplus ls")
command("orpheusplus init -n test_merge -s ./examples/sample_schema.csv")
command("orpheusplus insert -n test_merge -d ./examples/data_1.csv")
command("orpheusplus commit -n test_merge -m version_1")
command("orpheusplus insert -n test_merge -d ./examples/data_2.csv")
command("orpheusplus commit -n test_merge -m version_2")
command("orpheusplus update -n test_merge -d ./examples/data_2.csv ./examples/data_3.csv")
command("orpheusplus commit -n test_merge -m version_3")
command("orpheusplus delete -n test_merge -d ./examples/data_1.csv")
command("orpheusplus commit -n test_merge -m version_4")
command("orpheusplus checkout -n test_merge -v 1")
command("orpheusplus update -n test_merge -d ./examples/data_1.csv ./examples/data_4.csv")
command("orpheusplus commit -n test_merge -m version_5")
command("orpheusplus insert -n test_merge -d ./examples/data_2.csv")
command("orpheusplus commit -n test_merge -m version_6")
command("orpheusplus log -n test_merge")
command("orpheusplus merge -n test_merge -v 4")