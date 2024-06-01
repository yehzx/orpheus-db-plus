import os
import subprocess

from orpheusplus.command_utils import command

if os.name == "nt":
    subprocess.run("cls", shell=True)
elif os.name == "posix":
    subprocess.run("clear", shell=True)

command("orpheusplus init -n new_table -s ./examples/sample_schema.csv", sleep=1)
command("orpheusplus insert -n new_table -d ./examples/data_1.csv", sleep=1)
command("orpheusplus commit -n new_table -m version_1", sleep=1)
command('orpheusplus run -i "SELECT * FROM VTABLE new_table"', sleep=1)
command("orpheusplus update -n new_table -d ./examples/data_1.csv ./examples/data_2.csv", sleep=1)
command("orpheusplus commit -n new_table -m version_2")
command('orpheusplus run -i "SELECT * FROM VTABLE new_table"', sleep=1)
command("orpheusplus log -n new_table", sleep=2)
command("orpheusplus checkout -n new_table -v 1", sleep=1)
command('orpheusplus run -i "SELECT * FROM VTABLE new_table"', sleep=1)
command("orpheusplus drop -n new_table --all -y")
