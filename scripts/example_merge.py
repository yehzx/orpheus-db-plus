import os
import subprocess

from orpheusplus.command_utils import command

if os.name == "nt":
    subprocess.run("cls", shell=True)
elif os.name == "posix":
    subprocess.run("clear", shell=True)

command("orpheusplus init -n new_table -s ./examples/sample_schema.csv")
command("orpheusplus insert -n new_table -d ./examples/data_1.csv")
command("orpheusplus commit -n new_table -m version_1")
command("orpheusplus insert -n new_table -d ./examples/data_2.csv")
command("orpheusplus commit -n new_table -m version_2")
command("orpheusplus delete -n new_table -d ./examples/data_2.csv")
command("orpheusplus commit -n new_table -m version_3")
command("orpheusplus update -n new_table -d ./examples/data_1.csv ./examples/data_3.csv")
command("orpheusplus commit -n new_table -m version_4")
command("orpheusplus checkout -n new_table -v 1")
command("orpheusplus delete -n new_table -d ./examples/data_1.csv")
command("orpheusplus insert -n new_table -d ./examples/data_2.csv")
command("orpheusplus commit -n new_table -m version_5")
command("orpheusplus log -n new_table", sleep=3)
command('orpheusplus run -i "SELECT * FROM VTABLE new_table"', sleep=3)
command('orpheusplus run -i "SELECT * FROM VTABLE new_table OF VERSION 4"', sleep=3)
command("orpheusplus merge -n new_table -v 4", sleep=3)
command("orpheusplus merge -n new_table -v 4 -r ./examples/example_conflicts.csv", sleep=3)
command("orpheusplus log -n new_table", sleep=3)
command('orpheusplus run -i "SELECT * FROM VTABLE new_table"', sleep=3)
command("orpheusplus drop -n new_table --all -y")

