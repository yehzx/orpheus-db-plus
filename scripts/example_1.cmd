@goto(){
  set -x
  orpheusplus ls
  sleep 3
  orpheusplus init -n new_table -s ./examples/sample_schema.csv
  sleep 3
  orpheusplus insert -n new_table -d ./examples/data_1.csv
  sleep 3
  orpheusplus commit -n new_table -m version_1
  sleep 3
  orpheusplus run -i "SELECT * FROM VTABLE new_table"
  sleep 3
  orpheusplus insert -n new_table -d ./examples/data_2.csv
  sleep 3
  orpheusplus commit -n new_table -m version_2
  sleep 3
  orpheusplus run -i "SELECT * FROM VTABLE new_table"
  sleep 3
  orpheusplus checkout -n new_table -v 1
  sleep 3
  orpheusplus run -i "SELECT * FROM VTABLE new_table"
  sleep 3
  orpheusplus ls
  sleep 3
  orpheusplus log -n new_table
  sleep 3
  orpheusplus drop -n new_table --all -y
  set +x
}

@goto $@
exit

:(){
REM @echo off
orpheusplus ls
timeout /t 3 /nobreak >nul
orpheusplus init -n new_table -s ./examples/sample_schema.csv
timeout /t 3 /nobreak >nul
orpheusplus insert -n new_table -d ./examples/data_1.csv
timeout /t 3 /nobreak >nul
orpheusplus commit -n new_table -m version_1
timeout /t 3 /nobreak >nul
orpheusplus run -i "SELECT * FROM VTABLE new_table"
timeout /t 3 /nobreak >nul
orpheusplus insert -n new_table -d ./examples/data_2.csv
timeout /t 3 /nobreak >nul
orpheusplus commit -n new_table -m version_2
timeout /t 3 /nobreak >nul
orpheusplus run -i "SELECT * FROM VTABLE new_table"
timeout /t 3 /nobreak >nul
orpheusplus checkout -n new_table -v 1
timeout /t 3 /nobreak >nul
orpheusplus run -i "SELECT * FROM VTABLE new_table"
timeout /t 3 /nobreak >nul
orpheusplus ls
timeout /t 3 /nobreak >nul
orpheusplus log -n new_table
timeout /t 3 /nobreak >nul
orpheusplus drop -n new_table --all -y