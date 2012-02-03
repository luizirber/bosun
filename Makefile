pep8:
	pep8 fabfile.py
	pep8 tasks.py

pylint:
	- pylint -d c fabfile.py
	- pylint -d c tasks.py

pyflakes:
	- pyflakes fabfile.py
	- pyflakes tasks.py

clonedigger:
	- clonedigger -o output.html fabfile.py
	- clonedigger -o output.html tasks.py

check: pep8 pylint pyflakes clonedigger

clean:
	- rm -rf output.html workspace
	find . -iname '*.pyc' -exec rm {} +

run:
	fab -H tupa -u ${USER} deploy_and_run:name='expbase'

deploy_run_named:
	fab -H tupa -u ${USER} deploy_and_run:name=${NAME}

run_named:
	fab -H tupa -u ${USER} run:name=${NAME}

compile:
	fab -H tupa -u ${USER} compilation:comp=${COMP}
