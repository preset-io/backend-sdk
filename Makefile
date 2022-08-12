pyenv: .python-version

.python-version: setup.cfg
	if [ -z "`pyenv virtualenvs | grep backend-sdk`" ]; then\
	    pyenv virtualenv backend-sdk;\
	fi
	if [ ! -f .python-version ]; then\
	    pyenv local backend-sdk;\
	fi
	pip install -e '.[testing]'
	touch .python-version

test: pyenv
	pytest --cov=src/preset_cli -vv tests/ --doctest-modules src/preset_cli

clean:
	pyenv virtualenv-delete backend-sdk

spellcheck:
	codespell -S "*.json" src/preset_cli docs/*rst tests templates

requirements.txt: .python-version requirements.in
	pip install --upgrade pip
	pip-compile --no-annotate

dev-requirements.txt: dev-requirements.in requirements.in
	pip-compile dev-requirements.in --no-annotate

check:
	pre-commit run --all-files
