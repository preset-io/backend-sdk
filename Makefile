pyenv: .python-version

.python-version: setup.cfg
	if [ -z "`pyenv virtualenvs | grep preset-cli`" ]; then\
	    pyenv virtualenv preset-cli;\
	fi
	if [ ! -f .python-version ]; then\
	    pyenv local preset-cli;\
	fi
	pip install -e '.[testing]'
	touch .python-version

test: pyenv
	pytest --cov=src/preset_cli -vv tests/ --doctest-modules src/preset_cli

clean:
	pyenv virtualenv-delete preset-cli

spellcheck:
	codespell -S "*.json" src/preset_cli docs/*rst tests templates

requirements.txt: .python-version
	pip install --upgrade pip
	pip-compile --no-annotate
