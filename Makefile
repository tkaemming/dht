install-dev:
	pip install -qr requirements.development.txt

check: install-dev
	pyflakes ./

test: install-dev
	nosetests --verbose

.PHONY: check install-dev test
