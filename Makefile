
test: install
	@pyenv/bin/nosetests --with-coverage --cover-package=datamapper --cover-erase

install: pyenv/bin/python

pyenv/bin/python:
	virtualenv pyenv
	pyenv/bin/pip install --upgrade pip
	pyenv/bin/pip install wheel nose coverage unicodecsv dataset
	pyenv/bin/python setup.py develop

upload: clean install
	pyenv/bin/python setup.py sdist bdist_wheel upload

clean:
	rm -rf pyenv

testdata:
	curl -o tests/fixtures/companies.csv https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv
	curl -o tests/fixtures/financials.csv https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents-financials.csv
