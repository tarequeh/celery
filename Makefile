PEP8=pep8

pep8:
	(find . -name "*.py" | xargs pep8 | perl -nle'\
		print; $$a=1 if $$_}{exit($$a)')

cycomplex:
	find celery -type f -name "*.py" | xargs pygenie.py complexity

ghdocs:
	rm -rf docs/.build
	contrib/doc2ghpages

upload_github_docs: ghdocs

upload_pypi_docs:
	python setup.py build_sphinx && python setup.py upload_sphinx

upload_docs: upload_github_docs upload_pypi_docs

autodoc:
	contrib/doc4allmods celery

verifyindex:
	contrib/verify-reference-index.sh

flakes:
	find . -name "*.py" | xargs pyflakes

clean_readme:
	rm -f README.rst README

readme: clean_readme
	python contrib/sphinx-to-rst.py docs/templates/readme.txt > README.rst
	ln -s README.rst README

bump:
	contrib/bump -c celery

cover:
	(cd testproj; python manage.py test --coverage)

coverage: cover

quickcover:
	(cd testproj; env QUICKTEST=1 SKIP_RLIMITS=1 python manage.py test --coverage)

test:
	(cd testproj; python manage.py test)

quicktest:
	(cd testproj; SKIP_RLIMITS=1 python manage.py test)

testverbose:
	(cd testproj; python manage.py test --verbosity=2)

releaseok: pep8 autodoc verifyindex test gitclean

removepyc:
	find . -name "*.pyc" | xargs rm

release: releaseok ghdocs removepyc

gitclean: removepyc
	git clean -xdn

gitcleanforce: removepyc
	git clean -xdf

