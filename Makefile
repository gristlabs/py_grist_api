dist:
	@echo Build python distribution
	python setup.py sdist bdist_wheel

publish:
	@echo "Publish to PyPI at https://pypi.python.org/pypi/grist_api/"
	@echo "Version in setup.py is `python setup.py --version`"
	@echo "Git tag is `git describe --tags`"
	@echo "Run this manually: twine upload dist/grist_api-`python setup.py --version`*"

docs:
	@echo "Build documentation in docs/build/html using virtualenv in ./env (override with \$$ENV)"
	$${ENV:-env}/bin/sphinx-build -b html docs/source/ docs/build/html

clean:
	python setup.py clean

.PHONY: dist publish docs clean
