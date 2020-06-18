.PHONY: clean wheel dist tests buildenv install

NO_COLOR = \x1b[0m
OK_COLOR = \x1b[32;01m
ERROR_COLOR = \x1b[31;01m

PYCACHE := $(shell find . -name '__pycache__')
EGGS :=  $(shell find . -name '*.egg-info')

clean:
	@echo "$(OK_COLOR)=> Cleaning$(NO_COLOR)"
	@echo "Current version: $(CURRENT_VERSION)"
	@rm -fr build dist


prepare: clean
	@echo "$(OK_COLOR)=> Preparing ...$(NO_COLOR)"
	git add .
	git status
	git commit -m "cleanup before release"


# Dist commands
# TODO - use conda in future rather than virtual env for better compatibility to ensure we can setup correct version of python

# wheel:

dist:
	@echo "$(OK_COLOR)=> building dist of databricks_sync$(NO_COLOR)"
	@test -d dist || mkdir ./dist
	@cp -f ./databricks_sync.py ./dist/databricks_sync

package:
	rm databricks_sync.tgz
	tar -czf databricks_sync.tgz databricks_sync.py makefile
	tar -tvf databricks_sync.tgz


install:  dist
	@echo "$(OK_COLOR)=> Installing databricks_sync to /usr/local/bin$(NO_COLOR)"
	@cp -f -i ./dist/databricks_sync /usr/local/bin/
	@chmod a+x /usr/local/bin/databricks_sync
