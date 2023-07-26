To create new tests:
1. Add test file to travis-tests directory
2. In .travis.yml, under "script:", add the shell command to execute your test. Example formats:

script:
  - python3 --version
  - python3 travis-tests/test1.py

If any python packages are needed, you can add them under the "install" key in .travis.yml. Example formats:

install:
  - pip install pylama
  - pip install pylint

To do linting (Check coding standards), use any testing shell command from the "script" key. Example formats:

script:
  - pylama
  - pytest --pylint test1.py

These are all python tests, different languages & their setup methods are available on Travis CI's docs