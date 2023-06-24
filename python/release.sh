rm -rf build dist prophecy_spark_ai.egg-info
python setup.py bdist_wheel
python -m twine upload --repository pypi dist/*
