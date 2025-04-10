rm -rf build dist prophecy_spark_ai.egg-info
python3 setup.py bdist_wheel
python3 -m twine upload --repository pypi dist/*
