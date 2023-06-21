rm -rf build dist prophecy_spark_ai.egg-info
python3.10 setup.py bdist_wheel
python3.10 -m twine upload --repository testpypi dist/*
