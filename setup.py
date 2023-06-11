from setuptools import setup, find_packages

setup(
    name="spark-ai",
    version="1.0.0",
    url="https://github.com/prophecy-io/spark-ai",
    packages=find_packages(where="src"),
    package_dir={'spark_ai': 'src/spark_ai'},
    description="Helper library for prophecy generated code",
    long_description=open("README.md").read(),
    install_requires=["pyspark>=3.0.0", "slack-sdk==3.21.3", "openai==0.27.8", "numpy==1.24.3"],
    keywords=["python", "prophecy"],
    classifiers=[],
    zip_safe=False,
)
