from setuptools import setup, find_packages

setup(
    name="spark-ai",
    version="0.1.0",
    url="https://github.com/prophecy-io/spark-ai",
    packages=find_packages(exclude=["tests","tests.*"]),
    package_dir={'spark_ai': 'spark_ai'},
    description="High-performance AI/ML library for Spark to build and deploy your LLM applications in production.",
    long_description=open("README.md").read(),
    install_requires=["pyspark>=3.0.0", "slack-sdk==3.21.3", "openai==0.27.8", "numpy==1.24.3"],
    keywords=["python", "prophecy"],
    classifiers=[],
    zip_safe=False,
)
