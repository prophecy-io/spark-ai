from setuptools import setup, find_packages

setup(
    name="prophecy-spark-ai",
    version="0.1.9",
    url="https://github.com/prophecy-io/spark-ai",
    packages=find_packages(exclude=["tests", "tests.*"]),
    package_dir={'spark_ai': 'spark_ai'},
    description="High-performance AI/ML library for Spark to build and deploy your LLM applications in production.",
    long_description_content_type="text/markdown",
    long_description=open("../README.md").read(),
    install_requires=[
        "slack-sdk>=3.21.3",
        "openai[datalib]>=0.27.8",
        "pinecone-client>=2.2.2",
        "python-dotenv>=1.0.0",
        "requests>=2.31.0",
        "beautifulsoup4>=4.12.2",
        "unstructured>=0.7.4",
        "numpy>=1.24.3"
    ],
    keywords=["python", "prophecy"],
    classifiers=[],
    zip_safe=False,
)
