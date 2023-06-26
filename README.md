# Spark AI

Toolbox for building Generative AI applications on top of Apache Spark.

Many developers are companies are trying to leverage LLMs to enhance their existing applications or build completely new
ones. Thanks to LLMs most of them no longer have to train new ML models. However, still the major challenge is data and
infrastructure. This includes data ingestion, transformation, vectorization, lookup, and model serving.

Over the last few months, the industry has seen a spur of new tools and frameworks to help with these challenges.
However, none of them are easy to use, deploy to production, nor can deal with the scale of data.

This project aims to provide a toolbox of Spark extensions, data sources, and utilities to make building robust
data infrastructure on Spark for Generative AI applications easy.

[![PyPI version](https://badge.fury.io/py/prophecy-spark-ai.svg)](https://badge.fury.io/py/prophecy-spark-ai) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.prophecy/spark-ai_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.prophecy/spark-ai_2.12)

## Example Applications

Complete examples that anyone can start from to build their own Generative AI applications.

- [Chatbot Template](https://github.com/prophecy-samples/gen-ai-chatbot-template)
- [Medical Advisor Template](https://github.com/prophecy-samples/gen-ai-med-avisor-template)

## Quickstart

### Installation

Currently, the project is aimed mainly at PySpark users, however, because it also features high-performance connectors,
both the PySpark and Scala dependencies have to be present on the Spark cluster.

### Ingestion

How to read all the conversations from Slack

```python
from spark_ai.webapps.slack import SlackUtilities

slack = SlackUtilities(token='xoxb-...', spark=spark)
df_channels = slack.read_channels()
df_conversations = slack.read_conversations(df_channels)
```

### Pre-processing & Vectorization

```python
from spark_ai.llms.openai import OpenAiLLM
from spark_ai.dbs.pinecone import PineconeDB

OpenAiLLM(api_key='sk-...').register_udfs(spark=spark)
PineconeDB('8045...', 'us-east-1-aws').register_udfs(self.spark)

(df_conversations
    # Embed the text from every conversation into a vector
    .withColumn('embeddings', expr('openai_embed_texts(text)'))
    # Do some more pre-processing
    ... 
    # Upsert the embeddings into Pinecone
    .withColumn('status', expr('pinecone_upsert(\'index-name\', embeddings)'))
    # Save the status of the upsertion to a standard table
    .saveAsTable('pinecone_status'))
```

### Inference 

```python
df_messages = spark.readStream \
    .format("io_prophecy.spark_ai.SlackStreamingSourceProvider") \
    .option("token", token) \
    .load()

# Handle a live stream of messages from Slack here
```

## Roadmap

Data sources supported:

- 🚧 Slack
- 🗺️ PDFs
- 🗺️ Asana
- 🗺️ Notion
- 🗺️ Google Drive
- 🗺 Web-scrape

Vector databases supported:

- 🚧 Pinecone
- 🚧 Spark-ML (table store & cos sim)
- 🗺 ElasticSearch

LLMs supported:

- 🚧 OpenAI
- 🚧 Spark-ML
- 🗺️ Databrick's Dolly
- 🗺️ HuggingFace's Models

Application interfaces supported:

- 🚧 Slack
- 🗺️ Microsoft Teams

And many more are coming soon (feel free to request as issues)! 🚀

✅: General Availability; 🚧: Beta availability; 🗺️: Roadmap; 
