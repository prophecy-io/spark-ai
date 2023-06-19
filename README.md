# Spark AI

A code-based framework for building Generative AI applications on top of Apache Spark.

Many developers are companies are trying to leverage LLMs to enhance their existing applications or build completely new 
ones. However, even though they don't have to any longer train new ML models, still the major challenge is data and 
infrastructure. This includes data ingestion, transformation, vectorization, lookup, and model serving.

Over the last few months, the industry has seen a spur of new tools and frameworks to help with these challenges. However, 
none of them are easy to use, deploy to production, nor can deal with the scale of data.

This project aims to provide a toolbox of Spark extensions, data sources, and utilities to make building robust 
data infrastructure on Spark for Generative AI applications.

## Installation

Currently, the project is aimed mainly at PySpark users, however, because it also features high-performance connectors, 
both the PySpark and Scala dependencies have to be present on the Spark cluster.

## Getting Started



## Examples

Complete examples that anyone can start from to build their own Generative AI applications.

- [Chatbot Template](https://github.com/prophecy-samples/gen-ai-chatbot-template)
- [Medical Advisor Template](https://github.com/prophecy-samples/gen-ai-med-avisor-template)

## Roadmap

Data sources supported:

- 🚧 Slack
- 🗺️ PDFs
- 🗺️ Asana
- 🗺️ Google Drive

LLMs supported:

- 🗺️ OpenAI
- 🗺️ Databrick's Dolly
- 🗺️ HuggingFace's Models

Application interfaces supported:  

- 🚧 Slack
- 🗺️ Microsoft Teams

✅: General Availability; 🚧: Beta availability; 🗺️: Roadmap; 