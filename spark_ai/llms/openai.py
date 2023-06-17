from typing import List

import openai
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, FloatType, StringType


class OpenAiLLM:
    default_embedding_model = 'text-embedding-ada-002'
    default_chat_model = 'gpt-3.5-turbo'
    default_prompt_qa = """Answer the question based on the context below.

Context:
```
{context}
```

Question: 
```
{query}
```

Answer:
    """

    def __init__(self, api_key: str):
        self.api_key = api_key

    def register_openai(self):
        openai.api_key = self.api_key

    def register_udfs(self, spark: SparkSession):
        spark.udf.register('openai_embed_texts', self.embed_texts, returnType=self.embed_texts_type())

        spark.udf.register('openai_chat_complete', self.chat_complete, returnType=self.chat_complete_type())
        spark.udf.register('openai_answer_question', self.answer_question, returnType=self.chat_complete_type())

    def embed_texts(self, texts: List[str], model: str = default_embedding_model):
        self.register_openai()

        try:
            response = openai.Embedding.create(
                model=model,
                input=texts
            )

            return {'embeddings': [embedding['embedding'] for embedding in response['data']], 'error': None}
        except Exception as error:
            return {'embeddings': None, 'error': str(error)}

    @staticmethod
    def embed_texts_type():
        return StructType([
            StructField('embeddings', ArrayType(ArrayType(FloatType()))),
            StructField('error', StringType())
        ])

    def chat_complete(self, prompt: str, model: str = default_chat_model):
        self.register_openai()

        try:
            response = openai.ChatCompletion.create(
                model=model,
                messages=[self.chat_as_user(prompt)]
            )

            return {'choices': [choice['message']['content'] for choice in response['choices']], 'error': None}
        except Exception as error:
            return {'choices': None, 'error': str(error)}

    @staticmethod
    def chat_complete_type() -> StructType:
        return StructType([
            StructField('choices', ArrayType(StringType())),
            StructField('error', StringType())
        ])

    @staticmethod
    def chat_as_user(query: str):
        return {'role': 'user', 'content': query}

    @staticmethod
    def chat_as_assistant(query: str):
        return {'role': 'user', 'content': query}

    def answer_question(self, context: str, query: str, template: str = default_prompt_qa,
                        model: str = default_chat_model):
        return self.chat_complete(template.format(context=context, query=query), model=model)
