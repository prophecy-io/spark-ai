## Pipelines

### SageMakerEndpointTest

The **SageMakerEndpointTest** Spark pipeline is designed to test the functionality of a SageMaker endpoint. It takes an empty dataframe, converts text to vectors, and sends the data to the endpoint for prediction. The pipeline is useful for ensuring that the endpoint is working correctly and providing accurate predictions. Running the pipeline via Python in a Spark environment provides a streamlined and efficient way to test the endpoint.

### BedrockTest

The **BedrockTest** Spark pipeline is designed to test the functionality of various data processing functions, including empty dataframes, text-to-vector conversion, and embedding. Running the pipeline via Python in a Spark environment provides a comprehensive testing framework for data processing functions, ensuring the accuracy and reliability of data analysis.

## Gems

### SageMakerEndpoint

This code provides a framework for integrating Amazon SageMaker, a cloud machine learning service, with a data processing workflow. Here's a business-friendly description:

The code represents a component named "SageMakerEndpoint" that falls under the "Machine Learning" category. This component facilitates the interaction between data workflows and the SageMaker service by:

1. **Credential Management**: The component provides two ways for users to supply AWS credentials — either by fetching them securely from Databricks Secrets or by manually entering them.
2. **Operation Configuration**: Users can specify the type of machine learning operation they want to perform, with an option mainly focusing on answering questions based on a given context.
3. **Model Configuration**: Users can detail the specifics of the SageMaker model they want to use, such as the endpoint name, region, and some tuning parameters like the number of new tokens, the "Top P" value, and the model temperature.
4. **UI Interaction**: The component offers a dynamic user interface. Depending on choices made in the UI, different configuration fields are presented, allowing for seamless integration and model operation.
5. **Processing Data**: The final part of the code contains logic that takes data from a workflow, prepares it for the SageMaker service, and fetches results from the machine learning model. If using Databricks, the code securely retrieves AWS credentials. Then, it shapes the input data according to the desired machine learning task and sends it to the specified SageMaker endpoint.

Overall, this code bridges the gap between data workflows and machine learning in the AWS cloud, providing an intuitive interface for users and efficient data handling mechanisms.

### Bedrock

This code defines a component named "Bedrock" under the category "Machine Learning." This component is intended for use within a data workflow and focuses on computing text embeddings, a kind of transformation that converts textual data into numerical vectors.

Key features include:

1. **Configurable Credentials**: The component can authenticate using "Databricks Secrets" or hardcoded credentials. These authentication details primarily target AWS services.
2. **User Interface**: The dialog method establishes a user interface where users can specify the kind of credentials they want to use, choose the operation type (currently only "Compute text embeddings"), and set various other related properties like selecting an AWS region and specifying which column in their data contains the text to embed.
3. **Code Execution**: The main functionality, housed in the BedrockCode class, applies the embedding transformation on the input data. Depending on the authentication method chosen, it retrieves the necessary AWS credentials and leverages a library (BedrockLLM) to compute text embeddings for specified text columns.
4. **Validations and State Management**: The validate and onChange methods provide utilities for validation and managing state changes of the component, respectively.

In business terms, this component acts as a plug-and-play tool in a larger system, enabling users to seamlessly convert textual data into a machine-understandable format using a familiar UI while abstracting away the complexities of data transformation and AWS authentication.


*** Release notes for version: 0.1.9 ***

Adds the first version of Bedrock & SageMakerEndpoint gems 💎