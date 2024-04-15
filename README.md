# Flink Job showing integration with OpenAI to process social media data

This is a simple Flink job that reads data from a Kafka topic `messages`, sends them to OpenAI's GPT-3 API to get analysis, and then writes the analysis next to the message to a corresponding Kafka topic.

## Prerequisites

* You need the Aiven `integration_id` to retrieve the corresponding credential file, instruction are [here](https://aiven.io/docs/products/flink/howto/manage-credentials-jars)
* You need an OpenAI API key, you can get one [here](https://platform.openai.com/)

## Building the jar

```bash
mvn clean package
```

## Parameters to pass when running the job on Aiven 

In the Create new deployment dialog look for the Program args field.

* `serviceCredentials` - The Aiven integration id
* `openAIKey` - The OpenAI API key


