/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Skeleton code for the datastream walkthrough
 */
public class AiJob {
	public static void main(String[] args) throws Exception {
		ParameterTool parameters = ParameterTool.fromArgs(args);
		// integration key between Aiven for Apache Flink and Aiven for Apache Kafka
		String serviceCredentials = parameters.getRequired("serviceCredentials");
		// openAIKey
		String openAIKey = parameters.getRequired("openAIKey");

		// Set up the stream execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// access credentials file containing Apache Kafka bootstrap and security info
		String credentialsFilePath = System.getenv("AVN_CREDENTIALS_DIR") + "/" + serviceCredentials +".json";

		ObjectMapper objectMapper = new ObjectMapper();
		try (FileReader reader = new FileReader(credentialsFilePath)) {
			JsonNode rootNode = objectMapper.readTree(new File(credentialsFilePath));

			String bootstrapServers = rootNode.get("bootstrap_servers").asText();
			String securityProtocol = rootNode.get("security_protocol").asText();
			// Configure Kafka source with the extracted details
			KafkaSource<String> source = KafkaSource.<String>builder()
					.setProperty("bootstrap.servers", bootstrapServers)
					.setProperty("security.protocol", securityProtocol)
					.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
					.setTopics("messages")
					.setGroupId("flink-consumer-group")
					.build();
			// Configure Kafka sink to processed-messages topic
			KafkaSink<String> porocessedSink = KafkaSink.<String>builder()
					.setProperty("bootstrap.servers", bootstrapServers)
					.setProperty("security.protocol", securityProtocol)
					.setRecordSerializer(KafkaRecordSerializationSchema.builder()
							.setTopic("processed-messages")
							.setValueSerializationSchema(new SimpleStringSchema())
							.build()
					)
					.build();
			// Create Kafka Stream as a source
			DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks() ,"Kafka Source");

			// Create a processing stream
			// call AsyncHttpRequestFunction to process messages
			DataStream<Message> resultDataStream = AsyncDataStream.unorderedWait(kafkaStream, new AsyncHttpRequestFunction(openAIKey), 10000, TimeUnit.MILLISECONDS, 100);

			// For every processed message call transform function and send data to  Kafka sink topic
			resultDataStream
					.map(Message::transform)
					.sinkTo(porocessedSink);

			// Execute the Flink job
			env.execute("Kafka to Kafka Job");
		} catch (Exception e) {
			System.out.println("Error reading credentials file");
			e.printStackTrace();
		}
	}

	// AsyncFunction to make HTTP requests
	public static class AsyncHttpRequestFunction implements AsyncFunction<String, Message> {

		ObjectMapper objectMapper = new ObjectMapper();
		private String openAIKey;

		//constructor
		public AsyncHttpRequestFunction(String openAIKey) {
			this.openAIKey = openAIKey;
		}

		@Override
		public void asyncInvoke(String input, ResultFuture<Message> resultFuture) {

			// OpenAI API endpoint for text completion
			String apiUrl = "https://api.openai.com/v1/chat/completions";

			// OpenAI API key
			String apiKey = openAIKey;
			try {
				// Create a URL object
				URL url = new URL(apiUrl);

				// Open a connection to the URL
				HttpURLConnection connection = (HttpURLConnection) url.openConnection();
				connection.setRequestMethod("POST");
				connection.setRequestProperty("Authorization", "Bearer " + apiKey);
				connection.setRequestProperty("Content-Type", "application/json");
				connection.setDoOutput(true);

				String payloadString = generatePrompt(input);

				// Write the request payload to the connection
				try (DataOutputStream outputStream = new DataOutputStream(connection.getOutputStream())) {
					outputStream.writeBytes(payloadString);
					outputStream.flush();
				} catch (Exception e) {
					System.out.println("error: " + e.getMessage() );
				}

				// Build the response
				StringBuilder response = new StringBuilder();
				try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
					String inputLine;
					while ((inputLine = in.readLine()) != null) {
						response.append(inputLine);
					}
				}

				// Disconnect the connection
				connection.disconnect();
				JsonNode rootNode = objectMapper.readTree(response.toString());
				// Process response from OpenAI assuming existing response body
				String responseText = rootNode.get("choices").get(0).get("message").get("content").asText();
				System.out.println("responseText " + responseText);
				// Create a new message object based on response data
				Message message = new Message(input, responseText);
				resultFuture.complete(Collections.singleton(message));
			} catch (Exception e) {
				// Handle exceptions
				System.out.println("error" + e.getMessage());
			}
		}

		private static String generatePrompt(String input) throws JsonProcessingException {
			// In detail explain to LLM what we want to get
			String userMessage = "Your task is to process a social media message delimited by --- ." +
					"           Process the message and give me this information divided by symbol ; " +
					"			First is sentiment analysis to determine if the message is positive or negative." +
					"           Second, identify the language of the message: EN, RU, FR, etc" +
					"			Third, is there sarcasm in the message: has sarcasm, no sarcasm, maybe has sarcasm" +
					"			Fourth, closest category of the message: News, Politics, Entertainment, Sports, Technology, Health and Wellness, Pop Culture, Travel, Fashion and Beauty, Food, Social Issues" +
					"			Fifth, if it contains any offensive, rude, inappropriate language, or sex language: inappropriate, appropriate" +
					"            For example:" +
					"            - It is amazing my favourite sport team has won!, you should return POSITIVE;EN;no sarcasm;Sports;appropriate" +
					"            - Everyone is stupid!, you should return NEGATIVE;EN;no sarcasm;Social Issues;appropriate" +
					"             Answer with only a set of words, no additional description" +
					"            ---" +
					input +
					"            ---";

			// Construct the JSON object using Jackson
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode payload = mapper.createObjectNode();
			payload.put("model", "gpt-3.5-turbo");

			ArrayNode messagesArray = mapper.createArrayNode();

			ObjectNode systemMessage = mapper.createObjectNode();
			systemMessage.put("role", "system");
			systemMessage.put("content", "You are an AI processing social media content. You need to process the messages.");
			messagesArray.add(systemMessage);

			ObjectNode userMessageObject = mapper.createObjectNode();
			userMessageObject.put("role", "user");
			userMessageObject.put("content", userMessage);
			messagesArray.add(userMessageObject);

			payload.set("messages", messagesArray);
			System.out.println("payload " + payload);

			return mapper.writeValueAsString(payload);
		}
	}

}

