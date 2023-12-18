package com.example.demo;

import java.io.IOException;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.Scanner;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@SpringBootApplication
public class UseCase1Application {
	
	@Value("${mqtt.broker}")
	private static String broker = "tcp://localhost:1883";
	
	@Value("${mqtt.topic}")
	private static String topic = "myTopic";

	public static void main(String[] args) {
		SpringApplication.run(UseCase1Application.class, args);
		
//		String jsonFilePath = "json_message_format_UC1.json";
//		String jsonContent = loadJsonFromFile(jsonFilePath);
		
//		String broker = "tcp://localhost:1883";
//		String topic = "myTopic";
		
//		int count = 0;
		
		try {
			MqttClient mqttClient = new MqttClient(broker, MqttClient.generateClientId());
			mqttClient.connect();
			
			
			String inputFilePath = "C:/Users/baham/eclipse-workspace/UseCase1/src/main/resources/json_message_format_UC1_test.json";
//			String outputFilePath = "output_json.json";
//			
//			
//			//Reading and changing the existing json file
//			JsonNode jsonNode = readJsonFromFile(inputFilePath);
//			String propertyValue = getFieldValue(jsonNode, "deviceId");
//			System.out.println("ONO EST' " + propertyValue);
//			
//			modifyJson(jsonNode);			
//			writeJsonToFile(jsonNode, outputFilePath);
//					
//			//Creating a new json file
//			ObjectNode jsonObject = createJsonObject();			
//			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
//			String timeStamp = dateFormat.toString();
//			String outputFieName = "newly_output_" + timeStamp + ".json";			
//			writeJsonToFile(jsonObject, outputFilePath);
//			System.out.println("Json file created succesfully!");
//			System.out.println("Json file processing completed succesfully!");
			

			
			//Reading from json file
			
			ArrayNode outerArray = readJsonArrayFromFile(inputFilePath);
			for(JsonNode outerObject: outerArray) {
				
				String deviceId = outerObject.get("deviceId").asText();
				String timeStamp = outerObject.get("timestamp").asText();
				
				System.out.println("Device ID: " + deviceId);
				System.out.println("Timestamp: " + timeStamp);
				
				

					ArrayNode innerArray = (ArrayNode) outerObject.get("measurementsIngestedLast_24h");
					
					for(JsonNode measurementIngestedLast_24h: innerArray) {
						
						String timestampCreated = measurementIngestedLast_24h.get("timestampCreated").asText();
						String intervalStart = measurementIngestedLast_24h.get("intervalStart").asText();
						String intervalEnd = measurementIngestedLast_24h.get("intervalEnd").asText();
						
						
						
//						System.out.println(" " + timestampCreated);
//						System.out.println("Device Id " + deviceId);
//						System.out.println("Timestamp " + timeStamp);
//						System.out.println("IntervalStart: " + intervalStart);
//						System.out.println("IntervalEnd: " + intervalEnd);
//						System.out.println();
						
						DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssZ");
						LocalDateTime intervalStartDateTime = LocalDateTime.parse(intervalStart, formatter);
						LocalDateTime intervalEndDateTime = LocalDateTime.parse(intervalEnd, formatter);
						LocalDateTime formattedTimestampCreated = LocalDateTime.parse(timestampCreated, formatter);
//						
//						System.out.println(formattedTimestampCreated);
//						int forTimestampCreated = formattedTimestampCreated
						
//						System.out.println(intervalStartDateTime + "   " + intervalEndDateTime);
//						System.out.println();
						
						Duration duration = Duration.between(intervalStartDateTime, intervalEndDateTime);
						long minutes = duration.toMinutes();
//						System.out.println(minutes);
//						System.out.println();
						
						if(minutes < 120 || minutes > 120) {
//							count++;
//							System.out.println("Device Id " + deviceId);
							LocalDateTime currentTime = LocalDateTime.now();
							DateTimeFormatter realTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
							String formattedRealTime = currentTime.format(realTimeFormatter);
							
							
							
							String jsonMessage = String.format("{ "
									+ "\n   \"customerInstallation\": \"configuration_parameter_S&R\", "
									+ "\n   \"deviceId\": " + deviceId + ","
									+ "\n   \"topologyId\": \"top_1\","
									+ "\n   \"timestampAlarmCreation\": " + formattedRealTime + ","
									+ "\n   \"alarmType\": \"UC1\" "
									+ "\n   \"payload\": { \n      \"message\": \"Missing ingestion data in range\","
									+ "\n      \"range\": ["
									+ "\n         \"timestamp_interval_end_prev\": " + intervalStart + ","
									+ "\n         \"timestamp_interval_end_next\": " + intervalEnd + ","
									+ "\n       ]"
									+ "\n      \"meaurement_granularity\": 10m|15m"
									+ "\n    } \n}");

							MqttMessage mqttMessage = new MqttMessage(jsonMessage.getBytes());
							mqttClient.publish(topic, mqttMessage);
						}
					}
					
					System.out.println();

			}
		}catch(IOException ex) {
			ex.printStackTrace();
		} catch (MqttException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private static ArrayNode readJsonArrayFromFile(String filePath) throws IOException {
		ObjectMapper objectMapper = new ObjectMapper();
		return (ArrayNode) objectMapper.readTree(new File(filePath));
	}
	
//	//Reading from json file 
//	private static JsonNode readJsonFromFile(String filePath) throws IOException{
//		ObjectMapper objectMapper = new ObjectMapper();
//		return objectMapper.readTree(new File(filePath));
//	}
//	
//	
//	//Changing json file
//	private static void modifyJson(JsonNode jsonNode) {
//		if(jsonNode.has("deviceId")) {
//			ObjectNode objectNode = (ObjectNode) jsonNode;
//			objectNode.put("deviceId", "dalbaeb");
//		}else {
//			System.out.println("ТЫ что ебанутый ? ");
//		}
//	}
//	
//	
//	//Writing into json file
//	private static void writeJsonToFile(JsonNode jsonNode, String filePath) throws IOException{
//		ObjectMapper objectMapper = new ObjectMapper();
//		objectMapper.writeValue(new File(filePath), jsonNode);
//	}
//	
//	
//	//Taking value from some field
//	private static String getFieldValue(JsonNode jsonNode, String fieldName) {
//		if(jsonNode.has(fieldName)) {
//			String readingFieldName = jsonNode.get(fieldName).asText();
//			return readingFieldName;
//		}else {
//			System.out.println("a нихуя здесь нет");
//			return null;
//		}
//	}
//	
//	
//	
//	//Creating new json file method
//	private static ObjectNode createJsonObject() {
//		
//		ObjectMapper objectMapper = new ObjectMapper();
//        ObjectNode jsonObject = objectMapper.createObjectNode();
//
//        // Add fields to the JSON object
//        jsonObject.put("name", "John Doe");
//        jsonObject.put("age", 30);
//        jsonObject.put("city", "Example City");
//
//        return jsonObject;
//	}
//	
//	private static String loadJsonFromFile(String jsonFilePath) {
//		try (Scanner scanner = new Scanner(new ClassPathResource(jsonFilePath).getInputStream(),
//				StandardCharsets.UTF_8.name())){
//			return scanner.useDelimiter("\\A").next();
//		} catch (Exception ex) {
//			throw new RuntimeException("Error Reading JSON File", ex);
//		}
//	}

}
