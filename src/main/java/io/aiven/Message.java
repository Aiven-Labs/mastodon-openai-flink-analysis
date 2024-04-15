package io.aiven;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class Message {

    private String content;

    private String intelligentOutput;


    public Message(String content, String intelligentOutput) {
        this.content = content;
        this.intelligentOutput = intelligentOutput;
    }

    public String transform() {
        ObjectMapper dataMapper = new ObjectMapper();
        System.out.println("this.intelligentOutput " + this.intelligentOutput);
        // Create an empty JSON object
        ObjectNode result = dataMapper.createObjectNode();

        // Add text properties to the JSON object
        result.put("content", this.content);
        String[] values =  this.intelligentOutput.split(";");
        if(values.length == 5) {
            result.put("sentiment", values[0]);
            result.put("language", values[1]);
            result.put("sarcasm", values[2]);
            result.put("category", values[3]);
            result.put("appropriate language", values[4]);
        }

        return result.toString();
    }
}
