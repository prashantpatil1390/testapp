package com.tutuka.pubsubclient;

import com.tutuka.redis.PubSubConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@Component
public class PubSubClient {

  private final static Logger LOG = LoggerFactory.getLogger(PubSubConsumer.class);

  private final String url;

  public PubSubClient(@Value("${publish_url}") String url) {
    this.url = url;
  }

  public String publish(String payload, String channel) {
    try {
      URL url = new URL(this.url + "/" + channel);
      HttpURLConnection connection = (HttpURLConnection)url.openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setRequestProperty("Accept", "application/json");
      connection.setDoOutput(true);
      LOG.debug("Sending payload to " + url);
      try(OutputStream os = connection.getOutputStream()) {
        byte[] input = payload.getBytes(StandardCharsets.UTF_8);
        os.write(input, 0, input.length);
      }

      try(BufferedReader br = new BufferedReader(
              new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
        StringBuilder response = new StringBuilder();
        String responseLine = null;
        while ((responseLine = br.readLine()) != null) {
          response.append(responseLine.trim());
        }
        return response.toString();
      }

    } catch (Exception e) {
      LOG.debug("Exception Thrown: " + e.toString());
      return "";
    }
  }
}
