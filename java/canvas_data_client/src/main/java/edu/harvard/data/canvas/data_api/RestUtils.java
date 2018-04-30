package edu.harvard.data.canvas.data_api;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class RestUtils {
  private static final Logger log = LogManager.getLogger();

  private final String host;
  private final String key;
  private final String secret;
  private final ObjectMapper mapper;
  private static final int THROTTLE_SEC = 12;
  private Throwable error;

  public RestUtils(final String host, final String key, final String secret) {
    this.host = host;
    this.key = key;
    this.secret = secret;
    this.mapper = new ObjectMapper();
  }

  public <T> T makeApiCall(final String resourcePath, final int expectedStatus, final JavaType type)
      throws DataConfigurationException, UnexpectedApiResponseException, IOException {
    int retry = 3;
   
    while (true) {

      try {
        log.info("Waiting for " + THROTTLE_SEC + " seconds");
        Thread.sleep(THROTTLE_SEC);
      } catch (final InterruptedException e) {
        error = e;
      }
    	    	
      final String url = "https://" + host + resourcePath;
      try {
        log.debug("Making Canvas API call to " + url);
        final String date = getDate();
        final String signature = generateSignature(resourcePath, date);
        final HttpGet get = new HttpGet(url);
        get.addHeader("Authorization", "HMACAuth " + key + ":" + signature);
        get.addHeader("Date", date);
        try (final CloseableHttpClient httpClient = HttpClients.createDefault();
            final CloseableHttpResponse response = httpClient.execute(get);) {
          final int status = response.getStatusLine().getStatusCode();
          if (status != expectedStatus) {
            log.warn("Unexpected REST API response: " + status);
            if (response.getEntity().getContentLength() > 0) {
              final StringBuffer sb = new StringBuffer();
              try (BufferedReader in = new BufferedReader(
                  new InputStreamReader(response.getEntity().getContent()))) {
                String line = in.readLine();
                while (line != null) {
                  sb.append(line);
                  sb.append('\n');
                  line = in.readLine();
                }
              }
              log.warn(sb.toString());
            }
            throw new UnexpectedApiResponseException(expectedStatus, status, url);
          }
          final String responseValue = EntityUtils.toString(response.getEntity());
          // System.out.println(responseValue);
          return mapper.readValue(responseValue, type);
        }
      } catch (final UnexpectedApiResponseException e) {
        if (retry-- > 0) {
          log.error("Retrying request to " + url);
        } else {
          throw e;
        }
      }
    }
  }

  private String getDate() {
    final TimeZone tz = TimeZone.getTimeZone("GMT");
    final DateFormat df = new SimpleDateFormat("E, dd MMM Y HH:mm:ss z");
    df.setTimeZone(tz);
    final Date now = new Date();
    return df.format(now);
  }

  private String generateSignature(final String resourcePath, final String date)
      throws DataConfigurationException {
    final String method = "GET";
    final String hostHeader = host;
    final String contentTypeHeader = "";
    final String md5Header = "";
    final String queryParams = "";

    final String message = method + "\n" + hostHeader + "\n" + contentTypeHeader + "\n" + md5Header
        + "\n" + resourcePath + "\n" + queryParams + "\n" + date + "\n" + secret;

    final Mac hmac;
    try {
      hmac = Mac.getInstance("HmacSHA256");
    } catch (final NoSuchAlgorithmException e) {
      throw new DataConfigurationException(e);
    }
    final SecretKeySpec secretKeySpec = new SecretKeySpec(secret.getBytes(), "HmacSHA256");
    try {
      hmac.init(secretKeySpec);
    } catch (final InvalidKeyException e) {
      throw new DataConfigurationException(e);
    }
    final byte[] digest = hmac.doFinal(message.getBytes());
    return Base64.encodeBase64String(digest);
  }

  public void downloadFile(final String url, final File dest, final int expectedStatus)
      throws IOException, UnexpectedApiResponseException {
    dest.getParentFile().mkdirs();
    final HttpGet get = new HttpGet(url);
    int retries = 10;
    while (true) {
      try (final CloseableHttpClient httpClient = HttpClients.createDefault();
          final CloseableHttpResponse response = httpClient.execute(get);
          final FileOutputStream out = new FileOutputStream(dest.toString());) {
        final HttpEntity entity = response.getEntity();
        final long contentLength = response.getEntity().getContentLength();
        if (entity != null) {
          entity.writeTo(out);
        }
        if (dest.length() != contentLength) {
          throw new IOException("Downloaded incomplete file. Expected " + contentLength
              + " bytes, got " + dest.length());
        }
        return;
      } catch (final java.io.IOException e) {
        if (--retries < 0) {
          throw e;
        }
      }
    }
  }

}
