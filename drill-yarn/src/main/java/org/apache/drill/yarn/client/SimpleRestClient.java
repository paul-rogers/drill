package org.apache.drill.yarn.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.DefaultHttpClient;

public class SimpleRestClient
{
  public String send( String baseUrl, String resource, boolean isPost ) throws ClientException {
    String url = baseUrl + "/" + resource;
    try {
      HttpClient client = new DefaultHttpClient();
      HttpRequestBase request;
      if ( isPost ) {
        request = new HttpPost( url );
      } else {
        request = new HttpGet( url );
      }
      
      HttpResponse response = client.execute(request);
      BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
      StringBuilder buf = new StringBuilder( );
      String line = null;
      while ((line = rd.readLine()) != null) {
        buf.append( line );
      }
      return buf.toString().trim( );
    } catch (ClientProtocolException e) {
      throw new ClientException( "Internal REST error", e );
    } catch (IllegalStateException e) {
      throw new ClientException( "Internal REST error", e );
    } catch (IOException e) {
      throw new ClientException( "REST request failed: " + url, e );
    }
  }
}
