
# Generic REST API Storage Plugin
This plugin is intended to enable you to query APIs over HTTP/REST. At this point, the API reader will only accept JSON as input however in the future, it may be possible to
 add additional format readers to allow for APIs which return XML, CSV or other formats.  
 
Note:  This plugin should **NOT** be used for interacting with tools which have REST APIs such as Splunk or Solr. It will not be performant for those use cases.  

## Configuration
To configure the plugin, create a new storage plugin, and add the following configuration options which apply to ALL connections defined in this plugin:

```
{
  "type": "http",
  "connection": "https://<your url here>/",
  "cacheResults": true,
  "enabled": true
}
```
The options are:
* `type`:  This should be `http`
* `cacheResults`:  Enable caching of the HTTP responses

### Configuring the API Connections
The HTTP Storage plugin allows you to configure multiple APIS which you can query directly from this plugin. To do so, first add a `connections` parameter to the configuration
. Next give the connection a name, which will be used in queries.  For instance `stockAPI` or `jira`.

The `connection` can accept the following options:
* `url`: The base URL which Drill will query. You should include the ending slash if there are additional arguments which you are passing.
* `method`: The request method. Must be `get` or `post`. Other methods are not allowed and will default to `GET`.  
* `headers`: Often APIs will require custom headers as part of the authentication. This field allows you to define key/value pairs which are submitted with the http request
.  The format is:
```
headers: {
   "key1":, "Value1",
   "key2", "Value2"
}

```
* `authType`: If your API requires authentication, specify the authentication type. At the time of implementation, the plugin only supports basic authentication, however, the
 plugin will likely support OAUTH2 in the future. Defaults to `none`. If the `authType` is set to `basic`, `username` and `password` must be set in the configuration as well. 
 * `username`: The username for basic authentication. 
 * `password`: The password for basic authentication.
 * `postBody`: Contains data, in the form of key value pairs, which are sent during a `POST` request.


### Examples:
The API sunrise-sunset.org returns data in the following format:

 ```
   {
         "results":
         {
           "sunrise":"7:27:02 AM",
           "sunset":"5:05:55 PM",
           "solar_noon":"12:16:28 PM",
           "day_length":"9:38:53",
           "civil_twilight_begin":"6:58:14 AM",
           "civil_twilight_end":"5:34:43 PM",
           "nautical_twilight_begin":"6:25:47 AM",
           "nautical_twilight_end":"6:07:10 PM",
           "astronomical_twilight_begin":"5:54:14 AM",
           "astronomical_twilight_end":"6:38:43 PM"
         },
          "status":"OK"
       }
   }
```
To query this API, set the configuration as follows:

```
{
  "type": "http",
  "connection": "https://api.sunrise-sunset.org/",
  "enabled": true
}
```
Then, to execute a query:

    SELECT api_results.results.sunrise AS sunrise, 
    api_results.results.sunset AS sunset
    FROM http.`/json?lat=36.7201600&lng=-4.4203400&date=today` AS api_results;

Which yields the following results:
```
+------------+------------+
|  sunrise   |   sunset   |
+------------+------------+
| 7:17:46 AM | 5:01:33 PM |
+------------+------------+
1 row selected (0.632 seconds)
```

### Known Issues
1.  The plugin is supposed to follow redirects, however if you are using Authentication, you may encounter errors or empty responses if you are counting on the endpoint for
 redirection. 



