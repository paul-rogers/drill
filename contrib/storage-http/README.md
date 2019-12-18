
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
  "timeout": 0,
  "enabled": true
}
```
The options are:
* `type`:  This should be `http`
* `cacheResults`:  Enable caching of the HTTP responses
* `timeout`:  Sets the response timeout. Defaults to `0` which is no timeout. 

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
 * `postBody`: Contains data, in the form of key value pairs, which are sent during a `POST` request. Post body should be in the form:
 ```
key1=value1
key2=value2
```

## Examples:
### Example 1:  Reference Data, A Sunrise/Sunset API
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

### Example 2: JIRA
JIRA Cloud has a REST API which is [documented here](https://developer.atlassian.com/cloud/jira/platform/rest/v3/?utm_source=%2Fcloud%2Fjira%2Fplatform%2Frest%2F&utm_medium=302). 

To connect Drill to JIRA Cloud, use the following configuration:
```
{
  "type": "http",
  "cacheResults": false,
  "timeout" 5,
  "connections": {
    "sunrise": {
      "url": "https://api.sunrise-sunset.org/",
      "method": "get",
      "headers": null,
      "authType": "none",
      "userName": null,
      "password": null,
      "postBody": null
    },
    "jira": {
      "url": "https://<project>.atlassian.net/rest/api/3/",
      "method": "get",
      "headers": {
        "Accept": "application/json"
      },
      "authType": "basic",
      "userName": "<username>",
      "password": "<API Key>",
      "postBody": null
    }
  },
  "enabled": true
}
```

Once you've configured Drill to query the API, you can now easily access any of your data in JIRA. The JIRA API returns highly nested data, however with a little preparation, it
 is pretty straightforward to transform it into a more useful table. For instance, the
 query below:
```sql
SELECT jira_data.issues.key AS key, 
jira_data.issues.fields.issueType.name AS issueType,
SUBSTR(jira_data.issues.fields.created, 1, 10) AS created, 
SUBSTR(jira_data.issues.fields.updated, 1, 10) AS updated,
jira_data.issues.fields.assignee.displayName as assignee, 
jira_data.issues.fields.creator.displayName as creator,
jira_data.issues.fields.summary AS summary,
jira_data.issues.fields.status.name AS currentStatus,
jira_data.issues.fields.priority.name AS priority,
jira_data.issues.fields.labels AS labels,
jira_data.issues.fields.subtasks AS subtasks
FROM (
SELECT flatten(t1.issues) as issues 
FROM api.jira.`search?jql=project=GTKOPS&maxResults=100` AS t1
) AS jira_data
```
The query below counts the number of issues by priority:

```sql
SELECT 
jira_data.issues.fields.priority.name AS priority,
COUNT(*) AS issue_count
FROM (
SELECT flatten(t1.issues) as issues 
FROM api.jira.`search?jql=project=GTKOPS&maxResults=100` AS t1
) AS jira_data
GROUP BY priority
ORDER BY issue_count DESC
```

<img src="images/issue_count.png"  alt="Issue Count by Priority"/>


## Limitations
1.  The plugin is supposed to follow redirects, however if you are using Authentication, you may encounter errors or empty responses if you are counting on the endpoint for
 redirection. 
 
 2. At this time, the plugin does not support any authentication other than basic authentication. Future functionality may include OAUTH2 authentication and/or PKI
  authentication for REST APIs.
  
 3. This plugin does not implement filter pushdowns. Filter pushdown has the potential to improve performance.
 
 4. This plugin only reads JSON responses. Future functionality may include the ability to parse XML, CSV or other common rest responses.
  



