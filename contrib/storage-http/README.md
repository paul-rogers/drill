
# Generic API Storage Plugin
This plugin is intended to enable you to query APIs over HTTP/REST.  


storage config:

    {
      "type": "http",
      "connection": "http://btrabbit.com:8000",
      "resultKey": "results",
      "enabled": true
    }

samples:

    select sunrise, sunset from api.`/json?lat=36.7201600&lng=-4.4203400&date=today`;
 
    
not support (still working on):

    select name from (select name, length from http.`/e/api:search` where $q='avi' and $p=2) where length > 0

