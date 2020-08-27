 
{% extends 'README_DEFAULT.md' %} 

{% block specificdescription %}
Generalized query execution transform. It should

* Support query engines (MySQL, Hive, Presto) 
* Support templatized execution 
* Support arbitrary number of queries 

{% endblock %}

{% block specificconfiguration %} 
Configuration looks like::
  
    "args": {
	
	   "executors": {
	        "hive": {
			    "handler": "call_hive",
                "params": {
				     "cred": "hivecred",
				}
			}
	   },
	   'specs': [
	       {
		      "name": "inventory",
			  "executor": "hive",
			  "generator": "daily_generator",
			  "params": {
	              "path": "SQLs/inventory.sql",
                  "min_orders": 4
               }
		   }
	   ]
    }
{% endblock %} 

{% block specificoutputs %} 
Any extra information about outputs of this module
{% endblock  %} 

{% block specificdependencies %} 
Any information on dependencies 
{% endblock  %} 
 
