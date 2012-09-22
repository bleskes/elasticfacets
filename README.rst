= Elastic Facets =

A collection of facets (ehm, one at the moment) for ElasticSearch.

== Faceted Date Histogram ==

ElasticSearch comes with a powerfull built in facet called `Date Histogram <http://www.elasticsearch.org/guide/reference/api/search/facets/date-histogram-facet.html>`_. 
Using the date histogram facet you can get a statistical analysis of a field for different time intervals (week by week, hour by hour etc.).

::

   {
     "query": {
       "match_all": {}
     },
     "facets": {
       "pub_per_week": {
         "date_histogram": {
           "key_field": "published",
           "value_field": "copies",
           "interval": "week"
         }
       }
     }
   }

 
While this is very powerful, it is limitted to numerical fields.

The Faceted Date Histogram combines the power of Date Histogram with ***any*** facet in ElasticSearch. 
With it, you can replace the *value_field* parameter with a complete facet definition of your choice. For exmaple:

::

   {
     "query": {
       "match_all": {}
     },
     "facets": {
       "pub_per_week": {
         "faceted_date_histogram": {
           "field": "published",
           "interval": "week",
           "facet": {
             "terms": {
               "field": "username"
             }
           }
         }
       }
     }
   }

This snippet uses the Terms facet to return the top usernames on a week by week basis.

== Installation ==

In order to install the plugin, simply run: ``bin/plugin -install bleskes/elasticfacets`` 


 
