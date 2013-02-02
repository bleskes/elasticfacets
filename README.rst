============== 
Elastic Facets 
==============

A collection of facets_ and facet-related tools_ for ElasticSearch.

.. image:: https://travis-ci.org/bleskes/elasticfacets.png
   :alt: build status
   :align: right


Installation
============

WARNING/UPDATE: this installation methods fails since github disabled their download feature (http://www.elasticsearch.org/blog/2012/12/17/new-download-service.html ) . I'm working on an alternative.

In order to install the plugin, simply run: ``bin/plugin -install bleskes/elasticfacets/0.2``. Change the version number if needed (see Versions_).

Versions
========

    =============   =============
    ElasticFacets   ElasticSearch     
    =============   =============
    master          0.20.0 -> 0.20.2
    0.2             0.19.9 -> 0.19.11 
    0.1             0.19.8
    =============   =============            
    
.. _facets:  

Included facets
===============

Faceted Date Histogram
----------------------

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

Hashed Strings Facet
--------------------

** STILL UNDER DEVELOPMENT. Available in master only **

A drop in replacement [#]_ to standrand terms facet but with lower memory signature. Usefull when you 
want to facet on a string field with many many possible string values. Normally, all these strings will be loaded into memory which take a lot.
The Hashed Strings Facet only loads the hashes of these strings which considerably reduce its memory signatures. The facet onload loads the strings
needed to actually return a response. Of course, nothing comes for free. The price you is slightly more IO and potentially inacuracies due to hash collisions.
If the latter occur, you would see an appropriate WARN log message.

After installing the plugin you can call it as follows:

::

   {
     "query": {
       "match_all": {}
     },
     "facets": {
       "facet_name": {
         "hashed_terms": {
           "field": "username",
           "size": 10,
         }
       }
     }
   }


This will return the top 10 usernames in your index in exactly the same format the standard terms facet does.

.. [#] As the original string is no longer available at faceting time, these features of the standard term facets are not supported:

   * Regex filtering.
   * Term Scripts (although there is some control on output - see the extensions section).
   * Script Field
   * Term Ordering - alphabetically ordering terms is impossible. 
      

Extensions to the standard terms facet
``````````````````````````````````````

Next to the features offered by the terms facet, the Hashed Strings facet has some extra tricks to it:

::

   {
     "query": {
       "match_all": {}
     },
     "facets": {
       "facet_name": {
         "hashed_terms": {
           "field": "username",
           "size": 10,
           "fetch_size": 20,           # control over the number of terms returned by every shard before aggregation. 
           "output_script":            # Modify what is outputed via a script.
               "_source.username+' on '+_source.website" 
         }
       }
     }
   }


.. _tools:

Other Goodies
=============

Cache stats per field
---------------------

** STILL UNDER DEVELOPMENT. Available in master only **

Facets in ElasticSearch are powered by the FieldCache - a component that loads values into memory so they could be counted.
This can potentially lead to high memory usage. ElasticSearch comes with a cache statistics end point from which you can
get the current ***total*** cache size. This end points tells what is the cache size per field stored in it so you can find
the source of the problem. 

Usage:

::

  curl -XGET 'http://localhost:9200/_cluster/nodes/cache/fields/stats
  curl -XGET 'http://localhost:9200/_cluster/nodes/nodeId1,nodeId2/cache/fields/stats'

  # simplified
  curl -XGET 'http://localhost:9200/_nodes/cache/fields/stats'


Respones:

::

  {
  "cluster_name": "BoazMBP.local_buzzcapture_1.0"
    "nodes": {
        "node_id": {
          "timestamp": 1353134666971
          "name": "Frost, Deacon"
          "transport_address": "inet[/192.168.1.107:9300]"
          "hostname": "something.com"
          "fields": {
            "publish_date": {
              "size": 180
            }
            "copies": {
              "size": 180
            }
          }
        }
     }
  }


 
