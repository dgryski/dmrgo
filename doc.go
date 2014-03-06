/*
Package dmrgo is a Go library for writing map/reduce jobs.

It can be used with Hadoop's streaming protocol, but also includes a standalone
map/reduce implementation (including partitioner) for 'small' jobs (~5G-10G).

It is partially based on ideas from Yelp's MrJob package for Python, but since
the Go is statically typed I've tried to make the API match more closely with
Hadoop's Java API.

The traditional "word count" example is in the examples directory.

This code is licensed under the GPLv3, or at your option any later version.

Further reading:

MrJob:
   http://packages.python.org/mrjob/
   https://github.com/Yelp/mrjob

Hadoop map/reduce tutorial:
   http://hadoop.apache.org/common/docs/current/mapred_tutorial.html

Hadoop streaming protocol:
   http://hadoop.apache.org/common/docs/current/streaming.html
*/
package dmrgo
