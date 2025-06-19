# OpenRetriever

Goals: to be a datastore built on top of local fs and/or s3, supporting fast querying of traces/spans.

## Design

We are guaranteed to have:
* traceid
* spanid
* timestamp

Most queries will be:

* time range + operations
* time range + filters + operations
* traceid
* spanid



File based storage

* file per attribute
  * record per span with attribute
* a `record` has an id (maybe `spanid`? but this is not sortable) and a `value`
  * perhaps an `int` or similar, and a lookup to turn a `spanid` to an `index`
* File format plan or binary?
  * perhaps plain to start with
* do we care about datatypes?
  * otel attributes come with datatypes
  * filename `{attribute.path},{datatype},{format}`

Queries

* `distinct_count() where http.status_code == 200 since 15:00`
  * open timespans file
  * iterate to `range start`
  * for each record until `range end`
    * open `http.status_code,int,plain`
      * iterate records until `index`
        * if `value == 200`
          * add `index`, `value`, `timestamp` to result set

* `select trace where traceid == "aabbccdd"`
  * maybe a single file for a single trace too?


Layout

```
{dataset}
  times/{yyyy}/{mm}/{dd}/{hh}/{mm}/
    01
    02
    24
    57
  traces/{0-7}/{8-15}/{16-23}/
    {full trace id},trace,proto
  attributes/
    resource.id,string,plain
    resource.name,string,plain
    span.id,string,plain
    span.http.status_code,int,plain
    span.customer.id,uuid,plain
    span.user.name,string,plain
```


Querying

timerange => indexes
  * attribute file(indexes)



traceid  = open trace file


## S3 Layout

```
{dataset}
  attributes/
    {attribute},{type}/
      {spanid},plain
  traces/
    {traceid}/
      {spanid}
  spans/
    {spanid}
  times/{epoch}/
    {spanid}
```

## S3 Querying

* get a trace `aaaa-bbbb`:
  * list `{dataset}/traces/aaaa-bbbb`
  * read each file
  * combine
* all traces in range `123` to `156`
  * convert to epochs
  * find common prefix
  * list `{dataset}/times/{commonprefix}*`
* but with a filter `http.status >= 200`
  * list `{dataset}/attributes/http.status,int`
  * exclude files not in traceid list
  * open each file, read value
  * compute result
* but with filter `{span.name="GET" && span.http.path="/"}`
  * list `{dataset}/attributes/span.name`
    * exclude files not in traceid list
  * list `{dataset}/attributes/span.http.path`
    * exclude files not in traceid list
  * combine lists, AND
    

## ingestion

* figure out the dataset
  * either an api key/url per dataset
  * use `resource.name` - but querying would need to work across datasets, as a trace can be made up of multiple resources.
* increment the `dataset` `index` value
  * requires either a single retriever per dataset
  * or locking of some form
  * not that we can append to the files really anyway?
    * or did that new s3 feature mean we can do conditional writes now?
* write to the `traces` file
* flatten all attributes to `span` and `resource` scope
* write to all the span files
