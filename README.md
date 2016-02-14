# Scalastic

Scalastic is a library that organizes your Elasticsearch documents into partitions, as recommended [here](https://www.elastic.co/guide/en/elasticsearch/guide/current/faking-it.html). A partition provides methods for indexing documents (adding or updating), searching for them, and deleting them. Partitions are isolated: documents stored in one partition are not visible in other partitions.

Initially your partition lives in a single index; however, as it grows, you may extend it to another index, redirecting all new documents there and keeping all documents from the original index available for searches. Partitions use Elasticsearch's filtered aliases; a partition with id 1 will create alias "scalastic_1_index" for indexing, and "scalastic_1_search" for searching. The index alias always points to a single index; the search one may span across multiple indices, with the most recent ones at the top. 

Scalastic relies on the field "scalastic_partition_id" of type "long" to determine partition the document belongs to. The search alias for a partition has a term filter that returns only document belonging to that partition. The index alias, however, does not have a filter; if you're inserting documents into that alias directly (and not using Scalastic API), it becomes your responsibility to set this field to the correct value (which is partition id) for your document to show up in the partition. Note that delete API uses search alias to locate and delete documents to allow deletion of documents created in older indices.

## Configuring the environment for Scalastic
Every new index must be prepared before it can be used with scalastic:

```ruby
es_client = Elasticsearch::Client.new
es_client.indices.create index: 'my_index'
es_client.partitions.prepare index: 'my_index'
```

Preparing an index creates mappings for fields required by Scalastic. Note that preparing uses the _default_ mapping, which means that already existing document types will not be affected. Because of that **you should always prepare new indices before using them**.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'scalastic'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install scalastic

## Usage

Scalastic extends functionality of the Elasticsearch client by adding a property called "partitions" to the Elasticsearch client. That property serves as a gateway to all partitions' functionality.

### Creating a partition
```ruby
# Connect to Elasticsearch and get the partitions client
es_client = Elasticsearch::Client.new
partitions = es_client.partitions

# Create an index for the test.
es_client.indices.create index: 'create_partition_test'
partitions.prepare_index index: 'create_partition_test'   # Must be called once per each index

# Create a partition
partition = partitions.create index: 'create_partition_test', id: 1
raise 'Partition was not created' unless partition.exists?
```

### Listing all existing partitions
```ruby
# Set everything up
client = Elasticsearch::Client.new
client.indices.create index: 'list_partitions_test'
partitions = client.partitions
partitions.prepare_index index: 'list_partitions_test'    # Must be called once per each index

# Create a couple of partitions
partitions.create index: 'list_partitions_test', id: 1
partitions.create index: 'list_partitions_test', id: 2
partitions.create index: 'list_partitions_test', id: 3

# List all partitions
partitions.to_a.each{|p| puts "Found partition #{p.id}"}
```

### Deleting a partition
```ruby
# Connect to Elasticsearch and create an index
client = Elasticsearch::Client.new
partitions = client.partitions
client.indices.create index: 'delete_partition_test'
partitions.prepare_index index: 'delete_partition_test'

# Create partitions
partitions.create index: 'delete_partition_test', id: 1
partitions.create index: 'delete_partition_test', id: 2
partitions.create index: 'delete_partition_test', id: 3

# Delete one of the partitions
partitions.delete id: 2
raise "Partition still exists" if partitions[2].exists?
```

### Extending partition to another index
```ruby
# Connect to Elasticsearch and set up indices
client = Elasticsearch::Client.new
partitions = client.partitions
client.indices.create index: 'extend_partition_1'
partitions.prepare_index index: 'extend_partition_1'
client.indices.create index: 'extend_partition_2'
partitions.prepare_index index: 'extend_partition_2'

# Create a partition residing in extend_partition_1
partition = partitions.create(index: 'extend_partition_1', id: 1)

# Extend partition to index extend_partition_2. Now search will be performed in both indices, but 
# all new documents will be indexex into extend_partition_2.
partition.extend_to(index: 'extend_partition_2')
```

### Operating documents inside partitions
```ruby
client = Elasticsearch::Client.new
partitions = client.partitions

client.indices.create index: 'partition_index'
partitions.prepare_index index: 'partition_index'
partition1 = partitions.create(index: 'partition_index', id: 1)
partition2 = partitions.create(index: 'partition_index', id: 2)

partition1.index id: 1, type: 'document', body: {subject: 'Subject 1'}
partition1.index id: 2, type: 'document', body: {subject: 'Subject 2'}

# Partition 2 should have no documents
count = partition2.search(search_type: 'count', body: {query: {match_all: {}}})['hits']['total']
raise 'Partition 2 is not empty!' unless count == 0

# Partiton 1 should contain everything we just indexed
hits = partition1.search(type: 'document', body: {query:{match_all: {}}})['hits']['hits']
raise "Expected 2 documents, got #{hits.size}" unless hits.size == 2
h1 = hits.find{|h| h['_id'].to_i == 1}
raise 'Document 1 cannot be found' unless h1
raise 'Invalid scalastic_partition_id' unless h1['_source']['scalastic_partition_id'] == 1

# Now delete something from partition 1
partition1.delete type: 'document', id: 1
count = partition1.search(search_type: 'count', body: {query: {match_all: {}}})['hits']['total']
raise "Expected 1 document, got #{count}" unless count == 1
```

### Getting a document by its id
```ruby
client = Elasticsearch::Client.new
client.indices.create(index: 'document_get')
client.partitions.prepare_index(index: 'document_get')

p = client.partitions.create(id: 1, index: 'document_get')
p.index(id: 1, type: 'test', body: {title: 'Test'})

res = p.get(id: 1, type: 'test')
raise "Unexpected result: #{res}" unless res == {'_index' => 'document_get', '_type' => 'test', '_id' => '1', '_version' => 1, 'found' => true, '_source' => {'title' => 'Test', 'scalastic_partition_id' => 1}}

p = client.partitions.create(id: 2, index: 'document_get', routing: 12345)
p.index(id: 2, type: 'test', body: {title: 'Routing test'})

res = p.get(id: 2, type: 'test')
raise "Unexpected result: #{res}" unless res == {'_index' => 'document_get', '_type' => 'test', '_id' => '2', '_version' => 1, 'found' => true, '_source' => {'title' => 'Routing test', 'scalastic_partition_id' => 2}}
```

### Bulk operations
```ruby
client = Elasticsearch::Client.new
partitions = client.partitions

client.indices.create(index: 'bulk_operations')
partitions.prepare_index(index: 'bulk_operations')

partition = partitions.create(index: 'bulk_operations', id: 1)

partition.bulk(body: [
  {index: {_type: 'test', _id: 1, data: {subject: 'test1'}}},
  {create: {_type: 'test', _id: 2, data: {subject: 'test2'}}}
])

partition.bulk(body: [
  {index: {_type: 'test', _id: 3}},
  {subject: 'test3'},
  {create: {_type: 'test', _id: 4}},
  {subject: 'test4'}
])

partition.bulk(body: [
  {update: {_type: 'test', _id: 1, data: {doc: {body: 'Document 1'}}}},
  {update: {_type: 'test', _id: 2, data: {doc: {body: 'Document 2'}}}}
])

partition.bulk(body: [
  {update: {_type: 'test', _id: 3}},
  {doc: {body: 'Document 3'}},
  {update: {_type: 'test', _id: 4}},
  {doc: {body: 'Document 4'}}
])

client.indices.refresh    # Commit all pending writes

hits = partition.search['hits']['hits'].sort{|h1, h2| h1['_id'].to_i <=> h2['_id'].to_i}
raise 'Unexpected count' unless hits.size == 4

expected_hits = [
  {'_index' => 'bulk_operations', '_type' => 'test', '_id' => '1', '_score' => 1.0, '_source' => {'subject' => 'test1', 'body' => 'Document 1', 'scalastic_partition_id' => 1}},
  {'_index' => 'bulk_operations', '_type' => 'test', '_id' => '2', '_score' => 1.0, '_source' => {'subject' => 'test2', 'body' => 'Document 2', 'scalastic_partition_id' => 1}},
  {'_index' => 'bulk_operations', '_type' => 'test', '_id' => '3', '_score' => 1.0, '_source' => {'subject' => 'test3', 'body' => 'Document 3', 'scalastic_partition_id' => 1}},
  {'_index' => 'bulk_operations', '_type' => 'test', '_id' => '4', '_score' => 1.0, '_source' => {'subject' => 'test4', 'body' => 'Document 4', 'scalastic_partition_id' => 1}},
]

raise 'Unexpected results' unless hits == expected_hits

res = partition.bulk(body: [
  {delete: {_type: 'test', _id: 1}},
  {delete: {_type: 'test', _id: 2}},
  {delete: {_type: 'test', _id: 3}},
  {delete: {_type: 'test', _id: 4}},
])

client.indices.refresh    # Commit all pending writes

count = partition.search(search_type: 'count')['hits']['total']
raise 'Some documents were not removed' unless count == 0
```

### Deleting by query
Scalastic partitions support delete_by_query, but because it is no longer available in Elasticsearch core, we use our own implementation which uses scan/scroll searches and bulk operations for deletion.
```ruby
client = Elasticsearch::Client.new
partitions = client.partitions

client.indices.create(index: 'delete_by_query')
partitions.prepare_index(index: 'delete_by_query')

p = partitions.create(index: 'delete_by_query', id: 1)
p.index(id: 1, type: 'test')
p.index(id: 2, type: 'test')
p.index(id: 3, type: 'test')
client.indices.flush(index: 'delete_by_query')

p.delete_by_query(body:{query:{terms:{_id: [1,3]}}})
client.indices.flush(index: 'delete_by_query')

expected_hits = [{'_index' => 'delete_by_query', '_type' => 'test', '_id' => '2', '_score' => 1.0, '_source' => {'scalastic_partition_id' => 1}}]
actual_hits = p.search['hits']['hits']
raise "Unexpected results!: #{actual_hits}" unless actual_hits == expected_hits
```

### Notes
* Indices must be *prepared* before they can be used by Scalastic by calling "prepare" on the partitions client; doing so will create critical field mappings. Each index must be prepared only once. 
* All hash keys in arguments must be symbols; using anything else may result in unexpected behavior.


## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/aliakb/scalastic.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
