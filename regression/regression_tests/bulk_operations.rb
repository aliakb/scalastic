module RegressionTests
  module BulkOperations
    extend self

    def cleanup
      client = Elasticsearch::Client.new
      client.indices.delete index: 'bulk_operations' if client.indices.exists? index: 'bulk_operations'
    end

    def run
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
    end
  end
end
