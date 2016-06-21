module RegressionTests
  module Mget
    extend self

    def cleanup
      client = Elasticsearch::Client.new
      client.indices.delete(index: 'mget') if client.indices.exists?(index: 'mget')
    end

    def run
      client = Elasticsearch::Client.new
      client.indices.create index: 'mget'
      client.partitions.prepare_index index: 'mget'

      partition = client.partitions.create id: 1, index: 'mget'
      partition.index id: 1, type: 'test', body: {subject: 'Test 1'}
      partition.index id: 2, type: 'test', body: {subject: 'Test 2'}

      expected_results = [
        {'_index' => 'mget', '_type' => 'test', '_id' => '1', '_version' => 1, 'found' => true, '_source' => {'subject' => 'Test 1', 'scalastic_partition_id' => 1}},
        {'_index' => 'mget', '_type' => 'test', '_id' => '2', '_version' => 1, 'found' => true, '_source' => {'subject' => 'Test 2', 'scalastic_partition_id' => 1}},
        {'_index' => 'mget', '_type' => 'test', '_id' => '3', 'found' => false}
      ]

      results = partition.mget(type: 'test', body: {ids: [1, 2, 3]})['docs']
      raise "Expected: #{expected_results}, got: #{results}" unless expected_results == results

      results = partition.mget(body: {docs: [{_type: 'test', _id: 1}, {_type: 'test', _id: 2}, {_type: 'test', _id: 3}]})['docs']
      raise "Expected: #{expected_results}, got: #{results}" unless expected_results == results
    end
  end
end
