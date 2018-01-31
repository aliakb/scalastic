module RegressionTests
  module NestedSelector
    extend self

    def cleanup
      client = Elasticsearch::Client.new
      client.indices.delete index: 'nested_selector' if client.indices.exists? index: 'nested_selector'
    end

    def run
      client = Elasticsearch::Client.new
      partitions = client.partitions

      # Create an index for testing
      client.indices.create index: 'nested_selector'

      # Prepare the environment
      partitions.config.partition_selector = 'parent.child.partition_id'
      partitions.prepare_index index: 'nested_selector'     # Must be called only once for the index

      # Indexing should work
      partition = partitions.create index: 'nested_selector', id: 1
      partition.index type: 'document', id: 1, body: {title: 'Test', content: 'This is a test'}
      
      sleep 1.5

      # Searching should work, too
      results = partition.search(type: 'document', body: {query: {match_all: {}}})
      count = results['hits']['total']
      raise "Expected 1 result, got #{count}" unless count == 1

      hit = results['hits']['hits'].first['_source']
      expected = {'title'=>'Test', 'content'=>'This is a test', 'parent'=>{'child'=>{'partition_id'=>1}}}
      raise "Expected: #{expected}, got: #{hit}" unless expected == hit

      # Deleting should work
      partition.delete type: 'document', id: 1
      sleep 1.5
      count = partition.search(size: 0, body: {query: {match_all: {}}})['hits']['total']
      raise "Expected 0 records, got #{count}" unless count == 0
    end
  end
end
