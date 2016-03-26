module RegressionTests
  module NestedSelectorBulk
    extend self

    def cleanup
      client = Elasticsearch::Client.new
      client.indices.delete index: 'nested_selector_bulk' if client.indices.exists? index: 'nested_selector_bulk'
    end

    def run
      client = Elasticsearch::Client.new

      # Set up test environment
      client.indices.create index: 'nested_selector_bulk'
      partitions = client.partitions
      partitions.config.partition_selector = 'parent.child.partition_id'
      partitions.prepare_index index: 'nested_selector_bulk'          # Set up field mapping

      # Create partition
      partition = partitions.create index: 'nested_selector_bulk', id: 1

      # Index documents
      partition.bulk body: [
        {index: {_type: 'test', _id: 1, data: {subject: 'Test 1'}}},
        {create: {_type: 'test', _id: 2, data: {subject: 'Test 2'}}},
      ]

      partition.bulk body: [
        {index: {_type: 'test', _id: 3}},
        {subject: 'Test 3'},
        {create: {_type: 'test', _id: 4}},
        {subject: 'Test 4'}
      ]

      sleep 1.5

      hits = partition.search(body: {query: {match_all: {}}})['hits']['hits'].sort{|h1, h2| h1['_id'] <=> h2['_id']}
      expected_hits = [
        {'_index'=>'nested_selector_bulk', '_type' => 'test', '_id' => '1', '_score' => 1.0, '_source' => {'subject' => 'Test 1', 'parent' => {'child' => {'partition_id' => 1}}}},
        {'_index'=>'nested_selector_bulk', '_type' => 'test', '_id' => '2', '_score' => 1.0, '_source' => {'subject' => 'Test 2', 'parent' => {'child' => {'partition_id' => 1}}}},
        {'_index'=>'nested_selector_bulk', '_type' => 'test', '_id' => '3', '_score' => 1.0, '_source' => {'subject' => 'Test 3', 'parent' => {'child' => {'partition_id' => 1}}}},
        {'_index'=>'nested_selector_bulk', '_type' => 'test', '_id' => '4', '_score' => 1.0, '_source' => {'subject' => 'Test 4', 'parent' => {'child' => {'partition_id' => 1}}}},
      ]
      raise "Expected: #{expected_hits}, got: #{hits}" unless expected_hits == hits

      # Update documents
      partition.bulk body: [
        {update: {_type: 'test', _id: 1, data: {doc: {subject: 'Test 1a'}}}},
        {update: {_type: 'test', _id: 2, data: {doc: {subject: 'Test 2a'}}}},
        {update: {_type: 'test', _id: 3}},
        {doc: {subject: 'Test 3a'}}
      ]

      partition.bulk body: [
        {update: {_type: 'test', _id: 3}},
        {doc: {subject: 'Test 3a'}},
        {update: {_type: 'test', _id: 4}},
        {doc: {subject: 'Test 4a'}}
      ]

      sleep 1.5

      hits = partition.search(body: {query: {match_all: {}}})['hits']['hits'].sort{|h1, h2| h1['_id'] <=> h2['_id']}
      expected_hits = [
        {'_index'=>'nested_selector_bulk', '_type' => 'test', '_id' => '1', '_score' => 1.0, '_source' => {'subject' => 'Test 1a', 'parent' => {'child' => {'partition_id' => 1}}}},
        {'_index'=>'nested_selector_bulk', '_type' => 'test', '_id' => '2', '_score' => 1.0, '_source' => {'subject' => 'Test 2a', 'parent' => {'child' => {'partition_id' => 1}}}},
        {'_index'=>'nested_selector_bulk', '_type' => 'test', '_id' => '3', '_score' => 1.0, '_source' => {'subject' => 'Test 3a', 'parent' => {'child' => {'partition_id' => 1}}}},
        {'_index'=>'nested_selector_bulk', '_type' => 'test', '_id' => '4', '_score' => 1.0, '_source' => {'subject' => 'Test 4a', 'parent' => {'child' => {'partition_id' => 1}}}},
      ]
      raise "Expected: #{expected_hits}, got: #{hits}" unless expected_hits == hits

      partition.bulk body: [
        {delete: {_type: 'test', _id: 1}},
        {delete: {_type: 'test', _id: 2}},
        {delete: {_type: 'test', _id: 3}},
        {delete: {_type: 'test', _id: 4}},
      ]

      sleep 1.5

      hits = partition.search['hits']['hits']
      raise "Expected no hits, got: #{hits}" unless hits.empty?
    end
  end
end
