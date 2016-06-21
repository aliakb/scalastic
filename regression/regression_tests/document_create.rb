module RegressionTests
  module DocumentCreate
    extend self

    def cleanup
      client = Elasticsearch::Client.new
      client.indices.delete(index: 'document_create') if client.indices.exists?(index: 'document_create')
    end

    def run
      client = Elasticsearch::Client.new
      client.indices.create(index: 'document_create')
      client.partitions.prepare_index(index: 'document_create')

      partition = client.partitions.create(index: 'document_create', id: 1)
      partition.create(type: 'test', id: 1, body: {subject: 'Test 1'})
      partition.create(type: 'test', id: 2, body: {subject: 'Test 2'})

      res = partition.create(type: 'test', id: 1, body: {subject: 'Test 1'}) rescue :failed
      raise 'Indexing didn\'t fail' unless res == :failed
      sleep(1.5)

      hits = partition.search()['hits']['hits'].sort{|h1, h2| h1['_id'].to_i <=> h2['_id'].to_i}
      expected_hits = [
        {'_index' => 'document_create', '_type' => 'test', '_id' => '1', '_score' => 1.0, '_source' => {'subject' => 'Test 1', 'scalastic_partition_id' => 1}},
        {'_index' => 'document_create', '_type' => 'test', '_id' => '2', '_score' => 1.0, '_source' => {'subject' => 'Test 2', 'scalastic_partition_id' => 1}},
      ]

      raise "Expected: #{expected_hits}, got: #{hits}" unless expected_hits == hits
    end
  end
end
