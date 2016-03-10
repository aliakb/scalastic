module RegressionTests
  module DocumentGet
    extend self

    def cleanup
      client = Elasticsearch::Client.new
      client.indices.delete index: 'document_get' if client.indices.exists? index: 'document_get'
    end

    def run
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
      raise "Unexpected result: #{res}" unless res == {'_index' => 'document_get', '_type' => 'test', '_id' => '2', '_version' => 1, '_routing' => '12345', 'found' => true, '_source' => {'title' => 'Routing test', 'scalastic_partition_id' => 2}}
    end
  end
end
