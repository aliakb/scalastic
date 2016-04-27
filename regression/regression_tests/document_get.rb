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

      sleep 1.5

      res = p.get(id: 1, type: 'test')['_source']
      expected = {'title' => 'Test', 'scalastic_partition_id' => 1}
      raise "Expected: #{expected}, got: #{res}" unless res == expected

      p = client.partitions.create(id: 2, index: 'document_get', routing: 12345)
      p.index(id: 2, type: 'test', body: {title: 'Routing test'})

      sleep 1.5
      
      res = p.get(id: 2, type: 'test')['_source']
      expected = {'title' => 'Routing test', 'scalastic_partition_id' => 2}
      raise "Expected: #{expected}, got: #{res}" unless res == expected
    end
  end
end
