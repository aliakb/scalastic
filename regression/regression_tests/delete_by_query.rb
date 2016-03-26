module RegressionTests
  module DeleteByQuery
    extend self

    def cleanup
      client = Elasticsearch::Client.new
      client.indices.delete index: 'delete_by_query' if client.indices.exists? index: 'delete_by_query'
    end

    def run
      client = Elasticsearch::Client.new
      partitions = client.partitions

      client.indices.create(index: 'delete_by_query')
      partitions.prepare_index(index: 'delete_by_query')

      p = partitions.create(index: 'delete_by_query', id: 1)
      p.index(id: 1, type: 'test')
      p.index(id: 2, type: 'test')
      p.index(id: 3, type: 'test')
      sleep 1.5

      p.delete_by_query(body:{query:{terms:{_id: [1,3]}}})
      sleep 1.5

      expected_hits = [{'_index' => 'delete_by_query', '_type' => 'test', '_id' => '2', '_score' => 1.0, '_source' => {'scalastic_partition_id' => 1}}]
      actual_hits = p.search['hits']['hits']
      raise "Unexpected results!: #{actual_hits}" unless actual_hits == expected_hits
    end
  end
end
