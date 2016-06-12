module RegressionTests
  module ListPartitions
    extend self

    def cleanup
      client = Elasticsearch::Client.new
      client.indices.delete index: 'list_partitions' if client.indices.exists? index: 'list_partitions'
    end

    def run
      # Set everything up
      client = Elasticsearch::Client.new
      client.indices.create index: 'list_partitions'
      partitions = client.partitions
      partitions.prepare_index index: 'list_partitions'    # Must be called once per each index

      sleep 1.5

      # Create a couple of partitions
      partitions.create index: 'list_partitions', id: 1
      partitions.create index: 'list_partitions', id: 2
      partitions.create index: 'list_partitions', id: 3

      partitions[1].index type: 'test', body: {title: 'In partition 1'}
      partitions[2].index type: 'test', body: {title: 'In partition 2'}
      partitions[3].index type: 'test', body: {title: 'In partition 3'}

      sleep 1.5

      # List all partitions
      ids = partitions.to_a.map{|p| p.id}.sort
      expected_ids = %w(1 2 3)
      raise "Expected partitions #{expected_ids}, got #{ids}" unless ids == expected_ids
    end
  end
end
