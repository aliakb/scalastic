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

      # Create a couple of partitions
      partitions.create index: 'list_partitions', id: 1
      partitions.create index: 'list_partitions', id: 2
      partitions.create index: 'list_partitions', id: 3

      # List all partitions
      ids = partitions.to_a.map{|p| p.id}
      raise "Unexpected partitions: #{ids}" unless ids.sort == [1,2,3]
    end
  end
end
