module RegressionTests
  module DeletePartition
    extend self
    
    def cleanup
      client = Elasticsearch::Client.new
      client.indices.delete index: 'delete_partition' if client.indices.exists? index: 'delete_partition'
    end

    def run
      # Connect to Elasticsearch and create an index
      client = Elasticsearch::Client.new
      partitions = client.partitions
      client.indices.create index: 'delete_partition'
      partitions.prepare_index index: 'delete_partition'

      # Create partitions
      partitions.create index: 'delete_partition', id: 1
      partitions.create index: 'delete_partition', id: 2
      partitions.create index: 'delete_partition', id: 3

      # Delete one of the partitions
      partitions.delete id: 2
      raise 'Partition still exists' if partitions[2].exists?
    end
  end
end
