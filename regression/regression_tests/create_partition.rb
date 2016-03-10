require 'scalastic'
require 'elasticsearch'

module RegressionTests
  module CreatePartition
    extend self

    def cleanup
      client = Elasticsearch::Client.new
      client.indices.delete index: 'create_partition' if client.indices.exists? index: 'create_partition'
    end

    def run
      # Connect to Elasticsearch and get the partitions client
      es_client = Elasticsearch::Client.new
      partitions = es_client.partitions
      
      # Create an index for the test.
      es_client.indices.create index: 'create_partition'
      partitions.prepare_index index: 'create_partition'   # Needs to be called only once per index

      # Create a partition
      partition = partitions.create index: 'create_partition', id: 1
      raise 'Partition was not created' unless partition.exists?
    end
  end
end
