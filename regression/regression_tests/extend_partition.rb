module RegressionTests
  module ExtendPartition
    extend self

    def cleanup
      client = Elasticsearch::Client.new
      %w(extend_partition_1 extend_partition_2).each do |i|
        client.indices.delete index: i if client.indices.exists? index: i
      end
    end

    def run
      # Connect to Elasticsearch and set up indices
      client = Elasticsearch::Client.new
      partitions = client.partitions
      client.indices.create index: 'extend_partition_1'
      partitions.prepare_index index: 'extend_partition_1'
      client.indices.create index: 'extend_partition_2'
      partitions.prepare_index index: 'extend_partition_2'

      # Create a partition residing in extend_partition_1
      partition = partitions.create(index: 'extend_partition_1', id: 1)

      # Extend partition to index extend_partition_2. Now search will be performed in both indices, but 
      # all new documents will be indexex into extend_partition_2.
      partition.extend_to(index: 'extend_partition_2')
    end
  end
end
