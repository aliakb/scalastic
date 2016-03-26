module RegressionTests
  module CustomPartitionPrefix
    extend self

    def cleanup
      client = Elasticsearch::Client.new
      client.indices.delete index: 'custom_partition_prefix' if client.indices.exists? index: 'custom_partition_prefix'
    end

    def run
      client = Elasticsearch::Client.new
      partitions = client.partitions

      partitions.config.partition_prefix = 'custom'

      client.indices.create index: 'custom_partition_prefix'
      partitions.prepare_index index: 'custom_partition_prefix'     # Must be called only once per index

      partition = partitions.create index: 'custom_partition_prefix', id: 1
      sleep 1.5

      # Are aliases there? 
      aliases = client.indices.get_aliases index: 'custom_partition_prefix'
      expected_aliases = {"custom_partition_prefix"=>{"aliases"=>{"custom_1_index"=>{}, "custom_1_search"=>{"filter"=>{"term"=>{"scalastic_partition_id"=>1}}}}}}
      raise "Expected: #{expected_aliases}; got: #{aliases}" unless expected_aliases == aliases
    end
  end
end
