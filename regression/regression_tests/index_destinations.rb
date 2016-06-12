module RegressionTests
  module IndexDestinations
    extend self

    def cleanup
      client = Elasticsearch::Client.new
      client.indices.delete index: 'destinations_1' if client.indices.exists? index: 'destinations_1'
      client.indices.delete index: 'destinations_2' if client.indices.exists? index: 'destinations_2'
    end

    def run
      client = Elasticsearch::Client.new
      partitions = client.partitions

      client.indices.create index: 'destinations_1'
      client.indices.create index: 'destinations_2'

      partitions.prepare_index index: 'destinations_1'
      partitions.prepare_index index: 'destinations_2'

      p = partitions.create id: 1, index: 'destinations_1'
      p.extend_to(index: 'destinations_2')

      expected = {"destinations_1"=>{"aliases"=>{"scalastic_1_search"=>{"filter"=>{"term"=>{"scalastic_partition_id"=>1}}}}}, "destinations_2"=>{"aliases"=>{"scalastic_1_index"=>{}, "scalastic_1_search"=>{"filter"=>{"term"=>{"scalastic_partition_id"=>1}}}}}}
      actual = client.indices.get_aliases index: 'destinations_1,destinations_2', name: 'scalastic_1_*'
      raise "Expected #{expected}, got: #{actual}" unless expected == actual

      p = partitions[2]
      raise 'Partition should not exist!' if p.exists?
      p = partitions.create id: 2, index: 'destinations_1'
      raise 'Partition should exist!' unless p.exists?

      p.index_to nil

      expected = {"destinations_1"=>{"aliases"=>{"scalastic_2_search"=>{"filter"=>{"term"=>{"scalastic_partition_id"=>2}}}}}, "destinations_2"=>{"aliases"=>{}}}
      actual = client.indices.get_aliases index: 'destinations_1,destinations_2', name: "scalastic_2_*"
      raise "Expected: #{expected}, got: #{actual}" unless expected == actual
    end
  end
end
