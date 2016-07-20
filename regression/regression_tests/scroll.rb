module RegressionTests
  module Scroll
    extend self

    def cleanup
      client = Elasticsearch::Client.new
      client.indices.delete index: 'scrolling' if client.indices.exists? index: 'scrolling'
    end

    def run
      # Connect to Elasticsearch
      client = Elasticsearch::Client.new
      client.indices.create index: 'scrolling'
      partitions = client.partitions
      partitions.prepare_index index: 'scrolling'

      p = partitions.create id: 1, index: 'scrolling'

      # Create some test data
      10.times.each do |i|
        p.index id: i + 1, type: 'test', body: {subject: "Test ##{i + 1}"}
      end

      sleep 1.5

      # Get the hits. Size is set to 7 to test multiple calls to scroll
      actual_hits = p.scroll(type: 'test', size: 7).to_a.sort{|h1, h2| h1['_id'].to_i <=> h2['_id'].to_i}
      expected_hits = 10.times.map{|i| {'_index' => 'scrolling', '_type' => 'test', '_id' => "#{i + 1}", '_score' => 0.0, '_source' => {'subject' => "Test ##{i + 1}", 'scalastic_partition_id' => 1} }}

      raise "Expected: #{expected_hits}, got: #{actual_hits}" unless expected_hits == actual_hits
    end
  end
end
