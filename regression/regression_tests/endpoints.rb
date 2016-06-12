module RegressionTests
  module Endpoints
    extend self

    def cleanup
      client = Elasticsearch::Client.new
      %w(endpoints_1 endpoints_2).each do |i|
        client.indices.delete index: i if client.indices.exists? index: i
      end
    end

    def run
      client = Elasticsearch::Client.new
      partitions = client.partitions

      client.indices.create index: 'endpoints_1'
      client.indices.create index: 'endpoints_2'
      partitions.prepare_index index: 'endpoints_1'
      partitions.prepare_index index: 'endpoints_2'

      p = partitions[1]
      raise 'Partition should not exist!' if p.exists?
      raise 'Partition should be read only!' unless p.readonly?

      eps = p.get_endpoints
      raise 'Index endpoint is not nil!' unless eps.index.nil?
      raise 'Search endpoints must be empty!' if eps.search.any?

      p.index_to index: 'endpoints_1'
      eps = p.get_endpoints
      raise 'Partition shoudl exist!' unless p.exists?
      raise 'Unexpected index endpoint' unless eps.index == Scalastic::Partition::Endpoint.new('endpoints_1', nil)
      raise 'Partition should not be read only!' if p.readonly?

      p.index_to index: 'endpoints_2', routing: 123
      eps = p.get_endpoints
      raise 'Unexpected index endpoint' unless eps.index == Scalastic::Partition::Endpoint.new('endpoints_2', '123')

      p.index_to nil
      raise 'Partition should not exist' if p.exists?
      raise 'Partition is not read only' unless p.readonly?

      p.extend_to index: 'endpoints_1'
      eps = p.get_endpoints
      raise 'Unexpected search endpoints' unless eps.search == [Scalastic::Partition::Endpoint.new('endpoints_1', nil)]
      p.extend_to index: 'endpoints_2', routing: '22'
      eps = p.get_endpoints
      expected = [Scalastic::Partition::Endpoint.new('endpoints_1', nil), Scalastic::Partition::Endpoint.new('endpoints_2', '22')]
      raise 'Unexpected search endpoints' unless eps.search.size == expected.size && expected.all?{|ep| eps.search.include?(ep)}
    end
  end
end
