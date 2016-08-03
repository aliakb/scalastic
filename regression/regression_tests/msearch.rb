module RegressionTests
  module Msearch
    extend self

    def cleanup
      client = Elasticsearch::Client.new
      client.indices.delete index: 'msearch' if client.indices.exists? index: 'msearch'
    end

    def run
      client = Elasticsearch::Client.new
      partitions = client.partitions

      client.indices.create index: 'msearch'
      partitions.prepare_index index: 'msearch'

      partition = partitions.create(index: 'msearch', id: 1)
      partition.index(id: '1:1', type: 'test', body: {title: 'Document 1:1'})
      partition.index(id: '1:2', type: 'test', body: {title: 'Document 1:2'})

      other_partition = partitions.create(index: 'msearch', id: 2)

      other_partition.index(id: '2:1', type: 'test', body: {title: 'Document 2:1'})
      other_partition.index(id: '2:2', type: 'test', body: {title: 'Document 2:2'})

      sleep 1.5

      results = partition.msearch(body: [{search: {query: {term: {_id: '1:1'}}}}, {search: {query: {term: {_id: '1:2'}}}}, {search: {query: {term: {_id: '1:3'}}}}])
      found = results['responses'].map{|r| r['hits']['hits'].map{|h| h['_id']}}.flatten
      expected = ['1:1', '1:2']
      raise "Found #{found}, expected #{expected}" unless expected == found
    end
  end
end
