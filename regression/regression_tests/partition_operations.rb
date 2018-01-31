module RegressionTests
  module PartitionOperations
    extend self

    def cleanup
      client = Elasticsearch::Client.new
      client.indices.delete index: 'partition_operations' if client.indices.exists? index: 'partition_operations'
    end

    def run
      client = Elasticsearch::Client.new
      partitions = client.partitions

      client.indices.create index: 'partition_operations'
      partitions.prepare_index index: 'partition_operations'
      partition1 = partitions.create(index: 'partition_operations', id: 1)
      partition2 = partitions.create(index: 'partition_operations', id: 2)

      partition1.index id: 1, type: 'document', body: {subject: 'Subject 1'}
      partition1.index id: 2, type: 'document', body: {subject: 'Subject 2'}

      sleep 1.5

      # Partition 2 should have no documents
      count = partition2.search(size: 0, body: {query: {match_all: {}}})['hits']['total']
      raise 'Partition 2 is not empty!' unless count == 0

      # Partiton 1 should contain everything we just indexed
      hits = partition1.search(type: 'document', body: {query:{match_all: {}}})['hits']['hits']
      raise "Expected 2 documents, got #{hits.size}" unless hits.size == 2
      h1 = hits.find{|h| h['_id'].to_i == 1}
      raise 'Document 1 cannot be found' unless h1
      partition_id = h1['_source']['scalastic_partition_id']
      raise "Expected: 1; got: #{partition_id}" unless partition_id == 1

      # Now delete something from partition 1
      partition1.delete type: 'document', id: 1

      sleep 1.5

      count = partition1.search(size: 0, body: {query: {match_all: {}}})['hits']['total']
      raise "Expected 1 document, got #{count}" unless count == 1
    end
  end
end
