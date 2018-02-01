require 'scalastic/v1/partition'

module Scalastic
  module PartitionFactory
    extend self

    def create_partition(es_client, config, id)
      return V1::Partition.new(es_client, config, id)
    end
  end
end
