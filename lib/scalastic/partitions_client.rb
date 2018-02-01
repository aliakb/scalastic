require 'scalastic/partition_factory'
require 'scalastic/es_actions_generator'
require 'scalastic/normalizer'
require 'scalastic/partition_factory'

module Scalastic
  class PartitionsClient
    include Enumerable
    include Normalizer

    attr_reader(:es_client)
    attr_reader(:config)

    def initialize(es_client, config = Config.default.dup)
      raise(ArgumentError, 'ES client is nil') if es_client.nil?
      raise(ArgumentError, 'Config is nil') if config.nil?
      @es_client = es_client
      @config = config
    end

    def create(args = {})
      actions = [
        {add: EsActionsGenerator.new_search_alias(config, args)},
        {add: EsActionsGenerator.new_index_alias(config, args)},
      ]
      es_client.indices.update_aliases(body: {actions: actions})
      self[args[:id]]
    end

    def delete(args = {})
      id = args[:id].to_s
      raise(ArgumentError, 'Missing required argument :id') if id.nil? || id.empty?
      pairs = normalized(es_client.indices.get_aliases).map{|i, d| d['aliases'].keys.select{|a| config.get_partition_id(a) == id}.map{|a| [i, a]}}.flatten(1)
      unless pairs.any?
        #TODO: log a warning
        return
      end
      actions = pairs.map{|i, a| {remove: {index: i, alias: a}}}
      es_client.indices.update_aliases(body: {actions: actions})
    end

    def [](id)
      PartitionFactory.create_partition(es_client, config, id)
    end

    def each(&_block)
      partition_ids.each do |pid|
        yield PartitionFactory.create_partition(es_client, config, pid) if block_given?
      end
    end

    def prepare_index(args)
      index = args[:index] || raise(ArgumentError, 'Missing required argument :index')
      mapping = {properties: config.partition_selector_mapping}
      es_client.indices.put_mapping(index: index, type: '_default_', body: {'_default_' => mapping})
      es_client.indices.put_mapping(index: index, type: 'scalastic', body: {'scalastic' => mapping})
    end

    private

    def partition_ids
      aliases = normalized(es_client.indices.get_aliases)
      partition_ids = aliases.map{|_, data| data['aliases'].keys}.flatten.map{|a| config.get_partition_id(a)}.compact.uniq
    end
  end
end
