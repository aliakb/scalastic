require "scalastic/partition"
require "scalastic/es_actions_generator"

module Scalastic
  class PartitionsClient
    attr_reader(:es_client)
    attr_reader(:config)

    def initialize(es_client, config = Config.default)
      raise(ArgumentError, "ES client is nil") if es_client.nil?
      raise(ArgumentError, "Config is nil") if config.nil?
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
      id = args[:id] || raise(ArgumentError, "Missing required argument :id")
      pairs = es_client.indices.get_aliases.map{|i, d| d["aliases"].keys.select{|a| config.get_partition_id(a) == id}.map{|a| [i, a]}}.flatten(1)
      unless pairs.any?
        #TODO: log a warning
        return
      end
      actions = pairs.map{|i, a| {remove: {index: i, alias: a}}}
      es_client.indices.update_aliases(body: {actions: actions})
    end

    def [](id)
      Partition.new(es_client, config, id)
    end

    def to_a
      aliases = es_client.indices.get_aliases
      partition_ids = aliases.map{|_, data| data["aliases"].keys}.flatten.map{|a| config.get_partition_id(a)}.compact.uniq
      partition_ids.map{|id| Partition.new(es_client, config, id)}
    end

    def prepare_index(args)
      index = args[:index] || raise(ArgumentError, "Missing required argument :index")
      mapping = {properties: {config.partition_selector => {type: "long"}}}
      es_client.indices.put_mapping(index: index, type: "_default_", body: {"_default_" => mapping})
      es_client.indices.put_mapping(index: index, type: "scalastic", body: {"scalastic" => mapping})
    end
  end
end
