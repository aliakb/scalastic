require 'scalastic/es_actions_generator'

module Scalastic
  class Partition
    attr_reader(:es_client)
    attr_reader(:config)
    attr_reader(:id)

    def initialize(es_client, config, id)
      raise(ArgumentError, 'ES client is nil!') if es_client.nil?
      raise(ArgumentError, 'config is nil!') if config.nil?
      raise(ArgumentError, 'id is empty!') if id.nil? || id.to_s.empty?

      @es_client = es_client
      @config = config
      @id = id
    end

    def extend_to(args)
      index = args[:index]
      raise(ArgumentError, 'Missing required argument :index') if index.nil? || index.to_s.empty?

      index_alias = config.index_endpoint(id)
      indices = es_client.indices.get_aliases(name: index_alias).select{|i, d| d['aliases'].any?}.keys
      actions = indices.map{|i| {remove: {index: i, alias: index_alias}}}
      actions << {add: EsActionsGenerator.new_index_alias(config, args.merge(id: id))}
      actions << {add: EsActionsGenerator.new_search_alias(config, args.merge(id: id))}
      es_client.indices.update_aliases(body: {actions: actions})
    end

    def search(args = {})
      args = args.merge(index: config.search_endpoint(id))
      es_client.search(args)
    end

    def index(args)
      args = {body: {}}.merge(args)
      args[:body][config.partition_selector.to_sym] = id
      args = args.merge(index: config.index_endpoint(id))
      es_client.index(args)
    end

    def delete(args = {})
      args = args.merge(index: config.search_endpoint(id))
      es_client.delete(args)
    end

    def exists?
      names = [config.search_endpoint(id), config.index_endpoint(id)]
      all_aliases = es_client.indices.get_aliases name: names.join(',')
      all_aliases.any?{|_index, data| data['aliases'].any?}
    end

    #TODO: add bulk

    def inspect
      "ES partition #{id}"
    end
  end
end
