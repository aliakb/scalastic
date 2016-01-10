module Scalastic
  module EsActionsGenerator
    extend self

    def new_search_alias(config, args)
      index = args[:index] || raise(ArgumentError, "Missing required argument :index")
      id = args[:id] || raise(ArgumentError, "Missing required argument :id")
      routing = args[:routing]
      {index: index, alias: config.search_endpoint(id), filter: {term: {config.partition_selector => id}}}.tap do |op|
        op.merge!(routing: routing) if routing
      end
    end

    def new_index_alias(config, args)
      index = args[:index] || raise(ArgumentError, "Missing required argument :index")
      id = args[:id] || raise(ArgumentError, "Missing required argument :id")
      routing = args[:routing]
      {index: index, alias: config.index_endpoint(id)}.tap do |op|
        op.merge!(routing: routing) if routing
      end
    end
  end
end
