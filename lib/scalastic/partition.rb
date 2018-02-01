module Scalastic
  module Partition
    Endpoint = Struct.new(:index, :routing)
    Endpoints = Struct.new(:index, :search)

    attr_reader(:es_client)
    attr_reader(:config)
    attr_reader(:id)

    def search(args = {})
      args = args.merge(index: config.search_endpoint(id))
      es_client.search(args)
    end

    def msearch(args)
      endpoint = config.search_endpoint(id)

      es_client.msearch(args.merge(index: endpoint))
    end

    def get(args)
      args = args.merge(index: config.search_endpoint(id))
      es_client.get(args)
    end

    def mget(args)
      args = args.merge(index: config.search_endpoint(id))
      es_client.mget(args)
    end

    def index(args)
      args[:body] ||= {}
      selector.apply_to(args[:body])
      args = args.merge(index: config.index_endpoint(id))
      es_client.index(args)
    end

    def create(args)
      args[:body] ||= {}
      selector.apply_to(args[:body])
      args = args.merge(index: config.index_endpoint(id))
      es_client.create(args)
    end

    def delete(args = {})
      args = args.merge(index: config.search_endpoint(id))
      es_client.delete(args)
    end

    def bulk(args)
      body = args.clone[:body] || raise(ArgumentError, 'Missing required argument :body')

      new_ops = body.map{|entry| [operation_name(entry), entry]}.reduce([]){|acc, op| acc << [op.first, update_entry(acc, *op)]; acc}
      args[:body] = new_ops.map{|_op, entry| entry}

      es_client.bulk(args)
    end

    def delete_by_query(args)
      args = args.merge(scroll: '1m', size: 100, _source: false)

      scroll(args).each_slice(300) do |hits|
        ops = hits.map{|h| delete_op(h)}
        es_client.bulk(body: ops)
      end
    end

    def inspect
      "ES partition #{id}"
    end

    def get_aliases
      es_client.indices.get_aliases(name: "#{config.endpoints_prefix(id)}*");
    end
  end
end
