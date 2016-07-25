require 'scalastic/normalizer'

module Scalastic
  class Scroller
    include Enumerable
    include Normalizer

    def initialize(es_client, args)
      @es_client = es_client
      @args = args
      @scroll = '1m'
    end

    def scroll=(value)
      raise(ArgumentError, "scroll cannot be empty!") if value.nil? || value.empty?
      @scroll = value
    end

    attr_reader(:scroll)

    def each(&block)
      Enumerator.new do |enum|
        args = @args.merge(search_type: 'scan', scroll: scroll)
        res = @es_client.search(args)
        loop do
          scroll_id = safe_get(res, '_scroll_id')
          res = @es_client.scroll(body: scroll_id, scroll: scroll)
          hits = safe_get(res, 'hits', 'hits')
          break unless hits.any?
          hits.each{|h| enum << h}
        end
      end.each(&block)
    end
  end
end
