module Scalastic
  module Normalizer
    extend self

    def normalized(hash)
      return hash if hash.keys.first.is_a?(String)
      normalize_internal(hash)
    end

    def safe_get(hash, *keys)
      keys.reduce(hash) do |h, k|
        h && (h[k.to_s] || h[k.to_sym])
      end
    end

    private

    def normalize_internal(object)
      if (object.is_a?(Hash))
        Hash[object.map{|k, v| [k.to_s, normalize_internal(v)]}]
      elsif object.respond_to?(:map)
        object.map{|i| normalize_internal(i)}
      else
        object
      end
    end
  end
end
