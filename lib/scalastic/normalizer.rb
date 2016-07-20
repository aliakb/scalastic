module Scalastic
  module Normalizer
    extend self

    def normalized(hash)
      return hash if hash.keys.first.is_a?(String)
      normalize_internal(hash)
    end

    private

    def normalize_internal(object)
      if (object.is_a?(Hash))
        object.map{|k, v| [k.to_s, normalize_internal(v)]}.to_h
      elsif object.respond_to?(:map)
        object.map{|i| normalize_internal(i)}
      else
        object
      end
    end
  end
end
