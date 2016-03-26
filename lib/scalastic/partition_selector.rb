module Scalastic
  class PartitionSelector
    def initialize(full_field_name, id)
      @objects = full_field_name.split('.').map{|p| p.to_sym}
      @field = @objects.pop
      @id = id
    end

    def apply_to(document_body)
      @objects.reduce(document_body){|body, obj| body[obj] ||= {}}[@field] = @id
      document_body
    end
  end
end
