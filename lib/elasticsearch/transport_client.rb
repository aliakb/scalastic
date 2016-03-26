module Elasticsearch
  module Transport
    class Client
      def partitions
        @partitions_client ||= Scalastic::PartitionsClient.new(self)
      end
    end
  end
end
