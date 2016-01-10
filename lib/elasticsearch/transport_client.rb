module Elasticsearch
  module Transport
    class Client
      def partitions
        @partitions_client ||= Scalastic::PartitionsClient.new(self, Scalastic::Config.default)
      end
    end
  end
end
