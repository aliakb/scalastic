require "spec_helper"

describe Elasticsearch::Transport::Client do
  describe '#partitions' do
    it {is_expected.to respond_to :partitions}

    it "returns correct value" do
      expect(subject.partitions).to be_a Scalastic::PartitionsClient
    end
  end
end
