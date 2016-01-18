require 'spec_helper'

describe Scalastic::Config do
  let(:config) {subject}

  describe '.default' do
    let(:config) {described_class.default}

    it 'has correct partition_prefix' do
      expect(config.partition_prefix).to eq 'scalastic'
    end

    it 'has correct partition_selector' do
      expect(config.partition_selector).to eq :scalastic_partition_id
    end
  end

  describe '#partition_prefix' do
    it  'returns correct value' do
      expect(config.partition_prefix).to eq 'scalastic'
    end
  end

  describe '#partition_selector' do
    it 'returns correct value' do
      expect(config.partition_selector).to eq :scalastic_partition_id
    end
  end

  describe '#index_endpoint' do
    let(:id) {[1,2,3].sample}

    it 'returns correct value' do
      expect(config.index_endpoint(id)).to eq "scalastic_#{id}_index"
    end
  end

  describe '#search_endpoint' do
    let(:id) {[1,2,3].sample}

    it 'returns correct value' do
      expect(config.search_endpoint(id)).to eq "scalastic_#{id}_search"
    end
  end

  describe '#get_partition_id' do
    let(:id) {[1,2,3].sample}

    context 'with search alias' do
      let(:es_alias) {"scalastic_#{id}_search"}

      it 'returns correct value' do
        expect(config.get_partition_id(es_alias)).to eq id
      end
    end

    context 'with index alias' do
      let(:es_alias) {"scalastic_#{id}_index"}

      it 'returns correct value' do
        expect(config.get_partition_id(es_alias)).to eq id
      end
    end

    context 'with incomplete alias' do
      let(:es_alias) {["scalastic_#{id}_", "scalastic_#{id}"].sample}

      it 'returns nil' do
        expect(config.get_partition_id(es_alias)).to be_nil
      end
    end

    context 'with incorrect alias' do
      let(:es_alias) {["alias", "alias123", "123"].sample}

      it 'returns nil' do
        expect(config.get_partition_id(es_alias)).to be_nil
      end
    end
  end
end
