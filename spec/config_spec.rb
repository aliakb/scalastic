require 'spec_helper'
require 'SecureRandom'

describe Scalastic::Config do
  let(:config) {subject}

  describe '.default' do
    let(:config) {described_class.default}

    it 'has correct partition_prefix' do
      expect(config.partition_prefix).to eq 'scalastic'
    end

    it 'has correct partition_selector' do
      expect(config.partition_selector).to eq 'scalastic_partition_id'
    end
  end

  describe '#partition_prefix' do
    it 'returns correct value' do
      expect(config.partition_prefix).to eq 'scalastic'
    end

    it 'rejects nils' do
      expect{config.partition_prefix = nil}.to raise_error(ArgumentError, 'Empty partition prefix')
    end

    it 'rejects empty strings' do
      expect{config.partition_prefix = ''}.to raise_error(ArgumentError, 'Empty partition prefix')
    end

    it 'keeps the assigned value' do
      value = SecureRandom.uuid
      config.partition_prefix = value
      expect(config.partition_prefix).to eq value
    end
  end

  describe '#partition_selector' do
    it 'returns correct value' do
      expect(config.partition_selector).to eq 'scalastic_partition_id'
    end

    it 'rejects nils' do
      expect{config.partition_selector = nil}.to raise_error(ArgumentError, 'Empty partition selector')
    end

    it 'rejects empty strings' do
      expect{config.partition_selector = ''}.to raise_error(ArgumentError, 'Empty partition selector')
    end

    it 'keeps the assigned value' do
      value = SecureRandom.uuid
      config.partition_selector = value
      expect(config.partition_selector).to eq value
    end
  end

  describe '#partition_selector_type' do
    it 'is long by default' do
      expect(config.partition_selector_type).to eq 'long'
    end

    it 'rejects nils' do
      expect{config.partition_selector_type = nil}.to raise_error(ArgumentError, 'Unsupported selector type: . Supported types are: (string, long)')
    end

    it 'rejects empty strings' do
      expect{config.partition_selector_type = ''}.to raise_error(ArgumentError, 'Unsupported selector type: . Supported types are: (string, long)')
    end

    it 'accepts string type' do
      config.partition_selector_type = 'string'
      expect(config.partition_selector_type).to eq 'string'
    end

    it 'accepts long type' do
      config.partition_selector_type = 'long'
      expect(config.partition_selector_type).to eq 'long'
    end

    it 'accepts integer type' do
      config.partition_selector_type = 'integer'
      expect(config.partition_selector_type).to eq 'integer'
    end

    it 'rejects unknown types' do
      expect{config.partition_selector_type = 'foo'}.to raise_error(ArgumentError, 'Unsupported selector type: foo. Supported types are: (string, long)')
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
        expect(config.get_partition_id(es_alias)).to eq id.to_s
      end
    end

    context 'with index alias' do
      let(:es_alias) {"scalastic_#{id}_index"}

      it 'returns correct value' do
        expect(config.get_partition_id(es_alias)).to eq id.to_s
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
