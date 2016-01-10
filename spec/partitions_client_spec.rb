require 'spec_helper'

describe Scalastic::PartitionsClient do
  let(:config) {Scalastic::Config.new}
  let(:es_client) {mock_es_client}
  let(:indices_client) {double("indices client")}
  let(:subject) {described_class.new(es_client, config)}
  let(:client) {subject}

  def mock_es_client
    double('ES client').tap do |c|
      allow(c).to receive(:indices).and_return(indices_client)
    end
  end

  it {is_expected.to respond_to(:[])}

  describe '.new' do
    it 'rejects nil client' do
      expect{described_class.new(nil, config)}.to raise_error(ArgumentError, 'ES client is nil')
    end

    it 'rejects nil config' do
      expect{described_class.new(es_client, nil)}.to raise_error(ArgumentError, 'Config is nil')
    end

    it 'works fine with correct arguments' do
      expect{described_class.new(es_client, config)}.not_to raise_error
    end

    it 'assigns ES client' do
      expect(subject.es_client).to eq es_client
    end

    it 'assigns config' do
      expect(subject.config).to eq config
    end
  end

  describe '#create' do
    let(:index) {'test_index'}
    let(:partition_id) {[1,2,3].sample}
    let(:partition) {client.create(index: index, id: partition_id)}

    before(:each) do
      allow(indices_client).to receive(:update_aliases)
    end

    it 'throws without an index' do
      expect{client.create(id: partition_id)}.to raise_error(ArgumentError, 'Missing required argument :index')
    end

    it 'throws without an id' do
      expect{client.create(index: index)}.to raise_error(ArgumentError, 'Missing required argument :id')
    end

    it 'returns a partition object' do
      expect(partition).to be_a(Scalastic::Partition)
    end

    it 'instantiates a partition' do
      expect(Scalastic::Partition).to receive(:new).once.with(es_client, config, partition_id).and_return(partition)
      client.create(index: index, id: partition_id)
    end

    context 'with no routing' do
      it 'creates a search alias' do
        expect(indices_client).to receive(:update_aliases).once do |args|
          actions = args[:body][:actions]
          expect(actions).to include(add: {alias: "scalastic_#{partition_id}_search", index: index, filter: {term: {'scalastic_partition_id' => partition_id}}})
        end
        client.create(index: index, id: partition_id)
      end

      it 'creates an index alias' do
        expect(indices_client).to receive(:update_aliases).once do |args|
          actions = args[:body][:actions]
          expect(actions).to include(add: {alias: "scalastic_#{partition_id}_index", index: index})
        end
        client.create(index: index, id: partition_id)
      end
    end

    context 'with routing' do
      let(:routing) {[100,200,300].sample}

      it 'creates a search alias' do
        expect(indices_client).to receive(:update_aliases).once do |args|
          actions = args[:body][:actions]
          expect(actions).to include(add: {alias: "scalastic_#{partition_id}_search", index: index, routing: routing, filter: {term: {'scalastic_partition_id' => partition_id}}})
        end
        client.create(index: index, id: partition_id, routing: routing)
      end

      it 'creates an index alias' do
        expect(indices_client).to receive(:update_aliases).once do |args|
          actions = args[:body][:actions]
          expect(actions).to include(add: {alias: "scalastic_#{partition_id}_index", index: index, routing: routing})
        end
        client.create(index: index, id: partition_id, routing: routing)
      end
    end
  end

  describe '#delete' do
    let(:es_aliases) do {
      'index3' => {'aliases' => {}},
      'index2' => {
        'aliases' => {
          'scalastic_1_search' => {'filter' => {'term' => {'scalastic_partition_id' => 1}}},
          'scalastic_1_index' => {},
          'scalastic_2_search' => {'filter' => {'term' => {'scalastic_partition_id' => 2}}},
          'scalastic_2_index' => {},
          'unrelated' => 'some definition'
        },
      },
      'index1' => {
        'aliases' => {
          'scalastic_1_search' => {'filter' => {'term' => {'scalastic_partition_id' => 1}}},
        },
      }
    }
    end
    let(:id) {[1,2].sample}

    before(:each) do
      allow(indices_client).to receive(:get_aliases).and_return(es_aliases)
    end

    it 'throws without an id' do
      expect{client.delete}.to raise_error(ArgumentError, 'Missing required argument :id')
    end

    it 'throws with a nil id' do
      expect{client.delete(id: nil)}.to raise_error(ArgumentError, 'Missing required argument :id')
    end

    it 'updates aliases' do
      expect(indices_client).to receive(:update_aliases).once
      client.delete(id: id)
    end

    it 'deletes multi-index partitions' do
      expected_body = {
        actions: [
          {remove: {index: 'index2', alias: 'scalastic_1_search'}},
          {remove: {index: 'index2', alias: 'scalastic_1_index'}},
          {remove: {index: 'index1', alias: 'scalastic_1_search'}}
        ]
      }
      expect(indices_client).to receive(:update_aliases).once.with(body: expected_body)
      client.delete(id: 1)
    end

    it 'deletes single-index partitions' do
      expected_body = {
        actions: [
          {remove: {index: 'index2', alias: 'scalastic_2_search'}},
          {remove: {index: 'index2', alias: 'scalastic_2_index'}}
        ]
      }
      expect(indices_client).to receive(:update_aliases).once.with(body: expected_body)
      client.delete(id: 2)
    end

    it 'doesn\'t delete for unknown ids' do
      expect(indices_client).not_to receive(:update_aliases)
      client.delete(id: 500)
    end
  end

  describe '#[]' do
    let(:partition) {double('partition')}
    let(:id) {[1,2,3].sample}

    before(:each) do
      allow(Scalastic::Partition).to receive(:new).and_return(partition)
    end

    it 'creates a partition' do
      expect(Scalastic::Partition).to receive(:new).once.with(es_client, config, id).and_return(partition)
      client[id]
    end

    it 'return the partition' do
      expect(client[id]).to eq partition
    end
  end

  describe '#to_a' do
    let(:es_aliases) do {
      'index3' => {'aliases' => {}},
      'index2' => {
        'aliases' => {
          'scalastic_1_search' => {'filter' => {'term' => {'scalastic_partition_id' => 1}}},
          'scalastic_1_index' => {},
          'scalastic_2_search' => {'filter' => {'term' => {'scalastic_partition_id' => 2}}},
          'scalastic_2_index' => {},
          'unrelated' => 'some definition'
        },
      },
      'index1' => {
        'aliases' => {
          'scalastic_1_search' => {'filter' => {'term' => {'scalastic_partition_id' => 1}}},
        },
      }
    }
    end

    before(:each) do
      allow(indices_client).to receive(:get_aliases).and_return(es_aliases)
    end

    it 'returns all aliases' do
      expect(client.to_a.map{|p| p.id}).to eq [1,2]
    end
  end

  describe '#prepare_index' do
    let(:index) {'index'}
    let(:expected_mapping) {{properties: {config.partition_selector => {type: 'long'}}}}

    it 'rejects missing index' do
      expect{client.prepare_index({})}.to raise_error(ArgumentError, 'Missing required argument :index')
    end

    it 'rejects empty index' do
      expect{client.prepare_index(index: nil)}.to raise_error(ArgumentError, 'Missing required argument :index')
    end

    it 'sets the mappings' do
      expect(indices_client).to receive(:put_mapping).with(index: index, type: '_default_', body: {'_default_' => expected_mapping})
      expect(indices_client).to receive(:put_mapping).with(index: index, type: 'scalastic', body: {'scalastic' => expected_mapping})
      client.prepare_index(index: index)
    end
  end
end
