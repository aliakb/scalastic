require 'spec_helper'

describe Scalastic::Partition do
  let(:config) {Scalastic::Config.new}
  let(:es_client) {double('ES client')}
  let(:id) {[1, 2].sample}

  let(:partition) {described_class.new(es_client, config, id)}
  let(:index_endpoint) {config.index_endpoint(id)}
  let(:search_endpoint) {config.search_endpoint(id)}

  describe '.new' do
    it 'rejects nil ES client' do
      expect{described_class.new(nil, config, id)}.to raise_error(ArgumentError)
    end

    it 'rejects nil config' do
      expect{described_class.new(es_client, nil, id)}.to raise_error(ArgumentError)
    end

    it 'rejects nil id' do
      expect{described_class.new(es_client, config, nil)}.to raise_error(ArgumentError)
    end

    it 'rejects empty id' do
      expect{described_class.new(es_client, config, '')}.to raise_error(ArgumentError)
    end

    it 'accepts correct arguments' do
      expect{partition}.not_to raise_error
    end

    it 'assigns ES client' do
      expect(partition.es_client).to eq es_client
    end

    it 'assigns config' do
      expect(partition.config).to eq config
    end

    it 'assigns id' do
      expect(partition.id).to eq id
    end
  end

  describe '#extend_to' do
    let(:index) {'index'}
    let(:args) {{index: index}}
    let(:indices_client) {mock_indices_client}
    let(:existing_aliases_data) {{"index1"=>{"aliases"=>{"scalastic_#{id}_index"=>{}}}}}

    before(:each) do
      allow(es_client).to receive(:indices).and_return(indices_client)
    end

    def mock_indices_client
      double('ES indices client').tap do |c|
        allow(c).to receive(:get_aliases).and_return(existing_aliases_data)
        allow(c).to receive(:update_aliases)
      end
    end

    it 'rejects nil index' do
      expect{partition.extend_to(index: nil)}.to raise_error(ArgumentError)
    end

    it 'rejects empty index' do
      expect{partition.extend_to(index: '')}.to raise_error(ArgumentError)
    end

    it 'accepts correct index' do
      expect{partition.extend_to(index: index)}.not_to raise_error
    end
  end

  describe '#search' do
    let(:endpoint) {config.search_endpoint(id)}
    let(:search_results) {double('Search results')}

    let(:search_args_empty) {{}}
    let(:search_args_full) {{type: 'type', search_type: 'count', body: {query: {match_all: {}}}}}
    let(:search_args) {[[search_args_empty], [search_args_full], []].sample}

    before(:each) do
      allow(es_client).to receive(:search).and_return(search_results)
    end

    it 'calls ES' do
      expect(es_client).to receive(:search).once.and_return(search_results)
      partition.search(*search_args)
    end

    it 'returns search results' do
      expect(partition.search(*search_args)).to eq search_results
    end

    context 'with no args' do
      it 'calls ES client' do
        expect(es_client).to receive(:search).once.with(index: endpoint).and_return(search_results)
        partition.search
      end
    end

    context 'with empty args' do
      it 'calls ES client' do
        expect(es_client).to receive(:search).once.with(index: endpoint).and_return(search_results)
        partition.search({})
      end
    end

    context 'with full args' do
      it 'calls ES client' do
        expected_args = search_args_full.merge(index: endpoint)
        expect(es_client).to receive(:search).once.with(expected_args).and_return(search_results)
        partition.search(search_args_full)
      end
    end
  end

  describe '#get' do
    let(:endpoint) {config.search_endpoint(id)}
    let(:get_results) {double('get results')}
    let(:document_id) {[10,20].sample}
    let(:document_type) {'document_type'}

    before(:each) do
      allow(es_client).to receive(:get).and_return(get_results)
    end

    it 'calls ES' do
      expect(es_client).to receive(:get).once.with(index: endpoint, id: document_id, type: document_type).and_return(get_results)
      partition.get(id: document_id, type: document_type)
    end

    it 'returns correct results' do
      expect(partition.get(id: document_id, type: document_type)).to eq get_results
    end
  end

  describe '#index' do
    let(:endpoint) {config.index_endpoint(id)}
    let(:index_results) {double('Index results')}

    let(:index_args_no_body) {{type: 'type', id: id}}
    let(:index_args_full) {{type: 'type',  id: id, body: {field1: 1, field2: 2}}}
    let(:index_args) {[[index_args_no_body], [index_args_full]].sample}

    before(:each) do
      allow(es_client).to receive(:index).and_return(index_results)
    end

    it 'calls ES' do
      expect(es_client).to receive(:index).once.and_return(index_results)
      partition.index(*index_args)
    end

    it 'returns index results' do
      expect(partition.index(*index_args)).to eq index_results
    end

    context 'with no body' do
      it 'calls ES' do
        expect(es_client).to receive(:index).once.with(index: endpoint, type: 'type', id: id, body: {config.partition_selector.to_sym => id}).and_return(index_results)
        partition.index(index_args_no_body)
      end
    end

    context 'with body' do
      it 'calls ES' do
        expect(es_client).to receive(:index).once.with(index: endpoint, type: 'type', id: id, body: {config.partition_selector.to_sym => id, field1: 1, field2: 2}).and_return(index_results)
        partition.index(index_args_full)
      end
    end

    context 'with complex selector' do
      let(:partition_selector) {'parent.partition_id'}

      before(:each) do
        allow(config).to receive(:partition_selector).and_return(partition_selector)
      end

      it 'calls ES' do
        expect(es_client).to receive(:index).once.with(index: endpoint, type: 'type', id: id, body:{parent:{partition_id: id}}).and_return(index_results)
        partition.index(index_args_no_body)
      end
    end
  end

  describe '#delete' do
    let(:endpoint) {config.search_endpoint(id)}
    let(:delete_results) {double('Delete results')}
    let(:delete_args) {{type: 'type', id: id}}

    before(:each) do
      allow(es_client).to receive(:delete).and_return(delete_results)
    end

    it 'deletes from ES' do
      expect(es_client).to receive(:delete).once.with({index: endpoint}.merge(delete_args)).and_return(delete_results)
      partition.delete(delete_args)
    end

    it 'returns deletion resuts' do
      expect(partition.delete(delete_args)).to eq delete_results
    end
  end

  describe '#inspect' do
    it 'returns correct value' do
      expect(partition.inspect).to eq "ES partition #{id}"
    end
  end

  describe '#exists?' do
    let(:id) {[1,2,3].sample}
    let(:indices_client) {mock_indices_client}

    let(:es_aliases) {
      {
        "index1" => {
          "aliases" => {
            "scalastic_1_search" => {"filter" => {"term" => {"scalastic_partition_id" => 1}}},
            "scalastic_1_index" => {},
            "scalastic_2_search" => {"filter" => {"term" => {"scalastic_partition_id" => 2}}}
          }
        }
      }
    }

    before(:each) do
      allow(es_client).to receive(:indices).and_return(indices_client)
    end

    def mock_indices_client
      double('indices').tap do |i|
        allow(i).to receive(:get_aliases).and_return(es_aliases)
      end
    end

    it 'calls indices client' do
      endpoints = [config.search_endpoint(id), config.index_endpoint(id)].join(',')
      expect(indices_client).to receive(:get_aliases).once.with(name: endpoints).and_return(es_aliases)
      partition.exists?
    end

    context 'for a complete partition' do
      let(:id) {1}

      it 'is true' do
        expect(partition.exists?).to eq true
      end
    end

    context 'for a read only partition' do
      let(:id) {2}

      it 'is true' do
        expect(partition.exists?).to eq true
      end
    end

    context 'for a non-existing partition' do
      let(:id) {3}
      let(:es_aliases) {
        {
          "index1" => {"aliases" => {}},
          "index2" => {"aliases" => {}},
        }
      }

      it 'is false' do
        expect(partition.exists?).to eq false
      end
    end
  end

  describe '#bulk' do
    let(:input) {{body: [{index: {_id: 123, _type: 'test_type', data: {}}}]}}
    let(:endpoint) {config.index_endpoint(partition.id)}
    let(:partition_selector) {config.partition_selector}

    before(:each) do
      allow(es_client).to receive(:bulk)
    end

    it 'throws without :body' do
      expect{partition.bulk(key: 'value')}.to raise_error(ArgumentError, 'Missing required argument :body')
    end

    it 'calls ES' do
      expect(es_client).to receive(:bulk).once
      partition.bulk(input)
    end

    it 'returns values from ES' do
      expected_results = double('results')
      allow(es_client).to receive(:bulk).and_return(expected_results)

      expect(partition.bulk(input)).to eq expected_results
    end

    context 'creating' do
      let(:input) {{body: [{create: {_type: 'test', _id: 1, data: {field1: 'value1'}}}, {create: {_type: 'test', _id: 2}}, {field2: 'value2'}]}}

      it 'calls ES with correct arguments' do
        expected_es_input = {body: [
          {create: {_index: endpoint, _type: 'test', _id: 1, data: {field1: 'value1', partition_selector.to_sym => partition.id}}},
          {create: {_index: endpoint, _type: 'test', _id: 2}},
          {field2: 'value2', partition_selector.to_sym => partition.id}
        ]}
        expect(es_client).to receive(:bulk).with(expected_es_input)
        partition.bulk(input)
      end
    end

    context 'indexing' do
      let(:input) {{body: [{index: {_type: 'test', _id: 1, data: {field1: 'value1'}}}, {index: {_type: 'test', _id: 2}}, {field2: 'value2'}]}}

      it 'calls ES with correct arguments' do
        expected_es_input = {body: [
          {index: {_index: endpoint, _type: 'test', _id: 1, data: {field1: 'value1', partition_selector.to_sym => partition.id}}},
          {index: {_index: endpoint, _type: 'test', _id: 2}},
          {field2: 'value2', partition_selector.to_sym => partition.id}
        ]}
        expect(es_client).to receive(:bulk).with(expected_es_input)
        partition.bulk(input)
      end
    end

    context 'updating' do
      let(:input) {{body: [{update: {_type: 'test', _id: 1, data: {doc: {field1: 'value1'}}}}, {update: {_type: 'test', _id: 2}}, {doc: {field2: 'value2'}}]}}

      it 'calls ES with correct arguments' do
        expected_es_input = {body: [
          {update: {_index: endpoint, _type: 'test', _id: 1, data: {doc: {field1: 'value1'}}}},
          {update: {_index: endpoint, _type: 'test', _id: 2}},
          {doc: {field2: 'value2'}}
        ]}
        expect(es_client).to receive(:bulk).with(expected_es_input)
        partition.bulk(input)
      end
    end

    context 'deleting' do
      let(:input) {{body: [{delete: {_type: 'test', _id: 1}}, {delete: {_type: 'test', _id: 2}}]}}

      it 'calls ES with correct arguments' do
        expected_es_input = {body: [
          {delete: {_index: endpoint, _type: 'test', _id: 1}},
          {delete: {_index: endpoint, _type: 'test', _id: 2}},
        ]}
        expect(es_client).to receive(:bulk).with(expected_es_input)
        partition.bulk(input)
      end
    end
  end

  describe '#delete_by_query' do
    let(:input) {{body: {query: {term: {field: 'value'}}}}}
    let(:search_result) {{'_scroll_id' => 'scroll id 2'}}
    let(:scroll_results) do
      [
        {'_scroll_id' => 'scroll id 2', 'hits' => {'total' => 2, 'hits' => [{'_index' => 'index1', '_id' => '1', '_type' => 'type1'}]}},
        {'_scroll_id' => 'scroll id 3', 'hits' => {'total' => 2, 'hits' => [{'_index' => 'index2', '_id' => '2', '_type' => 'type2'}]}},
        {'_scroll_id' => 'scroll id 4', 'hits' => {'total' => 2, 'hits' => []}}
      ]
    end

    before(:each) do
      allow(es_client).to receive(:search).and_return(search_result)
      allow(es_client).to receive(:scroll).and_return(*scroll_results)
      allow(es_client).to receive(:bulk)
    end

    it 'deletes all documents' do
      expect(es_client).to receive(:bulk).ordered.with(body: [{delete: {_index: 'index1', _type: 'type1', _id: '1'}}])
      expect(es_client).to receive(:bulk).ordered.with(body: [{delete: {_index: 'index2', _type: 'type2', _id: '2'}}])
      partition.delete_by_query(input)
    end

    it 'performs a search' do
      expect(es_client).to receive(:search).once.with(input.merge(index: search_endpoint, search_type: 'scan', scroll: '1m', size: 500, fields: [])).and_return(search_result)
      partition.delete_by_query(input)
    end
  end
end
