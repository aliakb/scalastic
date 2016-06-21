require 'spec_helper'

describe Scalastic::Partition do
  let(:config) {Scalastic::Config.new}
  let(:es_client) {double('ES client')}
  let(:id) {[1, 2].sample}

  let(:partition) {described_class.new(es_client, config, id)}
  let(:index_endpoint) {config.index_endpoint(id)}
  let(:search_endpoint) {config.search_endpoint(id)}

  def mock_endpoint(args)
    double('endpoint', args)
  end

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

  describe '#get_endpoints' do
    let(:indices_client) {mock_indices_client}
    let(:missing_partition_response) {{'index_1' => {'aliases' => {}}}}
    let(:read_only_response) {{'index_1'=>{'aliases'=>{"scalastic_#{id}_search"=>{'filter'=>{'term'=>{'scalastic_partition_id'=>id}}}}}}}
    let(:single_index_response) {{'index_1'=>{'aliases'=>{"scalastic_#{id}_index"=>{}, "scalastic_#{id}_search"=>{'filter'=>{'term'=>{'scalastic_partition_id'=>id}}}}}}}
    let(:with_routing_response) {{'index_1'=>{'aliases'=>{"scalastic_#{id}_index"=>{'index_routing'=>'123', 'search_routing'=>'123'}, "scalastic_#{id}_search"=>{'filter'=>{'term'=>{'scalastic_partition_id'=>id}}, 'index_routing'=>'123', 'search_routing'=>'123'}}}}}
    let(:get_aliases_response) {[missing_partition_response,read_only_response,single_index_response,with_routing_response].sample}

    let(:endpoints) {partition.get_endpoints}

    before(:each) do
      allow(es_client).to receive(:indices).and_return(indices_client)
    end

    def mock_indices_client
      double('indices').tap do |ic|
        allow(ic).to receive(:get_aliases).and_return(get_aliases_response)
      end
    end

    it 'gets aliases from ES' do
      expected_names = [search_endpoint, index_endpoint].join(',')
      expect(es_client.indices).to receive(:get_aliases).once.with(name: expected_names).and_return(get_aliases_response)
      endpoints
    end

    it 'returns endpoints' do
      expect(endpoints).not_to be_nil
    end

    it 'returns frozen object' do
      expect(endpoints).to be_frozen
    end

    it 'returns search endpoints in an array' do
      expect(endpoints.search).to be_kind_of(Array)
    end

    it 'returns frozen array for search endpoints' do
      expect(endpoints.search).to be_frozen
    end

    context 'with a missing partition' do
      let(:get_aliases_response) {missing_partition_response}

      it 'returns nil for the index endpoint' do
        expect(endpoints.index).to be_nil
      end

      it 'returns an empty array for search endpoint' do
        expect(endpoints.search).to be_empty
      end
    end

    context 'with a read only partition' do
      let(:get_aliases_response) {read_only_response}

      it 'returns nil for the index endpoint' do
        expect(endpoints.index).to be_nil
      end

      it 'returns one search endpoint' do
        expect(endpoints.search.size).to eq 1
      end

      it 'returns correct search endpoint' do
        se = endpoints.search.first
        expect(se.index).to eq 'index_1'
        expect(se.routing).to be_nil
      end

      it 'returns frozen search endpoint' do
        expect(endpoints.search.first).to be_frozen
      end
    end

    context 'with a single index partition' do
      let(:get_aliases_response) {single_index_response}

      it 'has correct index endpoint' do
        expect(endpoints.index.index).to eq 'index_1'
        expect(endpoints.index.routing).to be_nil
      end

      it 'has frozen index endpoint' do
        expect(endpoints.index).to be_frozen
      end

      it 'has exactly one search endpoint' do
        expect(endpoints.search.size).to eq 1
      end

      it 'has correct search endpoint' do
        se = endpoints.search.first
        expect(se.index).to eq 'index_1'
        expect(se.routing).to be_nil
      end
    end

    context 'with routing' do
      let(:get_aliases_response) {with_routing_response}

      it 'has routing on index endpoint' do
        expect(endpoints.index.routing).to eq '123'
      end

      it 'has routing on search endpoint' do
        expect(endpoints.search.first.routing).to eq '123'
      end
    end
  end

  describe '#index_to' do
    let(:missing_partition_endpoints) {double('partitions', index: nil, search: [])}
    let(:read_only_partition_endpoints) {double('partitions', index: nil, search: mock_endpoint(index: 'index_1'))}
    let(:multi_index_partition_endpoints) {double('partitions', index: mock_endpoint(index: 'index_2'), search: [mock_endpoint(index: 'index_1'), mock_endpoint(index: 'index_2')])}

    let(:endpoints) {[missing_partition_endpoints, read_only_partition_endpoints, multi_index_partition_endpoints].sample}
    let(:indices_client) {mock_indices_client}

    before(:each) do
      allow(partition).to receive(:get_endpoints).and_return(endpoints)
      allow(es_client).to receive(:indices).and_return(indices_client)
    end

    def mock_indices_client
      double('indices').tap do |ic|
        allow(ic).to receive(:update_aliases)
      end
    end

    context 'to nil' do
      context 'with editable partition' do
        before(:each) do
          allow(partition).to receive(:get_endpoints).and_return(multi_index_partition_endpoints)
        end

        it 'deletes the index alias' do
          expect(indices_client).to receive(:update_aliases).once.with(body: {actions: [{remove: {index: 'index_2', alias: "scalastic_#{id}_index"}}]})
          partition.index_to nil
        end
      end

      context 'with missing partition' do
        before(:each) do
          allow(partition).to receive(:get_endpoints).once.and_return(missing_partition_endpoints)
        end

        it 'doesn\'t call ES' do
          expect(indices_client).not_to receive(:update_aliases)
          partition.index_to nil
        end
      end

      context 'with read only partition' do
        before(:each) do
          allow(partition).to receive(:get_endpoints).once.and_return(read_only_partition_endpoints)
        end

        it 'doesn\'t call ES' do
          expect(indices_client).not_to receive(:update_aliases)
          partition.index_to nil
        end
      end
    end

    context 'setting a new index' do
      let(:index) {'index_3'}

      before(:each) do
        allow(partition).to receive(:get_endpoints).and_return(read_only_partition_endpoints)
      end

      it 'updates ES' do
        expect(indices_client).to receive(:update_aliases).once.with(body: {actions: [{add: {index: 'index_3', alias: "scalastic_#{id}_index"}}]})
        partition.index_to index: index
      end

      context 'with routing' do
        let(:routing) {123}

        it 'updates ES' do
          expect(indices_client).to receive(:update_aliases).once.with(body: {actions: [{add: {index: 'index_3', alias: "scalastic_#{id}_index", routing: routing}}]})
          partition.index_to index: index, routing: routing
        end
      end
    end

    context 'when switching indices' do
      let(:index) {'index_1'}

      before(:each) do
        allow(partition).to receive(:get_endpoints).and_return(multi_index_partition_endpoints)
      end

      it 'updates ES' do
        expect(indices_client).to receive(:update_aliases).once.with(body: {actions: [{remove: {index: 'index_2', alias: "scalastic_#{id}_index"}}, {add: {index: 'index_1', alias: "scalastic_#{id}_index"}}]})
        partition.index_to index: index
      end
    end
  end

  describe '#readonly?' do
    let(:read_only_endpoints) {double('endpoints', index: nil, search: [mock_endpoint(index: 'index')])}
    let(:editable_endpoints) {double('endpoints', index: mock_endpoint(index: 'index'), search: [mock_endpoint(index: 'index')])}
    let(:endpoints) {[read_only_endpoints, editable_endpoints].sample}

    before(:each) do
      allow(partition).to receive(:get_endpoints).and_return(endpoints)
    end

    context 'with readonly partition' do
      let(:endpoints) {read_only_endpoints}

      it 'returns true' do
        expect(partition.readonly?).to be true
      end
    end

    context 'with editable partition' do
      let(:endpoints) {editable_endpoints}

      it 'returns false' do
        expect(partition.readonly?).to be false
      end
    end
  end

  describe '#mget' do
    let(:mget_results) {double('from mget')}
    let(:ids_input) {{body: {type: 'test', ids: [1,2,3]}}}
    let(:docs_input) {{body: {docs: [{_type: 'test', _id: 1}, {_type: 'test', _id: 2}]}}}
    let(:input) {[ids_input, docs_input].sample}

    before(:each) do
      allow(es_client).to receive(:mget).and_return(mget_results)
    end

    it 'returns mget results' do
      expect(partition.mget(input)).to eq mget_results
    end

    context 'with ids' do
      let(:input) {ids_input}

      it 'calls the ES' do
        expect(es_client).to receive(:mget).once.with({index: search_endpoint}.merge(input)).and_return mget_results
        partition.mget(input)
      end
    end

    context 'with docs' do
      let(:input) {docs_input}

      it 'calls the ES' do
        expect(es_client).to receive(:mget).once.with({index: search_endpoint}.merge(input)).and_return(mget_results)
        partition.mget(input)
      end
    end
  end

  describe '#create' do
    let(:create_results) {double('from create')}
    let(:args) {{id: 1, type: 'test', body: {subject: 'This is a test'}}}

    before(:each) do
      allow(es_client).to receive(:create).and_return(create_results)
    end

    it 'returns correct results' do
      expect(partition.create(args)).to eq create_results
    end

    it 'calls the ES' do
      expect(es_client).to receive(:create).once.with(args.merge(index: index_endpoint)).and_return(create_results)
      partition.create(args)
    end
  end
end
