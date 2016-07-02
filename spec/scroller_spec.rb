require 'spec_helper'
require 'scalastic/scroller'

describe Scalastic::Scroller  do
  let(:es_client) {mock_es_client}
  let(:args) {{index: 'whatever'}}

  let(:scroller) {described_class.new(es_client, args)}
  subject {scroller}

  def mock_es_client
    double('es client').tap do |c|
      allow(c).to receive(:search)
      allow(c).to receive(:scroll)
    end
  end

  it {is_expected.to be_kind_of Enumerable}
  it {is_expected.to respond_to :scroll}

  describe '#scroll' do
    it 'has a default value' do
      expect(scroller.scroll).to eq '1m'
    end

    it 'rejects nil value' do
      expect{scroller.scroll = nil}.to raise_error(ArgumentError, 'scroll cannot be empty!')
    end

    it 'accepts correct value' do
      scroller.scroll = '2m'
      expect(scroller.scroll).to eq '2m'
    end
  end

  describe '#each' do
    let(:scroll) {%w(1m 2m 3h).sample}
    let(:search_results) {{'_scroll_id' => 'scroll_1'}}
    let(:all_search_hits) {10.times.map{|i| double("Hit #{i + 1}")}}

    let(:scroll_results) {[
      {'_scroll_id' => 'scroll_2', 'hits' => {'hits' => all_search_hits[0..7]}},
      {'_scroll_id' => 'scroll_3', 'hits' => {'hits' => all_search_hits[8..9]}},
      {'hits' => {'hits' => []}}
    ]}

    before(:each) do
      scroller.scroll = scroll

      allow(es_client).to receive(:search).and_return(search_results)
      allow(es_client).to receive(:scroll).and_return(*scroll_results)
    end

    it 'extracts all hits' do
      expect(scroller.to_a).to eq all_search_hits
    end

    it 'passes correct arguments' do
      expect(es_client).to receive(:search).once.ordered.with(args.merge(search_type: 'scan', scroll: scroll)).and_return(search_results)
      expect(es_client).to receive(:scroll).ordered.with(body: 'scroll_1', scroll: scroll).and_return(scroll_results[0])
      expect(es_client).to receive(:scroll).ordered.with(body: 'scroll_2', scroll: scroll).and_return(scroll_results[1])
      expect(es_client).to receive(:scroll).ordered.with(body: 'scroll_3', scroll: scroll).and_return(scroll_results[2])

      scroller.to_a
    end
  end
end
