require 'spec_helper'

describe Scalastic::Normalizer do
  let(:normalizer) {Scalastic::Normalizer}

  describe '.normalized' do
    context 'with string keys' do
      let(:input) {{'key1' => 'value1', 'key2' => {'key2.1' => [{'key2.1.1' => 1}, {'key2.1.2' => 2}]}}}

      it 'normalizes the input' do
        expect(normalizer.normalized(input)).to eq input
      end
    end

    context 'with symbol keys' do
      let(:input) {{key1: 'value1', key2: {:'key2.1' => [{:'key2.1.1' => 1}, {:'key2.1.2' => 2}]}}}

      it 'normalizes the input' do
        expect(normalizer.normalized(input)).to eq ({'key1' => 'value1', 'key2' => {'key2.1' => [{'key2.1.1' => 1}, {'key2.1.2' => 2}]}})
      end
    end
  end

  describe '.safe_get' do
    def safe_get(*keys) 
      normalizer.safe_get(input, *keys)
    end

    let(:stringified_input) {{'key1' => 'whatever', 'key2' => {'key3' => 'nested'}}}
    let(:symbolized_input) {{key1: 'whatever', key2: {key3: 'nested'}}}
    let(:input) {[stringified_input, symbolized_input].sample}

    it 'finds top-level item by string' do
      expect(safe_get('key1')).to eq 'whatever'
    end

    it 'finds top-level item by symbol' do
      expect(safe_get(:key1)).to eq 'whatever'
    end

    it 'finds nested item' do
      expect(safe_get('key2', :key3)).to eq 'nested'
    end

    it 'doesn\'t find missing top-level key' do
      expect(safe_get('key_a')).to be nil
    end

    it 'doesn\'t find missing nested key' do
      expect(safe_get('key_a', 'key_b')).to be nil
    end
  end
end
