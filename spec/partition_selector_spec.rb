require 'spec_helper'

describe Scalastic::PartitionSelector do
  describe '#apply_to' do
    let(:partition_id) {[1,2].sample}
    let(:selector) {described_class.new(field_name, partition_id)}

    context 'with a simple field' do
      let(:field_name) {"partition_id"}

      it 'initializes an empty body' do
        expect(selector.apply_to({})).to eq ({partition_id: partition_id})
      end

      it 'updates the original body' do
        body = {}
        selector.apply_to(body)
        expect(body).to eq ({partition_id: partition_id})
      end

      it 'initializes non-empty body' do
        expect(selector.apply_to(field1: 1, field2: 2)).to eq ({partition_id: partition_id, field1: 1, field2: 2})
      end
    end

    context 'with a complex field' do
      let(:field_name) {'parent.child.partition_id'}

      it 'initializes an empty body' do
        expect(selector.apply_to({})).to eq ({parent: {child: {partition_id: partition_id}}})
      end

      it 'initializes non-empty body' do
        expect(selector.apply_to(top: 'top', parent:{child: {extra_field: 'extra'}})).to eq ({top: 'top', parent: {child: {extra_field: 'extra', partition_id: partition_id}}})
      end
    end
  end
end
