# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/site24x7"
require "logstash/codecs/plain"
require "logstash/event"

describe LogStash::Outputs::Site24x7 do
  let(:sample_event) { LogStash::Event.new }
  let(:output) { LogStash::Outputs::Site24x7.new }

  before do
    output.register
  end

  describe "receive message" do
    subject { output.receive(sample_event) }

    it "returns a string" do
      expect(subject).to eq("Event received")
    end
  end
end
