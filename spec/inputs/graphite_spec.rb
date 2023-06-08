require "logstash/devutils/rspec/spec_helper"
require "logstash/devutils/rspec/shared_examples"
require "logstash/inputs/graphite"
require "logstash/timestamp"
require "time"
require_relative "../spec_helper"

describe LogStash::Inputs::Graphite do

  before do
    srand(RSpec.configuration.seed)
  end

  let(:host) { "127.0.0.1" }
  let(:port) { rand(1024..65535) }
  let(:queue) { [] }
  let(:client) { Stud::try(5.times) { TCPSocket.new(host, port) } }
  let!(:helper) { TcpHelpers.new }

  subject { LogStash::Inputs::Graphite.new("host" => host, "port" => port) }

  after :each do
    subject.close rescue nil
  end

  describe "register" do
    it "should register without errors" do
      expect { subject.register }.to_not raise_error
    end
  end

  describe "receive" do

    before(:each) do
      subject.register
    end

    it "should parse a graphite message" do
      result = helper.pipelineless_input(subject, 1) do
        client.write "a.b.c 10 N\n"
      end
      expect(result.size).to eq(1)
      expect(result.first.to_hash).to include({"a.b.c" => 10})
    end

    it "should parse a graphite message with floats" do
      result = helper.pipelineless_input(subject, 1) do
        client.write "a.b.c 10.2 N\n"
      end
      expect(result.size).to eq(1)
      expect(result.first.to_hash).to include({"a.b.c" => 10.2})
    end

    it "should support using N as current timestamp" do
      result = helper.pipelineless_input(subject, 1) do
        client.write "a.b.c 10 N\n"
      end
      expect(result.size).to eq(1)
      # 10 seconds squew should provide ample margin for any tests run slowdown
      expect(result.first.get("@timestamp").to_i).to be_within(10).of(LogStash::Timestamp.now.to_i)
    end

    it "should support using N as current timestamp" do
      time = Time.now
      result = helper.pipelineless_input(subject, 1) do
        client.write "a.b.c 10 #{time.to_i}\n"
      end
      expect(result.size).to eq(1)
      expect(result.first.get("@timestamp")).to eq(LogStash::Timestamp.at(time.to_i))
    end
  end

  it_behaves_like "an interruptible input plugin" do
    let(:config) { { "port" => port } }
  end
end
