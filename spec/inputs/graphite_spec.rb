require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/graphite"
require "logstash/timestamp"
require "time"

describe LogStash::Inputs::Graphite do
  before do
    srand(RSpec.configuration.seed)
  end

  let(:host) { "127.0.0.1" }
  let(:port) { rand(5000) + 1025 }
  let(:queue) { [] }

  let(:client) { TCPSocket.new(host, port) }

  subject { LogStash::Inputs::Graphite.new("host" => host, "port" => port) }
  before :each do
    subject.register
    Thread.new { subject.run(queue) }
  end

  after :each do
    subject.teardown
  end

  it "should parse a graphite message" do
    client.write "a.b.c 10 N\n"
    sleep 0.01 until queue.size == 1
    expect(queue.first.to_hash).to include({"a.b.c" => 10})
  end

  it "should parse a graphite message with floats" do
    client.write "a.b.c 10.2 N\n"
    sleep 0.01 until queue.size == 1
    expect(queue.first.to_hash).to include({"a.b.c" => 10.2})
  end

  it "should support using N as current timestamp" do
    time = LogStash::Timestamp.new(Time.now)
    expect(Time).to receive(:now) { time }
    client.write "a.b.c 10 N\n"
    sleep 0.01 until queue.size == 1
    expect(queue.first["@timestamp"]).to eq(time)
  end

  it "should support using N as current timestamp" do
    time = Time.at(Time.now.to_i) # truncate at the second
    client.write "a.b.c 10 #{time.to_i}\n"
    sleep 0.01 until queue.size == 1
    expect(queue.first["@timestamp"]).to eq(LogStash::Timestamp.new(time))
  end

end
