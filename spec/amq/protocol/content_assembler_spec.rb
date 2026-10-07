# encoding: binary

module AMQ
  module Protocol
    RSpec.describe ContentAssembler do
      # Builds a decoded method frame from raw method arguments (without the
      # class/method id prefix), like a real IO adapter after frame parsing.
      def method_frame_for(klass, data, channel)
        packed = klass.instance_variable_get(:@packed_indexes)
        MethodFrame.new(packed.dup << data, channel)
      end

      def header_frame(channel, body_size, properties)
        HeaderFrame.new(Basic.encode_properties(body_size, properties), channel)
      end

      def deliver_method
        "\x04ctag\x00\x00\x00\x00\x00\x00\x00\x01\x00\x03foo\x00".b
      end

      def return_method
        "\x019\fNO_CONSUMERS\namq.fanout\x00".b
      end

      def publish_frames(channel, body, routing_key, frame_size = 512)
        Basic::Publish.encode(
          channel, body,
          {:content_type => "text/plain", :delivery_mode => 2},
          "amq.topic", routing_key, false, false, frame_size
        )
      end

      describe "#<< and #assemble" do
        it "assembles a Basic.Publish frameset and preserves method params, properties and body" do
          assembler = described_class.new
          frames    = publish_frames(1, "Hello World!", "example.key")

          expect(assembler << frames[0]).to be_nil
          expect(assembler << frames[1]).to be_nil
          content = assembler << frames[2]

          expect(content).to be_a(Content)
          expect(content.channel).to eq(1)
          expect(content.method).to be_a(Basic::Publish)
          expect(content.method.exchange).to eq("amq.topic")
          expect(content.method.routing_key).to eq("example.key")
          expect(content.method.mandatory).to eq(false)
          expect(content.method.immediate).to eq(false)
          expect(content.properties).to eq({:content_type => "text/plain", :delivery_mode => 2})
          expect(content.body).to eq("Hello World!")
          expect(assembler).to be_complete
        end

        it "decodes a received publish frame symmetrically with .encode" do
          frames = Basic::Publish.encode(
            3, "payload", {:content_type => "text/plain"},
            "ex", "rk", true, false, 512
          )

          method = frames[0].decode_payload
          expect(method).to be_a(Basic::Publish)
          expect(method.exchange).to eq("ex")
          expect(method.routing_key).to eq("rk")
          expect(method.mandatory).to eq(true)
          expect(method.immediate).to eq(false)
        end

        it "assembles a Basic.Deliver frameset" do
          assembler = described_class.new
          assembler << method_frame_for(Basic::Deliver, deliver_method, 1)
          assembler << header_frame(1, 3, {:content_type => "text/plain"})
          content = assembler << BodyFrame.new("abc", 1)

          expect(content.method).to be_a(Basic::Deliver)
          expect(content.method.consumer_tag).to eq("ctag")
          expect(content.method.delivery_tag).to eq(1)
          expect(content.method.exchange).to eq("foo")
          expect(content.body).to eq("abc")
        end

        it "assembles a Basic.Return frameset" do
          assembler = described_class.new
          assembler << method_frame_for(Basic::Return, return_method, 1)
          assembler << header_frame(1, 0, {:content_type => "text/plain"})
          completed = assembler.drain

          expect(completed.size).to eq(1)
          content = completed.first
          expect(content.method).to be_a(Basic::Return)
          expect(content.method.reply_code).to eq(313)
          expect(content.method.reply_text).to eq("NO_CONSUMERS")
          expect(content.method.exchange).to eq("amq.fanout")
          expect(content.body).to eq("")
        end

        it "concatenates several body frames up to the header-declared size" do
          assembler = described_class.new
          frames    = publish_frames(1, "x" * 20, "k", 21)

          expect(frames.size).to eq(4)
          content = assembler.assemble(frames).first

          expect(content.body).to eq("x" * 20)
          expect(content.body_size).to eq(20)
        end

        it "completes a zero-length body as soon as the header arrives" do
          assembler = described_class.new
          frames    = publish_frames(1, "", "k")

          expect(frames.size).to eq(2)
          content = assembler.assemble(frames).first

          expect(content.body).to eq("")
          expect(assembler.pending).to eq({})
        end
      end

      describe "interleaved channels" do
        it "tracks channels independently and completes messages in arrival order" do
          assembler = described_class.new
          frames_a  = publish_frames(1, "AAAA", "a")
          frames_b  = publish_frames(2, "BB", "b")

          completed = []
          completed << (assembler << frames_a[0])
          completed << (assembler << frames_b[0])
          expect(assembler.channels).to match_array([1, 2])
          expect(assembler[1][:status]).to eq(:awaiting_header)

          completed << (assembler << frames_b[1])
          completed << (assembler << frames_a[1])
          expect(assembler.pending[1][:status]).to eq(:awaiting_body)
          expect(assembler.pending[2][:remaining]).to eq(2)

          completed << (assembler << frames_b[2])
          completed << (assembler << frames_a[2])
          completed.compact!

          expect(completed.map { |content| [content.channel, content.body] }).to eq([
            [2, "BB"], [1, "AAAA"]
          ])
          expect(assembler).to be_complete
        end
      end

      describe "non-content frames" do
        it "ignores heartbeats without advancing any message" do
          assembler = described_class.new
          frames    = publish_frames(7, "abc", "k")

          assembler << frames[0]
          assembler << HeartbeatFrame.new("", 0)
          expect(assembler.pending?(7)).to eq(true)

          assembler << frames[1]
          assembler << HeartbeatFrame.new("", 0)
          expect(assembler.pending?(7)).to eq(true)
          expect(assembler[7][:received]).to eq(0)

          assembler << frames[2]
          expect(assembler.pending?(7)).to eq(false)
        end

        it "passes through non-content method frames without changing state" do
          assembler = described_class.new
          ack = Basic::Ack.encode(1, 99, false)
          expect(assembler << ack).to be_nil
          expect(assembler).to be_complete
        end
      end

      describe "error states" do
        it "raises UnexpectedMethodFrame with precise state when a method interrupts a body" do
          assembler = described_class.new
          frames    = publish_frames(1, "x" * 20, "first", 21)
          assembler.assemble(frames[0], frames[1], frames[2])

          error = nil
          begin
            assembler << publish_frames(1, "y", "second")[0]
          rescue UnexpectedMethodFrame => e
            error = e
          end

          expect(error).to be_a(UnexpectedMethodFrame)
          expect(error.channel).to eq(1)
          expect(error.method).to be_a(Basic::Publish)
          expect(error.state[:status]).to eq(:awaiting_body)
          expect(error.state[:expected]).to eq(20)
          expect(error.state[:received]).to eq(13)
          expect(error.state[:remaining]).to eq(7)
          expect(error.message).to include("basic.publish")
          expect(error.message).to include("7")
        end

        it "rejects a body frame after a zero-length header" do
          assembler = described_class.new
          frames    = publish_frames(1, "", "k")
          assembler.assemble(frames)

          expect {
            assembler << BodyFrame.new("unexpected", 1)
          }.to raise_error(UnexpectedBodyFrame) do |error|
            expect(error.channel).to eq(1)
            expect(error.state[:status]).to eq(:completed)
            expect(error.message).to include("zero-length body")
          end
        end

        it "rejects a header frame that is not preceded by a content method" do
          assembler = described_class.new

          expect {
            assembler << header_frame(1, 1, {:content_type => "text/plain"})
          }.to raise_error(UnexpectedHeaderFrame) do |error|
            expect(error.channel).to eq(1)
            expect(error.state[:status]).to eq(:idle)
          end
        end

        it "rejects a body frame without a header" do
          assembler = described_class.new

          expect {
            assembler << BodyFrame.new("abc", 1)
          }.to raise_error(ContentAssemblyError, /without a preceding method and header/)
        end

        it "rejects a body chunk larger than the remaining bytes" do
          assembler = described_class.new
          assembler << method_frame_for(Basic::Deliver, deliver_method, 1)
          assembler << header_frame(1, 3, {:content_type => "text/plain"})

          expect {
            assembler << BodyFrame.new("abcd", 1)
          }.to raise_error(ContentAssemblyError, /only 3 of 3 were expected/)
        end
      end

      describe "connection end" do
        it "reports which channels are still short of body bytes" do
          assembler = described_class.new
          frames    = publish_frames(4, "12345", "k")
          assembler << frames[0]
          assembler << frames[1]
          assembler << BodyFrame.new("12", 4)

          expect(assembler.finish[4]).to include(
            :status => :awaiting_body,
            :expected => 5,
            :received => 2,
            :remaining => 3
          )

          expect {
            assembler.finish!
          }.to raise_error(IncompleteContentError, /connection ended on channel 4 with 3 of 5 body bytes missing/)
        end

        it "reports a channel that never received its header" do
          assembler = described_class.new
          assembler << publish_frames(9, "abc", "k")[0]

          expect(assembler.finish[9][:status]).to eq(:awaiting_header)
          error = begin
                    assembler.finish!
                  rescue IncompleteContentError => e
                    e
                  end
          expect(error).to be_a(IncompleteContentError)
          expect(error.message).to include("channel 9 while waiting for the header frame")
        end

        it "returns nil from finish! when nothing is pending" do
          expect(described_class.new.finish!).to be_nil
        end
      end

      describe "frame-by-frame fixture" do
        it "shows which messages completed and which still wait for a body" do
          assembler = described_class.new
          log = []

          publish_frames(1, "abc", "one").each do |frame|
            content = assembler << frame
            log << [frame.class, content.nil? ? nil : content.channel, assembler.channels.sort]
          end

          expect(log).to eq([
            [MethodFrame,   nil, [1]],
            [HeaderFrame,   nil, [1]],
            [BodyFrame,     1,   []],
          ])
        end
      end
    end
  end
end
