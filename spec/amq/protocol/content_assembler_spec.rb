# encoding: binary

module AMQ
  module Protocol
    RSpec.describe ContentAssembler do
      subject(:assembler) { described_class.new }

      let(:properties) do
        {
          :content_type => "text/plain",
          :delivery_mode => 2,
          :headers => {"x" => 1}
        }
      end

      def short_string(value)
        [value.bytesize].pack(PACK_CHAR) + value
      end

      def return_frame(channel)
        payload = [313].pack(PACK_UINT16)
        payload << short_string("NO_CONSUMERS")
        payload << short_string("events")
        payload << short_string("returned")
        MethodFrame.new([Basic.method_id, Basic::Return.method_id].pack(PACK_UINT16_X2) + payload, channel)
      end

      def deliver_frame(channel)
        payload = short_string("consumer-1")
        payload << [99].pack(PACK_UINT64_BE)
        payload << [1].pack(PACK_CHAR)
        payload << short_string("events")
        payload << short_string("delivered")
        MethodFrame.new([Basic.method_id, Basic::Deliver.method_id].pack(PACK_UINT16_X2) + payload, channel)
      end

      def header_frame(channel, body, overrides = properties)
        HeaderFrame.new(Basic.encode_properties(body.bytesize, overrides), channel)
      end

      def publish_frames(channel, body, frame_size)
        Basic::Publish.encode(
          channel,
          body,
          properties,
          "events",
          "published",
          true,
          false,
          frame_size
        )
      end

      describe "#push" do
        it "assembles publish, return and deliver content while preserving method fields and properties" do
          publish = publish_frames(1, "publish body", 128)
          returned = return_frame(2)
          delivered = deliver_frame(3)

          publish_content = nil
          return_content = nil
          deliver_content = nil

          publish_content = assembler.push(publish[0])
          expect(publish_content).to be_nil
          expect(assembler.push(publish[1])).to be_nil
          publish_content = assembler.push(publish[2])

          expect(assembler.push(returned)).to be_nil
          expect(assembler.push(header_frame(2, "return body"))).to be_nil
          return_content = assembler.push(BodyFrame.new("return body", 2))

          expect(assembler.push(delivered)).to be_nil
          expect(assembler.push(header_frame(3, "deliver body"))).to be_nil
          deliver_content = assembler.push(BodyFrame.new("deliver body", 3))

          expect(publish_content).to have_attributes(
            :channel => 1,
            :body => "publish body",
            :properties => properties
          )
          expect(publish_content.method).to have_attributes(
            :exchange => "events",
            :routing_key => "published",
            :mandatory => true,
            :immediate => false
          )

          expect(return_content).to have_attributes(
            :channel => 2,
            :body => "return body",
            :properties => properties
          )
          expect(return_content.method).to have_attributes(
            :reply_code => 313,
            :reply_text => "NO_CONSUMERS",
            :exchange => "events",
            :routing_key => "returned"
          )

          expect(deliver_content).to have_attributes(
            :channel => 3,
            :body => "deliver body",
            :properties => properties
          )
          expect(deliver_content.method).to have_attributes(
            :consumer_tag => "consumer-1",
            :delivery_tag => 99,
            :redelivered => true,
            :exchange => "events",
            :routing_key => "delivered"
          )
        end

        it "allows frames from different channels to interleave across body frames" do
          publish = publish_frames(1, "hello world", 14)
          returned = return_frame(2)
          completed = []

          expect(assembler.push(publish[0])).to be_nil
          expect(assembler.push(returned)).to be_nil
          expect(assembler.push(HeartbeatFrame.new(EMPTY_STRING, 0))).to be_nil
          expect(assembler.push(publish[1])).to be_nil
          expect(assembler.push(publish[2])).to be_nil

          pending = assembler.pending
          expect(pending.keys).to eq([1, 2])
          expect(pending[1]).to have_attributes(
            :status => :waiting_for_body,
            :expected_bytes => 11,
            :received_bytes => 6,
            :remaining_bytes => 5
          )
          expect(pending[2]).to have_attributes(
            :status => :waiting_for_header,
            :method_class => Basic::Return
          )

          expect(assembler.push(HeartbeatFrame.new(EMPTY_STRING, 0))).to be_nil
          expect(assembler.pending[1].received_bytes).to eq(6)

          expect { completed << assembler.push(header_frame(2, "")) }.to change { completed.compact.size }.by(1)
          expect(completed.first.method).to be_a(Basic::Return)
          expect(assembler.pending.keys).to eq([1])

          completed << assembler.push(publish[3])
          expect(completed.last.body).to eq("hello world")
          expect(assembler.pending).to be_empty
          expect(assembler.finish).to eq([])
        end

        it "completes zero-byte content at the header frame" do
          publish = publish_frames(1, "", 128)

          expect(assembler.push(publish[0])).to be_nil
          content = assembler.push(publish[1])

          expect(content).to be_a(Content)
          expect(content).to have_attributes(
            :channel => 1,
            :body => EMPTY_STRING,
            :properties => properties
          )
          expect(assembler.pending).to be_empty
          expect(assembler.finish).to eq([])
        end

        it "ignores heartbeats and methods without content" do
          publish = publish_frames(1, "body", 128)

          expect(assembler.push(HeartbeatFrame.new(EMPTY_STRING, 0))).to be_nil
          expect(assembler.push(Basic::Qos.encode(1, 0, 0, false))).to be_nil
          expect(assembler.push(publish[0])).to be_nil
          expect(assembler.push(HeartbeatFrame.new(EMPTY_STRING, 0))).to be_nil
          expect(assembler.push(publish[1])).to be_nil
          expect(assembler.push(HeartbeatFrame.new(EMPTY_STRING, 0))).to be_nil
          content = assembler.push(publish[2])

          expect(content.body).to eq("body")
        end

        it "returns completed contents in frame completion order from #assemble" do
          first = publish_frames(1, "first", 128)
          second = publish_frames(2, "second", 128)

          contents = assembler.assemble([
            first[0],
            second[0],
            first[1],
            second[1],
            second[2],
            first[2]
          ])

          expect(contents.map { |content| [content.channel, content.body] }).to eq([
            [2, "second"],
            [1, "first"]
          ])
        end
      end

      describe "invalid frame sequences" do
        it "reports a new method while a channel is waiting for its header" do
          publish = publish_frames(1, "body", 128)
          assembler.push(publish[0])

          expect do
            assembler.push(return_frame(1))
          end.to raise_error(described_class::UnexpectedFrame) do |error|
            expect(error.channel).to eq(1)
            expect(error.status).to eq(:waiting_for_header)
            expect(error.assembler_state.method_class).to eq(Basic::Publish)
            expect(error.frame.method_class).to eq(Basic::Return)
            expect(error.message).to include("received basic.return method frame on channel 1")
            expect(error.message).to include("waiting for a header frame for basic.publish")
          end
        end

        it "reports a new method while a channel is still waiting for body bytes" do
          publish = publish_frames(1, "hello world", 14)
          assembler.push(publish[0])
          assembler.push(publish[1])
          assembler.push(publish[2])

          expect do
            assembler.push(deliver_frame(1))
          end.to raise_error(described_class::UnexpectedFrame) do |error|
            expect(error).to be_a(Protocol::UnexpectedFrame)
            expect(error.channel).to eq(1)
            expect(error.status).to eq(:waiting_for_body)
            expect(error.assembler_state).to have_attributes(
              :expected_bytes => 11,
              :received_bytes => 6,
              :remaining_bytes => 5
            )
            expect(error.frame.method_class).to eq(Basic::Deliver)
            expect(error.message).to include("waiting for 5 of 11 body bytes for basic.publish")
          end
        end

        it "reports a body frame after a header claimed zero bytes" do
          publish = publish_frames(1, "", 128)
          assembler.push(publish[0])
          assembler.push(publish[1])

          expect do
            assembler.push(BodyFrame.new("unexpected", 1))
          end.to raise_error(described_class::UnexpectedFrame) do |error|
            expect(error.channel).to eq(1)
            expect(error.status).to eq(:complete)
            expect(error.assembler_state).to have_attributes(
              :method_class => Basic::Publish,
              :expected_bytes => 0,
              :properties => properties
            )
            expect(error.message).to include("received body frame on channel 1")
            expect(error.message).to include("had already completed a 0-byte basic.publish content")
          end
        end

        it "reports a body frame that would exceed the size declared by the header" do
          publish = publish_frames(1, "abcd", 128)
          assembler.push(publish[0])
          assembler.push(publish[1])

          expect do
            assembler.push(BodyFrame.new("abcde", 1))
          end.to raise_error(described_class::UnexpectedFrame) do |error|
            expect(error.status).to eq(:waiting_for_body)
            expect(error.message).to include("received 5 body bytes on channel 1 while only 4 bytes remained")
          end
        end

        it "reports a header frame when no content method is active" do
          expect do
            assembler.push(header_frame(1, "body"))
          end.to raise_error(described_class::UnexpectedFrame) do |error|
            expect(error.channel).to eq(1)
            expect(error.status).to eq(:idle)
            expect(error.message).to include("was idle")
          end
        end

        it "reports a body frame when no content method is active" do
          expect do
            assembler.push(BodyFrame.new("body", 1))
          end.to raise_error(described_class::UnexpectedFrame) do |error|
            expect(error.channel).to eq(1)
            expect(error.status).to eq(:idle)
          end
        end
      end

      describe "#finish" do
        it "reports a channel that never received its header" do
          assembler.push(publish_frames(1, "body", 128)[0])

          expect do
            assembler.finish
          end.to raise_error(described_class::IncompleteContents) do |error|
            expect(error.pending_contents.map(&:channel)).to eq([1])
            expect(error.pending_contents.first).to have_attributes(
              :status => :waiting_for_header,
              :method_class => Basic::Publish
            )
            expect(error.message).to eq("connection ended with incomplete content: channel 1 was waiting for a header frame for basic.publish")
          end
        end

        it "reports the exact missing body length when the connection ends" do
          publish = publish_frames(1, "hello world", 14)
          assembler.push(publish[0])
          assembler.push(publish[1])
          assembler.push(publish[2])

          expect do
            assembler.finish
          end.to raise_error(described_class::IncompleteContents) do |error|
            state = error.pending_contents.first
            expect(state).to have_attributes(
              :channel => 1,
              :status => :waiting_for_body,
              :expected_bytes => 11,
              :received_bytes => 6,
              :remaining_bytes => 5
            )
            expect(state.method).to be_a(Basic::Publish)
            expect(state.properties).to eq(properties)
            expect(error.message).to include("channel 1 was waiting for 5 of 11 body bytes for basic.publish")
          end
        end

        it "reports every pending channel" do
          first = publish_frames(1, "abc", 128)
          second = publish_frames(2, "abc", 128)
          assembler.push(first[0])
          assembler.push(second[0])
          assembler.push(second[1])

          expect do
            assembler.finish
          end.to raise_error(described_class::IncompleteContents) do |error|
            expect(error.pending_contents.map(&:channel)).to eq([1, 2])
            expect(error.pending_contents.map(&:status)).to eq([:waiting_for_header, :waiting_for_body])
          end
        end
      end
    end
  end
end
