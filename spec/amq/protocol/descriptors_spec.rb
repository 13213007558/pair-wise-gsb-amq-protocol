# encoding: binary

module AMQ
  module Protocol
    RSpec.describe DescriptorRegistry do
      let(:registry) { DescriptorRegistry.new }

      # 16 properties force the property flags into two words:
      # indexes 0-14 live in word 0, index 15 lives in word 1 and
      # turns on the continuation bit of word 0
      let(:extension_properties) do
        [[:shortstr, :x_kind]] +
          (1..14).map { |i| [:octet, :"x_tag_#{i}"] } +
          [[:shortstr, :x_note]]
      end

      before do
        registry.register_class(120, :name => "x-broker", :properties => extension_properties)
        registry.register_method(120, 10, :name => "x-broker.declare", :fields => [
          [:shortstr, :name],
          [:bit, :durable],
          [:bit, :auto_delete],
          [:octet, :mode],
          [:bit, :nowait],
        ])
      end

      describe "method encoding" do
        it "packs adjacent bit fields into shared octets" do
          frame = registry.encode(120, 10, 1, "q", true, false, 7, true)
          expect(frame.payload).to eq("\x00\x78\x00\x0A\x01q\x01\x07\x01")
          expect(frame.encode).to eq(Frame.encode(:method, "\x00\x78\x00\x0A\x01q\x01\x07\x01", 1))
        end

        it "round-trips through the generated decoder" do
          frame = registry.encode(120, 10, 1, "q", true, false, 7, true)
          decoded = registry.decode_frame(frame)
          expect(decoded.name).to eq("q")
          expect(decoded.durable).to eq(true)
          expect(decoded.auto_delete).to eq(false)
          expect(decoded.mode).to eq(7)
          expect(decoded.nowait).to eq(true)
        end
      end

      describe "content property flags" do
        it "emits a second flags word with the continuation bit when more than 15 properties are declared" do
          result = registry.encode_properties(120, 10, {:x_kind => "a", :x_note => "b"})
          expect(result).to eq(
            "\x00\x78\x00\x00" +                 # class id 120, weight 0
            "\x00\x00\x00\x00\x00\x00\x00\x0A" + # body size 10
            "\x80\x01\x80\x00" +                 # flags: word 0 bit 15 + continuation, word 1 bit 15
            "\x01a\x01b"
          )
        end

        it "round-trips properties spread over two flags words" do
          properties = {:x_kind => "a", :x_tag_7 => 3, :x_note => "b"}
          payload = registry.encode_properties(120, 42, properties)
          decoded = registry.decode_properties(120, payload.byteslice(12..-1))
          expect(decoded).to eq(properties)
        end
      end

      describe "interoperability with built-in methods" do
        it "decodes built-in method frames and re-encodes them byte for byte" do
          frame = Confirm::Select.encode(3, true)
          decoded = registry.decode_frame(frame)
          expect(decoded).to be_a(Confirm::Select)
          expect(decoded.nowait).to eq(true)
          expect(Confirm::Select.encode(3, decoded.nowait).encode).to eq(frame.encode)
        end

        it "decodes built-in methods with packed bit fields byte for byte" do
          frame = Channel::Flow.encode(2, true)
          decoded = registry.decode_frame(frame)
          expect(decoded.active).to eq(true)
          expect(Channel::Flow.encode(2, decoded.active).encode).to eq(frame.encode)
        end

        it "decodes basic properties byte-compatibly with the built-in decoder" do
          basic_layout = [
            [:shortstr, :content_type], [:shortstr, :content_encoding], [:table, :headers],
            [:octet, :delivery_mode], [:octet, :priority], [:shortstr, :correlation_id],
            [:shortstr, :reply_to], [:shortstr, :expiration], [:shortstr, :message_id],
            [:timestamp, :timestamp], [:shortstr, :type], [:shortstr, :user_id],
            [:shortstr, :app_id], [:shortstr, :cluster_id],
          ]
          registry.register_class(121, :name => "x-basic-mirror", :properties => basic_layout)
          data = "\xB8@\x18application/octet-stream\x00\x00\x00\x10\x05helloS\x00\x00\x00\x05world\x02\x00\x00\x00\x00\x00\x00\x00\x00{"
          expect(registry.decode_properties(121, data)).to eq(Basic.decode_properties(data))
        end

        it "encodes properties byte-compatibly with the built-in encoder" do
          basic_layout = [
            [:shortstr, :content_type], [:shortstr, :content_encoding], [:table, :headers],
            [:octet, :delivery_mode], [:octet, :priority], [:shortstr, :correlation_id],
            [:shortstr, :reply_to], [:shortstr, :expiration], [:shortstr, :message_id],
            [:timestamp, :timestamp], [:shortstr, :type], [:shortstr, :user_id],
            [:shortstr, :app_id], [:shortstr, :cluster_id],
          ]
          registry.register_class(122, :name => "x-basic-mirror", :properties => basic_layout)
          properties = {:priority => 0, :delivery_mode => 2, :content_type => "application/octet-stream"}
          expected = Basic.encode_properties(10, properties)
          actual = registry.encode_properties(122, 10, properties)
          expect(actual.byteslice(2..-1)).to eq(expected.byteslice(2..-1))
        end
      end

      describe "registration conflicts" do
        it "rejects class ids occupied by built-in classes" do
          expect { registry.register_class(60, :name => "basic-again") }.to raise_error(DescriptorConflictError)
        end

        it "rejects duplicate registrations within the same registry" do
          expect { registry.register_class(120, :name => "x-broker-again") }.to raise_error(DescriptorConflictError)
          expect { registry.register_method(120, 10, :name => "x-broker.declare-again") }.to raise_error(DescriptorConflictError)
        end

        it "rejects methods on unregistered or built-in classes" do
          expect { registry.register_method(60, 10, :name => "basic.qos-again") }.to raise_error(DescriptorError)
        end

        it "rejects content-bearing methods on classes without properties" do
          registry.register_class(130, :name => "x-plain")
          expect { registry.register_method(130, 10, :name => "x-plain.push", :has_content => true) }.to raise_error(DescriptorError)
        end

        it "rejects unknown field types instead of guessing" do
          expect { registry.register_method(120, 20, :name => "x-broker.bogus", :fields => [[:float, :x]]) }.to raise_error(ArgumentError)
          expect { registry.register_class(123, :name => "x-bit-props", :properties => [[:bit, :x]]) }.to raise_error(ArgumentError)
        end
      end

      describe "instance isolation" do
        it "does not leak registrations into other instances or the built-in tables" do
          other = DescriptorRegistry.new
          expect(other.registered?(120)).to eq(false)
          expect { other.encode(120, 10, 1, "q", true, false, 7, true) }.to raise_error(DescriptorError)
          expect(AMQ::Protocol::METHODS).not_to have_key(120 << 16 | 10)
        end
      end

      describe "unknown methods" do
        it "preserves them as raw payloads with identity" do
          frame = MethodFrame.new([200, 7].pack(PACK_UINT16_X2) << "\xDE\xAD", 5)
          decoded = registry.decode_frame(frame)
          expect(decoded).to be_a(RawMethod)
          expect(decoded.class_id).to eq(200)
          expect(decoded.method_id).to eq(7)
          expect(decoded.channel).to eq(5)
          expect(decoded.payload).to eq("\xDE\xAD")
          expect(decoded.encode.encode).to eq(frame.encode)
        end
      end

      describe "strict decoding" do
        it "refuses payloads with trailing bytes" do
          frame = registry.encode(120, 10, 1, "q", true, false, 7, true)
          expect { registry.decode(120, 10, frame.payload.byteslice(4..-1) << "\xFF") }.to raise_error(FieldLayoutError)
        end

        it "refuses truncated payloads" do
          expect { registry.decode(120, 10, "\x01") }.to raise_error(FieldLayoutError)
        end

        it "refuses property flags without a declared property" do
          expect { registry.decode_properties(120, "\x00\x01\x40\x00") }.to raise_error(FieldLayoutError)
        end

        it "refuses undeclared property keys" do
          expect { registry.encode_properties(120, 0, {:nope => 1}) }.to raise_error(FieldLayoutError)
        end
      end
    end
  end
end
