# encoding: binary

require "spec_helper"

module AMQ
  module Protocol
    RSpec.describe ExtensionRegistry do
      subject(:registry) { ExtensionRegistry.new }

      let(:acme_properties) do
        [
          [:content_type, :shortstr],
          [:content_encoding, :shortstr],
          [:headers, :table],
          [:delivery_mode, :octet],
          [:priority, :octet],
          [:correlation_id, :shortstr],
          [:reply_to, :shortstr],
          [:expiration, :shortstr],
          [:message_id, :shortstr],
          [:timestamp, :timestamp],
          [:type, :shortstr],
          [:user_id, :shortstr],
          [:app_id, :shortstr],
          [:cluster_id, :shortstr],
          [:tenant_id, :shortstr],
          [:schema_id, :shortstr],
          [:compression, :shortstr]
        ]
      end

      def register_acme_class
        registry.register_class(200, :name => "acme", :properties => acme_properties)
      end

      describe "registration" do
        it "rejects a class id used by a built-in class" do
          expect { registry.register_class(60, :name => "basic") }.to raise_error(RegistrationConflictError)
          expect { registry.register_class(10, :name => "connection") }.to raise_error(RegistrationConflictError)
        end

        it "rejects a duplicate class id in the same registry" do
          registry.register_class(200, :name => "acme")
          expect { registry.register_class(200, :name => "acme2") }.to raise_error(RegistrationConflictError)
        end

        it "rejects a method id used by a built-in method" do
          expect {
            registry.register_method(60, 10, :name => "basic.qos")
          }.to raise_error(RegistrationConflictError)
          expect {
            registry.register_method(10, 10, :name => "connection.start")
          }.to raise_error(RegistrationConflictError)
        end

        it "rejects a duplicate method in the same registry" do
          register_acme_class
          registry.register_method(200, 10, :name => "acme.ping")
          expect {
            registry.register_method(200, 10, :name => "acme.ping-again")
          }.to raise_error(RegistrationConflictError)
        end

        it "rejects unknown field types instead of guessing a layout" do
          register_acme_class
          expect {
            registry.register_method(200, 10, :name => "acme.ping", :fields => [[:blob, :mystery]])
          }.to raise_error(UnknownFieldTypeError)
        end

        it "rejects methods on classes it knows nothing about" do
          expect {
            registry.register_method(201, 10, :name => "ghost.ping")
          }.to raise_error(UnknownMethodError)
        end

        it "rejects content-carrying methods without a registered property layout" do
          expect {
            registry.register_method(60, 130, :name => "basic.acme-push", :has_content => true)
          }.to raise_error(UnknownMethodError)
        end

        it "is isolated per instance" do
          other = ExtensionRegistry.new
          register_acme_class
          registry.register_method(200, 10, :name => "acme.ping")

          expect(registry.method_registered?(200, 10)).to be(true)
          expect(other.method_registered?(200, 10)).to be(false)
          expect(other.class_registered?(200)).to be(false)
          expect {
            other.decode_method(200, 10, "")
          }.to raise_error(UnknownMethodError)
        end
      end

      describe "method encoding and decoding" do
        before { register_acme_class }

        it "produces the same argument bytes as the generated code for an identical layout" do
          # same layout as basic.qos: long, short, bit
          registry.register_method(200, 10, :name => "acme.qos", :fields => [
            [:prefetch_size, :long],
            [:prefetch_count, :short],
            [:global, :bit]
          ])

          extension = registry.encode_method(200, 10, 3, :prefetch_size => 0, :prefetch_count => 16, :global => true)
          builtin    = Basic::Qos.encode(3, 0, 16, true)

          expect(extension.payload.byteslice(4..-1)).to eq(builtin.payload.byteslice(4..-1))
          expect(extension.payload.byteslice(0, 4)).to eq([200, 10].pack("n2"))
          expect(extension.channel).to eq(3)
        end

        it "produces the same argument bytes as connection.tune-ok for an identical layout" do
          registry.register_method(200, 11, :name => "acme.tune", :fields => [
            [:channel_max, :short],
            [:frame_max, :long],
            [:heartbeat, :short]
          ])

          extension = registry.encode_method(200, 11, 0, :channel_max => 2047, :frame_max => 131072, :heartbeat => 60)
          builtin    = Connection::TuneOk.encode(2047, 131072, 60)

          expect(extension.payload.byteslice(4..-1)).to eq(builtin.payload.byteslice(4..-1))
        end

        it "packs adjacent bit fields into a single octet" do
          registry.register_method(200, 12, :name => "acme.declare", :fields => [
            [:ticket, :short],
            [:passive, :bit],
            [:durable, :bit],
            [:auto_delete, :bit],
            [:nowait, :bit],
            [:label, :shortstr]
          ])

          frame = registry.encode_method(200, 12, 1,
            :ticket => 0, :passive => true, :durable => false,
            :auto_delete => true, :nowait => true, :label => "q")

          expected = [200, 12].pack("n2") +
            [0].pack("n") +
            [0b1101].pack("C") +
            "q".bytesize.chr + "q"
          expect(frame.payload).to eq(expected)
        end

        it "spills more than eight adjacent bits into a second octet" do
          fields = (1..9).map { |i| ["bit_#{i}".to_sym, :bit] }
          registry.register_method(200, 13, :name => "acme.bits", :fields => fields)

          values = Hash[fields.map { |(name, _)| [name, true] }]
          frame = registry.encode_method(200, 13, 1, values)

          expect(frame.payload).to eq([200, 13].pack("n2") + [0xFF, 0x01].pack("C2"))
        end

        it "starts a new octet for bits that follow a non-bit field" do
          registry.register_method(200, 14, :name => "acme.mixed", :fields => [
            [:first, :bit],
            [:marker, :octet],
            [:second, :bit]
          ])

          frame = registry.encode_method(200, 14, 1, :first => true, :marker => 7, :second => true)
          expect(frame.payload).to eq([200, 14].pack("n2") + [0x01, 7, 0x01].pack("C3"))
        end

        it "round-trips all supported field types" do
          registry.register_method(200, 15, :name => "acme.everything", :fields => [
            [:ticket, :short],
            [:exchange, :shortstr],
            [:urgent, :bit],
            [:mandatory, :bit],
            [:payload_id, :long],
            [:serial, :longlong],
            [:body_hint, :longstr],
            [:priority, :octet],
            [:arguments, :table],
            [:not_before, :timestamp]
          ])

          values = {
            :ticket => 1,
            :exchange => "acme.events",
            :urgent => true,
            :mandatory => false,
            :payload_id => 42,
            :serial => 2**40 + 7,
            :body_hint => "a" * 300,
            :priority => 9,
            :arguments => {"kind" => "metric", "retries" => 3},
            :not_before => Time.at(1_700_000_000)
          }

          frame = registry.encode_method(200, 15, 4, values)
          decoded = registry.decode_method(200, 15, frame.payload.byteslice(4..-1))

          expect(decoded).to eq(values.merge(:not_before => Time.at(1_700_000_000)))
        end
      end

      describe "content properties" do
        before { register_acme_class }

        it "matches Basic.encode_properties byte for byte on the shared layout" do
          shared = acme_properties.first(14)
          registry.register_class(201, :name => "acme-basic", :properties => shared)

          properties = {
            :priority => 0,
            :delivery_mode => 2,
            :content_type => "application/octet-stream",
            :timestamp => 123
          }

          extension = registry.encode_properties(201, 10, properties)
          builtin    = Basic.encode_properties(10, properties)

          # everything after class id, weight and body size
          expect(extension.byteslice(12..-1)).to eq(builtin.byteslice(12..-1))
          expect(extension.byteslice(0, 2)).to eq([201].pack("n"))
        end

        it "emits a single flags word for up to fifteen properties" do
          registry.register_class(202, :name => "acme-small", :properties => acme_properties.first(2))
          result = registry.encode_properties(202, 0, {:content_type => "text/plain"})
          expect(result.byteslice(12, 2)).to eq([0x8000].pack("n"))
        end

        it "emits a continuation flags word when more than fifteen properties are set" do
          properties = {:content_type => "text/plain", :schema_id => "42", :compression => "zstd"}
          result = registry.encode_properties(200, 5, properties)

          # first word: content_type (bit 15) + continuation bit
          expect(result.byteslice(12, 2)).to eq([0x8001].pack("n"))
          # second word: schema_id is property 16 (bit 15), compression is property 17 (bit 14)
          expect(result.byteslice(14, 2)).to eq([0xC000].pack("n"))

          expected = [200, 0].pack("n2") +
            AMQ::Pack.pack_uint64_big_endian(5) +
            [0x8001, 0xC000].pack("n2") +
            "text/plain".bytesize.chr + "text/plain" +
            "42".bytesize.chr + "42" +
            "zstd".bytesize.chr + "zstd"
          expect(result).to eq(expected)
        end

        it "round-trips properties across a two-word flags field" do
          properties = {
            :content_type => "application/octet-stream",
            :headers => {"trace" => "abc"},
            :delivery_mode => 2,
            :timestamp => Time.at(123),
            :tenant_id => "acme",
            :compression => "zstd"
          }

          encoded = registry.encode_properties(200, 128, properties)
          decoded = registry.decode_properties(200, encoded.byteslice(12..-1))

          expect(decoded).to eq(properties)
        end

        it "decodes flags words until the continuation bit clears" do
          encoded = registry.encode_properties(200, 0, {:compression => "none"})
          # sanity: two flags words were emitted
          expect(encoded.byteslice(12, 2)).to eq([0x0001].pack("n"))
          expect(registry.decode_properties(200, encoded.byteslice(12..-1))).to eq({:compression => "none"})
        end

        it "emits one empty flags word when no properties are set" do
          result = registry.encode_properties(200, 0, {})
          expect(result.byteslice(12..-1)).to eq([0].pack("n"))
        end
      end

      describe "unknown methods on the wire" do
        it "decodes frames with unknown method ids into RawMethod preserving identity" do
          payload = [200, 10].pack("n2") + "\x01\x02\x03"
          frame = MethodFrame.new(payload, 1)

          expect(frame.method_class).to be_nil
          expect(frame.final?).to be(true)

          raw = frame.decode_payload
          expect(raw).to be_a(RawMethod)
          expect(raw.class_id).to eq(200)
          expect(raw.method_id).to eq(10)
          expect(raw.payload).to eq("\x01\x02\x03")
        end

        it "hands raw payloads to a registry that knows the layout" do
          register_acme_class
          registry.register_method(200, 10, :name => "acme.qos", :fields => [
            [:prefetch_size, :long],
            [:prefetch_count, :short],
            [:global, :bit]
          ])

          encoded = registry.encode_method(200, 10, 2, :prefetch_size => 0, :prefetch_count => 16, :global => true)
          raw = MethodFrame.new(encoded.payload, 2).decode_payload

          expect(raw).to be_a(RawMethod)
          expect(registry.decode_method(raw.class_id, raw.method_id, raw.payload)).to eq(
            :prefetch_size => 0, :prefetch_count => 16, :global => true
          )
        end
      end
    end
  end
end
