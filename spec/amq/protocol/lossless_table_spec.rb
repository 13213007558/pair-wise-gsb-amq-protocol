# encoding: binary

module AMQ
  module Protocol
    RSpec.describe LosslessTable do
      # wraps a field payload into a full table frame
      def table_binary(payload)
        [payload.bytesize].pack("N") + payload
      end

      def entry(key, type_tag, payload)
        key.bytesize.chr + key + type_tag + payload
      end

      describe ".decode" do
        it "returns a LosslessTable, not a Hash" do
          expect(LosslessTable.decode(table_binary(""))).to be_a(LosslessTable)
        end

        it "decodes an empty table" do
          table = LosslessTable.decode("\x00\x00\x00\x00")
          expect(table.size).to eq(0)
          expect(table.entries).to be_empty
        end

        it "preserves integer width of every integer wire type" do
          payload = entry("byte",  "b", [200].pack("C")) +
                    entry("short", "s", [-1000].pack("s>")) +
                    entry("int",   "I", [42].pack("N")) +
                    entry("long",  "l", [2**40].pack("q>"))
          binary  = table_binary(payload)
          table   = LosslessTable.decode(binary)

          expect(table["byte"].type).to eq("b")
          expect(table["byte"].value).to eq(200)
          expect(table["short"].type).to eq("s")
          expect(table["short"].value).to eq(-1000)
          expect(table["int"].type).to eq("I")
          expect(table["int"].value).to eq(42)
          expect(table["long"].type).to eq("l")
          expect(table["long"].value).to eq(2**40)
          expect(table.encode).to eq(binary)
        end

        it "preserves decimal scale" do
          payload = entry("price", "D", [3, 12345].pack("CN"))
          binary  = table_binary(payload)
          table   = LosslessTable.decode(binary)

          expect(table["price"].value).to eq(BigDecimal("12.345"))
          expect(table["price"].scale).to eq(3)
          expect(table.encode).to eq(binary)
        end

        it "distinguishes strings from byte arrays" do
          payload = entry("string", "S", [5].pack("N") + "hello") +
                    entry("bytes",  "x", [2].pack("N") + "\x00\x01")
          binary  = table_binary(payload)
          table   = LosslessTable.decode(binary)

          expect(table["string"].type).to eq("S")
          expect(table["string"].value).to eq("hello")
          expect(table["bytes"].type).to eq("x")
          expect(table["bytes"].value).to eq("\x00\x01")
          expect(table.encode).to eq(binary)
        end

        it "preserves the original bit pattern of 32-bit floats" do
          float_bytes = [1.5].pack("f")
          payload     = entry("ratio", "f", float_bytes)
          binary      = table_binary(payload)
          table       = LosslessTable.decode(binary)

          expect(table["ratio"].value).to eq(1.5)
          expect(table["ratio"].bits).to eq(float_bytes.unpack1("N"))
          expect(table.encode).to eq(binary)
        end

        it "re-encodes 64-bit floats bit-exactly, including NaN payloads" do
          nan_bytes = [0x7FF8000000000001].pack("Q>")
          payload   = entry("nan", "d", nan_bytes) +
                      entry("pi",  "d", [3.14].pack("G"))
          binary    = table_binary(payload)
          table     = LosslessTable.decode(binary)

          expect(table["nan"].value).to be_nan
          expect(table.encode).to eq(binary)
        end

        it "round-trips booleans, nil, timestamps, arrays and nested tables byte-exactly" do
          nested  = entry("inner", "s", [7].pack("s>"))
          ary     = "l" + [1].pack("q>") + "S" + [2].pack("N") + "hi"
          payload = entry("bool",  "t", "\x01") +
                    entry("nil",   "V", "") +
                    entry("time",  "T", [1293849042].pack("Q>")) +
                    entry("hash",  "F", table_binary(nested)) +
                    entry("array", "A", [ary.bytesize].pack("N") + ary)
          binary  = table_binary(payload)
          table   = LosslessTable.decode(binary)

          expect(table["bool"].value).to eq(true)
          expect(table["nil"].value).to be_nil
          expect(table["time"].value).to eq(Time.at(1293849042))
          expect(table["hash"].value).to be_a(LosslessTable)
          expect(table["hash"].value["inner"].type).to eq("s")
          expect(table["array"].value.map(&:type)).to eq(["l", "S"])
          expect(table.encode).to eq(binary)
        end

        it "keeps duplicate field names observable instead of overwriting them" do
          payload = entry("dup", "b", [1].pack("C")) +
                    entry("other", "b", [9].pack("C")) +
                    entry("dup", "b", [2].pack("C"))
          binary  = table_binary(payload)
          table   = LosslessTable.decode(binary)

          expect(table.size).to eq(3)
          expect(table.duplicate_keys).to eq(["dup"])
          expect(table.entries.map { |_, v| v.value }).to eq([1, 9, 2])
          expect(table["dup"].value).to eq(1)
          expect(table.to_h).to eq({ "dup" => 2, "other" => 9 })
          expect(table.encode).to eq(binary)
        end

        it "raises ArgumentError for unknown type bytes" do
          binary = table_binary(entry("key", "?", ""))
          expect { LosslessTable.decode(binary) }.to raise_error(ArgumentError, /Not a valid type/)
        end
      end

      describe "#to_h" do
        it "matches what Table.decode returns" do
          payload = entry("byte",  "b", [200].pack("C")) +
                    entry("int",   "I", [42].pack("N")) +
                    entry("long",  "l", [2**40].pack("q>")) +
                    entry("price", "D", [3, 12345].pack("CN")) +
                    entry("str",   "S", [5].pack("N") + "hello") +
                    entry("bytes", "x", [2].pack("N") + "\x00\x01") +
                    entry("pi",    "d", [3.14].pack("G")) +
                    entry("hash",  "F", table_binary(entry("inner", "s", [7].pack("s>"))))
          binary  = table_binary(payload)

          expect(LosslessTable.decode(binary).to_h).to eq(Table.decode(binary))
          expect(Table.decode(binary)).to be_a(Hash)
        end
      end

      describe "value modification" do
        it "re-encodes a modified nested value with its original wire type" do
          nested  = entry("count", "s", [5].pack("s>")) +
                    entry("name",  "S", [3].pack("N") + "old")
          payload = entry("outer", "F", table_binary(nested))
          binary  = table_binary(payload)
          table   = LosslessTable.decode(binary)

          table["outer"].value["count"].value = 99

          expected_nested  = entry("count", "s", [99].pack("s>")) +
                             entry("name",  "S", [3].pack("N") + "old")
          expect(table.encode).to eq(table_binary(entry("outer", "F", table_binary(expected_nested))))
        end

        it "keeps unmodified entries byte-identical when a sibling changes" do
          payload = entry("a", "I", [1].pack("N")) +
                    entry("b", "D", [2, 500].pack("CN")) +
                    entry("c", "x", [3].pack("N") + "\x01\x02\x03")
          binary  = table_binary(payload)
          table   = LosslessTable.decode(binary)

          table["a"] = 2

          expected = table_binary(entry("a", "I", [2].pack("N")) +
                                  entry("b", "D", [2, 500].pack("CN")) +
                                  entry("c", "x", [3].pack("N") + "\x01\x02\x03"))
          expect(table.encode).to eq(expected)
        end

        it "drops the preserved bit pattern when a 32-bit float is replaced" do
          payload = entry("ratio", "f", [1.5].pack("f"))
          table   = LosslessTable.decode(table_binary(payload))

          table["ratio"].value = 2.5

          expect(table["ratio"].bits).to be_nil
          expect(table.encode).to eq(table_binary(entry("ratio", "f", [2.5].pack("f"))))
        end

        it "re-encodes a replaced decimal with the retained scale" do
          payload = entry("price", "D", [3, 12345].pack("CN"))
          table   = LosslessTable.decode(table_binary(payload))

          table["price"].value = BigDecimal("7.5")

          expect(table.encode).to eq(table_binary(entry("price", "D", [3, 7500].pack("CN"))))
        end

        it "appends new fields with inferred type tags" do
          table = LosslessTable.decode(table_binary(entry("a", "b", [1].pack("C"))))

          table["added"] = 42

          expect(table["added"].type).to eq("l")
          expect(Table.decode(table.encode)).to eq({ "a" => 1, "added" => 42 })
        end
      end

      describe ".from_hash" do
        it "encodes plain hashes like Table.encode" do
          input = {
            "bool"  => true,
            "int"   => 1,
            "str"   => "Test",
            "float" => 3.14,
            "nil"   => nil,
            "time"  => Time.at(1293849042),
            "array" => [1, "two"],
            "hash"  => { "nested" => "value" }
          }

          expect(LosslessTable.from_hash(input).encode).to eq(Table.encode(input))
        end

        it "infers decimal scale for BigDecimal values" do
          table = LosslessTable.from_hash({ "price" => BigDecimal("5E-3") })

          expect(table["price"].scale).to eq(3)
          expect(Table.decode(table.encode)).to eq({ "price" => BigDecimal("5E-3") })
        end
      end
    end
  end
end
