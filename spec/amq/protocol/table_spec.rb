require 'time'

module AMQ
  module Protocol
    RSpec.describe Table do
      timestamp    = Time.utc(2010, 12, 31, 23, 58, 59)
      bigdecimal_1 = BigDecimal("1.0")
      bigdecimal_2 = BigDecimal("5E-3")

      DATA = [
          [{},                       "\x00\x00\x00\x00"],
          [{"test" => 1},            "\x00\x00\x00\x0E\x04testl\x00\x00\x00\x00\x00\x00\x00\x01"],
          [{"float" => 1.92},        "\x00\x00\x00\x0F\x05floatd?\xFE\xB8Q\xEB\x85\x1E\xB8"],
          [{"test" => "string"},     "\x00\x00\x00\x10\x04testS\x00\x00\x00\x06string"],
          [{"test" => {}},           "\x00\x00\x00\n\x04testF\x00\x00\x00\x00"],
          [{"test" => bigdecimal_1}, "\x00\x00\x00\v\x04testD\x00\x00\x00\x00\x01"],
          [{"test" => bigdecimal_2}, "\x00\x00\x00\v\x04testD\x03\x00\x00\x00\x05"],
          [{"test" => timestamp},    "\x00\x00\x00\x0e\x04testT\x00\x00\x00\x00M\x1enC"]
      ]

      describe ".encode" do
        it "should return \"\x00\x00\x00\x00\" for nil" do
          encoded_value = "\x00\x00\x00\x00"

          expect(Table.encode(nil)).to eql(encoded_value)
        end

        it "should serialize { :test => true }" do
          expect(Table.encode(:test => true)).
            to eql("\x00\x00\x00\a\x04testt\x01".force_encoding(Encoding::ASCII_8BIT))
        end

        it "should serialize { :test => false }" do
          expect(Table.encode(:test => false)).
            to eql("\x00\x00\x00\a\x04testt\x00".force_encoding(Encoding::ASCII_8BIT))
        end

        it "should serialize { :coordinates => { :latitude  => 59.35 } }" do
          expect(Table.encode(:coordinates => { :latitude  => 59.35 })).
            to eql("\x00\x00\x00#\vcoordinatesF\x00\x00\x00\x12\blatituded@M\xAC\xCC\xCC\xCC\xCC\xCD".force_encoding(Encoding::ASCII_8BIT))
        end

        it "should serialize { :coordinates => { :longitude => 18.066667 } }" do
          expect(Table.encode(:coordinates => { :longitude => 18.066667 })).
            to eql("\x00\x00\x00$\vcoordinatesF\x00\x00\x00\x13\tlongituded@2\x11\x11\x16\xA8\xB8\xF1".force_encoding(Encoding::ASCII_8BIT))
        end

        it "should serialize long UTF-8 strings and symbols" do
          long_utf8 = "à" * 192
          long_ascii8 = long_utf8.dup.force_encoding(::Encoding::ASCII_8BIT)

          input = { "utf8_string" => long_utf8, "utf8_symbol" => long_utf8.to_sym }
          output = { "utf8_string" => long_ascii8, "utf8_symbol" => long_ascii8 }

          expect(Table.decode(Table.encode(input))).to eq(output)
        end

        DATA.each do |data, encoded|
          it "should return #{encoded.inspect} for #{data.inspect}" do
            expect(Table.encode(data)).to eql(encoded.force_encoding(Encoding::ASCII_8BIT))
          end
        end
      end

      describe ".decode" do
        DATA.each do |data, encoded|
          it "should return #{data.inspect} for #{encoded.inspect}" do
            expect(Table.decode(encoded)).to eql(data)
          end

          it "is capable of decoding what it encodes" do
            expect(Table.decode(Table.encode(data))).to eq(data)
          end
        end # DATA.each


        it "is capable of decoding boolean table values" do
          input1   = { "boolval" => true }
          expect(Table.decode(Table.encode(input1))).to eq(input1)


          input2   = { "boolval" => false }
          expect(Table.decode(Table.encode(input2))).to eq(input2)
        end


        it "is capable of decoding nil table values" do
          input   = { "nilval" => nil }
          expect(Table.decode(Table.encode(input))).to eq(input)
        end

        it "is capable of decoding nil table in nested hash/map values" do
          input   = { "hash" => {"nil" => nil} }
          expect(Table.decode(Table.encode(input))).to eq(input)
        end

        it "is capable of decoding string table values" do
          input   = { "stringvalue" => "string" }
          expect(Table.decode(Table.encode(input))).to eq(input)

          expect(Table.decode("\x00\x00\x00\x17\vstringvalueS\x00\x00\x00\x06string")).to eq(input)
        end

        it "is capable of decoding byte array table values (as Ruby strings)" do
          expect(Table.decode("\x00\x00\x00\x17\vstringvaluex\x00\x00\x00\x06string")).to eq({"stringvalue" => "string"})
        end

        it "is capable of decoding string table values with UTF-8 characters" do
          input   = {
            "строка".force_encoding(::Encoding::ASCII_8BIT) => "значение".force_encoding(::Encoding::ASCII_8BIT)
          }
          expect(Table.decode(Table.encode(input))).to eq(input)
        end


        it "is capable of decoding integer table values" do
          input   = { "intvalue" => 10 }
          expect(Table.decode(Table.encode(input))).to eq(input)
        end


        it "is capable of decoding signed integer table values" do
          input   = { "intvalue" => -10 }
          expect(Table.decode(Table.encode(input))).to eq(input)
        end


        it "is capable of decoding long table values" do
          input   = { "longvalue" => 912598613 }
          expect(Table.decode(Table.encode(input))).to eq(input)
        end



        it "is capable of decoding float table values" do
          input   = { "floatvalue" => 100.0 }
          expect(Table.decode(Table.encode(input))).to eq(input)
        end



        it "is capable of decoding time table values" do
          input   = { "intvalue" => Time.parse("2011-07-14 01:17:46 +0400") }
          expect(Table.decode(Table.encode(input))).to eq(input)
        end



        it "is capable of decoding empty hash table values" do
          input   = { "hashvalue" => Hash.new }
          expect(Table.decode(Table.encode(input))).to eq(input)
        end



        it "is capable of decoding empty array table values" do
          input   = { "arrayvalue" => Array.new }
          expect(Table.decode(Table.encode(input))).to eq(input)
        end


        it "is capable of decoding single string value array table values" do
          input   = { "arrayvalue" => ["amq-protocol"] }
          expect(Table.decode(Table.encode(input))).to eq(input)
        end



        it "is capable of decoding simple nested hash table values" do
          input   = { "hashvalue" => { "a" => "b" } }
          expect(Table.decode(Table.encode(input))).to eq(input)
        end



        it "is capable of decoding nil table values" do
          input   = { "nil" => nil }
          expect(Table.decode(Table.encode(input))).to eq(input)
        end

        it 'is capable of decoding 8bit signed integers' do
          output = TableValueDecoder.decode_byte("\xC0",0).first
          expect(output).to eq(192)
        end

        it 'is capable of decoding 16bit signed integers' do
          output = TableValueDecoder.decode_short("\x06\x8D", 0).first
          expect(output).to eq(1677)
        end

        it "decodes TYPE_INTEGER (32-bit unsigned) from raw binary" do
          # "\x03key" = 1-byte key length + "key", then 'I' type + 4-byte uint32
          binary = [9].pack("N") + "\x03keyI" + [42].pack("N")
          expect(Table.decode(binary.force_encoding("BINARY"))).to eq("key" => 42)
        end

        it "decodes TYPE_BYTE from raw binary" do
          # "\x01k" = 1-byte key length + "k", then 'b' type + 1-byte value
          binary = [4].pack("N") + "\x01kb\xFF"
          expect(Table.decode(binary.force_encoding("BINARY"))).to eq("k" => 255)
        end

        it "decodes TYPE_SIGNED_16BIT from raw binary" do
          binary = [5].pack("N") + "\x01ks" + [1000].pack("s>")
          expect(Table.decode(binary.force_encoding("BINARY"))).to eq("k" => 1000)
        end

        it "decodes TYPE_32BIT_FLOAT from raw binary" do
          binary = [7].pack("N") + "\x01kf" + [1.5].pack("g")
          result = Table.decode(binary.force_encoding("BINARY"))
          expect(result["k"]).to be_within(0.001).of(1.5)
        end

        it "raises ArgumentError for unknown type bytes" do
          # "\x03key" key + unknown type '?'
          binary = [5].pack("N") + "\x03key?"
          expect { Table.decode(binary.force_encoding("BINARY")) }.to raise_error(ArgumentError, /Not a valid type/)
        end

        it "is capable of decoding tables" do
          input   = {
            "boolval"      => true,
            "intval"       => 1,
            "strval"       => "Test",
            "timestampval" => Time.parse("2011-07-14 01:17:46 +0400"),
            "floatval"     => 3.14,
            "longval"      => 912598613,
            "hashval"      => { "protocol" => "AMQP091", "true" => true, "false" => false, "nil" => nil }
          }
          expect(Table.decode(Table.encode(input))).to eq(input)
        end



        it "is capable of decoding deeply nested tables" do
          input   = {
            "hashval"    => {
              "protocol" => {
                "name"  => "AMQP",
                "major" => 0,
                "minor" => "9",
                "rev"   => 1.0,
                "spec"  => {
                  "url"  => "http://bit.ly/hw2ELX",
                  "utf8" => "à bientôt"
                }
              },
              "true"     => true,
              "false"    => false,
              "nil"      => nil
            }
          }
          expect(Table.decode(Table.encode(input))).to eq(input.tap { |r| r["hashval"]["protocol"]["spec"]["utf8"].force_encoding(::Encoding::ASCII_8BIT) })
        end



        it "is capable of decoding array values in tables" do
          input1   = {
            "arrayval1" => [198, 3, 77, 8.0, ["inner", "array", { "oh" => "well", "it" => "should work", "3" => 6 }], "two", { "a" => "value", "is" => nil }],
            "arrayval2" => [198, 3, 77, "two", { "a" => "value", "is" => nil }, 8.0, ["inner", "array", { "oh" => "well", "it" => "should work", "3" => 6 }]]
          }
          expect(Table.decode(Table.encode(input1))).to eq(input1)

          now = Time.now
          input2 = {
                       "coordinates" => {
                         "latitude"  => 59.35,
                         "longitude" => 18.066667
                       },
                       "participants" => 11,
                       "venue"        => "Stockholm",
                       "true_field"   => true,
                       "false_field"  => false,
                       "nil_field"    => nil,
                       "ary_field"    => ["one", 2.0, 3]
                     }

          expect(Table.decode(Table.encode(input2))).to eq(input2)

          input3 = { "timely" => { "now" => now } }
          expect(Table.decode(Table.encode(input3))["timely"]["now"].to_i).to eq(now.to_i)
        end

      end # describe

      describe ".decode_with_types" do
        let(:wire) do
          "\x00\x00\x00\x9D" \
          "\x04byteb\x01" \
          "\x05shorts\x00\x02" \
          "\x04uintI\x00\x00\x00\x03" \
          "\x04longl\x00\x00\x00\x00\x00\x00\x00\x04" \
          "\adecimalD\x03\x00\x00\x04\xE2" \
          "\x06stringS\x00\x00\x00\x01a" \
          "\x05bytesx\x00\x00\x00\x01a" \
          "\x03f32f?\xC0\x00\x00" \
          "\x03f64d?\xF8\x00\x00\x00\x00\x00\x00" \
          "\x06nestedF\x00\x00\x00\v\x05innerI\x00\x00\x00\t" \
          "\x04listA\x00\x00\x00\bb\ax\x00\x00\x00\x01z" \
          "\x04sameb\x01" \
          "\x04sames\x00\x02".b
        end

        it "returns ordered entries with AMQP wire types" do
          entries = described_class.decode_with_types(wire)

          expect(entries).to be_an(Array)
          expect(entries.map(&:first)).to eq(%w[byte short uint long decimal string bytes f32 f64 nested list same same])

          by_name = entries.to_h
          expect(by_name["byte"]).to have_attributes(type: "b", value: 1)
          expect(by_name["short"]).to have_attributes(type: "s", value: 2)
          expect(by_name["uint"]).to have_attributes(type: "I", value: 3)
          expect(by_name["long"]).to have_attributes(type: "l", value: 4)
          expect(by_name["string"]).to have_attributes(type: "S", value: "a".b)
          expect(by_name["bytes"]).to have_attributes(type: "x", value: "a".b)
          expect(by_name["decimal"]).to have_attributes(type: "D", value: BigDecimal("1.250"), scale: 3)
          expect(by_name["f32"]).to have_attributes(type: "f", value: 1.5, representation: "?\xC0\x00\x00".b)
          expect(by_name["f64"]).to have_attributes(type: "d", value: 1.5, representation: "?\xF8\x00\x00\x00\x00\x00\x00".b)
        end

        it "decodes nested tables and arrays without converting them to hashes" do
          nested = described_class.decode_with_types(wire).assoc("nested").last
          list = described_class.decode_with_types(wire).assoc("list").last

          expect(nested.type).to eq("F")
          expect(nested.value).to eq([["inner", Table::TypedValue.new("I", 9)]])
          expect(list.type).to eq("A")
          expect(list.value.map(&:type)).to eq(%w[b x])
        end

        it "preserves duplicate field names as separate entries" do
          duplicates = described_class.decode_with_types(wire).last(2)

          expect(duplicates.map { |key, value| [key, value.type, value.value] }).to eq([
            ["same", "b", 1],
            ["same", "s", 2]
          ])
        end

        it "round trips unmodified values exactly" do
          expect(described_class.encode_with_types(described_class.decode_with_types(wire))).to eq(wire)
        end

        it "re-encodes unchanged values with their original representation after a nested edit" do
          entries = described_class.decode_with_types(wire)
          entries.assoc("nested").last.value.assoc("inner").last.value = 10

          reencoded = described_class.encode_with_types(entries)
          decoded = described_class.decode_with_types(reencoded)

          expect(decoded.assoc("nested").last.value.assoc("inner").last.value).to eq(10)
          expect(decoded.assoc("f32").last.representation).to eq("?\xC0\x00\x00".b)
          expect(decoded.assoc("decimal").last.scale).to eq(3)
          expect(decoded.assoc("list").last.value.map(&:type)).to eq(%w[b x])
        end

        it "uses the modified value width when a typed value is replaced" do
          entries = described_class.decode_with_types(wire)
          entries.assoc("uint").last.value = 4_000_000_001
          entries.assoc("f32").last.value = 2.5

          decoded = described_class.decode_with_types(described_class.encode_with_types(entries))

          expect(decoded.assoc("uint").last).to have_attributes(type: "I", value: 4_000_000_001)
          expect(decoded.assoc("f32").last).to have_attributes(type: "f", value: 2.5)
        end

        it "allows nested typed values to be replaced with plain Ruby values" do
          entries = described_class.decode_with_types(wire)
          entries.assoc("list").last.value[0] = 42

          decoded = described_class.decode_with_types(described_class.encode_with_types(entries))
          first = decoded.assoc("list").last.value.first

          expect(first).to have_attributes(type: "l", value: 42)
        end

        it "preserves non-canonical float bit patterns" do
          float_wire = described_class.encode_with_types([
            ["double", Table::TypedValue.new("d", 0.0, representation: "\x7F\xF8\x00\x00\x00\x00\x00\x01".b)],
            ["float", Table::TypedValue.new("f", 0.0, representation: "\x7F\xC0\x00\x01".b)]
          ])

          decoded = described_class.decode_with_types(float_wire)

          expect(decoded.assoc("double").last.value).to be_nan
          expect(decoded.assoc("float").last.value).to be_nan
          expect(described_class.encode_with_types(decoded)).to eq(float_wire)
        end

        it "does not change the existing hash return type" do
          expect(described_class.decode(wire)).to be_a(Hash)
        end
      end

      describe ".length" do
        it "returns the table length from binary data" do
          encoded = Table.encode({"test" => 1})
          expect(Table.length(encoded)).to eq(14)
        end

        it "returns 0 for empty table" do
          encoded = Table.encode({})
          expect(Table.length(encoded)).to eq(0)
        end
      end

      describe ".hash_size" do
        it "calculates size for simple hash" do
          size = Table.hash_size({"key" => "value"})
          expect(size).to be > 0
        end

        it "returns 0 for empty hash" do
          size = Table.hash_size({})
          expect(size).to eq(0)
        end
      end

      describe "Table::InvalidTableError" do
        it "formats error message with key and value" do
          error = Table::InvalidTableError.new("mykey", Object.new)
          expect(error.message).to include("mykey")
          expect(error.message).to include("Object")
        end
      end
    end
  end
end
