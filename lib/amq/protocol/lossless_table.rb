# encoding: binary
# frozen_string_literal: true

require "amq/protocol/type_constants"
require "amq/protocol/float_32bit"

module AMQ
  module Protocol

    # A field table value decoded together with its AMQP 0-9-1 wire type.
    #
    # Unlike plain Hash decoding (see AMQ::Protocol::Table.decode), a
    # TaggedValue retains what is needed to re-encode the value exactly as
    # it was received: integer width, decimal scale, string vs. byte array
    # type tag, and the original bit pattern of 32-bit floats.
    class TaggedValue

      #
      # Behaviors
      #

      include TypeConstants

      # Pack format strings use explicit endianness
      PACK_UINT32_BE    = 'N'.freeze
      PACK_INT16_BE     = 's>'.freeze
      PACK_INT64_BE     = 'q>'.freeze
      PACK_UINT64_BE    = 'Q>'.freeze
      PACK_UCHAR        = 'C'.freeze
      PACK_UCHAR_UINT32 = 'CN'.freeze
      PACK_32BIT_FLOAT  = 'f'.freeze  # single precision float (native endian, matches TableValueEncoder)
      PACK_64BIT_FLOAT  = 'G'.freeze  # big-endian double precision float

      #
      # API
      #

      # AMQP 0-9-1 type tag of this value, e.g. "l" for a signed 64-bit integer
      attr_reader :type
      # Scale (number of decimal places) of a TYPE_DECIMAL value, nil otherwise
      attr_reader :scale
      # Original bit pattern of a TYPE_32BIT_FLOAT value as an unsigned
      # 32-bit integer, nil for other types and once the value is replaced
      attr_reader :bits
      attr_reader :value

      def initialize(type, value, scale: nil, bits: nil)
        @type  = type
        @value = value
        @scale = scale
        @bits  = bits
      end


      # Replaces the wrapped value. The type tag (and decimal scale) are
      # retained, so the new value is encoded using the same wire type.
      def value=(new_value)
        @value = new_value
        # a replaced 32-bit float no longer carries its original bit pattern
        @bits  = nil
      end


      # Serializes this value with its retained type tag
      def encode
        @type.dup << encode_payload
      end


      # The value with all wire type information stripped: nested tables
      # become plain hashes, array elements become plain values
      def plain_value
        case @type
        when TYPE_HASH
          @value.to_h
        when TYPE_ARRAY
          @value.map { |element| element.is_a?(TaggedValue) ? element.plain_value : element }
        else
          @value
        end
      end


      # Wraps a plain Ruby value, choosing a type tag the same way
      # AMQ::Protocol::TableValueEncoder does
      def self.infer(value)
        case value
        when TaggedValue               then value
        when String                    then new(TYPE_STRING, value)
        when Symbol                    then new(TYPE_STRING, value.to_s)
        when Integer                   then new(TYPE_SIGNED_64BIT, value)
        when AMQ::Protocol::Float32Bit then new(TYPE_32BIT_FLOAT, value.value)
        when Float                     then new(TYPE_64BIT_FLOAT, value)
        when true, false               then new(TYPE_BOOLEAN, value)
        when Time                      then new(TYPE_TIME, value)
        when nil                       then new(TYPE_VOID, nil)
        when Array                     then new(TYPE_ARRAY, value.map { |element| infer(element) })
        when Hash                      then new(TYPE_HASH, LosslessTable.from_hash(value))
        when LosslessTable             then new(TYPE_HASH, value)
        else
          # We don't want to require these libraries.
          if defined?(BigDecimal) && value.is_a?(BigDecimal)
            _sign, digits, _base, exponent = value.split
            new(TYPE_DECIMAL, value, scale: [digits.length - exponent, 0].max)
          else
            raise ArgumentError, "Unsupported value of type #{value.class.name}"
          end # if
        end # case
      end # self.infer(value)


      # Decodes a single typed value at the given offset, returning
      # the TaggedValue and the new offset
      def self.decode(type, data, offset)
        case type
        when TYPE_STRING, TYPE_BYTE_ARRAY
          v, offset = decode_long_string(data, offset)
          [new(type, v), offset]
        when TYPE_INTEGER
          v = data.byteslice(offset, 4).unpack1(PACK_UINT32_BE)
          [new(TYPE_INTEGER, v), offset + 4]
        when TYPE_DECIMAL
          scale, raw = data.byteslice(offset, 5).unpack(PACK_UCHAR_UINT32)
          v = BigDecimal(raw.to_s) * (BigDecimal(TEN) ** -scale)
          [new(TYPE_DECIMAL, v, scale: scale), offset + 5]
        when TYPE_TIME
          v = Time.at(data.byteslice(offset, 8).unpack1(PACK_UINT64_BE))
          [new(TYPE_TIME, v), offset + 8]
        when TYPE_HASH
          length = data.byteslice(offset, 4).unpack1(PACK_UINT32_BE)
          v = LosslessTable.decode(data.byteslice(offset, length + 4))
          [new(TYPE_HASH, v), offset + 4 + length]
        when TYPE_BOOLEAN
          [new(TYPE_BOOLEAN, data.getbyte(offset) == 1), offset + 1]
        when TYPE_BYTE
          [new(TYPE_BYTE, data.getbyte(offset)), offset + 1]
        when TYPE_SIGNED_16BIT
          v = data.byteslice(offset, 2).unpack1(PACK_INT16_BE)
          [new(TYPE_SIGNED_16BIT, v), offset + 2]
        when TYPE_SIGNED_64BIT
          v = data.byteslice(offset, 8).unpack1(PACK_INT64_BE)
          [new(TYPE_SIGNED_64BIT, v), offset + 8]
        when TYPE_32BIT_FLOAT
          bytes = data.byteslice(offset, 4)
          [new(TYPE_32BIT_FLOAT, bytes.unpack1(PACK_32BIT_FLOAT), bits: bytes.unpack1(PACK_UINT32_BE)), offset + 4]
        when TYPE_64BIT_FLOAT
          v = data.byteslice(offset, 8).unpack1(PACK_64BIT_FLOAT)
          [new(TYPE_64BIT_FLOAT, v), offset + 8]
        when TYPE_VOID
          [new(TYPE_VOID, nil), offset]
        when TYPE_ARRAY
          array_length = data.byteslice(offset, 4).unpack1(PACK_UINT32_BE)
          offset += 4
          limit = offset + array_length
          elements = []
          while offset < limit
            element_type = data.byteslice(offset, 1)
            element, offset = decode(element_type, data, offset + 1)
            elements << element
          end
          [new(TYPE_ARRAY, elements), offset]
        else
          raise ArgumentError, "Not a valid type: " + type.inspect
        end # case
      end # self.decode(type, data, offset)


      def self.decode_long_string(data, offset)
        length = data.byteslice(offset, 4).unpack1(PACK_UINT32_BE)
        offset += 4
        v = data.byteslice(offset, length)

        [v, offset + length]
      end


      private

      def encode_payload
        case @type
        when TYPE_STRING, TYPE_BYTE_ARRAY
          bytes = @value.to_s
          [bytes.bytesize].pack(PACK_UINT32_BE) << bytes.dup.force_encoding(Encoding::BINARY)
        when TYPE_BYTE         then [@value].pack(PACK_UCHAR)
        when TYPE_SIGNED_16BIT then [@value].pack(PACK_INT16_BE)
        when TYPE_INTEGER      then [@value].pack(PACK_UINT32_BE)
        when TYPE_SIGNED_64BIT then [@value].pack(PACK_INT64_BE)
        when TYPE_DECIMAL
          raw = (@value * (10**@scale)).to_i
          [@scale, raw].pack(PACK_UCHAR_UINT32)
        when TYPE_TIME         then [@value.to_i].pack(PACK_UINT64_BE)
        when TYPE_BOOLEAN      then @value ? BOOLEAN_TRUE : BOOLEAN_FALSE
        when TYPE_32BIT_FLOAT  then @bits ? [@bits].pack(PACK_UINT32_BE) : [@value].pack(PACK_32BIT_FLOAT)
        when TYPE_64BIT_FLOAT  then [@value].pack(PACK_64BIT_FLOAT)
        when TYPE_VOID         then String.new
        when TYPE_HASH
          table = @value.is_a?(LosslessTable) ? @value : LosslessTable.from_hash(@value)
          table.encode
        when TYPE_ARRAY
          payload = @value.inject(String.new) do |acc, element|
            acc << (element.is_a?(TaggedValue) ? element : TaggedValue.infer(element)).encode
          end
          [payload.bytesize].pack(PACK_UINT32_BE) << payload
        else
          raise ArgumentError, "Not a valid type: " + @type.inspect
        end # case
      end # encode_payload

    end # TaggedValue


    # A field table decoded without losing AMQP 0-9-1 wire type information.
    #
    # AMQ::Protocol::Table.decode collapses a field table into a plain Hash:
    # integer width, decimal scale, string vs. byte array tags and 32-bit
    # float bit patterns are lost, and duplicate field names silently
    # overwrite each other. LosslessTable keeps an ordered list of
    # [field name, TaggedValue] entries instead, so a decoded table
    # re-encodes to the exact same bytes and duplicate names stay
    # observable. Plain Ruby values can be assigned to entries; they keep
    # the wire type of the entry they replace.
    class LosslessTable

      #
      # Behaviors
      #

      include Enumerable
      include TypeConstants

      # Pack format string
      PACK_UINT32_BE = 'N'.freeze

      #
      # API
      #

      # Ordered [field name, TaggedValue] pairs, including duplicates
      attr_reader :entries

      def self.decode(data)
        table = new
        table_length = data.unpack1(PACK_UINT32_BE)

        return table if table_length.zero?

        offset = 4
        limit  = 4 + table_length
        while offset < limit
          key_length = data.getbyte(offset)
          key = data.byteslice(offset + 1, key_length)
          offset += 1 + key_length
          type = data.byteslice(offset, 1)
          tagged, offset = TaggedValue.decode(type, data, offset + 1)
          table.append(key, tagged)
        end

        table
      end


      # Builds a table from a plain Hash, choosing type tags the same way
      # AMQ::Protocol::TableValueEncoder does
      def self.from_hash(hash)
        table = new
        hash.each { |key, value| table.append(key.to_s, TaggedValue.infer(value)) }
        table
      end


      def initialize
        @entries = []
      end


      def each(&block)
        return enum_for(:each) unless block_given?

        @entries.each(&block)
      end


      # Appends an entry without replacing existing ones, so duplicate
      # field names stay observable
      def append(key, tagged_value)
        @entries << [key, tagged_value]
        self
      end


      # The TaggedValue of the first entry with this field name, if any
      def [](key)
        pair = @entries.find { |k, _| k == key }
        pair && pair[1]
      end


      # Replaces the value of the first entry with this field name,
      # retaining its wire type, or appends a new entry
      def []=(key, value)
        pair = @entries.find { |k, _| k == key }
        if value.is_a?(TaggedValue)
          pair ? (pair[1] = value) : append(key, value)
        elsif pair
          pair[1].value = value
        else
          append(key, TaggedValue.infer(value))
        end
      end


      def key?(key)
        @entries.any? { |k, _| k == key }
      end


      def size
        @entries.size
      end


      # Field names that occur more than once, in order of first appearance
      def duplicate_keys
        counts = Hash.new(0)
        @entries.each { |key, _| counts[key] += 1 }
        counts.select { |_, count| count > 1 }.keys
      end


      # A plain Hash of plain values, equivalent to what
      # AMQ::Protocol::Table.decode returns; later duplicates overwrite
      # earlier ones
      def to_h
        result = {}
        @entries.each { |key, tagged| result[key] = tagged.plain_value }
        result
      end


      def encode
        buffer = String.new
        @entries.each do |key, tagged|
          key = key.to_s
          buffer << key.bytesize.chr << key
          buffer << tagged.encode
        end

        [buffer.bytesize].pack(PACK_UINT32_BE) << buffer
      end

    end # LosslessTable
  end # Protocol
end # AMQ
