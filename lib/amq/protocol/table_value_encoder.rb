# encoding: binary
# frozen_string_literal: true

require "bigdecimal"

require "amq/protocol/type_constants"
require "amq/protocol/table_typed_value"
require "date"

require "amq/protocol/float_32bit"

module AMQ
  module Protocol

    class TableValueEncoder

      #
      # Behaviors
      #

      include TypeConstants

      PACK_UINT32       = 'N'.freeze
      PACK_UINT64_BE    = 'Q>'.freeze
      PACK_INT64_BE     = 'q>'.freeze
      PACK_FLOAT32_BE   = 'g'.freeze
      PACK_FLOAT64      = 'G'.freeze
      PACK_UCHAR_UINT32 = 'CN'.freeze
      TEN_BIG_DECIMAL   = BigDecimal(TEN).freeze

      #
      # API
      #

      def self.encode(value, accumulator = String.new)
        case value
        when Table::TypedValue then
          return encode_typed(value, accumulator)
        when String then
          accumulator << TYPE_STRING
          accumulator << [value.bytesize].pack(PACK_UINT32)
          accumulator << value.dup.force_encoding(accumulator.encoding)
        when Symbol then
          str = value.to_s
          accumulator << TYPE_STRING
          accumulator << [str.bytesize].pack(PACK_UINT32)
          accumulator << str.force_encoding(accumulator.encoding)
        when Integer then
          accumulator << TYPE_SIGNED_64BIT
          accumulator << [value].pack(PACK_INT64_BE)
        when AMQ::Protocol::Float32Bit then
          accumulator << TYPE_32BIT_FLOAT
          accumulator << [value.value].pack(PACK_FLOAT32_BE)
        when Float then
          accumulator << TYPE_64BIT_FLOAT
          accumulator << [value].pack(PACK_FLOAT64)
        when true, false then
          accumulator << TYPE_BOOLEAN
          accumulator << (value ? BOOLEAN_TRUE : BOOLEAN_FALSE)
        when Time then
          accumulator << TYPE_TIME
          accumulator << [value.to_i].pack(PACK_INT64_BE)
        when nil then
          accumulator << TYPE_VOID
        when Array then
          accumulator << TYPE_ARRAY
          accumulator << [self.array_size(value)].pack(PACK_UINT32)

          value.each { |v| accumulator << self.encode(v) }
        when Hash then
          accumulator << TYPE_HASH
          accumulator << AMQ::Protocol::Table.encode(value)
        else
          # We don't want to require these libraries.
          if value.is_a?(BigDecimal)
            accumulator << TYPE_DECIMAL
            encode_big_decimal(accumulator, value)
          else
            raise ArgumentError, "Unsupported value #{value.inspect} of type #{value.class.name}"
          end # if
        end # case

        accumulator
      end # self.encode(value)

      def self.encode_typed(value, accumulator = String.new)
        return encode(value, accumulator) unless value.is_a?(Table::TypedValue)

        accumulator << value.type

        case value.type
        when TYPE_STRING, TYPE_BYTE_ARRAY
          string = value.value.to_s
          accumulator << [string.bytesize].pack(PACK_UINT32)
          accumulator << string.dup.force_encoding(accumulator.encoding)
        when TYPE_BYTE
          accumulator << pack_integer(value.value, 'C', 0, (1 << 8) - 1)
        when TYPE_SIGNED_16BIT
          accumulator << pack_integer(value.value, 's>', -(1 << 15), (1 << 15) - 1)
        when TYPE_INTEGER
          accumulator << pack_integer(value.value, PACK_UINT32, 0, (1 << 32) - 1)
        when TYPE_SIGNED_64BIT
          accumulator << pack_integer(value.value, PACK_INT64_BE, -(1 << 63), (1 << 63) - 1)
        when TYPE_DECIMAL
          encode_big_decimal(accumulator, value.value, value.scale)
        when TYPE_TIME
          timestamp = value.value.to_i
          accumulator << pack_integer(timestamp, PACK_UINT64_BE, 0, (1 << 64) - 1)
        when TYPE_BOOLEAN
          accumulator << (value.value ? BOOLEAN_TRUE : BOOLEAN_FALSE)
        when TYPE_VOID
        when TYPE_32BIT_FLOAT
          accumulator << (value.representation || [value.value].pack(PACK_FLOAT32_BE))
        when TYPE_64BIT_FLOAT
          accumulator << (value.representation || [value.value].pack(PACK_FLOAT64))
        when TYPE_ARRAY
          encode_typed_array(accumulator, value.value)
        when TYPE_HASH
          accumulator << Table.encode_entries(value.value)
        else
          raise ArgumentError, "Unsupported AMQP type #{value.type.inspect}"
        end

        accumulator
      end




      def self.field_value_size(value)
        # the type tag takes 1 byte
        acc = 1

        case value
        when Table::TypedValue, AMQ::Protocol::Float32Bit then
          encode(value).bytesize
        when BigDecimal then
          encode(value).bytesize
        when String then
          acc += (value.bytesize + 4)
        when Integer then
          acc += 8
        when Float then
          acc += 8
        when Time, DateTime then
          acc += 8
        when true, false then
          acc += 1
        when nil then
          # nothing, type tag alone is enough
        when Hash then
          acc += (4 + Table.hash_size(value))
        when Array then
          acc += (4 + self.array_size(value))
        end

        acc
      end # self.field_value_size(value)


      def self.array_size(value)
        acc = 0
        value.each { |v| acc += self.field_value_size(v) }

        acc
      end # self.array_size(value)

      def self.encode_big_decimal(accumulator, value, explicit_scale = nil)
        scale = explicit_scale || value.scale
        scaled = value * (TEN_BIG_DECIMAL ** scale)
        raw = scaled.to_i
        unless scale >= 0 && scale <= 255 && scaled == raw && raw >= 0 && raw < (1 << 32)
          raise RangeError, "BigDecimal #{value.inspect} cannot be encoded with scale #{scale}"
        end

        accumulator << [scale, raw].pack(PACK_UCHAR_UINT32)
      end

      def self.encode_typed_array(accumulator, values)
        body = +''
        values.each { |value| encode_typed(value, body) }
        accumulator << [body.bytesize].pack(PACK_UINT32)
        accumulator << body
      end

      def self.pack_integer(value, directive, minimum, maximum)
        unless value.is_a?(Integer) && value >= minimum && value <= maximum
          raise RangeError, "Integer #{value.inspect} cannot be encoded with #{directive.inspect}"
        end

        [value].pack(directive)
      end

      private_class_method :encode_big_decimal, :encode_typed_array, :pack_integer

    end # TableValueEncoder

  end # Protocol
end # AMQ
