# encoding: binary
# frozen_string_literal: true

require "amq/pack"

require "amq/protocol/table"
require "amq/protocol/frame"

module AMQ
  module Protocol
    # Raised when an extension registration conflicts with a built-in
    # protocol element or with another registration in the same registry
    class RegistrationConflictError < StandardError; end

    # Raised when a descriptor mentions a field type this library
    # does not know how to serialize
    class UnknownFieldTypeError < StandardError; end

    # Raised when encoding or decoding is requested for a class or method
    # the registry has no descriptor for. The wire layout of unknown
    # methods is never guessed
    class UnknownMethodError < StandardError; end

    # Runtime registry for broker-specific AMQP 0-9-1 extension classes and
    # methods. Encoders and decoders are derived from declared class ids,
    # method ids and field layouts, so extensions do not require changes to
    # the generated AMQ::Protocol::Client code.
    #
    # Each instance is isolated: registrations in one registry are not
    # visible to any other instance, and nothing is shared globally.
    class ExtensionRegistry
      METHOD_FIELD_TYPES = [:bit, :octet, :short, :long, :longlong, :shortstr, :longstr, :table, :timestamp].freeze
      PROPERTY_TYPES     = (METHOD_FIELD_TYPES - [:bit]).freeze

      # 15 properties per flags word, the low bit is the continuation flag
      PROPERTIES_PER_FLAGS_WORD = 15
      CONTINUATION_BIT          = 0x0001

      ClassDescriptor  = Struct.new(:id, :name, :properties)
      MethodDescriptor = Struct.new(:class_id, :method_id, :name, :fields, :has_content)

      def initialize
        @classes = {}
        @methods = {}
      end

      # @param class_id [Integer] AMQP class id, must not be a built-in one
      # @param name [String, Symbol]
      # @param properties [Array<Array(Symbol, Symbol)>] content properties as
      #   [name, type] pairs, in wire order
      def register_class(class_id, name:, properties: [])
        if self.class.builtin_class_id?(class_id)
          raise RegistrationConflictError, "class id #{class_id} is used by a built-in AMQP class"
        end
        if @classes.key?(class_id)
          raise RegistrationConflictError, "class id #{class_id} is already registered in this registry"
        end

        props = normalize_fields(properties)
        props.each { |(_, type)| validate_type!(type, PROPERTY_TYPES) }

        @classes[class_id] = ClassDescriptor.new(class_id, name.to_s, props)
        self
      end

      # @param class_id [Integer] a class registered via #register_class or a built-in class id
      # @param method_id [Integer] AMQP method id, must not collide with a built-in method
      # @param name [String, Symbol]
      # @param fields [Array<Array(Symbol, Symbol)>] method arguments as
      #   [name, type] pairs, in wire order
      # @param has_content [Boolean] whether the method carries content
      #   (header + body frames); requires a registered class with a
      #   declared property layout
      def register_method(class_id, method_id, name:, fields: [], has_content: false)
        index = method_index(class_id, method_id)
        if AMQ::Protocol::METHODS.key?(index)
          raise RegistrationConflictError, "method #{class_id}.#{method_id} is used by a built-in AMQP method"
        end
        if @methods.key?(index)
          raise RegistrationConflictError, "method #{class_id}.#{method_id} is already registered in this registry"
        end
        unless @classes.key?(class_id) || self.class.builtin_class_id?(class_id)
          raise UnknownMethodError, "class id #{class_id} is not registered in this registry"
        end
        if has_content && !@classes.key?(class_id)
          raise UnknownMethodError, "cannot declare content for class id #{class_id}: its property layout is not registered"
        end

        normalized = normalize_fields(fields)
        normalized.each { |(_, type)| validate_type!(type, METHOD_FIELD_TYPES) }

        @methods[index] = MethodDescriptor.new(class_id, method_id, name.to_s, normalized, has_content)
        self
      end

      def class_registered?(class_id)
        @classes.key?(class_id)
      end

      def method_registered?(class_id, method_id)
        @methods.key?(method_index(class_id, method_id))
      end

      # Encodes a registered method into a MethodFrame
      #
      # @param values [Hash] field values keyed by field name
      # @return [AMQ::Protocol::MethodFrame]
      def encode_method(class_id, method_id, channel, values = {})
        desc = fetch_method(class_id, method_id)

        buffer = [class_id, method_id].pack(PACK_UINT16_X2)
        bit_buffer = nil
        bit_position = 0
        desc.fields.each do |(field_name, field_type)|
          if field_type == :bit
            bit_buffer ||= 0
            bit_buffer |= (1 << bit_position) if values[field_name]
            bit_position += 1
            if bit_position == 8
              buffer << [bit_buffer].pack(PACK_CHAR)
              bit_buffer = nil
              bit_position = 0
            end
          else
            if bit_buffer
              buffer << [bit_buffer].pack(PACK_CHAR)
              bit_buffer = nil
              bit_position = 0
            end
            buffer << encode_scalar(field_type, values[field_name])
          end
        end
        buffer << [bit_buffer].pack(PACK_CHAR) if bit_buffer

        MethodFrame.new(buffer, channel)
      end

      # Decodes the arguments payload (without the 4-byte class/method id
      # prefix) of a registered method
      #
      # @return [Hash] field values keyed by field name
      def decode_method(class_id, method_id, data)
        desc = fetch_method(class_id, method_id)

        offset = 0
        result = {}
        bit_buffer = nil
        bit_position = 0
        desc.fields.each do |(field_name, field_type)|
          if field_type == :bit
            if bit_buffer.nil? || bit_position == 8
              bit_buffer = data.getbyte(offset)
              offset += 1
              bit_position = 0
            end
            result[field_name] = (bit_buffer & (1 << bit_position)) != 0
            bit_position += 1
          else
            bit_buffer = nil
            bit_position = 0
            value, offset = decode_scalar(field_type, data, offset)
            result[field_name] = value
          end
        end
        result
      end

      # Encodes a content header payload (class id, weight, body size,
      # property flags and property values) for a registered class,
      # matching AMQ::Protocol::Basic.encode_properties
      #
      # @return [String] binary string
      def encode_properties(class_id, body_size, properties)
        desc = fetch_class(class_id)
        props = desc.properties

        flags_words = []
        pieces = []
        properties.each do |key, value|
          next if value.nil?

          position = props.index { |(prop_name, _)| prop_name == key }
          raise UnknownMethodError, "class #{desc.name} has no declared property #{key.inspect}" if position.nil?

          word = position / PROPERTIES_PER_FLAGS_WORD
          flags_words[word] ||= 0
          flags_words[word] |= 1 << (PROPERTIES_PER_FLAGS_WORD - (position % PROPERTIES_PER_FLAGS_WORD))
          pieces[position] = encode_scalar(props[position][1], value)
        end

        result = [class_id, 0].pack(PACK_UINT16_X2)
        result << AMQ::Pack.pack_uint64_big_endian(body_size)
        flags_words = [0] if flags_words.empty?
        last_word = flags_words.length - 1
        flags_words.each_with_index do |word, i|
          word ||= 0
          word |= CONTINUATION_BIT if i < last_word
          result << [word].pack(PACK_UINT16)
        end
        pieces_joined = pieces.join(EMPTY_STRING)
        result.force_encoding(pieces_joined.encoding) + pieces_joined
      end

      # Decodes property flags and values from a content header payload
      # (the part after class id, weight and body size)
      #
      # @return [Hash] property values keyed by property name
      def decode_properties(class_id, data)
        desc = fetch_class(class_id)

        offset = 0
        words = []
        loop do
          word = data.byteslice(offset, 2).unpack1(PACK_UINT16)
          offset += 2
          words << word
          break if (word & CONTINUATION_BIT).zero?
        end

        properties = {}
        desc.properties.each_with_index do |(prop_name, prop_type), position|
          word = words[position / PROPERTIES_PER_FLAGS_WORD]
          next if word.nil?
          next if (word & (1 << (PROPERTIES_PER_FLAGS_WORD - (position % PROPERTIES_PER_FLAGS_WORD)))).zero?

          value, offset = decode_scalar(prop_type, data, offset)
          properties[prop_name] = value
        end
        properties
      end

      def self.builtin_class_id?(class_id)
        Protocol::Class.classes.any? { |klass| klass.method_id == class_id }
      end

      private

      def method_index(class_id, method_id)
        class_id << 16 | method_id
      end

      def fetch_method(class_id, method_id)
        @methods.fetch(method_index(class_id, method_id)) do
          raise UnknownMethodError, "method #{class_id}.#{method_id} is not registered in this registry"
        end
      end

      def fetch_class(class_id)
        @classes.fetch(class_id) do
          raise UnknownMethodError, "class id #{class_id} is not registered in this registry"
        end
      end

      def normalize_fields(fields)
        fields.map do |field|
          name, type = field
          [name.to_sym, type.to_sym]
        end
      end

      def validate_type!(type, allowed)
        return if allowed.include?(type)

        raise UnknownFieldTypeError, "unknown field type #{type.inspect}, expected one of #{allowed.map(&:inspect).join(', ')}"
      end

      def encode_scalar(type, value)
        case type
        when :octet
          [value].pack(PACK_CHAR)
        when :short
          [value].pack(PACK_UINT16)
        when :long
          [value].pack(PACK_UINT32)
        when :longlong
          AMQ::Pack.pack_uint64_big_endian(value)
        when :shortstr
          (value.to_s.bytesize.chr + value.to_s).b
        when :longstr
          ([value.to_s.bytesize].pack(PACK_UINT32) + value.to_s).b
        when :table
          AMQ::Protocol::Table.encode(value)
        when :timestamp
          AMQ::Pack.pack_uint64_big_endian(value.to_i)
        end
      end

      # @return [Array(Object, Integer)] decoded value and the new offset
      def decode_scalar(type, data, offset)
        case type
        when :octet
          [data.getbyte(offset), offset + 1]
        when :short
          [data.byteslice(offset, 2).unpack1(PACK_UINT16), offset + 2]
        when :long
          [data.byteslice(offset, 4).unpack1(PACK_UINT32), offset + 4]
        when :longlong
          [data.byteslice(offset, 8).unpack1(PACK_UINT64_BE), offset + 8]
        when :shortstr
          length = data.getbyte(offset)
          [data.byteslice(offset + 1, length), offset + 1 + length]
        when :longstr
          length = data.byteslice(offset, 4).unpack1(PACK_UINT32)
          [data.byteslice(offset + 4, length), offset + 4 + length]
        when :table
          length = AMQ::Protocol::Table.length(data.byteslice(offset, 4))
          [AMQ::Protocol::Table.decode(data.byteslice(offset, length + 4)), offset + length + 4]
        when :timestamp
          [Time.at(data.byteslice(offset, 8).unpack1(PACK_UINT64_BE)), offset + 8]
        end
      end
    end
  end
end
