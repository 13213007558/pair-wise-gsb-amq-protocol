# encoding: binary
# frozen_string_literal: true

require "amq/pack"

require "amq/protocol/constants"
require "amq/protocol/exceptions"
require "amq/protocol/frame"
require "amq/protocol/table"

module AMQ
  module Protocol
    # Base class for descriptor registry errors.
    class DescriptorError < Protocol::Error; end

    # Raised when a registration collides with a built-in or an
    # already registered class or method id.
    class DescriptorConflictError < DescriptorError; end

    # Raised when a payload cannot be decoded with the declared field
    # layout. The registry never guesses layouts: undeclared field
    # types, truncated payloads, trailing bytes and undeclared property
    # flags are all hard errors.
    class FieldLayoutError < DescriptorError; end

    # An unknown method preserved as a raw payload together with its
    # identity (class id, method id and channel), so it can be
    # inspected and re-encoded byte for byte.
    class RawMethod
      attr_reader :class_id, :method_id, :channel, :payload

      def initialize(class_id, method_id, payload, channel = 0)
        @class_id  = class_id
        @method_id = method_id
        @payload   = payload
        @channel   = channel
      end

      def index
        @class_id << 16 | @method_id
      end

      def encode
        MethodFrame.new([@class_id, @method_id].pack(PACK_UINT16_X2) << @payload, @channel)
      end
    end

    # Runtime registry of AMQP method descriptors. Broker extensions
    # can declare class ids, method ids and field layouts and get
    # encoders/decoders generated from the declarations, including
    # adjacent bit field packing and content property flags.
    #
    # Each instance is isolated: registrations never mutate the
    # built-in protocol tables, and registering an id occupied by a
    # built-in class or method raises DescriptorConflictError.
    class DescriptorRegistry
      FIELD_TYPES = [:bit, :octet, :short, :long, :longlong, :shortstr, :longstr, :table, :timestamp].freeze

      PROPERTY_TYPES = (FIELD_TYPES - [:bit]).freeze

      CONTINUATION_BIT = 0x0001

      PROPERTIES_PER_FLAGS_WORD = 15

      ClassDescriptor = Struct.new(:class_id, :name, :properties)

      class MethodDescriptor
        attr_reader :class_id, :method_id, :name, :fields

        def initialize(class_id, method_id, name, fields, has_content)
          @class_id    = class_id
          @method_id   = method_id
          @name        = name
          @fields      = fields
          @has_content = has_content
          @struct      = Struct.new(*fields.map(&:last))
        end

        def has_content?
          @has_content
        end

        def index
          @class_id << 16 | @method_id
        end

        def decode_values(values)
          @struct.new(*values)
        end
      end

      def initialize
        @classes = {}
        @methods = {}
      end

      # Declares an extension class. +properties+ is an ordered list of
      # [type, name] pairs used for content header property flags.
      def register_class(class_id, name:, properties: [])
        class_id = validate_id(class_id, "class id")
        if self.class.builtin_class_ids.include?(class_id) || @classes.key?(class_id)
          raise DescriptorConflictError, "class id #{class_id} is already taken"
        end

        properties = normalize_fields(properties, PROPERTY_TYPES, "property")
        descriptor = ClassDescriptor.new(class_id, name, properties)
        @classes[class_id] = descriptor
        descriptor
      end

      # Declares an extension method on a previously registered class.
      # +fields+ is an ordered list of [type, name] pairs; adjacent
      # :bit fields are packed into shared octets.
      def register_method(class_id, method_id, name:, fields: [], has_content: false)
        class_id  = validate_id(class_id, "class id")
        method_id = validate_id(method_id, "method id")
        klass = @classes[class_id] || raise(DescriptorError, "class id #{class_id} is not registered")
        index = class_id << 16 | method_id
        if AMQ::Protocol::METHODS.key?(index) || @methods.key?(index)
          raise DescriptorConflictError, "method #{class_id}.#{method_id} is already taken"
        end
        if has_content && klass.properties.empty?
          raise DescriptorError, "#{name} declares content but class #{klass.name} has no properties"
        end

        descriptor = MethodDescriptor.new(class_id, method_id, name, normalize_fields(fields, FIELD_TYPES, "field"), has_content)
        @methods[index] = descriptor
        descriptor
      end

      def registered?(class_id, method_id = nil)
        if method_id.nil?
          @classes.key?(class_id)
        else
          @methods.key?(class_id << 16 | method_id)
        end
      end

      # Encodes a registered method into a MethodFrame. Values are
      # positional, in field declaration order.
      def encode(class_id, method_id, channel, *values)
        descriptor = @methods.fetch(class_id << 16 | method_id) do
          raise DescriptorError, "method #{class_id}.#{method_id} is not registered"
        end
        unless values.length == descriptor.fields.length
          raise ArgumentError, "#{descriptor.name} takes #{descriptor.fields.length} values, got #{values.length}"
        end

        MethodFrame.new([class_id, method_id].pack(PACK_UINT16_X2) << encode_fields(descriptor.fields, values), channel)
      end

      # Decodes a method frame. Registered methods are decoded with
      # their declared layout, built-in methods are delegated to the
      # built-in classes, and unknown methods are preserved as
      # RawMethod instances.
      def decode_frame(frame)
        class_id, method_id = frame.payload.unpack(PACK_UINT16_X2)
        decode(class_id, method_id, frame.payload.byteslice(4..-1), channel: frame.channel)
      end

      def decode(class_id, method_id, data, channel: 0)
        index = class_id << 16 | method_id
        if (descriptor = @methods[index])
          descriptor.decode_values(decode_fields(descriptor.fields, data))
        elsif (builtin = AMQ::Protocol::METHODS[index])
          # some built-in methods (e.g. basic.qos) are encode-only
          builtin.respond_to?(:decode) ? builtin.decode(data) : RawMethod.new(class_id, method_id, data, channel)
        else
          RawMethod.new(class_id, method_id, data, channel)
        end
      end

      # Encodes a content header payload (class id, weight, body size,
      # property flags and values) for a registered class, in the same
      # wire format as Basic.encode_properties.
      def encode_properties(class_id, body_size, properties)
        descriptor = @classes.fetch(class_id) do
          raise DescriptorError, "class id #{class_id} is not registered"
        end
        declared = descriptor.properties
        values = []
        set_indexes = []
        properties.each do |key, value|
          next if value.nil?
          index = declared.index { |(_, name)| name == key.to_sym }
          raise FieldLayoutError, "no property #{key.inspect} declared on class #{descriptor.name}" unless index
          set_indexes << index
          values << [index, value]
        end

        word_count = set_indexes.empty? ? 1 : (set_indexes.max / PROPERTIES_PER_FLAGS_WORD) + 1
        words = Array.new(word_count, 0)
        set_indexes.each { |i| words[i / PROPERTIES_PER_FLAGS_WORD] |= 1 << (15 - (i % PROPERTIES_PER_FLAGS_WORD)) }
        (0...word_count - 1).each { |w| words[w] |= CONTINUATION_BIT }

        result = [class_id, 0].pack(PACK_UINT16_X2)
        result << AMQ::Pack.pack_uint64_big_endian(body_size)
        result << words.pack("n" + word_count.to_s)
        values.sort_by(&:first).each do |(index, value)|
          result << encode_scalar(declared[index].first, value)
        end
        result
      end

      # Decodes content properties starting at the property flags, in
      # the same wire format as Basic.decode_properties. Flag bits
      # without a declared property raise FieldLayoutError.
      def decode_properties(class_id, data)
        descriptor = @classes.fetch(class_id) do
          raise DescriptorError, "class id #{class_id} is not registered"
        end
        declared = descriptor.properties
        # all flags words come first, then all property values
        selected = []
        offset = 0
        word_index = 0
        loop do
          flags = unpack_at(data, offset, 2, PACK_UINT16)
          offset += 2
          PROPERTIES_PER_FLAGS_WORD.times do |bit|
            next unless (flags & (1 << (15 - bit))) != 0
            property = declared[word_index * PROPERTIES_PER_FLAGS_WORD + bit]
            unless property
              raise FieldLayoutError, "no property declared for flags bit #{15 - bit} of word #{word_index} on class #{descriptor.name}"
            end
            selected << property
          end
          break if (flags & CONTINUATION_BIT).zero?
          word_index += 1
        end
        properties = {}
        selected.each do |property|
          value, offset = decode_scalar(property.first, data, offset)
          properties[property.last] = value
        end
        unless offset == data.bytesize
          raise FieldLayoutError, "#{data.bytesize - offset} trailing bytes after properties of class #{descriptor.name}"
        end
        properties
      end

      # @return [Array<Integer>] class ids occupied by the built-in protocol classes
      def self.builtin_class_ids
        AMQ::Protocol.classes.map(&:method_id)
      end

      private

      def validate_id(value, label)
        unless value.is_a?(Integer) && value >= 0 && value <= 0xFFFF
          raise ArgumentError, "#{label} has to be an integer in range 0..65535, was #{value.inspect}"
        end
        value
      end

      def normalize_fields(declarations, allowed_types, label)
        declarations.map do |declaration|
          type, name = declaration
          unless allowed_types.include?(type)
            raise ArgumentError, "unknown #{label} type #{type.inspect}, expected one of #{allowed_types.inspect}"
          end
          [type, name.to_sym]
        end
      end

      def encode_fields(fields, values)
        buffer = +""
        bit_buffer = nil
        bit_position = 0
        fields.each_with_index do |(type, _), i|
          if type == :bit
            bit_buffer ||= 0
            bit_buffer |= (1 << bit_position) if values[i]
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
            buffer << encode_scalar(type, values[i])
          end
        end
        buffer << [bit_buffer].pack(PACK_CHAR) if bit_buffer
        buffer
      end

      def decode_fields(fields, data)
        offset = 0
        values = []
        i = 0
        while i < fields.length
          type = fields[i].first
          if type == :bit
            byte = data.getbyte(offset) || raise(FieldLayoutError, "truncated bit octet at offset #{offset}")
            offset += 1
            bit_position = 0
            while i < fields.length && fields[i].first == :bit && bit_position < 8
              values << ((byte & (1 << bit_position)) != 0)
              bit_position += 1
              i += 1
            end
          else
            value, offset = decode_scalar(type, data, offset)
            values << value
            i += 1
          end
        end
        unless offset == data.bytesize
          raise FieldLayoutError, "#{data.bytesize - offset} trailing bytes after declared fields"
        end
        values
      end

      def encode_scalar(type, value)
        case type
        when :octet     then [value].pack(PACK_CHAR)
        when :short     then [value].pack(PACK_UINT16)
        when :long      then [value].pack(PACK_UINT32)
        when :longlong  then AMQ::Pack.pack_uint64_big_endian(value)
        when :shortstr  then value.to_s.bytesize.chr << value.to_s
        when :longstr   then [value.to_s.bytesize].pack(PACK_UINT32) << value.to_s
        when :table     then Table.encode(value)
        when :timestamp then AMQ::Pack.pack_uint64_big_endian(value.to_i)
        end
      end

      def decode_scalar(type, data, offset)
        case type
        when :octet
          [data.getbyte(offset) || raise(FieldLayoutError, "truncated octet at offset #{offset}"), offset + 1]
        when :short
          [unpack_at(data, offset, 2, PACK_UINT16), offset + 2]
        when :long
          [unpack_at(data, offset, 4, PACK_UINT32), offset + 4]
        when :longlong
          [unpack_at(data, offset, 8, PACK_UINT64_BE), offset + 8]
        when :shortstr
          size = data.getbyte(offset) || raise(FieldLayoutError, "truncated shortstr at offset #{offset}")
          [byteslice_at(data, offset + 1, size), offset + 1 + size]
        when :longstr
          size = unpack_at(data, offset, 4, PACK_UINT32)
          [byteslice_at(data, offset + 4, size), offset + 4 + size]
        when :table
          size = 4 + unpack_at(data, offset, 4, PACK_UINT32)
          [Table.decode(byteslice_at(data, offset, size)), offset + size]
        when :timestamp
          [Time.at(unpack_at(data, offset, 8, PACK_UINT64_BE)), offset + 8]
        end
      end

      def unpack_at(data, offset, size, directive)
        byteslice_at(data, offset, size).unpack1(directive)
      end

      def byteslice_at(data, offset, size)
        slice = data.byteslice(offset, size)
        if slice.nil? || slice.bytesize != size
          raise FieldLayoutError, "truncated payload: need #{size} bytes at offset #{offset}, have #{data.bytesize - offset}"
        end
        slice
      end
    end
  end
end
