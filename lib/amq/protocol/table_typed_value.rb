# encoding: binary
# frozen_string_literal: true

module AMQ
  module Protocol
    class Table
      class TypedValue
        attr_reader :type, :value, :representation
        attr_accessor :scale

        def initialize(type, value, bits: nil, bytes: nil, raw: nil, representation: nil, scale: nil)
          @type = type
          @value = value
          @representation = bits || bytes || raw || representation
          @scale = scale
        end

        def value=(value)
          @value = value
          @representation = nil
        end

        def bytes
          representation
        end

        def bytes=(value)
          @representation = value
        end

        def bits
          representation
        end

        def bits=(value)
          @representation = value
        end

        alias raw bytes
        alias raw= bytes=

        def ==(other)
          other.is_a?(self.class) &&
            type == other.type &&
            scale == other.scale &&
            representation == other.representation &&
            (representation ? true : value == other.value)
        end
        alias eql? ==

        def hash
          [self.class, type, value, scale, representation].hash
        end

        def inspect
          "#<#{self.class.name} type=#{type.inspect} value=#{value.inspect}#{inspect_scale}#{inspect_representation}>"
        end

        private

        def inspect_scale
          return "" if scale.nil?

          " scale=#{scale.inspect}"
        end

        def inspect_representation
          return "" if representation.nil?

          " bytes=#{representation.inspect}"
        end
      end
    end
  end
end
