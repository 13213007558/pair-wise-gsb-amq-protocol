# encoding: binary
# frozen_string_literal: true

module AMQ
  module Protocol
    # Represents a method frame whose (class id, method id) pair is not
    # known to this library, for example a broker-specific extension method.
    # The arguments are kept as a raw, unparsed payload together with the
    # method identity so they can be inspected or passed on to an
    # AMQ::Protocol::ExtensionRegistry that knows the field layout.
    class RawMethod
      attr_reader :class_id, :method_id, :payload

      def initialize(class_id, method_id, payload)
        @class_id  = class_id
        @method_id = method_id
        @payload   = payload
      end

      def index
        @class_id << 16 | @method_id
      end

      def ==(other)
        other.is_a?(RawMethod) &&
          other.class_id == @class_id &&
          other.method_id == @method_id &&
          other.payload == @payload
      end
      alias_method :eql?, :==
    end
  end
end
