# frozen_string_literal: true

module AMQ
  module Protocol
    class Error < StandardError
      DEFAULT_MESSAGE = "AMQP error".freeze

      def self.inherited(subclass)
        @_subclasses ||= []
        @_subclasses << subclass
      end # self.inherited(subclazz)

      def self.subclasses_with_values
        @_subclasses.select{ |k| defined?(k::VALUE) }
      end # self.subclasses_with_values

      def self.[](code)
        if result = subclasses_with_values.detect { |klass| klass::VALUE == code }
          result
        else
          raise "No such exception class for code #{code}" unless result
        end # if
      end # self.[]

      def initialize(message = self.class::DEFAULT_MESSAGE)
        super(message)
      end
    end

    class FrameTypeError < Protocol::Error
      def initialize(types)
        super("Must be one of #{types.inspect}")
      end
    end

    class EmptyResponseError < Protocol::Error
      DEFAULT_MESSAGE = "Empty response received from the server."

      def initialize(message = self.class::DEFAULT_MESSAGE)
        super(message)
      end
    end

    class BadResponseError < Protocol::Error
      def initialize(argument, expected, actual)
        super("Argument #{argument} has to be #{expected.inspect}, was #{actual.inspect}")
      end
    end

    # Raised while assembling a content frameset (method + header + body
    # frames) when the frames of a channel do not form a valid sequence.
    class ContentAssemblyError < Protocol::Error
      attr_reader :channel, :state, :method

      def initialize(message, channel, state, method = nil)
        @channel = channel
        @state   = state
        @method  = method
        super(message)
      end
    end

    # Raised when a new method frame arrives on a channel whose previous
    # content frameset has not received all of its body yet.
    class UnexpectedMethodFrame < ContentAssemblyError
      def initialize(channel, state, method)
        super(
          "received method frame #{method.class.name.inspect} on channel #{channel} " \
          "while #{state[:received].inspect} of #{state[:expected].inspect} body bytes were assembled " \
          "(#{state[:remaining].inspect} still missing)",
          channel, state, method
        )
      end
    end

    # Raised when a body frame is received although the preceding header
    # declared a zero-length body.
    class UnexpectedBodyFrame < ContentAssemblyError
      def initialize(channel, state)
        super(
          "received a body frame on channel #{channel} although the header " \
          "declared a zero-length body",
          channel, state
        )
      end
    end

    # Raised when a header frame is received where a body frame or nothing
    # at all was expected.
    class UnexpectedHeaderFrame < ContentAssemblyError
      def initialize(channel, state)
        super(
          "received a header frame on channel #{channel} while its state was #{state[:status].inspect}",
          channel, state
        )
      end
    end

    # Raised by ContentAssembler#finish! when one or more channels are still
    # waiting for body bytes when the connection ends.
    class IncompleteContentError < ContentAssemblyError
      def initialize(pending)
        channel, state = pending.first
        message = if state[:status] == :awaiting_header
                    "connection ended on channel #{channel} while waiting for the header frame"
                  else
                    "connection ended on channel #{channel} with #{state[:remaining].inspect} of " \
                    "#{state[:expected].inspect} body bytes missing"
                  end
        super(message, channel, state, state[:method])
      end
    end

    class SoftError < Protocol::Error
      def self.inherited(subclass)
        Error.inherited(subclass)
      end # self.inherited(subclass)
    end

    class HardError < Protocol::Error
      def self.inherited(subclass)
        Error.inherited(subclass)
      end # self.inherited(subclass)
    end
  end
end
