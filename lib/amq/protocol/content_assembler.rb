# encoding: binary
# frozen_string_literal: true

require "amq/protocol/client"

module AMQ
  module Protocol
    class Basic::Publish
      def self.decode(data)
        offset = 0
        ticket = data.byteslice(offset, 2).unpack1(PACK_UINT16)
        offset += 2

        exchange_size = data.getbyte(offset)
        offset += 1
        exchange = data.byteslice(offset, exchange_size)
        offset += exchange_size

        routing_key_size = data.getbyte(offset)
        offset += 1
        routing_key = data.byteslice(offset, routing_key_size)
        offset += routing_key_size

        flags = data.getbyte(offset)
        mandatory = (flags & 1) != 0
        immediate = (flags & (1 << 1)) != 0

        new(ticket, exchange, routing_key, mandatory, immediate)
      end

      attr_reader :ticket, :exchange, :routing_key, :mandatory, :immediate

      def initialize(ticket = 0, exchange = EMPTY_STRING, routing_key = EMPTY_STRING, mandatory = false, immediate = false)
        @ticket = ticket
        @exchange = exchange
        @routing_key = routing_key
        @mandatory = mandatory
        @immediate = immediate
      end
    end

    class Content
      attr_reader :channel, :method, :properties, :body

      def initialize(channel, method, properties, body)
        @channel = channel
        @method = method
        @properties = properties
        @body = body.dup.force_encoding(Encoding::BINARY)
      end

      def ==(other)
        other.is_a?(self.class) &&
          channel == other.channel &&
          method == other.method &&
          properties == other.properties &&
          body == other.body
      end
      alias_method :eql?, :==

      def hash
        [self.class, channel, method, properties, body].hash
      end
    end

    class ContentAssembler
      class Error < Protocol::Error; end

      class UnexpectedFrame < Protocol::UnexpectedFrame
        attr_reader :channel, :status, :assembler_state, :frame, :pending_content
        alias_method :state, :status

        def initialize(message, frame:, assembler_state:)
          super(message)
          @frame = frame
          @channel = frame.channel
          @assembler_state = assembler_state
          @status = assembler_state.is_a?(State) ? assembler_state.status : assembler_state
          @pending_content = assembler_state if assembler_state.is_a?(State) && assembler_state.pending?
        end
      end

      class IncompleteContents < Error
        attr_reader :pending_contents

        def initialize(pending_contents)
          @pending_contents = pending_contents
          super(describe(pending_contents))
        end

        private

        def describe(contents)
          descriptions = contents.map do |content|
            "channel #{content.channel} #{ContentAssembler.state_description(content)}"
          end
          "connection ended with incomplete content: #{descriptions.join('; ')}"
        end
      end

      class State
        attr_reader :channel, :status, :method, :method_class, :properties, :body, :expected_bytes

        alias_method :state, :status

        def self.method_started(channel, method)
          new(channel: channel, status: :waiting_for_header, method: method, method_class: method.class)
        end

        def self.completed(channel, method, method_class, properties, expected_bytes)
          new(
            channel: channel,
            status: :complete,
            method: method,
            method_class: method_class,
            properties: properties,
            body: EMPTY_STRING,
            expected_bytes: expected_bytes
          )
        end

        def initialize(channel:, status:, method: nil, method_class: nil, properties: nil, body: EMPTY_STRING, expected_bytes: nil)
          @channel = channel
          @status = status
          @method = method
          @method_class = method_class || method&.class
          @properties = properties
          @body = body.dup.force_encoding(Encoding::BINARY)
          @expected_bytes = expected_bytes
          freeze
        end

        def with_header(properties, expected_bytes)
          self.class.new(
            channel: channel,
            status: :waiting_for_body,
            method: method,
            method_class: method_class,
            properties: properties,
            body: body,
            expected_bytes: expected_bytes
          )
        end

        def with_body(body)
          self.class.new(
            channel: channel,
            status: :waiting_for_body,
            method: method,
            method_class: method_class,
            properties: properties,
            body: body,
            expected_bytes: expected_bytes
          )
        end

        def waiting_for_header?
          status == :waiting_for_header
        end

        def waiting_for_body?
          status == :waiting_for_body
        end

        def complete?
          status == :complete
        end

        def pending?
          waiting_for_header? || waiting_for_body?
        end

        def received_bytes
          body.bytesize
        end

        def remaining_bytes
          return nil unless expected_bytes

          expected_bytes - received_bytes
        end
      end

      CONTENT_METHODS = [Basic::Publish, Basic::Return, Basic::Deliver].freeze

      def self.state_description(state)
        case state.status
        when :waiting_for_header
          "was waiting for a header frame for #{state.method_class.name}"
        when :waiting_for_body
          "was waiting for #{state.remaining_bytes} of #{state.expected_bytes} body bytes for #{state.method_class.name} (received #{state.received_bytes})"
        when :complete
          "had already completed a #{state.expected_bytes}-byte #{state.method_class.name} content"
        else
          "was idle"
        end
      end

      def initialize
        @channels = {}
      end

      def push(frame)
        case frame
        when HeartbeatFrame
          nil
        when MethodFrame
          handle_method_frame(frame)
        when HeaderFrame
          handle_header_frame(frame)
        when BodyFrame
          handle_body_frame(frame)
        else
          raise ArgumentError, "expected a decoded AMQP frame, got #{frame.class}"
        end
      end
      alias_method :<<, :push
      alias_method :call, :push

      def assemble(frames)
        frames.each_with_object([]) do |frame, contents|
          content = push(frame)
          contents << content if content
        end
      end

      def finish
        incomplete = @channels.values.select(&:pending?)
        return [] if incomplete.empty?

        raise IncompleteContents.new(incomplete)
      end
      alias_method :close, :finish

      def pending
        @channels.each_with_object({}) do |(channel, state), result|
          result[channel] = state if state.pending?
        end
      end
      alias_method :pending_contents, :pending

      def pending?(channel = nil)
        return pending.key?(channel) if channel

        @channels.values.any?(&:pending?)
      end

      private

      def handle_method_frame(frame)
        channel = frame.channel
        state = @channels[channel]
        method_class = frame.method_class

        if state&.pending?
          message = "received #{method_class.name} method frame on channel #{channel} while the assembler #{self.class.state_description(state)}"
          raise UnexpectedFrame.new(message, frame: frame, assembler_state: state)
        end

        return nil unless CONTENT_METHODS.include?(method_class)

        method = frame.decode_payload
        @channels[channel] = State.method_started(channel, method)
        nil
      end

      def handle_header_frame(frame)
        channel = frame.channel
        state = @channels[channel]

        unless state&.waiting_for_header?
          message = "received header frame on channel #{channel} while the assembler #{state ? self.class.state_description(state) : 'was idle'}"
          raise UnexpectedFrame.new(message, frame: frame, assembler_state: state || :idle)
        end

        frame.decode_payload
        unless frame.klass_id == Basic.method_id
          message = "received class #{frame.klass_id} header on channel #{channel} for #{state.method_class.name} content"
          raise UnexpectedFrame.new(message, frame: frame, assembler_state: state)
        end

        properties = frame.properties
        expected_bytes = frame.body_size

        if expected_bytes.zero?
          complete_content(channel, state.method, state.method_class, properties, EMPTY_STRING, expected_bytes)
        else
          @channels[channel] = state.with_header(properties, expected_bytes)
          nil
        end
      end

      def handle_body_frame(frame)
        channel = frame.channel
        state = @channels[channel]

        unless state&.waiting_for_body?
          message = "received body frame on channel #{channel} while the assembler #{state ? self.class.state_description(state) : 'was idle'}"
          raise UnexpectedFrame.new(message, frame: frame, assembler_state: state || :idle)
        end

        payload = frame.decode_payload.dup.force_encoding(Encoding::BINARY)
        if payload.bytesize > state.remaining_bytes
          message = "received #{payload.bytesize} body bytes on channel #{channel} while only #{state.remaining_bytes} bytes remained for #{state.method_class.name}"
          raise UnexpectedFrame.new(message, frame: frame, assembler_state: state)
        end

        body = state.body.dup.force_encoding(Encoding::BINARY) << payload
        return complete_content(channel, state.method, state.method_class, state.properties, body, state.expected_bytes) if body.bytesize == state.expected_bytes

        @channels[channel] = state.with_body(body)
        nil
      end

      def complete_content(channel, method, method_class, properties, body, expected_bytes)
        @channels[channel] = State.completed(channel, method, method_class, properties, expected_bytes)
        Content.new(channel, method, properties, body)
      end
    end
  end
end
