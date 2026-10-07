# encoding: binary
# frozen_string_literal: true

require "amq/protocol/frame"
require "amq/protocol/exceptions"

module AMQ
  module Protocol
    # A fully assembled content: a content-bearing method (Basic::Publish,
    # Basic::Deliver, Basic::Return, Basic::GetOk, ...), the decoded
    # properties from the header frame and the concatenated body.
    class Content
      attr_reader :channel, :method, :properties, :body

      def initialize(channel, method, properties, body)
        @channel    = channel
        @method     = method
        @properties = properties
        @body       = body
      end

      # The AMQP class the content belongs to, e.g. Basic.
      def protocol_class
        @method.class.ancestors[1]
      end

      def body_size
        @body.bytesize
      end

      def complete?
        true
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

    # Assembles already decoded frames into complete Content objects without
    # depending on a socket or any IO adapter.
    #
    # Frames for different channels may be interleaved freely: assembly state
    # is tracked per channel. Heartbeat frames and method frames of methods
    # that do not carry content simply pass through and never advance a
    # pending message.
    #
    #   assembler = AMQ::Protocol::ContentAssembler.new
    #   content   = assembler << frame
    #   content.nil? # => true while a message is still waiting for its body
    #   assembler.pending # => {1 => {...}} channels still waiting for body bytes
    #   assembler.finish! # raises if any message was left incomplete
    class ContentAssembler
      def initialize
        # channel => {status, method, properties, body, received, expected, remaining}
        @states = {}
        # channel => finalized state of the most recently completed zero-body
        # message. Lets a following body frame be reported precisely instead
        # of being mistaken for a frame without any context.
        @zero_body_states = {}
        # Content objects produced by the most recent frames, in channel order.
        @completed = []
      end

      # Feeds one decoded frame. Returns the assembled Content when the frame
      # completed a content frameset, nil otherwise. Non-content method and
      # heartbeat frames always return nil without altering assembly state.
      def <<(frame)
        handle(frame)
      end

      # Feeds a frame (alias of #<<). Kept for readers that prefer a verb.
      def push(frame)
        handle(frame)
      end

      # Feeds any number of frames in order and returns every Content object
      # completed by them, in completion order.
      def assemble(*frames)
        completed = []
        frames.flatten.each do |frame|
          content = handle(frame)
          completed << content if content
        end
        completed
      end

      # Returns the assembly state for a single channel, or nil if the channel
      # is not in the middle of assembling a message. The hash contains:
      #
      # * :status     - :awaiting_header or :awaiting_body
      # * :method     - the decoded method object
      # * :properties - decoded properties (nil before the header frame)
      # * :received   - body bytes accumulated so far
      # * :expected   - total body size declared by the header
      # * :remaining  - body bytes still expected
      def [](channel)
        state = @states[channel]
        state && dup_state(state)
      end

      # Returns a copy of the per-channel states still waiting for frames,
      # keyed by channel number.
      def pending
        @states.each_with_object({}) do |(channel, state), hash|
          hash[channel] = dup_state(state)
        end
      end

      def pending?(channel = nil)
        return !@states.empty? if channel.nil?

        @states.key?(channel)
      end

      # Channels currently holding an incomplete content frameset.
      def channels
        @states.keys
      end

      # Returns true when every channel has received a complete frameset.
      def complete?
        @states.empty?
      end

      # Drains completed Content objects accumulated so far, in completion
      # order. Useful when frames are fed without capturing return values.
      def drain
        @completed.shift(@completed.length)
      end

      # Raises IncompleteContentError if any channel is still short of body
      # bytes when the connection ends. Returns nil otherwise.
      def finish!
        unless @states.empty?
          raise IncompleteContentError.new(pending)
        end

        nil
      end

      # Non-raising counterpart of #finish!: returns the pending state hash
      # when a message is incomplete, nil when everything was assembled.
      def finish
        @states.empty? ? nil : pending
      end

      # Drops all assembly state, including completed but undrained content.
      def reset
        @states.clear
        @zero_body_states.clear
        @completed.clear
        self
      end

      private

      def handle(frame)
        case frame
        when MethodFrame
          handle_method_frame(frame)
        when HeaderFrame
          handle_header_frame(frame)
        when BodyFrame
          handle_body_frame(frame)
        when HeartbeatFrame
          nil
        else
          raise ContentAssemblyError.new(
            "cannot assemble #{frame.class} frames",
            frame.respond_to?(:channel) ? frame.channel : nil,
            {:status => :idle}
          )
        end
      end

      def handle_method_frame(frame)
        method = frame.decode_payload

        unless method.class.has_content?
          return nil
        end

        channel = frame.channel
        if (state = @states[channel])
          raise UnexpectedMethodFrame.new(channel, finalized_state(state), method)
        end

        @zero_body_states.delete(channel)
        @states[channel] = {
          :status     => :awaiting_header,
          :method     => method,
          :properties => nil,
          :body       => +String.new,
          :received   => 0,
          :expected   => nil,
          :remaining  => nil
        }
        nil
      end

      def handle_header_frame(frame)
        channel = frame.channel
        state   = @states[channel]

        unless state
          raise UnexpectedHeaderFrame.new(channel, {:status => :idle})
        end
        unless state[:status] == :awaiting_header
          raise UnexpectedHeaderFrame.new(channel, finalized_state(state))
        end

        frame.decode_payload
        expected = frame.body_size

        state[:expected]   = expected
        state[:remaining]  = expected
        state[:properties] = frame.properties

        if expected.zero?
          @zero_body_states[channel] = {
            :status     => :completed,
            :method     => state[:method],
            :properties => state[:properties],
            :received   => 0,
            :expected   => 0,
            :remaining  => 0
          }
          complete(channel, state)
        else
          state[:status] = :awaiting_body
          nil
        end
      end

      def handle_body_frame(frame)
        channel = frame.channel
        state   = @states[channel]

        unless state
          if (zero_body_state = @zero_body_states[channel])
            raise UnexpectedBodyFrame.new(channel, zero_body_state)
          end
          raise ContentAssemblyError.new(
            "received a body frame on channel #{channel} without a preceding method and header frame",
            channel,
            {:status => :idle}
          )
        end
        if state[:status] == :awaiting_header
          raise ContentAssemblyError.new(
            "received a body frame on channel #{channel} before its header frame",
            channel,
            finalized_state(state)
          )
        end

        payload = frame.payload
        chunk   = payload.bytesize

        if chunk > state[:remaining]
          raise ContentAssemblyError.new(
            "received #{chunk} body bytes on channel #{channel} but only " \
            "#{state[:remaining]} of #{state[:expected]} were expected",
            channel,
            finalized_state(state),
            state[:method]
          )
        end

        state[:body] << payload
        state[:received]  += chunk
        state[:remaining] -= chunk

        if state[:remaining].zero?
          complete(channel, state)
        else
          nil
        end
      end

      def complete(channel, state)
        content = Content.new(
          channel,
          state[:method],
          state[:properties],
          state[:body].dup.force_encoding(Encoding::BINARY)
        )
        @states.delete(channel)
        @completed << content
        content
      end

      def finalized_state(state)
        {
          :status     => state[:status],
          :method     => state[:method],
          :properties => state[:properties],
          :received   => state[:received],
          :expected   => state[:expected],
          :remaining  => state[:remaining]
        }
      end

      def dup_state(state)
        copy = finalized_state(state)
        copy[:body] = state[:body].dup if state[:body]
        copy
      end
    end
  end
end
