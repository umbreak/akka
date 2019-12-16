/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal.delivery

import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.internal.delivery.ConsumerController.SequencedMessage
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.TimerScheduler
import akka.actor.typed.scaladsl.LoggerOps
import akka.util.Timeout

// FIXME Scaladoc describes how it works, internally. Rewrite for end user and keep internals as impl notes.

/**
 * The producer will start the flow by sending a [[ProducerController.Start]] message to the `ProducerController` with
 * message adapter reference to convert [[ProducerController.RequestNext]] message.
 * The `ProducerController` sends `RequestNext` to the producer, which is then allowed to send one message to
 * the `ProducerController`.
 *
 * The producer and `ProducerController` are supposed to be local so that these messages are fast and not lost.
 *
 * The `ProducerController` sends the first message to the `ConsumerController` without waiting for
 * a `Request` from the `ConsumerController`. The main reason for this is that when used with
 * Cluster Sharding the first message will typically create the `ConsumerController`. It's
 * also a way to connect the ProducerController and ConsumerController in a dynamic way, for
 * example when the ProducerController is replaced.
 *
 * When the first message is received by the `ConsumerController` it sends back the initial `Request`,
 * with demand of how many messages it can accept.
 *
 * Apart from the first message the `ProducerController` will not send more messages than requested
 * by the `ConsumerController`.
 *
 * When there is demand from the consumer side the `ProducerController` sends `RequestNext` to the
 * actual producer, which is then allowed to send one more message.
 *
 * Each message is wrapped by the `ProducerController` in [[ConsumerController.SequencedMessage]] with
 * a monotonically increasing sequence number without gaps, starting at 1.
 *
 * In other words, the "request" protocol to the application producer and consumer is one-by-one, but
 * between the `ProducerController` and `ConsumerController` it's window of messages in flight.
 *
 * The `Request` message also contains a `confirmedSeqNr` that is the acknowledgement
 * from the consumer that it has received and processed all messages up to that sequence number.
 *
 * The `ConsumerController` will send [[ProducerController.Internal.Resend]] if a lost message is detected
 * and then the `ProducerController` will resend all messages from that sequence number. The producer keeps
 * unconfirmed messages in a buffer to be able to resend them. The buffer size is limited
 * by the request window size.
 *
 * The resending is optional, and the `ConsumerController` can be started with `resendLost=false`
 * to ignore lost messages, and then the `ProducerController` will not buffer unconfirmed messages.
 * In that mode it provides only flow control but no reliable delivery.
 */
object ProducerController {

  sealed trait InternalCommand

  sealed trait Command[A] extends InternalCommand

  final case class Start[A](producer: ActorRef[RequestNext[A]]) extends Command[A]

  final case class RequestNext[A](
      producerId: String,
      currentSeqNr: Long,
      confirmedSeqNr: Long,
      sendNextTo: ActorRef[A],
      askNextTo: ActorRef[MessageWithConfirmation[A]])

  final case class RegisterConsumer[A](consumerController: ActorRef[ConsumerController.Command[A]]) extends Command[A]

  /**
   * For sending confirmation message back to the producer when the message has been fully delivered, processed,
   * and confirmed by the consumer. Typically used with `ask` from the producer.
   */
  final case class MessageWithConfirmation[A](message: A, replyTo: ActorRef[Long]) extends InternalCommand

  object Internal {
    final case class Request(confirmedSeqNr: Long, upToSeqNr: Long, supportResend: Boolean, viaTimeout: Boolean)
        extends InternalCommand {
      require(confirmedSeqNr <= upToSeqNr, s"confirmedSeqNr [$confirmedSeqNr] should be <= upToSeqNr [$upToSeqNr]")
    }
    final case class Resend(fromSeqNr: Long) extends InternalCommand
    final case class Ack(confirmedSeqNr: Long) extends InternalCommand
  }

  private case class Msg[A](msg: A) extends InternalCommand
  private case object ResendFirst extends InternalCommand

  private case class LoadStateReply[A](state: DurableProducerQueue.State[A]) extends InternalCommand
  private case class LoadStateFailed(attempt: Int) extends InternalCommand
  private case class StoreMessageSentReply(ack: DurableProducerQueue.StoreMessageSentAck)
  private case class StoreMessageSentFailed[A](messageSent: DurableProducerQueue.MessageSent[A], attempt: Int)
      extends InternalCommand

  private case class StoreMessageSentCompleted[A](messageSent: DurableProducerQueue.MessageSent[A])
      extends InternalCommand

  private final case class State[A](
      requested: Boolean,
      currentSeqNr: Long,
      confirmedSeqNr: Long,
      requestedSeqNr: Long,
      pendingReplies: Vector[(Long, ActorRef[Long])],
      supportResend: Boolean,
      unconfirmed: Vector[ConsumerController.SequencedMessage[A]],
      firstSeqNr: Long,
      producer: ActorRef[ProducerController.RequestNext[A]],
      send: ConsumerController.SequencedMessage[A] => Unit)

  def apply[A: ClassTag](
      producerId: String,
      durableQueueBehavior: Option[Behavior[DurableProducerQueue.Command[A]]]): Behavior[Command[A]] = {
    Behaviors
      .setup[InternalCommand] { context =>
        context.setLoggerName(classOf[ProducerController[_]])
        context.log.info(s"ProducerController created ${context.self}, producerId $producerId") //FIXME
        val durableQueue = askLoadState(context, durableQueueBehavior)
        waitingForStart[A](context, None, None, durableQueue, createInitialState(durableQueue.nonEmpty)) {
          (producer, consumerController, loadedState) =>
            val send: ConsumerController.SequencedMessage[A] => Unit = consumerController ! _
            becomeActive(producerId, durableQueue, createState(context.self, producerId, send, producer, loadedState))
        }
      }
      .narrow
  }

  /**
   * For custom `send` function. For example used with Sharding where the message must be wrapped in
   * `ShardingEnvelope(SequencedMessage(msg))`.
   */
  def apply[A: ClassTag](
      producerId: String,
      durableQueueBehavior: Option[Behavior[DurableProducerQueue.Command[A]]],
      send: ConsumerController.SequencedMessage[A] => Unit): Behavior[Command[A]] = {
    Behaviors
      .setup[InternalCommand] { context =>
        context.setLoggerName(classOf[ProducerController[_]])
        val durableQueue = askLoadState(context, durableQueueBehavior)
        // ConsumerController not used here
        waitingForStart[A](
          context,
          None,
          consumerController = Some(context.system.deadLetters),
          durableQueue,
          createInitialState(durableQueue.nonEmpty)) { (producer, _, loadedState) =>
          becomeActive(producerId, durableQueue, createState(context.self, producerId, send, producer, loadedState))
        }
      }
      .narrow
  }

  private def askLoadState[A: ClassTag](
      context: ActorContext[InternalCommand],
      durableQueueBehavior: Option[Behavior[DurableProducerQueue.Command[A]]])
      : Option[ActorRef[DurableProducerQueue.Command[A]]] = {

    durableQueueBehavior.map { b =>
      val ref = context.spawn(b, "durable")
      context.watch(ref) // FIXME handle terminated, but it's supposed to be restarted so death pact is alright
      askLoadState(context, Some(ref), attempt = 1)
      ref
    }
  }

  private def askLoadState[A: ClassTag](
      context: ActorContext[InternalCommand],
      durableQueue: Option[ActorRef[DurableProducerQueue.Command[A]]],
      attempt: Int): Unit = {
    implicit val loadTimeout: Timeout = 3.seconds // FIXME config
    durableQueue.foreach { ref =>
      context.ask[DurableProducerQueue.LoadState[A], DurableProducerQueue.State[A]](
        ref,
        askReplyTo => DurableProducerQueue.LoadState[A](askReplyTo)) {
        case Success(s) => LoadStateReply(s)
        case Failure(_) => LoadStateFailed(attempt) // timeout
      }
    }
  }

  private def createInitialState[A: ClassTag](hasDurableQueue: Boolean) = {
    if (hasDurableQueue) None else Some(DurableProducerQueue.State.empty[A])
  }

  private def createState[A: ClassTag](
      self: ActorRef[InternalCommand],
      producerId: String,
      send: SequencedMessage[A] => Unit,
      producer: ActorRef[RequestNext[A]],
      loadedState: DurableProducerQueue.State[A]): State[A] = {
    val unconfirmed = loadedState.unconfirmed.zipWithIndex.map {
      case (u, i) => SequencedMessage[A](producerId, u.seqNr, u.msg, i == 0, u.ack)(self)
    }
    State(
      requested = false,
      currentSeqNr = loadedState.currentSeqNr,
      confirmedSeqNr = loadedState.confirmedSeqNr,
      requestedSeqNr = 1L,
      pendingReplies = Vector.empty,
      supportResend = true,
      unconfirmed = unconfirmed,
      firstSeqNr = loadedState.confirmedSeqNr + 1,
      producer,
      send)
  }

  private def waitingForStart[A: ClassTag](
      context: ActorContext[InternalCommand],
      producer: Option[ActorRef[RequestNext[A]]],
      consumerController: Option[ActorRef[ConsumerController.Command[A]]],
      durableQueue: Option[ActorRef[DurableProducerQueue.Command[A]]],
      initialState: Option[DurableProducerQueue.State[A]])(
      thenBecomeActive: (
          ActorRef[RequestNext[A]],
          ActorRef[ConsumerController.Command[A]],
          DurableProducerQueue.State[A]) => Behavior[InternalCommand]): Behavior[InternalCommand] = {
    Behaviors.receiveMessagePartial[InternalCommand] {
      case RegisterConsumer(c: ActorRef[ConsumerController.Command[A]] @unchecked) =>
        context.log.info(s"ProducerController RegisterConsumer ${context.self}, c $c") //FIXME
        (producer, initialState) match {
          case (Some(p), Some(s)) => thenBecomeActive(p, c, s)
          case (_, _)             => waitingForStart(context, producer, Some(c), durableQueue, initialState)(thenBecomeActive)
        }
      case start: Start[A] @unchecked =>
        context.log.info(s"ProducerController Start ${context.self}, start $start") //FIXME
        (consumerController, initialState) match {
          case (Some(c), Some(s)) => thenBecomeActive(start.producer, c, s)
          case (_, _) =>
            waitingForStart(context, Some(start.producer), consumerController, durableQueue, initialState)(
              thenBecomeActive)
        }
      case load: LoadStateReply[A] @unchecked =>
        (producer, consumerController) match {
          case (Some(p), Some(c)) => thenBecomeActive(p, c, load.state)
          case (_, _) =>
            waitingForStart(context, producer, consumerController, durableQueue, Some(load.state))(thenBecomeActive)
        }
      case LoadStateFailed(attempt) =>
        // FIXME attempt counter, and give up
        context.log.info("LoadState attempt [{}] failed, retrying.", attempt)
        // retry
        askLoadState(context, durableQueue, attempt + 1)
        Behaviors.same
    }
  }

  private def becomeActive[A: ClassTag](
      producerId: String,
      durableQueue: Option[ActorRef[DurableProducerQueue.Command[A]]],
      state: State[A]): Behavior[InternalCommand] = {

    Behaviors.setup { ctx =>
      Behaviors.withTimers { timers =>
        val msgAdapter: ActorRef[A] = ctx.messageAdapter(msg => Msg(msg))
        val requested =
          if (state.unconfirmed.isEmpty) {
            state.producer ! RequestNext(producerId, 1L, 0L, msgAdapter, ctx.self)
            true
          } else {
            ctx.self ! ResendFirst
            false
          }
        ctx.log.info(s"ProducerController becomeActive, requested [$requested]")
        new ProducerController[A](ctx, producerId, durableQueue, msgAdapter, timers)
          .active(state.copy(requested = requested))
      }
    }
  }

}

private class ProducerController[A: ClassTag](
    ctx: ActorContext[ProducerController.InternalCommand],
    producerId: String,
    durableQueue: Option[ActorRef[DurableProducerQueue.Command[A]]],
    msgAdapter: ActorRef[A],
    timers: TimerScheduler[ProducerController.InternalCommand]) {
  import ProducerController._
  import ProducerController.Internal._
  import ConsumerController.SequencedMessage
  import DurableProducerQueue.StoreMessageSent
  import DurableProducerQueue.StoreMessageSentAck
  import DurableProducerQueue.StoreMessageConfirmed
  import DurableProducerQueue.MessageSent

  private implicit val askTimeout: Timeout = 3.seconds // FIXME config

  private def active(s: State[A]): Behavior[InternalCommand] = {

    def onMsg(m: A, newPendingReplies: Vector[(Long, ActorRef[Long])], ack: Boolean): Behavior[InternalCommand] = {
      checkOnMsgRequestedState()
      // FIXME adjust all logging, most should probably be debug
      ctx.log.info("sent [{}]", s.currentSeqNr)
      val seqMsg = SequencedMessage(producerId, s.currentSeqNr, m, s.currentSeqNr == s.firstSeqNr, ack)(ctx.self)
      val newUnconfirmed =
        if (s.supportResend) s.unconfirmed :+ seqMsg
        else Vector.empty // no resending, no need to keep unconfirmed

      if (s.currentSeqNr == s.firstSeqNr)
        timers.startTimerWithFixedDelay(ResendFirst, ResendFirst, 1.second)

      s.send(seqMsg)
      val newRequested =
        if (s.currentSeqNr == s.requestedSeqNr)
          false
        else {
          s.producer ! RequestNext(producerId, s.currentSeqNr + 1, s.confirmedSeqNr, msgAdapter, ctx.self)
          true
        }
      active(
        s.copy(
          requested = newRequested,
          currentSeqNr = s.currentSeqNr + 1,
          pendingReplies = newPendingReplies,
          unconfirmed = newUnconfirmed))
    }

    def checkOnMsgRequestedState(): Unit = {
      if (!s.requested || s.currentSeqNr > s.requestedSeqNr) {
        throw new IllegalStateException(
          s"Unexpected Msg when no demand, requested ${s.requested}, " +
          s"requestedSeqNr ${s.requestedSeqNr}, currentSeqNr ${s.currentSeqNr}")
      }
    }

    def storeMessageSent(messageSent: MessageSent[A], attempt: Int): Unit = {
      ctx.ask[StoreMessageSent[A], StoreMessageSentAck](
        durableQueue.get,
        askReplyTo => StoreMessageSent(messageSent, askReplyTo)) {
        case Success(_) => StoreMessageSentCompleted(messageSent)
        case Failure(_) => StoreMessageSentFailed(messageSent, attempt) // timeout
      }
    }

    def onAck(newConfirmedSeqNr: Long): State[A] = {
      val (replies, newPendingReplies) = s.pendingReplies.partition { case (seqNr, _) => seqNr <= newConfirmedSeqNr }
      if (replies.nonEmpty)
        ctx.log.info("Confirmation replies from [{}] to [{}]", replies.head._1, replies.last._1)
      replies.foreach {
        case (seqNr, replyTo) => replyTo ! seqNr
      }

      val newUnconfirmed =
        if (s.supportResend) s.unconfirmed.dropWhile(_.seqNr <= newConfirmedSeqNr)
        else Vector.empty

      if (newConfirmedSeqNr == s.firstSeqNr)
        timers.cancel(ResendFirst)

      val newMaxConfirmedSeqNr = math.max(s.confirmedSeqNr, newConfirmedSeqNr)

      durableQueue.foreach { d =>
        // Storing the confirmedSeqNr can be "write behind", at-least-once delivery
        // FIXME to reduce number of writes we could consider to only StoreMessageConfirmed for the Request messages and not for each Ack
        if (newMaxConfirmedSeqNr != s.confirmedSeqNr)
          d ! StoreMessageConfirmed(newMaxConfirmedSeqNr)
      }

      s.copy(confirmedSeqNr = newMaxConfirmedSeqNr, pendingReplies = newPendingReplies, unconfirmed = newUnconfirmed)
    }

    def resendUnconfirmed(newUnconfirmed: Vector[SequencedMessage[A]]): Unit = {
      if (newUnconfirmed.nonEmpty)
        ctx.log.info("resending [{} - {}]", newUnconfirmed.head.seqNr, newUnconfirmed.last.seqNr)
      newUnconfirmed.foreach(s.send)
    }

    Behaviors.receiveMessage {
      case MessageWithConfirmation(m: A, replyTo) =>
        val newPendingReplies = s.pendingReplies :+ s.currentSeqNr -> replyTo
        if (durableQueue.isEmpty) {
          onMsg(m, newPendingReplies, ack = true)
        } else {
          storeMessageSent(MessageSent(s.currentSeqNr, m, ack = true), attempt = 1)
          active(s.copy(pendingReplies = newPendingReplies))
        }

      case Msg(m: A) =>
        if (durableQueue.isEmpty) {
          onMsg(m, s.pendingReplies, ack = false)
        } else {
          storeMessageSent(MessageSent(s.currentSeqNr, m, ack = false), attempt = 1)
          Behaviors.same
        }

      case StoreMessageSentCompleted(MessageSent(seqNr, m: A, ack)) =>
        if (seqNr != s.currentSeqNr)
          throw new IllegalStateException(s"currentSeqNr [${s.currentSeqNr}] not matching stored seqNr [$seqNr]")

        val newPendingReplies =
          if (s.pendingReplies.isEmpty) {
            s.pendingReplies
          } else {
            val (headSeqNr, replyTo) = s.pendingReplies.head
            if (headSeqNr != seqNr)
              throw new IllegalStateException(s"Unexpected pending reply [$headSeqNr] after storage of [$seqNr].")
            ctx.log.info("Confirmation reply to [{}]", seqNr)
            replyTo ! seqNr
            s.pendingReplies.tail
          }

        onMsg(m, newPendingReplies, ack)

      case f: StoreMessageSentFailed[A] @unchecked =>
        // FIXME attempt counter, and give up
        ctx.log.info(s"StoreMessageSent seqNr [{}] failed, attempt [{}], retrying.", f.messageSent.seqNr, f.attempt)
        // retry
        storeMessageSent(f.messageSent, attempt = f.attempt + 1)
        Behaviors.same

      case Request(newConfirmedSeqNr, newRequestedSeqNr, supportResend, viaTimeout) =>
        ctx.log.infoN(
          "Request, confirmed [{}], requested [{}], current [{}]",
          newConfirmedSeqNr,
          newRequestedSeqNr,
          s.currentSeqNr)

        val stateAfterAck = onAck(newConfirmedSeqNr)

        val newUnconfirmed =
          if (supportResend) stateAfterAck.unconfirmed
          else Vector.empty

        if ((viaTimeout || newConfirmedSeqNr == s.firstSeqNr) && supportResend) {
          // the last message was lost and no more message was sent that would trigger Resend
          resendUnconfirmed(newUnconfirmed)
        }

        if (newRequestedSeqNr > s.requestedSeqNr) {
          if (!s.requested && (newRequestedSeqNr - s.currentSeqNr) > 0)
            s.producer ! RequestNext(producerId, s.currentSeqNr, newConfirmedSeqNr, msgAdapter, ctx.self)
          active(
            stateAfterAck.copy(
              requested = true,
              requestedSeqNr = newRequestedSeqNr,
              supportResend = supportResend,
              unconfirmed = newUnconfirmed))
        } else {
          active(stateAfterAck.copy(supportResend = supportResend, unconfirmed = newUnconfirmed))
        }

      case Ack(newConfirmedSeqNr) =>
        ctx.log.infoN("Ack, confirmed [{}], current [{}]", newConfirmedSeqNr, s.currentSeqNr)
        val stateAfterAck = onAck(newConfirmedSeqNr)
        if (newConfirmedSeqNr == s.firstSeqNr && stateAfterAck.unconfirmed.nonEmpty) {
          resendUnconfirmed(stateAfterAck.unconfirmed)
        }
        active(stateAfterAck)

      case Resend(fromSeqNr) =>
        val newUnconfirmed = s.unconfirmed.dropWhile(_.seqNr < fromSeqNr)
        resendUnconfirmed(newUnconfirmed)
        active(s.copy(unconfirmed = newUnconfirmed))

      case ResendFirst =>
        if (s.unconfirmed.nonEmpty && s.unconfirmed.head.seqNr == s.firstSeqNr) {
          ctx.log.info("resending first, [{}]", s.firstSeqNr)
          s.send(s.unconfirmed.head.copy(first = true)(ctx.self))
        } else {
          if (s.currentSeqNr > s.firstSeqNr)
            timers.cancel(ResendFirst)
        }
        Behaviors.same

      case start: Start[A] @unchecked =>
        ctx.log.info("Register new Producer [{}], currentSeqNr [{}].", start.producer, s.currentSeqNr)
        if (s.requested)
          start.producer ! RequestNext(producerId, s.currentSeqNr, s.confirmedSeqNr, msgAdapter, ctx.self)
        active(s.copy(producer = start.producer))

      case RegisterConsumer(consumerController: ActorRef[ConsumerController.Command[A]] @unchecked) =>
        val newFirstSeqNr =
          if (s.unconfirmed.isEmpty) s.currentSeqNr
          else s.unconfirmed.head.seqNr
        ctx.log.info(
          "Register new ConsumerController [{}], starting with seqNr [{}].",
          consumerController,
          newFirstSeqNr)
        if (s.unconfirmed.nonEmpty) {
          timers.startTimerWithFixedDelay(ResendFirst, ResendFirst, 1.second)
          ctx.self ! ResendFirst
        }
        // update the send function
        val newSend = consumerController ! _
        active(s.copy(firstSeqNr = newFirstSeqNr, send = newSend))
    }
  }
}
