/********************************************************************************
 * Copyright (c) 2026 Cirrus Link Solutions and others
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *   Cirrus Link Solutions - initial implementation
 ********************************************************************************/

package org.eclipse.tahu.mqtt.test;

import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttWireMessage;
import org.eclipse.tahu.exception.TahuException;
import org.eclipse.tahu.mqtt.ClientCallback;
import org.eclipse.tahu.mqtt.MqttClientId;
import org.eclipse.tahu.mqtt.MqttServerName;
import org.eclipse.tahu.mqtt.MqttServerUrl;
import org.eclipse.tahu.mqtt.RandomStartupDelay;
import org.eclipse.tahu.mqtt.TahuClient;
import org.eclipse.tahu.mqtt.TahuMqttAsyncClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for the FIFO publish buffer added to {@link TahuClient}.
 *
 * Regression coverage for the UNS transmitter thread explosion and OOM.
 *
 * These exercise the class against a fake Paho client rather than a broker, so the in-flight window can be held at zero
 * deterministically - which is the state that used to deadlock the client permanently.
 */
public class TahuClientPublishBufferTest {

	private static final long TIMEOUT_MS = 5000;

	/* The pre-LWT drain budget, plus the Paho workaround sleep the disconnect pays regardless. */
	private static final long LWT_DRAIN_BUDGET_MS = 2000;

	/*
	 * Comfortably under the keepAlive-second LWT confirmation wait (30s here) and comfortably over the second the
	 * close legitimately costs, so it fails on the wait and not on the grace period.
	 */
	private static final long LWT_CONFIRMATION_BUDGET_MS = 5000;

	/* Long enough for several full teardowns, each of which pays the unconditional 1000ms Paho workaround sleep. */
	private static final long STRESS_DURATION_MS = 6000;

	/* Long enough that a lock held for its duration is unmistakable, and that the probe lands well inside it. */
	private static final long STARTUP_DELAY_MS = 3000;
	private static final long LOCK_PROBE_BUDGET_MS = 1000;

	/* The BIRTH backpressure budget (500ms) plus the Paho workaround sleep (1000ms), with room to settle. */
	private static final long BIRTH_WAIT_PLUS_SLACK = 2200;
	private static final long DRAIN_POLL_MS = 250;

	private TahuClient tahuClient;
	private FakeMqttClient fakeClient;
	private RecordingCallback callback;

	@BeforeMethod
	public void setUp() throws Exception {
		callback = new RecordingCallback();
		tahuClient = newTahuClient(callback);
		fakeClient = new FakeMqttClient();
	}

	@AfterMethod
	public void tearDown() throws Exception {
		if (tahuClient != null) {
			/*
			 * Bounded, because the shutdown needs publishOrderLock and a deadlock regression is exactly the state
			 * where nobody can have it. Left unbounded, a client wedged that way would hang the JVM here and the
			 * run would die by CI timeout with no failing test named. This turns that into a teardown failure
			 * that says which test wedged the client.
			 */
			Thread shutdown = new Thread(() -> {
				try {
					invoke(tahuClient, "shutdownPublishBufferDrainThread");
				} catch (Exception e) {
					// Reported by the join below if it matters
				}
			}, "test-teardown");
			shutdown.setDaemon(true);
			shutdown.start();
			shutdown.join(2000);
			Assert.assertFalse(shutdown.isAlive(),
					"The client could not be shut down - something is holding publishOrderLock, which is what a "
							+ "deadlock regression looks like from here");
		}
		if (fakeClient != null) {
			fakeClient.shutdownAckThread();
		}
	}

	// ------------------------------------------------------------------------------------------------------------
	// The deadlock itself
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * The regression test for the publish deadlock.
	 *
	 * With the window exhausted, publish() must return promptly instead of parking while holding messageLock. If this
	 * hangs, the deadlock is back.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void publishDoesNotBlockWhenNoPermitsAvailable() throws Exception {
		wire(0, 8);

		long start = System.currentTimeMillis();
		IMqttDeliveryToken token = tahuClient.publish("topic/a", "a".getBytes(), 1, false);
		long elapsed = System.currentTimeMillis() - start;

		Assert.assertNull(token, "A buffered publish must return null, not a token");
		Assert.assertTrue(elapsed < 1000, "publish() blocked for " + elapsed + "ms - it must never wait for a permit");
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1);
		Assert.assertEquals(fakeClient.publishedTopics().size(), 0, "Nothing should have reached the MQTT client");
	}

	/**
	 * deliveryComplete() must be able to return a permit while a publisher is in flight. Under the old code the
	 * publisher held messageLock while parked in acquire(), so this callback could never run.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void deliveryCompleteCanReturnPermitsWhilePublishing() throws Exception {
		wire(1, 8);

		IMqttDeliveryToken token = tahuClient.publish("topic/a", "a".getBytes(), 1, false);
		Assert.assertNotNull(token, "First publish should go inline while a permit is free");
		Assert.assertEquals(availablePermits(), 0, "Precondition: the in-flight window is now exhausted");

		/*
		 * A second publisher meeting the exhausted window. This is the state that deadlocked: the old code took
		 * messageLock and then blocked in semaphore.acquire() inside it, so the publisher held the monitor
		 * deliveryComplete() needs to hand a permit back, and neither could proceed. It must now return instead -
		 * buffered, with a null token - without holding anything deliveryComplete() needs.
		 */
		final CountDownLatch publisherEntered = new CountDownLatch(1);
		final CountDownLatch publisherReturned = new CountDownLatch(1);
		final AtomicReference<Object> publisherResult = new AtomicReference<>();
		Thread publisherThread = new Thread(() -> {
			publisherEntered.countDown();
			try {
				publisherResult.set(tahuClient.publish("topic/blocked", "b".getBytes(), 1, false));
			} catch (Throwable t) {
				publisherResult.set(t);
			}
			publisherReturned.countDown();
		}, "fake-publisher");
		publisherThread.setDaemon(true);
		publisherThread.start();
		Assert.assertTrue(publisherEntered.await(2, TimeUnit.SECONDS), "Publisher thread never started");

		// The Paho callback thread returning the permit - this is the call that used to block forever
		final CountDownLatch done = new CountDownLatch(1);
		Thread callbackThread = new Thread(() -> {
			tahuClient.deliveryComplete(token);
			done.countDown();
		}, "fake-MQTT-Call");
		callbackThread.setDaemon(true);
		callbackThread.start();

		Assert.assertTrue(done.await(2, TimeUnit.SECONDS),
				"deliveryComplete() blocked while a publisher met the exhausted window - the deadlock has returned");
		Assert.assertTrue(publisherReturned.await(2, TimeUnit.SECONDS),
				"The publisher parked inside the publish path instead of buffering - it is holding the monitor "
						+ "deliveryComplete() needs");
		Assert.assertNull(publisherResult.get(),
				"A publisher with no permit must buffer and return null, not block and not throw: "
						+ publisherResult.get());

		// The permit does not come to rest as a free permit: the drain takes it straight back for the message that
		// was waiting on it. Asserting that end state proves the whole chain, rather than just that nothing hung.
		awaitBufferDepth(0);
		Assert.assertEquals(fakeClient.publishedTopics(), List.of("topic/a", "topic/blocked"),
				"The permit deliveryComplete() returned must have carried the buffered message out");
	}

	// ------------------------------------------------------------------------------------------------------------
	// Ordering
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * QoS 0 is never buffered. It takes no permit and is not subject to backpressure, so it goes to Paho immediately
	 * even while QoS > 0 messages are queued - which means it can overtake them. That is the intended trade-off:
	 * MQTT only orders within a QoS level, and holding live fire-and-forget data behind a stalled acknowledged queue
	 * would make it stale for no delivery benefit.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void qos0IsSentImmediatelyEvenWhenBufferIsNonEmpty() throws Exception {
		wire(0, 8);

		tahuClient.publish("topic/qos1", "1".getBytes(), 1, false);
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1, "QoS 1 with no permit must buffer");

		IMqttDeliveryToken token = tahuClient.publish("topic/qos0", "0".getBytes(), 0, false);

		Assert.assertNotNull(token, "An immediately published QoS 0 message must return a token, not null");
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1, "QoS 0 must not be added to the buffer");
		Assert.assertEquals(fakeClient.publishedTopics(), List.of("topic/qos0"),
				"QoS 0 must reach Paho immediately, ahead of the queued QoS 1");
	}

	/** QoS 0 must stay immediate even when the buffer is at capacity - it is not subject to the capacity limit. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void qos0IsSentImmediatelyWhenBufferIsFull() throws Exception {
		wire(0, 8);
		tahuClient.setPublishBufferCapacity(2);

		tahuClient.publish("topic/a", "x".getBytes(), 1, false);
		tahuClient.publish("topic/b", "x".getBytes(), 1, false);
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 2, "Buffer should be at capacity");

		tahuClient.publish("topic/qos0", "0".getBytes(), 0, false);

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 2);
		Assert.assertEquals(fakeClient.publishedTopics(), List.of("topic/qos0"),
				"A full buffer must not block QoS 0");
	}

	/** With an empty buffer there is nothing to overtake, so QoS 0 goes straight out. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void qos0PublishesInlineWhenBufferIsEmpty() throws Exception {
		wire(0, 8);

		tahuClient.publish("topic/qos0", "0".getBytes(), 0, false);

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0);
		Assert.assertEquals(fakeClient.publishedTopics(), List.of("topic/qos0"));
	}

	/**
	 * Buffered QoS > 0 messages come out of the drain in publish order while every send succeeds.
	 *
	 * This asserts the happy path only, and deliberately so: FIFO here is best effort, not a guarantee. It is not held
	 * across a failed send - publishOrderLock is released between the failure and the re-queue, so a concurrent
	 * publisher can go inline ahead of the message going back to the head. That gap is accepted rather than fixed
	 * (MQTT does not order across an in-flight window greater than one either, and Sparkplug sequence-bearing traffic
	 * publishes at QoS 0 and never reaches this buffer), so do not tighten this test into a guarantee the class does
	 * not make.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void drainPreservesPublishOrderOfBufferedMessages() throws Exception {
		wire(0, 8);

		List<String> expected = new ArrayList<>();
		for (int i = 0; i < 8; i++) {
			String topic = "topic/" + i;
			tahuClient.publish(topic, "x".getBytes(), 1, false);
			expected.add(topic);
		}
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 8);

		startDrain();
		releasePermits(8);

		awaitBufferDepth(0);
		Assert.assertEquals(fakeClient.publishedTopics(), expected, "Drain must preserve FIFO order");
	}

	// ------------------------------------------------------------------------------------------------------------
	// Buffer contents and head-of-line behaviour
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * The buffer must only ever hold QoS > 0. This is the invariant the drain depends on: it takes a permit before
	 * dequeuing, so a buffered QoS 0 message - which needs no permit and is never ACKed - would stall the buffer for
	 * as long as the in-flight window stayed exhausted.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void bufferNeverHoldsQos0Messages() throws Exception {
		wire(0, 8);

		for (int i = 0; i < 10; i++) {
			tahuClient.publish("topic/qos1-" + i, "x".getBytes(), 1, false);
			tahuClient.publish("topic/qos0-" + i, "x".getBytes(), 0, false);
		}

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 10, "Only the QoS 1 messages should be buffered");
		Assert.assertEquals(fakeClient.publishedTopics().size(), 10, "Every QoS 0 message should have gone out");
		for (String topic : fakeClient.publishedTopics()) {
			Assert.assertTrue(topic.startsWith("topic/qos0-"), "Only QoS 0 should have been published: " + topic);
		}

		// Not asserted here that QoS 0 took no permit: the window starts empty in this test, so a permit count of
		// zero would hold either way. qos0DoesNotConsumeAPermit checks it where it can fail.
	}

	/** QoS 0 takes no in-flight permit, checked with permits available so the assertion can actually fail. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void qos0DoesNotConsumeAPermit() throws Exception {
		wire(8, 8);

		for (int i = 0; i < 5; i++) {
			tahuClient.publish("topic/qos0-" + i, "x".getBytes(), 0, false);
		}

		Assert.assertEquals(fakeClient.publishedTopics().size(), 5, "Precondition: every QoS 0 message went out");
		Assert.assertEquals(availablePermits(), 8, "QoS 0 is not acknowledged and must not hold a permit");
	}

	/** A QoS 1 head with no permits must stay put rather than being dropped or reordered. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void qos1HeadWaitsForAPermit() throws Exception {
		wire(0, 8);

		tahuClient.publish("topic/first", "x".getBytes(), 1, false);
		tahuClient.publish("topic/second", "y".getBytes(), 1, false);

		startDrain();
		Thread.sleep(DRAIN_POLL_MS * 3);

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 2, "Nothing should drain with no permits available");
		Assert.assertEquals(fakeClient.publishedTopics().size(), 0);

		releasePermits(2);
		awaitBufferDepth(0);
		Assert.assertEquals(fakeClient.publishedTopics(), List.of("topic/first", "topic/second"));
	}

	// ------------------------------------------------------------------------------------------------------------
	// Capacity
	// ------------------------------------------------------------------------------------------------------------

	/** At capacity the newest publish is rejected so the caller can fall back to store-and-forward. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void bufferRejectsWhenFull() throws Exception {
		wire(0, 8);
		tahuClient.setPublishBufferCapacity(3);

		for (int i = 0; i < 3; i++) {
			tahuClient.publish("topic/" + i, "x".getBytes(), 1, false);
		}
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 3);

		try {
			tahuClient.publish("topic/overflow", "x".getBytes(), 1, false);
			Assert.fail("Publishing past capacity must throw so the caller can store the message");
		} catch (TahuException e) {
			Assert.assertTrue(e.getMessage().contains("buffer is full"), "Unexpected message: " + e.getMessage());
		}

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 3, "A rejected publish must not be buffered");
		Assert.assertEquals(tahuClient.getPublishBufferRejectedMessageCount(), 1,
				"A capacity rejection must be counted so the loss is visible to an operator");
		Assert.assertEquals(tahuClient.getPublishBufferDiscardedMessageCount(), 0,
				"A refusal the caller was told about is not a discard - publish() threw, so nothing was lost here");
	}

	// ------------------------------------------------------------------------------------------------------------
	// Failed sends
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * An inline send failure has no buffer to fall back on, so it must surface to the caller rather than being
	 * swallowed - the caller is the only layer that can decide to store or resend it.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void inlineSendFailureReachesCallerAndReturnsPermit() throws Exception {
		wire(4, 8);

		fakeClient.failNextPublishes(1);
		try {
			tahuClient.publish("topic/first", "x".getBytes(), 1, false);
			Assert.fail("An inline send failure must reach the caller");
		} catch (TahuException expected) {
			// expected
		}

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0, "An inline failure must not silently buffer");
		Assert.assertEquals(availablePermits(), 4, "A failed inline send must return its permit");
	}

	/**
	 * A buffered message that keeps failing is retried across a window of wall clock, then dropped and counted.
	 *
	 * The budget is time, not attempts, and that distinction is the whole point. Nothing on the failure path delays -
	 * the buffer is non-empty so the drain's wait() is skipped, the client still reports connected so its poll sleep
	 * is skipped, and tryAcquire() returns at once because the failed send released its permit - so a pure attempt
	 * count was consumed in about two milliseconds and gave a transient failure no chance to clear.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 3)
	public void bufferedSendFailureIsRetriedAcrossTheWindowThenDroppedAndCounted() throws Exception {
		wire(0, 8);

		tahuClient.publish("topic/poison", "x".getBytes(), 1, false);
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1);

		fakeClient.failNextPublishes(Integer.MAX_VALUE);
		startDrain();
		releasePermits(8);

		long start = System.currentTimeMillis();
		awaitBufferDepth(0, TIMEOUT_MS * 2);
		long elapsed = System.currentTimeMillis() - start;

		Assert.assertTrue(elapsed >= 500,
				"The retry budget must span wall clock, not iterations - dropped after only " + elapsed + "ms");
		Assert.assertTrue(fakeClient.publishAttempts() >= 3,
				"The window should still afford about three attempts, made " + fakeClient.publishAttempts());
		Assert.assertEquals(availablePermits(), 8, "Every permit taken for a failed send must be returned");
		Assert.assertEquals(tahuClient.getPublishBufferDiscardedMessageCount(), 1,
				"A dropped message is lost data and must be counted - publish() told the caller it was queued");
		Assert.assertEquals(tahuClient.getPublishBufferRejectedMessageCount(), 0,
				"A message this client accepted and then lost is a discard, not a capacity rejection");
	}

	/**
	 * A message that fails once then succeeds must still be delivered, ahead of what is still queued behind it.
	 *
	 * Single-threaded on purpose. It proves the re-queue goes to the head rather than the tail; it does NOT prove the
	 * message keeps its place against a concurrent publisher, which it would not - see
	 * {@link #drainPreservesPublishOrderOfBufferedMessages()} for why that gap is accepted.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void requeuedMessageKeepsItsPlaceInLine() throws Exception {
		wire(0, 8);

		tahuClient.publish("topic/first", "x".getBytes(), 1, false);
		tahuClient.publish("topic/second", "x".getBytes(), 1, false);

		fakeClient.failNextPublishes(1);
		startDrain();
		releasePermits(8);

		awaitBufferDepth(0);
		Assert.assertEquals(fakeClient.publishedTopics(), List.of("topic/first", "topic/second"),
				"A re-queued message must go back to the head, not the tail");
	}

	// ------------------------------------------------------------------------------------------------------------
	// Permit accounting
	// ------------------------------------------------------------------------------------------------------------

	/** The pre-existing leak: a publish() that threw left its permit unreturned because token was null. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void failedPublishDoesNotLeakAPermit() throws Exception {
		wire(8, 8);

		for (int i = 0; i < 5; i++) {
			fakeClient.failNextPublishes(1);
			try {
				tahuClient.publish("topic/" + i, "x".getBytes(), 1, false);
			} catch (TahuException expected) {
				// expected
			}
		}

		Assert.assertEquals(availablePermits(), 8, "Permits must be returned when the send never happened");
	}

	/** A successful publish hands its permit to deliveryComplete(), which returns it on the ACK. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void permitsBalanceAcrossAFullPublishAckCycle() throws Exception {
		wire(8, 8);

		List<IMqttDeliveryToken> tokens = new ArrayList<>();
		for (int i = 0; i < 8; i++) {
			tokens.add(tahuClient.publish("topic/" + i, "x".getBytes(), 1, false));
		}
		Assert.assertEquals(availablePermits(), 0);

		for (IMqttDeliveryToken token : tokens) {
			tahuClient.deliveryComplete(token);
		}

		Assert.assertEquals(availablePermits(), 8, "Permits must return to full after every message is ACKed");
	}

	// ------------------------------------------------------------------------------------------------------------
	// End-to-end stall and recovery
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * The full backpressure cycle with permits returned the way the real system returns them - through
	 * deliveryComplete() on a separate callback thread - rather than by releasing the semaphore directly.
	 *
	 * This is the closest unit-level analogue of the incident: publish far past the in-flight window, let the buffer
	 * absorb the excess, and let ACKs alone drive it back down. It also exercises the lock ordering under contention,
	 * because the ACK thread must take messageLock while a publisher may be holding publishOrderLock -> messageLock.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 6)
	public void bufferDrainsAsAcksReturnPermits() throws Exception {
		final int window = 4;
		final int total = 40;
		wire(window, window);

		// Stand in for the MQTT server: ACK every delivered message on its own thread, as Paho's callback
		// thread does. This is the ONLY thing returning permits in this test.
		fakeClient.ackOnSeparateThread(tahuClient);
		startDrain();

		List<String> expected = new ArrayList<>();
		for (int i = 0; i < total; i++) {
			String topic = "topic/" + i;
			tahuClient.publish(topic, "x".getBytes(), 1, false);
			expected.add(topic);
		}

		// No releasePermits() anywhere - the buffer can only drain if ACKs are being processed
		awaitBufferDepth(0, TIMEOUT_MS * 4);

		Assert.assertEquals(fakeClient.publishedTopics(), expected, "Every message must be delivered, in publish order");
		awaitPermits(window, TIMEOUT_MS);
	}

	// ------------------------------------------------------------------------------------------------------------
	// Concurrency
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * Concurrent publishers must not lose or duplicate messages, and the buffer must account for every one of them.
	 * The publish-or-queue decision is made under a single lock precisely so this holds.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 4)
	public void concurrentPublishersDoNotLoseMessages() throws Exception {
		final int threads = 8;
		final int perThread = 50;
		wire(0, 4096);
		tahuClient.setPublishBufferCapacity(threads * perThread);

		final CountDownLatch startGate = new CountDownLatch(1);
		final CountDownLatch finished = new CountDownLatch(threads);
		final AtomicInteger failures = new AtomicInteger();

		for (int t = 0; t < threads; t++) {
			final int threadIndex = t;
			Thread thread = new Thread(() -> {
				try {
					startGate.await();
					for (int i = 0; i < perThread; i++) {
						tahuClient.publish("topic/" + threadIndex + "/" + i, "x".getBytes(), 1, false);
					}
				} catch (Throwable e) {
					failures.incrementAndGet();
				} finally {
					finished.countDown();
				}
			}, "publisher-" + t);
			thread.setDaemon(true);
			thread.start();
		}

		startGate.countDown();
		Assert.assertTrue(finished.await(TIMEOUT_MS * 3, TimeUnit.MILLISECONDS),
				"Publishers did not finish - possible deadlock");
		Assert.assertEquals(failures.get(), 0, "No publisher should have failed");
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), threads * perThread,
				"Every message must be accounted for in the buffer");

		startDrain();
		releasePermits(threads * perThread);
		awaitBufferDepth(0);
		Assert.assertEquals(fakeClient.publishedTopics().size(), threads * perThread, "Every message must be delivered");
	}

	// ------------------------------------------------------------------------------------------------------------
	// Disconnect and the LWT
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * The LWT is the only death certificate on a clean DISCONNECT, because the MQTT server suppresses the Will.
	 * With the in-flight window exhausted the acknowledged publish would be buffered - and the buffer is torn down
	 * moments later - so it must fall back to QoS 0 and actually leave the process.
	 *
	 * Asserts the fallback rather than the disconnect ordering: it calls publishLwt() directly, so it does not need a
	 * connected Paho client to tear down.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void lwtFallsBackToQos0WhenItCannotBePublishedAtItsConfiguredQos() throws Exception {
		wire(0, 8);
		configureLwt(1);

		tahuClient.publishLwt(false);

		Assert.assertEquals(fakeClient.publishedTopics(), List.of(LWT_TOPIC),
				"The LWT must reach the MQTT client even with no in-flight permit available");
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0,
				"The LWT must not be left sitting in the buffer - the fallback exists so it is not queued at all");
		Assert.assertEquals(availablePermits(), 0, "QoS 0 must not consume an in-flight permit");
	}

	/** With a permit free there is nothing to fall back from, so the configured QoS is used and acknowledged. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void lwtUsesItsConfiguredQosWhenAPermitIsAvailable() throws Exception {
		wire(4, 8);
		configureLwt(1);

		tahuClient.publishLwt(false);

		Assert.assertEquals(fakeClient.publishedTopics(), List.of(LWT_TOPIC));
		Assert.assertEquals(availablePermits(), 3, "A QoS 1 LWT must take a permit, so it can be acknowledged");
	}

	/**
	 * The drain thread is started by connect() before the Paho client is built, so a disconnect that finds no
	 * client still has one to stop. Moving the shutdown below the LWT publish put it inside the 'client != null'
	 * arm, which is exactly how this path would have been left orphaned.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void disconnectStopsTheDrainThreadWhenThereIsNoClient() throws Exception {
		wire(0, 8);
		startDrain();
		Assert.assertNotNull(get(tahuClient, "publishBufferDrainThread"), "Precondition: the drain thread is running");

		set(tahuClient, "client", null);
		tahuClient.disconnect(0, 1, false, false, false);

		Assert.assertNull(get(tahuClient, "publishBufferDrain"), "The drain must be stopped by a no-client disconnect");
		Assert.assertNull(get(tahuClient, "publishBufferDrainThread"), "The drain thread reference must be cleared");
	}

	/**
	 * The drain wait is what lets the LWT keep its configured QoS. With the buffer already empty it returns at once and
	 * the LWT goes inline at QoS 1, taking a permit so it can be acknowledged - no fallback.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void disconnectPublishesTheLwtAtItsConfiguredQosWhenTheBufferIsClear() throws Exception {
		wire(4, 8);
		configureLwt(1);

		tahuClient.disconnect(0, 1, true, true, false);

		Assert.assertEquals(fakeClient.publishedTopics(), List.of(LWT_TOPIC));
		Assert.assertTrue(fakeClient.disconnectSent, "A delivered LWT must still be followed by a clean DISCONNECT");
	}

	/**
	 * Last resort. Every route to publishing the death certificate has failed, so a clean DISCONNECT would
	 * suppress the Will and leave subscribers with nothing. The connection must drop instead, so the MQTT server
	 * publishes the Will it already holds from connect().
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void disconnectSuppressesTheDisconnectPacketWhenTheLwtCannotBePublished() throws Exception {
		wire(4, 8);
		configureLwt(1);
		fakeClient.failNextPublishes(Integer.MAX_VALUE);

		tahuClient.disconnect(0, 1, true, true, false);

		Assert.assertTrue(fakeClient.disconnectForciblyCalled, "The disconnect must still complete");
		Assert.assertFalse(fakeClient.disconnectSent,
				"With no death certificate published, the DISCONNECT must be withheld so the Will fires");
	}

	/** A client with no LWT configured has nothing to escalate over, so the disconnect stays clean. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void disconnectSendsTheDisconnectPacketWhenThereIsNoLwt() throws Exception {
		wire(4, 8);

		tahuClient.disconnect(0, 1, true, true, false);

		Assert.assertTrue(fakeClient.disconnectSent, "No LWT configured is not a failure to publish one");
	}

	/**
	 * Nothing buffered before a disconnect may be published on the next session.
	 *
	 * The guarantee is real but indirect: connect() clears nothing itself, it relies on calling disconnect() first,
	 * and disconnect() clears the buffer only as a side effect of stopping the drain thread. Since
	 * startPublishBufferDrainThread() returns early when a drain already exists, any future path that reached
	 * connect() without a disconnect would carry the old buffer and its live drain thread straight into the new
	 * session and replay stale messages against it. This pins the invariant at the disconnect boundary.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void bufferedMessagesAreNotReplayedOnTheNextSession() throws Exception {
		wire(0, 8);
		startDrain();
		for (int i = 0; i < 5; i++) {
			tahuClient.publish("topic/stale-" + i, "x".getBytes(), 1, false);
		}
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 5, "Precondition: the messages are queued");
		Object drainBeforeDisconnect = get(tahuClient, "publishBufferDrain");
		Assert.assertNotNull(drainBeforeDisconnect, "Precondition: a drain thread owns that buffer");

		tahuClient.disconnect(0, 1, false, false, false);

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0, "The disconnect must leave nothing queued");
		Assert.assertNull(get(tahuClient, "publishBufferDrain"), "The drain that owned the old buffer must be gone");

		// Stand in for the next connect(): the server is up, permits are restored and the drain is restarted,
		// exactly as connect() does. The session has to be live or the drain would decline to publish anyway and
		// the assertion below would prove nothing.
		fakeClient.markConnected();
		wire(8, 8);
		startDrain();
		Assert.assertNotSame(get(tahuClient, "publishBufferDrain"), drainBeforeDisconnect,
				"The new session must get a new drain, not the one still holding the old buffer");

		// Give the new drain longer than its poll interval to publish anything it might still be holding
		Thread.sleep(DRAIN_POLL_MS * 3);

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0);
		Assert.assertEquals(fakeClient.publishedTopics(), List.of(),
				"No message queued before the disconnect may reach the MQTT server on the next session");
	}

	/**
	 * Once the drain is gone, a message that would be buffered is refused rather than accepted.
	 *
	 * The client still reports connected from the drain shutdown until disconnectForcibly(), so publish() used to
	 * accept into a buffer nothing owned - a concurrent publisher, an AsyncPublisher thread, or publishLwt(), which
	 * has call sites outside disconnect(). Those entries could then be inherited by the next session's drain and
	 * flushed onto it; a retained death certificate replayed that way tells subscribers a live host is offline.
	 *
	 * Refusing is the honest answer: publish() returning null means "queued, not lost", which cannot be true of a
	 * message no thread will ever send. The throw reaches the caller in time for it to store the message itself.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void publishIsRefusedWhenNoDrainOwnsTheBuffer() throws Exception {
		wire(0, 8);
		invoke(tahuClient, "shutdownPublishBufferDrainThread");
		Assert.assertNull(get(tahuClient, "publishBufferDrain"), "Precondition: nothing owns the buffer now");

		try {
			tahuClient.publish(LWT_TOPIC, "death".getBytes(), 1, true);
			Assert.fail("A publish with no drain to send it must reach the caller as a failure, not return null");
		} catch (TahuException e) {
			Assert.assertTrue(e.getMessage().contains("no publish buffer drain"), "Unexpected message: " + e);
		}

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0, "A refused message must not be buffered");
		Assert.assertEquals(tahuClient.getPublishBufferRejectedMessageCount(), 1,
				"A refusal the caller was told about is a rejection, counted with the capacity refusals");
		Assert.assertEquals(tahuClient.getPublishBufferDiscardedMessageCount(), 0,
				"Nothing was accepted, so nothing was discarded");
	}

	/**
	 * A shutdown discards the buffer whether or not a drain is still registered.
	 *
	 * Belt and braces behind publishIsRefusedWhenNoDrainOwnsTheBuffer: that refusal is what keeps entries out of an
	 * unowned buffer, and this is what guarantees any that got there anyway do not survive into the next session.
	 * The discard used to sit below the publishBufferDrain == null guard, so the second shutdown - the one
	 * connect() performs before every attempt - returned without clearing.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void shutdownDiscardsTheBufferEvenWithNoDrainRegistered() throws Exception {
		wire(0, 8);
		tahuClient.publish(LWT_TOPIC, "death".getBytes(), 1, true);
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1, "Precondition: the message is queued");

		// Drop the drain reference without touching the buffer, as the teardown window leaves it.
		set(tahuClient, "publishBufferDrain", null);
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1, "Precondition: the buffer outlived its drain");

		invoke(tahuClient, "shutdownPublishBufferDrainThread");

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0,
				"A shutdown must discard the buffer whether or not a drain is still registered");
		Assert.assertEquals(tahuClient.getPublishBufferDiscardedMessageCount(), 1,
				"A discarded message is lost data and must be counted - publish() told the caller it was queued");

		// Stand in for the new session, as bufferedMessagesAreNotReplayedOnTheNextSession does.
		fakeClient.markConnected();
		wire(8, 8);
		Thread.sleep(DRAIN_POLL_MS * 3);

		Assert.assertEquals(fakeClient.publishedTopics(), List.of(),
				"Nothing queued in the old session may reach the MQTT server on the next one");
	}

	/** Discards are a lifetime total for the client, so they accumulate across sessions rather than resetting. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void discardedMessagesAccumulateOnTheLifetimeCounter() throws Exception {
		wire(0, 8);
		startDrain();
		for (int i = 0; i < 3; i++) {
			tahuClient.publish("topic/first-session-" + i, "x".getBytes(), 1, false);
		}
		invoke(tahuClient, "shutdownPublishBufferDrainThread");
		Assert.assertEquals(tahuClient.getPublishBufferDiscardedMessageCount(), 3);

		fakeClient.markConnected();
		wire(0, 8);
		startDrain();
		for (int i = 0; i < 2; i++) {
			tahuClient.publish("topic/second-session-" + i, "x".getBytes(), 1, false);
		}
		invoke(tahuClient, "shutdownPublishBufferDrainThread");
		Assert.assertEquals(tahuClient.getPublishBufferDiscardedMessageCount(), 5,
				"The counter is for the life of the client, not the life of a session");
		Assert.assertEquals(tahuClient.getPublishBufferRejectedMessageCount(), 0,
				"Nothing was refused at the door here - the buffer never reached capacity");
	}

	/**
	 * A QoS 0 publish must not wait for the buffer to drain.
	 *
	 * QoS 0 takes no permit and is never buffered, so nothing about it depends on the acknowledged queue - holding it
	 * behind one makes live data stale for no delivery benefit. Every Sparkplug publish in tahu is QoS 0, and
	 * EdgeClient publishes while holding its own clientLock, so a QoS 0 publisher stuck behind the drain stalls the
	 * whole Sparkplug path including sequence number allocation.
	 *
	 * Timing-based by necessity, with a wide margin: the drain holds the MQTT client for 5 x 200ms here, and the
	 * assertion only fails if the QoS 0 publish waited for more than a couple of those.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 2)
	public void qos0DoesNotWaitForTheBufferToDrain() throws Exception {
		wire(0, 8);
		for (int i = 0; i < 5; i++) {
			tahuClient.publish("topic/queued-" + i, "x".getBytes(), 1, false);
		}
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 5, "Precondition: the drain has work to do");

		fakeClient.setPublishDelay(200);
		releasePermits(8);
		// Let the drain get inside a send, so the QoS 0 publish arrives while the client is genuinely busy
		Thread.sleep(100);

		long start = System.currentTimeMillis();
		tahuClient.publish("topic/live", "x".getBytes(), 0, false);
		long elapsed = System.currentTimeMillis() - start;

		// The two regimes are far apart: taking publishOrderLock measured 500-904ms across runs, against a drain
		// holding the client for 5 x 200ms. Bypassing it costs only this message's own send - a flat 201ms here.
		Assert.assertTrue(elapsed < 450, "QoS 0 waited " + elapsed + "ms for the buffer to drain - it must not queue "
				+ "behind acknowledged traffic, and the wait scales with buffer depth");
	}

	// ------------------------------------------------------------------------------------------------------------
	// BIRTH
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * A BIRTH that would be buffered is not a published BIRTH, and must not leave the session announcing itself.
	 *
	 * publishBirthMessage() recovers only from a TahuException, which was sound while publish() blocked until the
	 * message reached Paho - "returned without throwing" meant "on the wire". A buffered publish returns null
	 * instead, so the recovery could not fire: the caller marked the session online with no BIRTH sent, and the
	 * queued copy was then dropped when its retry window expired or discarded on the next reconnect.
	 *
	 * Reached from setOnlineState() mid-session. connect() rebuilds the semaphore before the Paho client exists, so
	 * the connectComplete() route always finds a full window and an empty buffer.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void birthIsNotConsideredPublishedWhenItWouldBeBuffered() throws Exception {
		wire(0, 8);
		configureBirth();

		tahuClient.publishBirthMessage();

		Assert.assertEquals(fakeClient.publishedTopics(), List.of(),
				"No BIRTH reached the MQTT server, so none may be reported as sent");
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0,
				"A queued BIRTH would be delivered late, out of session, or dropped - it must not be queued at all");
		awaitTrue(() -> fakeClient.disconnectForciblyCalled,
				"With no BIRTH on the wire the session must be dropped so connect() can publish one on a fresh "
						+ "session, which is the recovery the exception path already performs");
	}

	/**
	 * The teardown re-arms recovery, because disconnect() is what removes it.
	 *
	 * disconnect() stops the connection monitor, stops the connect runnable and clears the client field, and Paho
	 * raises no connectionLost for a locally requested disconnect - ClientComms.shutdownConnection passes a null
	 * cause and CommsCallback forwards only a non-null one. A host application whose only reconnect is the
	 * connectionLost callback would otherwise never reconnect, leaving a retained offline STATE on its topic.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void theBirthTeardownReArmsTheConnectionLostCallback() throws Exception {
		wire(0, 8);
		configureBirth();

		tahuClient.publishBirthMessage();

		awaitTrue(() -> callback.connectionLostCount.get() == 1,
				"The teardown must fire connectionLost once, since Paho fires none for a local disconnect");
		Assert.assertNotNull(callback.lastCause.get(),
				"The cause must be non-null - TahuHostCallback only reconnects on a forwarded callback");
	}

	/**
	 * The Paho close must not run while clientLock is held.
	 *
	 * CommsCallback.stop() takes a shortcut only for its own callback thread; every other caller spin waits, with no
	 * cap, for that thread to leave its run loop. Three of the four callbacks it dispatches need clientLock, so a
	 * teardown that holds the lock across the close waits for a thread that is waiting for the lock. Neither side has
	 * a timeout, and clientLock is never released again.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void theTeardownDoesNotHoldClientLockAcrossThePahoClose() throws Exception {
		wire(8, 8);
		final Object clientLock = get(tahuClient, "clientLock");
		final CountDownLatch lockTaken = new CountDownLatch(1);

		/*
		 * Recorded inside the close, not after it. Asserting on the latch afterwards proves nothing: the contender
		 * gets the lock the moment disconnect() returns either way, so the check passes against a build that held
		 * the lock throughout. What has to be true is that the lock was free WHILE the close was running.
		 */
		final AtomicBoolean lockFreeDuringClose = new AtomicBoolean(false);

		// Stands in for the callback thread needing clientLock while stop() waits for it
		fakeClient.duringDisconnectForcibly = () -> {
			Thread contender = new Thread(() -> {
				synchronized (clientLock) {
					lockTaken.countDown();
				}
			}, "lock-contender");
			contender.setDaemon(true);
			contender.start();
			try {
				lockFreeDuringClose.set(lockTaken.await(2, TimeUnit.SECONDS));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		};

		tahuClient.disconnect(0, 1, false, false, false);

		Assert.assertTrue(lockFreeDuringClose.get(),
				"clientLock was held across the Paho close - a thread that needs it cannot get in, which is the "
						+ "deadlock when that thread is the one the close is waiting for");
	}

	/**
	 * The pre-LWT flush gives up when nothing is servicing it, whichever thread is waiting.
	 *
	 * The wait is starved whenever the callback thread cannot reach deliveryComplete(), and that is not only when
	 * the waiter is that thread. EdgeClient holds its own clientLock across tahuClient.disconnect() while
	 * handleStateMessage() blocks on the same lock from the callback thread - deliveryComplete queues behind it and
	 * never runs. A guard keyed on thread identity cannot see that, because the waiter is EdgeClient's thread and
	 * the lock belongs to the embedder. Progress can be seen from here; topology cannot.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void theDrainWaitGivesUpWhenNothingIsServicingIt() throws Exception {
		wire(0, 8);
		for (int i = 0; i < 3; i++) {
			tahuClient.publish("topic/queued-" + i, "x".getBytes(), 1, false);
		}
		awaitBufferDepth(3);

		/*
		 * The wait itself, not the whole disconnect. Measuring disconnect() puts the unconditional 1000ms Paho
		 * workaround sleep inside the assertion, which left about 180ms of headroom over four sleeps - a threshold
		 * the passing path very nearly reaches, so it would fail on a loaded runner with no defect present.
		 */
		Method awaitDrained = TahuClient.class.getDeclaredMethod("awaitPublishBufferDrained", long.class);
		awaitDrained.setAccessible(true);

		long start = System.currentTimeMillis();
		awaitDrained.invoke(tahuClient, LWT_DRAIN_BUDGET_MS);
		long elapsed = System.currentTimeMillis() - start;

		Assert.assertTrue(elapsed < LWT_DRAIN_BUDGET_MS, "The flush waited " + elapsed + "ms with neither the buffer "
				+ "depth nor the permit count moving - nothing was servicing it and it can only reach its deadline");

		/*
		 * Bounded below as well, because "gives up" and "gives up too early" are different defects and only the
		 * first was being checked. The no-progress window is half the budget, so a give-up well short of that is a
		 * threshold that does not scale with the budget it is spending - and a fixed one cannot distinguish a
		 * stalled servicer from a server whose acknowledgements are merely slower than the constant, which is the
		 * case this must not mistake. 100ms of slack: the loop samples every 50ms and the bound is 1000ms.
		 */
		Assert.assertTrue(elapsed >= (LWT_DRAIN_BUDGET_MS / 2) - 100,
				"The flush gave up after " + elapsed + "ms, short of half its " + LWT_DRAIN_BUDGET_MS + "ms budget - "
						+ "a no-progress window that does not scale with the budget spends only a fraction of it and "
						+ "reads a slow acknowledgement round trip as a stalled one");
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 3, "Nothing could drain, so nothing may have drained");
	}

	/**
	 * A teardown that claims nothing must not release another one's claim.
	 *
	 * The gate was a plain boolean with no owner. A scoped teardown whose session is no longer live returns without
	 * ever setting it, and the finally cleared it anyway - so a second teardown disarmed the first's window while
	 * its Paho client was still open, reopening every guard that reads the gate for exactly the interval they exist
	 * to cover.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void aScopedTeardownThatDoesNothingDoesNotClearAnotherTeardownsGate() throws Exception {
		wire(8, 8);
		final CountDownLatch inTeardown = new CountDownLatch(1);
		final CountDownLatch releaseTeardown = new CountDownLatch(1);
		final AtomicBoolean gateHeldThroughout = new AtomicBoolean(true);

		fakeClient.duringDisconnectForcibly = () -> {
			inTeardown.countDown();
			try {
				releaseTeardown.await(2, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			gateHeldThroughout.set(tahuClient.isDisconnectInProgress());
		};

		Thread disconnector = new Thread(() -> {
			try {
				tahuClient.disconnect(0, 1, false, false, false);
			} catch (Exception e) {
				// reported by the assertions below
			}
		}, "disconnector");
		disconnector.setDaemon(true);
		disconnector.start();
		Assert.assertTrue(inTeardown.await(3, TimeUnit.SECONDS), "Precondition: the first teardown must be in flight");

		// A second teardown for a session that is not live: it claims nothing, so it must release nothing
		Method disconnectSession = TahuClient.class.getDeclaredMethod("disconnectSession", TahuMqttAsyncClient.class,
				long.class, long.class, boolean.class, boolean.class, boolean.class);
		disconnectSession.setAccessible(true);
		disconnectSession.invoke(tahuClient, new FakeMqttClient(), 0L, 1L, false, false, false);

		releaseTeardown.countDown();
		disconnector.join(3000);

		Assert.assertTrue(gateHeldThroughout.get(),
				"A teardown that tore nothing down cleared the gate of one that was still closing its client");
	}

	/**
	 * connect() must mark itself in progress before it releases the lock, not after the close.
	 *
	 * detachSession() clears the in-progress flag, and the close that follows runs for at least a second with the
	 * lock released. Arming only afterwards left a window where the client is null, no connect is in progress and no
	 * teardown is either - so a second caller passed every gate and started its own ConnectRunnable, leaving two
	 * live Paho clients for one client id with a reference to only one of them.
	 *
	 * Asserted from inside the close, which is the middle of that window - not afterwards, when both orderings look
	 * the same.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void aConnectArmsTheGateBeforeItReleasesTheLock() throws Exception {
		wire(8, 8);
		final AtomicBoolean armedDuringClose = new AtomicBoolean(false);

		fakeClient.duringDisconnectForcibly = () -> {
			try {
				armedDuringClose.set(state(tahuClient));
			} catch (Exception e) {
				// reported by the assertion below
			}
		};

		try {
			tahuClient.connect();

			Assert.assertTrue(armedDuringClose.get(),
					"connect() left the gate unarmed while it closed the previous session - a second connect would "
							+ "pass every gate in that window and start a second session for the same client id");
		} finally {
			// The connect attempt this starts is not a daemon, and there is no MQTT server for it to reach
			Object connectRunnable = get(tahuClient, "connectRunnable");
			if (connectRunnable != null) {
				invoke(connectRunnable, "stopConnectAttempts");
			}
			Thread connectThread = (Thread) get(tahuClient, "connectRunnableThread");
			if (connectThread != null) {
				connectThread.interrupt();
			}
		}
	}

	/**
	 * Drive the session lifecycle from several threads at once and assert the coordination state survives it.
	 *
	 * Every finding in rounds 3 to 6 was an interleaving, and none of them is visible to a single threaded test -
	 * the code is correct when one thread runs it. Mutation testing asks whether a test notices a changed line; it
	 * cannot ask what happens between two lines. This is the shape of test that can.
	 *
	 * It asserts the invariants the coordination state has to hold rather than any particular outcome: the teardown
	 * count never goes negative and returns to zero, no operation throws anything unexpected, and - the point of the
	 * timeOut - nothing wedges. A deadlock fails this by never finishing.
	 */
	@Test(
			timeOut = 60000)
	public void concurrentLifecycleOperationsDoNotWedgeOrCorruptState() throws Exception {
		wire(8, 8);
		configureBirth();
		configureLwt(1);

		final AtomicInteger teardowns = (AtomicInteger) get(tahuClient, "teardownsInFlight");
		final AtomicReference<Throwable> failure = new AtomicReference<>();
		final AtomicBoolean negativeSeen = new AtomicBoolean(false);
		final AtomicBoolean stop = new AtomicBoolean(false);
		final CountDownLatch started = new CountDownLatch(7);

		Runnable guard = () -> {
			started.countDown();
			while (!stop.get()) {
				if (teardowns.get() < 0) {
					negativeSeen.set(true);
				}
				Thread.yield();
			}
		};

		Runnable reinstaller = () -> {
			started.countDown();
			while (!stop.get()) {
				try {
					// Stands in for a connect completing: a fresh session appears under the old one's feet
					set(tahuClient, "client", new FakeMqttClient());
					set(tahuClient, "semaphore", new Semaphore(8, true));
					Thread.sleep(40);
				} catch (Throwable t) {
					failure.compareAndSet(null, t);
					return;
				}
			}
		};

		Runnable disconnector = () -> {
			started.countDown();
			while (!stop.get()) {
				try {
					tahuClient.disconnect(0, 1, false, false, false);
				} catch (TahuException expected) {
					// A disconnect racing another one is allowed to fail; wedging is not
				} catch (Throwable t) {
					failure.compareAndSet(null, t);
					return;
				}
			}
		};

		Runnable publisher = () -> {
			started.countDown();
			while (!stop.get()) {
				try {
					tahuClient.publish("topic/stress", "x".getBytes(), 1, false);
				} catch (TahuException expected) {
					// Rejected because the session went away mid publish - the documented outcome
				} catch (Throwable t) {
					failure.compareAndSet(null, t);
					return;
				}
			}
		};

		Runnable callbacks = () -> {
			started.countDown();
			while (!stop.get()) {
				try {
					// Paho delivers these from one thread; here they race everything else
					tahuClient.messageArrived("spBv1.0/STATE/host-1", new MqttMessage("{}".getBytes()));
					tahuClient.publishBirthMessage();
				} catch (Throwable t) {
					failure.compareAndSet(null, t);
					return;
				}
			}
		};

		Runnable lwt = () -> {
			started.countDown();
			while (!stop.get()) {
				try {
					tahuClient.publishLwt(false);
				} catch (TahuException | MqttException expected) {
					// Same as publish: losing the session mid call is allowed
				} catch (Throwable t) {
					failure.compareAndSet(null, t);
					return;
				}
			}
		};

		/*
		 * connect() has to be in the mix. It is where the gate, the claim, the in-progress flag and the deferral all
		 * interact, and a stress test that leaves it out exercises the state without exercising its coordination -
		 * which is how the first version of this test passed against code with four known defects in it.
		 *
		 * Its ConnectRunnable reaches a real socket, refused immediately since nothing is listening, and every
		 * disconnect above stops whichever one is current.
		 */
		Runnable connector = () -> {
			started.countDown();
			while (!stop.get()) {
				try {
					tahuClient.connect();
					Thread.sleep(30);
				} catch (Throwable t) {
					failure.compareAndSet(null, t);
					return;
				}
			}
		};

		List<Thread> threads = new ArrayList<>();
		for (Runnable r : List.of(guard, reinstaller, disconnector, publisher, callbacks, lwt, connector)) {
			Thread t = new Thread(r, "stress-" + threads.size());
			t.setDaemon(true);
			threads.add(t);
			t.start();
		}

		Assert.assertTrue(started.await(5, TimeUnit.SECONDS), "Stress threads never started");
		Thread.sleep(STRESS_DURATION_MS);
		stop.set(true);
		for (Thread t : threads) {
			t.join(15000);
			if (t.isAlive()) {
				Assert.fail("Thread " + t.getName() + " did not finish - something is wedged\n" + dumpThreads());
			}
		}

		// Stop whatever connect attempt is still running before asserting
		Object connectRunnable = get(tahuClient, "connectRunnable");
		if (connectRunnable != null) {
			invoke(connectRunnable, "stopConnectAttempts");
		}
		Thread connectThread = (Thread) get(tahuClient, "connectRunnableThread");
		if (connectThread != null) {
			connectThread.interrupt();
		}

		if (failure.get() != null) {
			throw new AssertionError("A lifecycle operation threw under contention", failure.get());
		}
		Assert.assertFalse(negativeSeen.get(),
				"The teardown count went negative - a claim was released by something that never took one");
		/*
		 * Settled rather than read once. The client starts workers of its own - the deferred connect and the BIRTH
		 * recovery - which the test cannot join, so one can be mid teardown with a claim taken at the instant the
		 * stress threads finish. Reading the counter there measures the test's timing, not the client's balance.
		 */
		long settleBy = System.currentTimeMillis() + 10000;
		while (teardowns.get() != 0 && System.currentTimeMillis() < settleBy) {
			Thread.sleep(50);
		}
		if (teardowns.get() != 0) {
			Assert.fail("Teardown claims did not settle - " + teardowns.get() + " still outstanding after 10s, so one "
					+ "was taken and never released and the gate is stuck closed.\n" + dumpThreads());
		}
	}

	/**
	 * A disconnect that finds no client must still clear the connect-in-progress flag.
	 *
	 * A connect attempt that is still building its Paho client has state.inProgress() true and the field still
	 * null. detachSession() stops that attempt, so nothing is in progress once it returns - but its early exit for
	 * a null client skipped the finally that says so. The flag then stayed true with the only thread that would
	 * have cleared it already stopped, and every later connect() was refused at the autoReconnect gate. Permanent,
	 * for the life of the client.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void aDisconnectWithNoClientStillClearsTheConnectInProgressFlag() throws Exception {
		// Deliberately no wire() - the client field is null, as it is while a connect attempt is building one
		setInProgress(tahuClient, true);

		tahuClient.disconnect(0, 1, false, false, false);

		Assert.assertFalse(state(tahuClient),
				"The disconnect stopped the connect attempt, so it must not leave the flag claiming one is still "
						+ "running - that refuses every later connect() for the life of the client");
	}

	/**
	 * The pre-reconnect client discard must not hold clientLock across the Paho close either.
	 *
	 * ConnectRunnable drops any leftover client before building a new one, and it did that inside
	 * synchronized (clientLock) - the same shape closeDetachedSession() exists to avoid. It runs on
	 * connectRunnableThread, so CommsCallback.stop() takes its uncapped spin wait for the callback thread, and that
	 * thread needs clientLock through three of the four callbacks it dispatches. Pre-existing rather than a
	 * regression, but it is the path taken before every reconnect attempt.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void theReconnectDiscardDoesNotHoldClientLockAcrossThePahoClose() throws Exception {
		wire(8, 8);
		final Object clientLock = get(tahuClient, "clientLock");
		final CountDownLatch lockTaken = new CountDownLatch(1);
		final AtomicBoolean lockFreeDuringClose = new AtomicBoolean(false);

		fakeClient.duringDisconnectForcibly = () -> {
			Thread contender = new Thread(() -> {
				synchronized (clientLock) {
					lockTaken.countDown();
				}
			}, "lock-contender");
			contender.setDaemon(true);
			contender.start();
			try {
				lockFreeDuringClose.set(lockTaken.await(2, TimeUnit.SECONDS));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		};

		invoke(tahuClient, "discardClientForReconnect");

		Assert.assertTrue(lockFreeDuringClose.get(),
				"The pre-reconnect discard held clientLock across the Paho close - the callback thread that close "
						+ "waits for needs the same lock");
		Assert.assertNull(get(tahuClient, "client"), "The discarded client must not be left installed");
	}

	/**
	 * A reconnect must not hold clientLock waiting for the old session's death certificate to be confirmed.
	 *
	 * connect() tears the previous session down before building a new one, and that teardown published the LWT and
	 * then waited for delivery confirmation - isLwtDeliveryComplete() polls for keepAlive * 4 quarter seconds, so
	 * keepAlive seconds, 30 here. The confirmation arrives on Paho's callback thread, and clientLock is held
	 * throughout, so every other operation on the client queues behind a reconnect for that long. Found by the
	 * concurrency stress test, which wedged here; it predates this branch.
	 *
	 * The LWT still goes out - only the wait for its acknowledgement is gone, and closeDetachedSession() pays an
	 * unconditional second with the lock released, which is the same grace period.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 3)
	public void aReconnectDoesNotHoldClientLockWaitingForLwtConfirmation() throws Exception {
		wire(8, 8);
		configureLwt(1);

		// Nothing will acknowledge the LWT, so a wait for confirmation can only run to its full keepAlive budget
		fakeClient.shutdownAckThread();

		try {
			long start = System.currentTimeMillis();
			tahuClient.connect();
			long elapsed = System.currentTimeMillis() - start;

			Assert.assertTrue(elapsed < LWT_CONFIRMATION_BUDGET_MS,
					"connect() took " + elapsed + "ms - it waited under clientLock for an LWT acknowledgement that "
							+ "was never coming, blocking every other operation on this client meanwhile");
		} finally {
			Object connectRunnable = get(tahuClient, "connectRunnable");
			if (connectRunnable != null) {
				invoke(connectRunnable, "stopConnectAttempts");
			}
			Thread connectThread = (Thread) get(tahuClient, "connectRunnableThread");
			if (connectThread != null) {
				connectThread.interrupt();
			}
		}
	}

	/**
	 * A connect refused during a teardown must be replayed, not lost.
	 *
	 * Refusing is right - a connect admitted mid-teardown brings up a second session for the same client id while
	 * the old socket is open. But a caller whose only reconnect trigger is one-shot has nothing to retry with:
	 * TahuHostCallback.connectionLost() calls connect() exactly once, and HostApplication has no run loop behind it.
	 * Dropping the request leaves that client offline for good, which is the outage the re-arm exists to prevent.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void aConnectRefusedDuringATeardownIsDeferredNotDropped() throws Exception {
		wire(8, 8);
		final CountDownLatch inTeardown = new CountDownLatch(1);
		final CountDownLatch releaseTeardown = new CountDownLatch(1);

		fakeClient.duringDisconnectForcibly = () -> {
			inTeardown.countDown();
			try {
				releaseTeardown.await(2, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		};

		Thread disconnector = new Thread(() -> {
			try {
				tahuClient.disconnect(0, 1, false, false, false);
			} catch (Exception e) {
				// reported by the assertions below
			}
		}, "disconnector");
		disconnector.setDaemon(true);
		disconnector.start();
		Assert.assertTrue(inTeardown.await(3, TimeUnit.SECONDS), "Precondition: the teardown must be in flight");

		try {
			tahuClient.connect();
			Assert.assertNull(get(tahuClient, "connectRunnable"),
					"Precondition: the connect must be refused while the teardown holds the gate");

			releaseTeardown.countDown();

			awaitTrue(() -> {
				try {
					return get(tahuClient, "connectRunnable") != null;
				} catch (Exception e) {
					return false;
				}
			}, "The refused connect must run once the teardown finishes - a one-shot caller has no second trigger");
		} finally {
			releaseTeardown.countDown();
			Object connectRunnable = get(tahuClient, "connectRunnable");
			if (connectRunnable != null) {
				invoke(connectRunnable, "stopConnectAttempts");
			}
			Thread connectThread = (Thread) get(tahuClient, "connectRunnableThread");
			if (connectThread != null) {
				connectThread.interrupt();
			}
		}
	}

	/**
	 * The replayed connect must not run with its own interrupt flag set.
	 *
	 * detachSession() cancels a pending deferral and interrupts the worker that would run it, so that a connect
	 * refused moments before a shutdown cannot still land after it. The worker runs connect(), and connect() calls
	 * detachSession() - so on the replay path that cancellation is the worker interrupting itself, every time.
	 *
	 * It is not cosmetic. Everything the teardown does after that point is a timed wait: awaitPublishBufferDrained()
	 * returns at its first sleep, so the pre-LWT drain is skipped and whatever was buffered is discarded, and
	 * closeDetachedSession()'s unconditional second - the grace period the LWT needs to reach the server before the
	 * socket is torn down - is skipped too. connect() gives up waiting for the LWT acknowledgement specifically
	 * because that second is there to cover it, so on this one path the death certificate has no cover at all.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 3)
	public void theDeferredConnectReplayDoesNotInterruptItself() throws Exception {
		wire(8, 8);
		configureLwt(1);

		final CountDownLatch inTeardown = new CountDownLatch(1);
		final CountDownLatch releaseTeardown = new CountDownLatch(1);

		fakeClient.duringDisconnectForcibly = () -> {
			inTeardown.countDown();
			try {
				releaseTeardown.await(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		};

		Thread disconnector = new Thread(() -> {
			try {
				tahuClient.disconnect(0, 1, false, false, false);
			} catch (Exception e) {
				// reported by the assertions below
			}
		}, "disconnector");
		disconnector.setDaemon(true);
		disconnector.start();
		Assert.assertTrue(inTeardown.await(3, TimeUnit.SECONDS), "Precondition: the teardown must be in flight");

		/*
		 * A session for the replay's own teardown to find. A reconnect normally has one - connect() tears the
		 * previous session down before building the next - and without it detachSession() returns null and the close
		 * this measures never runs.
		 */
		FakeMqttClient replaced = new FakeMqttClient();
		set(tahuClient, "client", replaced);

		try {
			tahuClient.connect();
			Assert.assertNull(get(tahuClient, "connectRunnable"),
					"Precondition: the connect must be refused while the teardown holds the gate");

			long start = System.currentTimeMillis();
			releaseTeardown.countDown();

			awaitTrue(() -> replaced.disconnectForciblyCalled,
					"The replayed connect must tear down the session it is replacing");
			long elapsed = System.currentTimeMillis() - start;

			Assert.assertTrue(replaced.publishedTopics().contains(LWT_TOPIC),
					"The replayed connect must still publish the LWT for the session it replaces");
			Assert.assertTrue(elapsed >= 1000, "The replayed connect reached disconnectForcibly in " + elapsed
					+ "ms, so it skipped the unconditional Paho grace period - it is running interrupted, and the "
					+ "LWT it just published is cut off before it can reach the server");
		} finally {
			releaseTeardown.countDown();
			Object connectRunnable = get(tahuClient, "connectRunnable");
			if (connectRunnable != null) {
				invoke(connectRunnable, "stopConnectAttempts");
			}
			Thread connectThread = (Thread) get(tahuClient, "connectRunnableThread");
			if (connectThread != null) {
				connectThread.interrupt();
			}
		}
	}

	/**
	 * A deferral armed before a shutdown must not connect after it.
	 *
	 * HostApplication.shutdown() clears autoReconnect and then disconnects, publishing the retained offline STATE.
	 * A replay that runs after that resurrects a session the application has finished with, and connectComplete()
	 * republishes the retained STATE as online for a host that is down - which every edge node bound to that primary
	 * host ID believes, and none of them fails over.
	 *
	 * connect()'s own gate cannot catch this: getAutoReconnect() && state.inProgress() stops testing anything the
	 * moment autoReconnect goes false. The check has to be on the replay path, which is where all three of them are
	 * - runPendingConnect(), the worker, and connect(deferredReplay = true) under clientLock. This test covers the
	 * first two; the one under the lock is
	 * {@link #aReplayIsRefusedUnderTheLockWhenAutoReconnectWasClearedWhileItWaited}.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 2)
	public void aDeferredConnectIsDroppedWhenTheClientStopsAutoReconnecting() throws Exception {
		wire(8, 8);
		final CountDownLatch inTeardown = new CountDownLatch(1);
		final CountDownLatch releaseTeardown = new CountDownLatch(1);

		fakeClient.duringDisconnectForcibly = () -> {
			inTeardown.countDown();
			try {
				releaseTeardown.await(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		};

		Thread disconnector = new Thread(() -> {
			try {
				tahuClient.disconnect(0, 1, false, false, false);
			} catch (Exception e) {
				// reported by the assertions below
			}
		}, "disconnector");
		disconnector.setDaemon(true);
		disconnector.start();
		Assert.assertTrue(inTeardown.await(3, TimeUnit.SECONDS), "Precondition: the teardown must be in flight");

		try {
			tahuClient.connect();
			Assert.assertTrue((Boolean) get(tahuClient, "deferredConnectPending"),
					"Precondition: the refused connect must have armed a replay");

			// What shutdown() does, in the window the replay is waiting in
			tahuClient.setAutoReconnect(false);
			releaseTeardown.countDown();

			awaitFalse(() -> {
				try {
					return (Boolean) get(tahuClient, "deferredConnectPending");
				} catch (Exception e) {
					return true;
				}
			}, "The armed replay must be resolved once the teardown finishes");

			Thread.sleep(500);
			Assert.assertNull(get(tahuClient, "connectRunnable"),
					"A connect ran after the client stopped auto reconnecting - the session the application shut down "
							+ "is back up, and its BIRTH republishes the retained STATE as online");
		} finally {
			releaseTeardown.countDown();
			tahuClient.setAutoReconnect(true);
			Object connectRunnable = get(tahuClient, "connectRunnable");
			if (connectRunnable != null) {
				invoke(connectRunnable, "stopConnectAttempts");
			}
			Thread connectThread = (Thread) get(tahuClient, "connectRunnableThread");
			if (connectThread != null) {
				connectThread.interrupt();
			}
		}
	}

	/**
	 * A replay must be abandoned even if the shutdown lands after the worker's own check.
	 *
	 * runPendingConnect() and its worker both test autoReconnect, but the worker's test is a few bytecodes ahead of
	 * connect()'s critical section, and connect() had no autoReconnect gate of its own on that path - its
	 * getAutoReconnect() && state.inProgress() test stops testing anything once the flag is false. A shutdown
	 * interleaving in that gap therefore still reached the connect, resurrecting a session the application had
	 * finished with. Very low probability - it has to land in a few bytecodes against a teardown that takes over a
	 * second - but the outcome is the retained STATE republished as online for a host that is down.
	 *
	 * The window itself is not what this asserts: a race that narrow cannot be hit reliably, and a test that tried
	 * would guard nothing. This drives the replay entry point directly - connect(deferredReplay = true) with
	 * autoReconnect already false - which is deterministic and fails if the guard is removed.
	 *
	 * The scope matters as much as the guard, so {@link #aConnectWithAutoReconnectOffStillConnects} holds the other
	 * side: an unconditional refusal here would break the single attempt ConnectRunnable implements for
	 * autoReconnect false, which EdgeClient uses to try each server definition once.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void aReplayIsRefusedUnderTheLockWhenAutoReconnectWasClearedWhileItWaited() throws Exception {
		wire(8, 8);
		tahuClient.setAutoReconnect(false);

		Method connect = TahuClient.class.getDeclaredMethod("connect", boolean.class);
		connect.setAccessible(true);
		connect.invoke(tahuClient, true);

		Assert.assertNull(get(tahuClient, "connectRunnable"),
				"A replay connected after the client stopped auto reconnecting. The worker's own check is ahead of "
						+ "connect()'s critical section, so the guard inside that section is what closes the window");
	}

	/**
	 * ...and a caller that is not a replay must still connect with autoReconnect false.
	 *
	 * The other side of the guard above, and the reason it is not the unconditional one-line refusal it could have
	 * been. Connecting once without auto reconnect is a supported path: ConnectRunnable has an explicit branch for
	 * it, and EdgeClient sets autoReconnect false and connects immediately, to try each MQTT server definition in
	 * turn. A guard that refused every caller would stop every edge node connecting at all.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void aConnectWithAutoReconnectOffStillConnects() throws Exception {
		wire(8, 8);
		tahuClient.setAutoReconnect(false);

		try {
			tahuClient.connect();

			Assert.assertNotNull(get(tahuClient, "connectRunnable"),
					"A single connect with autoReconnect off must still run - ConnectRunnable implements that "
							+ "branch and EdgeClient depends on it");
		} finally {
			Object connectRunnable = get(tahuClient, "connectRunnable");
			if (connectRunnable != null) {
				invoke(connectRunnable, "stopConnectAttempts");
			}
			Thread connectThread = (Thread) get(tahuClient, "connectRunnableThread");
			if (connectThread != null) {
				connectThread.interrupt();
			}
			tahuClient.setAutoReconnect(true);
		}
	}

	/**
	 * A teardown must cancel a replay armed for the session it is tearing down.
	 *
	 * The deferral records an intent, not a session, so nothing in it distinguishes "reconnect the session that just
	 * went away" from "reconnect whatever is there when the teardown finishes". Cancelling on teardown is what makes
	 * the intent scoped: a connect refused moments before a disconnect is a connect for a session that no longer
	 * exists by the time it could run.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void aTeardownCancelsAReplayArmedForTheSessionItIsTearingDown() throws Exception {
		wire(8, 8);
		Assert.assertNull(get(tahuClient, "connectRunnable"), "Precondition: nothing has connected yet");
		set(tahuClient, "deferredConnectPending", true);

		tahuClient.disconnect(0, 1, false, false, false);

		/*
		 * The observable is the connect, not the flag. releaseTeardownClaim() clears the flag on its way past
		 * whether or not the teardown cancelled it, so asserting the flag alone passes against a teardown that
		 * cancels nothing - it just watches the replay path consume the intent and then run it.
		 */
		Thread.sleep(1500);
		Assert.assertNull(get(tahuClient, "connectRunnable"),
				"A replay armed before the teardown connected after it. Nothing ties the intent to the session that "
						+ "asked for it, so it reconnects whatever is there when the last claim drops");
		Assert.assertFalse((Boolean) get(tahuClient, "deferredConnectPending"),
				"The replay must not still be armed once the teardown has finished");
	}

	/**
	 * The random startup delay must not be served holding clientLock.
	 *
	 * RandomStartupDelay is configured in milliseconds and exists to stagger a fleet's reconnects, so it is meant
	 * to be long. It is drawn fresh before every attempt in the retry loop, which is the feature - clients sharing
	 * a connect retry interval return from a common outage in lockstep and only new jitter re-scatters them - and
	 * that loop spins for as long as the server is unreachable. Served under the lock, every publish, teardown and
	 * connectComplete on this client queues behind a timer, and so does disconnect().
	 *
	 * The probe measures the property directly rather than the delay: while the delay is being served, another
	 * thread must be able to take clientLock. The margins are a whole second either side of a three second delay,
	 * so this is not a threshold the passing path comes near.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 4)
	public void theRandomStartupDelayIsNotServedHoldingClientLock() throws Exception {
		wire(8, 8);
		set(tahuClient, "randomStartupDelay", new RandomStartupDelay(STARTUP_DELAY_MS + "-" + STARTUP_DELAY_MS));
		final Object clientLock = get(tahuClient, "clientLock");

		Thread connector = new Thread(() -> tahuClient.connect(), "connector");
		connector.setDaemon(true);
		connector.start();

		try {
			awaitTrue(() -> {
				try {
					return get(tahuClient, "connectRunnableThread") != null;
				} catch (Exception e) {
					return false;
				}
			}, "Precondition: the connect runnable must start");

			// Past the brief acquisitions run() makes before the loop, and well inside the delay itself
			Thread.sleep(400);

			final CountDownLatch acquired = new CountDownLatch(1);
			Thread probe = new Thread(() -> {
				synchronized (clientLock) {
					acquired.countDown();
				}
			}, "lock-probe");
			probe.setDaemon(true);
			probe.start();

			Assert.assertTrue(acquired.await(LOCK_PROBE_BUDGET_MS, TimeUnit.MILLISECONDS),
					"clientLock was still held " + LOCK_PROBE_BUDGET_MS + "ms into a " + STARTUP_DELAY_MS
							+ "ms startup delay. Every operation on this client is queued behind a timer for the "
							+ "length of it, on every attempt, for as long as the server stays unreachable");
		} finally {
			Object connectRunnable = get(tahuClient, "connectRunnable");
			if (connectRunnable != null) {
				invoke(connectRunnable, "stopConnectAttempts");
			}
			Thread connectThread = (Thread) get(tahuClient, "connectRunnableThread");
			if (connectThread != null) {
				connectThread.interrupt();
			}
			connector.join(5000);
		}
	}

	/**
	 * A graceful disconnect must not hold clientLock waiting for the death certificate to be acknowledged.
	 *
	 * isLwtDeliveryComplete() polls for keepAlive * 4 quarter seconds - keepAlive seconds, 30 here - and the
	 * confirmation it waits for arrives on Paho's callback thread by way of deliveryComplete(). That thread needs
	 * clientLock for connectComplete, so waiting under the lock blocks one of the threads the wait depends on, and
	 * every other operation on the client for the same period. HostApplication's shutdown is the caller that
	 * reaches it: disconnect(100, 100, true, true), whose four argument overload forwards publishLwt = true.
	 *
	 * The wait itself is kept - a graceful disconnect wants the certificate confirmed, unlike the reconnect path,
	 * which drops it because the session is being replaced anyway. Only the lock is given up, and the wait is bound
	 * to the token it published rather than to the live field.
	 *
	 * This test probes the lock and nothing else. It passes trivially when no wait happens at all, so it is not
	 * evidence that the wait survives - it did not, and it said nothing. That the wait still happens is
	 * {@link #aGracefulDisconnectDoesNotReturnUntilTheDeathCertificateIsAcknowledged}.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 4)
	public void aGracefulDisconnectDoesNotHoldClientLockWaitingForTheLwtAcknowledgement() throws Exception {
		wire(8, 8);
		configureLwt(1);

		// Nothing will acknowledge it, so the wait can only run to its full keepAlive budget
		fakeClient.shutdownAckThread();
		final Object clientLock = get(tahuClient, "clientLock");

		Thread disconnector = new Thread(() -> {
			try {
				tahuClient.disconnect(0, 1, false, true, true);
			} catch (Exception e) {
				// reported by the assertion below
			}
		}, "disconnector");
		disconnector.setDaemon(true);
		disconnector.start();

		// Past the detach and into the wait, with the whole keepAlive budget still ahead of it
		Thread.sleep(1200);

		final CountDownLatch acquired = new CountDownLatch(1);
		Thread probe = new Thread(() -> {
			synchronized (clientLock) {
				acquired.countDown();
			}
		}, "lock-probe");
		probe.setDaemon(true);
		probe.start();

		Assert.assertTrue(acquired.await(LOCK_PROBE_BUDGET_MS, TimeUnit.MILLISECONDS),
				"clientLock was still held " + LOCK_PROBE_BUDGET_MS + "ms into the LWT acknowledgement wait. That "
						+ "wait runs for keepAlive seconds and is satisfied by Paho's callback thread, which needs "
						+ "this lock - so it blocks a thread it depends on, and every other operation meanwhile");
	}

	/**
	 * A graceful disconnect must actually wait for the death certificate to be acknowledged.
	 *
	 * Moving the wait out of clientLock left it keyed off lwtDeliveryToken while detachSession()'s finally cleared
	 * that field on every path out - so awaitLwtDelivery() compared a null field against the non-null token just
	 * published, returned on its first iteration, and logged a confirmation it had not obtained. Deterministic, not
	 * a race: the field was always null by then, because the same thread had just run the finally that cleared it.
	 *
	 * The outcome was never read - it goes to a local and a trace log - so nothing escalated wrongly. What was lost
	 * is time: up to keepAlive seconds for the retained QoS 1 certificate to leave Paho's outbound queue, collapsed
	 * to closeDetachedSession()'s unconditional second. Against a server that has stopped acknowledging - the case
	 * this whole path exists for - the socket then goes while the PUBLISH is still queued, and the server sees a
	 * graceful DISCONNECT and suppresses the Will it holds from connect(). HostApplication.shutdown() is the one
	 * in-tree caller that asks for the confirmation, so a host that has finished shutting down can leave a retained
	 * STATE of online: true standing for every edge node bound to it.
	 *
	 * One-sided deliberately: it asserts the disconnect has NOT returned yet. A correct wait runs for keepAlive
	 * seconds - 30 here - so no amount of load can turn this green, while the defect returns in about a second.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 4)
	public void aGracefulDisconnectDoesNotReturnUntilTheDeathCertificateIsAcknowledged() throws Exception {
		wire(8, 8);
		configureLwt(1);

		// Nothing acknowledges it, so the token never completes and the wait can only run to its full budget
		fakeClient.shutdownAckThread();

		final CountDownLatch returned = new CountDownLatch(1);
		Thread disconnector = new Thread(() -> {
			try {
				tahuClient.disconnect(0, 1, false, true, true);
			} catch (Exception e) {
				// Reported by the assertions below
			} finally {
				returned.countDown();
			}
		}, "disconnector");
		disconnector.setDaemon(true);
		disconnector.start();

		boolean finished = returned.await(LWT_CONFIRMATION_BUDGET_MS, TimeUnit.MILLISECONDS);

		/*
		 * Unwedged before the assertion, not after, so a failing run does not leave the rest of the keepAlive
		 * budget behind it. awaitLwtDelivery() returns on the interrupt.
		 */
		disconnector.interrupt();
		disconnector.join(TIMEOUT_MS);

		Assert.assertTrue(fakeClient.publishedTopics().contains(LWT_TOPIC),
				"Precondition: the death certificate must have been published, or there is nothing to confirm");
		Assert.assertFalse(finished,
				"disconnect(waitForLwt=true) returned within " + LWT_CONFIRMATION_BUDGET_MS + "ms with nothing "
						+ "acknowledging the death certificate. The confirmation wait is a no-op: it compares the "
						+ "token it published against lwtDeliveryToken, which detachSession()'s finally has already "
						+ "cleared, so it returns on its first iteration and logs a confirmation it never obtained");
	}

	/**
	 * ...and must stop waiting once the acknowledgement arrives.
	 *
	 * The other side of the test above, and the reason the fix binds the wait to the token rather than removing the
	 * clear that broke it: a wait that cannot observe the acknowledgement would spend keepAlive seconds on every
	 * graceful disconnect. Paho completes the token when the PUBACK lands, from its comms thread rather than the
	 * callback thread that dispatches deliveryComplete(), so the confirmation does not depend on a thread this
	 * class has been restructured to stop blocking.
	 *
	 * This one does not prove the wait happens - it passes against a wait that returns immediately, which is
	 * exactly what the test above is for. It proves the wait ends.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 4)
	public void aGracefulDisconnectStopsWaitingOnceTheDeathCertificateIsAcknowledged() throws Exception {
		wire(8, 8);
		configureLwt(1);

		// The acknowledgement the test above withholds
		fakeClient.ackOnSeparateThread(tahuClient);

		long start = System.currentTimeMillis();
		tahuClient.disconnect(0, 1, false, true, true);
		long elapsed = System.currentTimeMillis() - start;

		Assert.assertTrue(fakeClient.publishedTopics().contains(LWT_TOPIC),
				"Precondition: the death certificate must have been published");
		Assert.assertTrue(elapsed < LWT_CONFIRMATION_BUDGET_MS,
				"The disconnect took " + elapsed + "ms with the death certificate acknowledged. The wait must end "
						+ "on the acknowledgement rather than running its keepAlive-second budget out");
	}

	/**
	 * A correction to an unbalanced release must still run the replay it was about to run.
	 *
	 * releaseTeardownClaim() floors the count at zero when a caller unbalances it, and the replay is gated on the
	 * count reaching zero. Correcting the field but not the local leaves the gate reading a negative number that
	 * never equals zero, so a connect refused during a teardown stays armed with nothing left to fire it - the
	 * counter is repaired and the client stays offline, which is the outcome the deferral exists to prevent.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 2)
	public void aCorrectedUnbalancedReleaseStillRunsThePendingConnect() throws Exception {
		wire(8, 8);
		final AtomicInteger teardowns = (AtomicInteger) get(tahuClient, "teardownsInFlight");
		Assert.assertEquals(teardowns.get(), 0, "Precondition: no teardown is in flight");
		set(tahuClient, "deferredConnectPending", true);

		try {
			// One more release than there were claims - the case the correction exists for
			invoke(tahuClient, "releaseTeardownClaim");

			Assert.assertEquals(teardowns.get(), 0, "The count must be floored at zero, not left negative");
			awaitTrue(() -> {
				try {
					return get(tahuClient, "connectRunnable") != null;
				} catch (Exception e) {
					return false;
				}
			}, "The armed replay must still run - the count was corrected to zero, so the gate it feeds must see "
					+ "zero too");
		} finally {
			Object connectRunnable = get(tahuClient, "connectRunnable");
			if (connectRunnable != null) {
				invoke(connectRunnable, "stopConnectAttempts");
			}
			Thread connectThread = (Thread) get(tahuClient, "connectRunnableThread");
			if (connectThread != null) {
				connectThread.interrupt();
			}
		}
	}

	/**
	 * A connect that throws before it claims must not release a claim.
	 *
	 * connect() takes its teardown claim partway through a try whose catch releases one. Everything above the
	 * increment can throw - the timer reset and the in-progress gate both run first - and releasing there drives
	 * the count negative. The correction in releaseTeardownClaim() then resets it to zero, which is a claim
	 * another teardown may still be holding, and reaching zero also consumes any armed replay.
	 *
	 * Driven by removing the connect state, which is what the gate above the increment dereferences. Any throw on
	 * that stretch does the same thing; this is the one a test can produce.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void aConnectThatThrowsBeforeClaimingDoesNotReleaseAClaim() throws Exception {
		wire(8, 8);
		final AtomicInteger teardowns = (AtomicInteger) get(tahuClient, "teardownsInFlight");
		Object state = get(tahuClient, "state");
		set(tahuClient, "deferredConnectPending", true);

		try {
			set(tahuClient, "state", null);
			tahuClient.connect();

			Assert.assertEquals(teardowns.get(), 0, "The count must not have moved - this call never claimed");
			Assert.assertTrue((Boolean) get(tahuClient, "deferredConnectPending"),
					"A connect that failed before claiming released a claim it never took. The release drove the "
							+ "count negative, and reaching zero consumed a replay that is still waiting for the "
							+ "teardown which actually holds the gate");
		} finally {
			set(tahuClient, "state", state);
		}
	}

	/**
	 * connectComplete must not wait for an LWT acknowledgement it is itself preventing.
	 *
	 * On a client whose online state is false, connectComplete() publishes the death certificate rather than the
	 * BIRTH, and it asked for delivery to be confirmed. It runs on Paho's callback thread, and deliveryComplete()
	 * - the callback that clears the token and ends that wait - is dispatched by the same thread. So the wait can
	 * never be satisfied from here: it runs its full keepAlive * 4 quarter seconds - keepAlive seconds, 30 here -
	 * and returns having confirmed nothing, with clientLock held for all of it. Deterministic rather than a race.
	 *
	 * The rule was already recorded on birthRecoveryThread, for the same thread and the same reason. It was
	 * written for publishBirthMessage() and never applied to this arm of the same if.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 4)
	public void connectCompleteDoesNotWaitForAnLwtAcknowledgementOnTheCallbackThread() throws Exception {
		wire(8, 8);
		configureLwt(1);
		set(tahuClient, "onlineState", false);

		// Nothing acknowledges, so a wait can only run to its full budget
		fakeClient.shutdownAckThread();

		final CountDownLatch returned = new CountDownLatch(1);
		Thread callbackThread = new Thread(() -> {
			tahuClient.connectComplete(false, "tcp://localhost:1883");
			returned.countDown();
		}, "fake-paho-callback");
		callbackThread.setDaemon(true);
		callbackThread.start();

		try {
			Assert.assertTrue(returned.await(LWT_CONFIRMATION_BUDGET_MS, TimeUnit.MILLISECONDS),
					"connectComplete() has not returned after " + LWT_CONFIRMATION_BUDGET_MS + "ms. It is waiting "
							+ "for an LWT acknowledgement that arrives on the thread it is running on, so the wait "
							+ "can only time out - and it holds clientLock until it does");
			Assert.assertTrue(fakeClient.publishedTopics().contains(LWT_TOPIC),
					"The death certificate must still be published - only the wait for it is dropped");
		} finally {
			Object monitorThread = get(tahuClient, "connectionMonitorThread");
			if (monitorThread != null) {
				((Thread) monitorThread).interrupt();
			}
		}
	}

	/**
	 * A connect during a teardown must be refused, not raced.
	 *
	 * The teardown clears the client field before closing the Paho client, so for the length of that close -
	 * at least the unconditional Paho workaround sleep - isConnected() reports false while the old socket is still
	 * open. A poll loop that reconnects on that brings up a second session with the same client ID, and MQTT 3.1.1
	 * requires the server to disconnect the older one, publishing its Will: a retained offline STATE arriving after
	 * the new session's BIRTH.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void connectIsRefusedWhileADisconnectIsInProgress() throws Exception {
		wire(8, 8);
		final CountDownLatch inTeardown = new CountDownLatch(1);
		final CountDownLatch releaseTeardown = new CountDownLatch(1);

		fakeClient.duringDisconnectForcibly = () -> {
			inTeardown.countDown();
			try {
				releaseTeardown.await(2, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		};

		Thread disconnector = new Thread(() -> {
			try {
				tahuClient.disconnect(0, 1, false, false, false);
			} catch (Exception e) {
				// The assertions below report it
			}
		}, "disconnector");
		disconnector.setDaemon(true);
		disconnector.start();

		Assert.assertTrue(inTeardown.await(3, TimeUnit.SECONDS), "Precondition: the teardown must be in flight");

		try {
			tahuClient.connect();

			Assert.assertNull(get(tahuClient, "connectRunnableThread"),
					"A connect while the old session is still closing must be refused - two sessions with one client "
							+ "id makes the MQTT server drop the older one and publish its Will");
		} finally {
			releaseTeardown.countDown();
		}
	}

	/**
	 * A session being torn down has no use for a BIRTH, and the caller should not queue behind the teardown to
	 * learn it.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void aBirthIsNotPublishedWhileADisconnectIsInProgress() throws Exception {
		wire(8, 8);
		configureBirth();
		((AtomicInteger) get(tahuClient, "teardownsInFlight")).incrementAndGet();

		tahuClient.publishBirthMessage();

		Assert.assertEquals(fakeClient.publishedTopics(), List.of(),
				"A BIRTH must not be published into a session that is being torn down");
	}

	/**
	 * Another caller must not publish an LWT into a session that is already being torn down.
	 *
	 * The disconnect publishes the death certificate itself, through publishLwtNow(). This guard is on the public
	 * entry only, for callers arriving from elsewhere - TahuHostCallback corrects a mismatched STATE this way. It
	 * must not catch the teardown's own publish, which is what the LWT tests around disconnect() pin.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void anLwtFromAnotherCallerIsNotPublishedWhileADisconnectIsInProgress() throws Exception {
		wire(8, 8);
		configureLwt(1);
		((AtomicInteger) get(tahuClient, "teardownsInFlight")).incrementAndGet();

		tahuClient.publishLwt(true);

		Assert.assertEquals(fakeClient.publishedTopics(), List.of(),
				"A second death certificate must not go out while the disconnect is publishing its own");
	}

	/**
	 * A worker started for one session must not act on a later one.
	 *
	 * The wait is bounded but the clientLock that follows it is not, and a session can be lost and replaced in that
	 * gap - connectionLost reconnects synchronously. Re-validating only "some client is up" let a stale worker
	 * publish a second retained BIRTH onto a session that had already announced itself, or tear down a healthy one.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void aStaleRecoveryWorkerDoesNotActOnANewSession() throws Exception {
		wire(0, 8);
		configureBirth();

		// Starts a worker that will wait for the window, bound to this session
		tahuClient.publishBirthMessage();

		// The session it was started for goes away and a healthy one replaces it, mid wait
		FakeMqttClient newSession = new FakeMqttClient();
		set(tahuClient, "client", newSession);
		set(tahuClient, "semaphore", new Semaphore(8, true));

		Thread.sleep(BIRTH_WAIT_PLUS_SLACK);

		Assert.assertEquals(newSession.publishedTopics(), List.of(),
				"A stale worker must not publish a second BIRTH onto a session that already announced itself");
		Assert.assertFalse(newSession.disconnectForciblyCalled,
				"A stale worker must not tear down a healthy session it was never started for");
		Assert.assertEquals(callback.connectionLostCount.get(), 0,
				"No teardown happened, so recovery must not be re-armed");
	}

	/**
	 * A teardown scoped to one session must refuse to touch a different live one.
	 *
	 * The recovery worker checks identity under clientLock, then releases it before tearing down - so the session
	 * can change in between. This re-check is what makes that release safe, and without it the worker's own check
	 * only narrows the race rather than closing it.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void aTeardownScopedToAnOldSessionRefusesToTouchTheLiveOne() throws Exception {
		wire(8, 8);
		FakeMqttClient replacedSession = new FakeMqttClient();

		Method disconnectSession = TahuClient.class.getDeclaredMethod("disconnectSession", TahuMqttAsyncClient.class,
				long.class, long.class, boolean.class, boolean.class, boolean.class);
		disconnectSession.setAccessible(true);
		boolean tornDown =
				(Boolean) disconnectSession.invoke(tahuClient, replacedSession, 0L, 1L, false, false, false);

		Assert.assertFalse(tornDown, "A teardown for a session that is no longer live must report that it did none");
		Assert.assertFalse(fakeClient.disconnectForciblyCalled,
				"The live session must be left alone - it is not the one the caller meant to tear down");
		Assert.assertNotNull(get(tahuClient, "client"), "The live session must still be installed");
	}

	/**
	 * A worker left over from a dead session must not suppress a new session's recovery.
	 *
	 * The other half of binding recovery to a session. Tracking only "is a worker alive" makes the guard one
	 * recovery per thread rather than per session, so a stale worker still waiting on behalf of a session that has
	 * gone swallows the next session's genuine BIRTH failure with a debug line - and that session is then left
	 * announcing itself with no BIRTH on the wire, which is the defect all of this exists to prevent.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void aStaleWorkerDoesNotSuppressANewSessionsRecovery() throws Exception {
		wire(0, 8);
		configureBirth();

		// Worker for the first session, which will sit in its wait
		tahuClient.publishBirthMessage();

		// That session is replaced while the worker is still alive, and the new one is also under backpressure
		FakeMqttClient newSession = new FakeMqttClient();
		set(tahuClient, "client", newSession);
		set(tahuClient, "semaphore", new Semaphore(0, true));

		tahuClient.publishBirthMessage();

		awaitTrue(() -> newSession.disconnectForciblyCalled,
				"The new session's BIRTH failure must start its own recovery, not be dropped because a worker for a "
						+ "session that no longer exists happens to still be alive");
	}

	/**
	 * A reconnect must close the previous session with clientLock released.
	 *
	 * connect() tears the old session down before building a new one, and it used to do that from inside its own
	 * synchronized block. Monitors are reentrant, so a nested disconnect() exits its own block with the lock still
	 * held by the outer frame - which puts the Paho close back under the lock on the path taken before every
	 * reconnect, the most travelled path there is.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void aReconnectClosesThePreviousSessionOutsideClientLock() throws Exception {
		wire(8, 8);
		final Object clientLock = get(tahuClient, "clientLock");
		final CountDownLatch lockTaken = new CountDownLatch(1);
		final AtomicBoolean lockFreeDuringClose = new AtomicBoolean(false);

		fakeClient.duringDisconnectForcibly = () -> {
			Thread contender = new Thread(() -> {
				synchronized (clientLock) {
					lockTaken.countDown();
				}
			}, "lock-contender");
			contender.setDaemon(true);
			contender.start();
			try {
				lockFreeDuringClose.set(lockTaken.await(2, TimeUnit.SECONDS));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		};

		try {
			tahuClient.connect();

			Assert.assertTrue(lockFreeDuringClose.get(),
					"connect() held clientLock across the close of the previous session - the callback thread the "
							+ "close waits for needs that lock");
		} finally {
			// The connect attempt this starts is not a daemon, and there is no MQTT server for it to reach
			Object connectRunnable = get(tahuClient, "connectRunnable");
			if (connectRunnable != null) {
				invoke(connectRunnable, "stopConnectAttempts");
			}
			Thread connectThread = (Thread) get(tahuClient, "connectRunnableThread");
			if (connectThread != null) {
				connectThread.interrupt();
			}
		}
	}

	/**
	 * The re-arm must fire even when the close throws.
	 *
	 * The session is detached - monitor stopped, connect runnable stopped, client field cleared - before anything
	 * that can throw runs, so a close that fails still leaves the session destroyed. Treating the throw as "not torn
	 * down" skipped the notification and left the host permanently offline, which is the failure the re-arm exists to
	 * prevent. close() throwing is reachable: it is the Paho race the unconditional sleep works around.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void theReArmFiresEvenWhenTheCloseThrows() throws Exception {
		wire(0, 8);
		configureBirth();
		fakeClient.closeThrows = true;

		tahuClient.publishBirthMessage();

		awaitTrue(() -> callback.connectionLostCount.get() == 1,
				"A close that throws still destroyed the session, so recovery must still be re-armed");
		Assert.assertNull(get(tahuClient, "client"), "The session must be gone whether or not the close threw");
	}

	/**
	 * A second BIRTH after a teardown must not throw.
	 *
	 * The guard was birthTopic != null && client.isConnected(), with no null check on the client. The previous
	 * escalation left the field pointing at a closed Paho client, so the guard read false and a second call was a
	 * harmless no-op; disconnect() clears the field instead, so the same call dereferenced null while holding
	 * clientLock. HostApplication.setOnlineState() iterates every client, so one dead client took the rest with it.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void aBirthAfterTheSessionIsTornDownIsANoOp() throws Exception {
		wire(0, 8);
		configureBirth();
		tahuClient.publishBirthMessage();
		awaitTrue(() -> fakeClient.disconnectForciblyCalled, "Precondition: the session must be torn down first");
		awaitTrue(() -> {
			try {
				return get(tahuClient, "client") == null;
			} catch (Exception e) {
				return false;
			}
		}, "Precondition: disconnect() must have cleared the client field");

		tahuClient.publishBirthMessage();

		Assert.assertEquals(fakeClient.publishedTopics(), List.of(),
				"With no client there is nothing to publish and nothing to throw");
	}

	/**
	 * The BIRTH retry must not run on the thread that would satisfy it.
	 *
	 * Paho dispatches deliveryComplete(), messageArrived() and connectComplete() from one callback thread, and two
	 * of the three routes into publishBirthMessage() arrive on it. A wait taken there blocks the only thread that
	 * could release the permit being waited for, so it always runs to its deadline - 500ms of frozen callback
	 * dispatch, all of it holding clientLock, before escalating exactly as it would have anyway.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void theBirthRetryDoesNotBlockTheCallingThread() throws Exception {
		wire(0, 8);
		configureBirth();

		long start = System.currentTimeMillis();
		tahuClient.publishBirthMessage();
		long elapsed = System.currentTimeMillis() - start;

		Assert.assertTrue(elapsed < 250, "publishBirthMessage() blocked its caller for " + elapsed + "ms - on the "
				+ "production callers that is Paho's callback thread, which is what would clear the backpressure");
	}

	/** With a permit free, the BIRTH goes out inline and the session stands. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void birthIsPublishedInlineWhenTheWindowHasRoom() throws Exception {
		wire(8, 8);
		configureBirth();

		tahuClient.publishBirthMessage();

		Assert.assertEquals(fakeClient.publishedTopics(), List.of(BIRTH_TOPIC), "The BIRTH must go out inline");
		Assert.assertFalse(fakeClient.disconnectForciblyCalled, "A published BIRTH is not a reason to reconnect");
	}

	/**
	 * A drain that has been retired must not re-queue into the next session's buffer.
	 *
	 * publishWithPermit() throws from inside a synchronized (publishOrderLock) block that the drain's catch sits
	 * outside of, so the monitor is released and re-acquired between the failed send and the re-queue. A disconnect
	 * can win it in that gap: the buffer is discarded, this drain retired, and connect() starts another - and the
	 * old drain then hands its message to the new session's drain, at the head of the queue.
	 *
	 * Measured before the fix, driving exactly that state: the stranded entry sat silently (no notify), then went
	 * out first on the new session - [spBv1.0/STATE/host-1, spBv1.0/G1/NDATA/E1]. A retained STATE online:false
	 * with the previous session's timestamp, landing on a live session ahead of its BIRTH.
	 *
	 * Driven directly rather than by racing the two threads: the window is real but narrow - 2 hits in 400
	 * iterations - and a test that reproduces it 0.5% of the time guards nothing.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void aRetiredDrainDoesNotRequeueIntoTheNextSession() throws Exception {
		wire(0, 8);
		tahuClient.publish("topic/old-session", "x".getBytes(), 1, false);

		// What the drain still holds on its stack when publishWithPermit() throws
		Class<?> bufferedPublishClass = Class.forName("org.eclipse.tahu.mqtt.TahuClient$BufferedPublish");
		Constructor<?> ctor =
				bufferedPublishClass.getDeclaredConstructor(String.class, byte[].class, int.class, boolean.class);
		ctor.setAccessible(true);
		Object attempted = ctor.newInstance(LWT_TOPIC, "death".getBytes(), 1, true);
		Object retiredDrain = get(tahuClient, "publishBufferDrain");

		// The disconnect that wins the monitor in the gap, then the next session
		invoke(tahuClient, "shutdownPublishBufferDrainThread");
		fakeClient.markConnected();
		wire(8, 8);
		Assert.assertNotSame(get(tahuClient, "publishBufferDrain"), retiredDrain,
				"Precondition: the new session has a different drain");
		long discardedBefore = tahuClient.getPublishBufferDiscardedMessageCount();

		Method requeueOrDrop = TahuClient.class.getDeclaredMethod("requeueOrDrop", bufferedPublishClass,
				Throwable.class, Class.forName("org.eclipse.tahu.mqtt.TahuClient$PublishBufferDrain"));
		requeueOrDrop.setAccessible(true);
		Object delay = requeueOrDrop.invoke(tahuClient, attempted, new RuntimeException("send failed"), retiredDrain);

		Assert.assertEquals(delay, 0L, "A retired drain must be told to stop retrying, not to wait and try again");
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0,
				"A dead session's message must not be queued for the next one");
		Assert.assertEquals(tahuClient.getPublishBufferDiscardedMessageCount(), discardedBefore + 1,
				"The message is lost data and must be counted, not dropped quietly");

		tahuClient.publish("spBv1.0/G1/NDATA/E1", "live".getBytes(), 1, false);
		awaitBufferDepth(0);
		Assert.assertEquals(fakeClient.publishedTopics(), List.of("spBv1.0/G1/NDATA/E1"),
				"Only the new session's own traffic may reach the MQTT server");
	}

	/**
	 * The pre-LWT drain wait must give up as soon as it cannot succeed.
	 *
	 * The drain declines to publish while the client is disconnected, so with the socket down the buffer cannot
	 * empty and the poll ran to its full deadline - then publishLwt() skipped the LWT anyway, because the client was
	 * not connected. That is the state after a server stalls and drops the link, which is exactly the state
	 * connect() disconnects from before every reconnect attempt, and all of it is spent holding clientLock.
	 *
	 * Measured: 2009ms of dead time with nothing published, against 1ms once the wait checks.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void theLwtDrainWaitGivesUpWhenTheClientCannotPublish() throws Exception {
		wire(0, 8);
		for (int i = 0; i < 3; i++) {
			tahuClient.publish("topic/queued-" + i, "x".getBytes(), 1, false);
		}
		fakeClient.markDisconnected();

		Method await = TahuClient.class.getDeclaredMethod("awaitPublishBufferDrained", long.class);
		await.setAccessible(true);
		long start = System.currentTimeMillis();
		await.invoke(tahuClient, 2000L);
		long elapsed = System.currentTimeMillis() - start;

		Assert.assertTrue(elapsed < 500, "Waited " + elapsed + "ms for a buffer that cannot drain - the drain will "
				+ "not publish while the client is disconnected, so this wait can only ever reach its deadline");
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 3, "Precondition: nothing could have drained");
	}

	/**
	 * A still-connected disconnect gets its bounded flush even when it publishes no LWT.
	 *
	 * The futile-wait fix is the isConnected() guard, covered by the test above. Scoping the wait to publishLwt as
	 * well removed the flush window from every caller passing false - EdgeClient.handleStateMessage() does exactly
	 * that, on any offline STATE for the configured primary host, which a peer can publish. The drain shutdown below
	 * discards the buffer unconditionally, and publish() returning null had told those callers their messages were
	 * queued rather than lost, so there is no way for them to know or retry.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 2)
	public void aConnectedDisconnectWithNoLwtStillFlushesTheBuffer() throws Exception {
		wire(0, 8);
		for (int i = 0; i < 3; i++) {
			tahuClient.publish("topic/queued-" + i, "x".getBytes(), 1, false);
		}
		awaitBufferDepth(3);

		/*
		 * Released only after disconnect() has started, so the flush can only happen inside the wait. Releasing them
		 * first lets the drain publish everything before disconnect() is even entered, and the assertions below are
		 * then satisfied without the wait running at all - measured at 3 vacuous passes in 12 runs against a build
		 * with the wait scoped back under publishLwt.
		 */
		Thread releaser = new Thread(() -> {
			try {
				Thread.sleep(200);
				releasePermits(3);
			} catch (Exception e) {
				// The assertions below report it
			}
		}, "late-permit-flush");
		releaser.setDaemon(true);
		releaser.start();

		tahuClient.disconnect(0, 1, false, false, false);

		Assert.assertEquals(tahuClient.getPublishBufferDiscardedMessageCount(), 0,
				"Accepted messages must get their bounded flush before the drain shutdown discards the buffer");
		Assert.assertEquals(fakeClient.publishedTopics().size(), 3,
				"All three queued messages must reach the MQTT server, not be discarded unsent");
	}

	/**
	 * A death certificate downgraded to QoS 0 must not buy a clean DISCONNECT.
	 *
	 * The QoS 0 fallback returns Paho's token, which says the message was accepted locally - not that the MQTT
	 * server received it. There is no PUBACK, and this path is only reached when that server is already not
	 * acknowledging. Treating it as success sent a clean DISCONNECT, which tells the server to suppress the Will it
	 * has held since connect() - spending the last remaining chance to protect a publish that carries no guarantee.
	 *
	 * The Will is registered at QoS 1 and retained, from the same payload and timestamp, so it is strictly the
	 * stronger of the two. Sparkplug requires a Host Application's STATE death at QoS 1, which the downgrade is not.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void aDowngradedLwtDoesNotSuppressTheWill() throws Exception {
		wire(0, 8);
		configureLwt(1);

		tahuClient.disconnect(0, 1, true, true, false);

		Assert.assertEquals(fakeClient.publishedTopics(), List.of(LWT_TOPIC),
				"The QoS 0 fallback must still go out - it is free and may arrive");
		Assert.assertFalse(fakeClient.disconnectSent,
				"A downgraded LWT is not acknowledged, so the DISCONNECT must be suppressed and the QoS 1 Will "
						+ "allowed to fire");
		Assert.assertTrue(fakeClient.disconnectForciblyCalled, "The disconnect must still complete");
	}

	/** The converse: an LWT that went out at its configured QoS is acknowledged, so the clean DISCONNECT stands. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void anAcknowledgedLwtStillSendsTheDisconnect() throws Exception {
		wire(8, 8);
		configureLwt(1);

		tahuClient.disconnect(0, 1, true, true, false);

		Assert.assertEquals(fakeClient.publishedTopics(), List.of(LWT_TOPIC));
		Assert.assertTrue(fakeClient.disconnectSent,
				"With the death certificate acknowledged there is nothing for the Will to add, and a clean "
						+ "DISCONNECT is the correct close");
	}

	/**
	 * Backpressure that clears must not cost the session.
	 *
	 * An exhausted window or a queued message is the ordinary condition the publish buffer exists to absorb, and it
	 * clears on the next acknowledgement. Testing once and giving up meant one QoS 1 message queued for
	 * milliseconds forcibly disconnected the client - measured: permits=0 depth=1 in, connected=false out, with the
	 * buffer and its drain thread left live and nothing published.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void transientBackpressureDoesNotCostTheSession() throws Exception {
		wire(0, 8);
		configureBirth();
		tahuClient.publish("topic/one-queued", "x".getBytes(), 1, false);

		// The acknowledgement that was always going to arrive, a moment later
		Thread releaser = new Thread(() -> {
			try {
				Thread.sleep(100);
				startDrain();
				releasePermits(8);
			} catch (Exception e) {
				// The assertions below report it
			}
		}, "late-permit");
		releaser.setDaemon(true);
		releaser.start();

		tahuClient.setOnlineState(true);

		awaitTrue(() -> fakeClient.publishedTopics().contains(BIRTH_TOPIC),
				"The BIRTH must go out once the window opens, not tear the session down while it is closed");
		Assert.assertFalse(fakeClient.disconnectForciblyCalled,
				"Backpressure that clears within the budget is not a reason to drop the connection");
		Assert.assertEquals(callback.connectionLostCount.get(), 0,
				"A session that recovered was never torn down, so recovery must not be re-armed");
	}

	/**
	 * Backpressure that does not clear still refuses the BIRTH - and tears the session down cleanly.
	 *
	 * The escalation used to drop the socket and leave everything else: buffer intact, drain thread still
	 * registered, client field still pointing at a closed Paho client.
	 *
	 * The teardown is what recovery has to be re-armed after, not what performs it. disconnect() stops the
	 * connection monitor, so nothing is left to observe the cleared client field - the monitor holds its own final
	 * reference captured at construction and never reads this one. theBirthTeardownReArmsTheConnectionLostCallback
	 * covers the mechanism that does bring the client back.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void persistentBackpressureRefusesTheBirthAndTearsDown() throws Exception {
		wire(0, 8);
		configureBirth();
		tahuClient.publish("topic/stuck", "x".getBytes(), 1, false);

		tahuClient.setOnlineState(true);

		awaitTrue(() -> fakeClient.disconnectForciblyCalled, "The session must not be left announcing itself");
		Assert.assertFalse(fakeClient.publishedTopics().contains(BIRTH_TOPIC),
				"No BIRTH reached the MQTT server, so none may be reported as sent");
		Assert.assertNull(get(tahuClient, "publishBufferDrain"), "The drain thread must not outlive the session");
		Assert.assertNull(get(tahuClient, "client"), "The session is gone, so the client field must not outlive it");
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0, "The buffer must not outlive the session");
		Assert.assertEquals(tahuClient.getPublishBufferDiscardedMessageCount(), 1,
				"What was queued is lost data and must be counted, not silently dropped");
	}

	// ------------------------------------------------------------------------------------------------------------
	// Byte capacity
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * The buffer is bounded by bytes as well as by message count.
	 *
	 * A count alone does not bound heap, which is the dimension that OOMs: payloads are held by reference and sized
	 * by the caller, so the default 10,000 messages was 10,000 x an unknown. Measured before this bound existed, 100
	 * x 1MB QoS 1 publishes were admitted without complaint and retained ~200MB.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void bufferRejectsAtItsByteCapacity() throws Exception {
		wire(0, 8);
		tahuClient.setPublishBufferByteCapacity(1000);

		tahuClient.publish("topic/a", new byte[400], 1, false);
		tahuClient.publish("topic/b", new byte[400], 1, false);
		Assert.assertEquals(tahuClient.getPublishBufferBytes(), 800, "Precondition: both are queued and accounted");

		try {
			tahuClient.publish("topic/c", new byte[400], 1, false);
			Assert.fail("A publish past the byte budget must throw so the caller can store the message");
		} catch (TahuException e) {
			Assert.assertTrue(e.getMessage().contains("byte capacity"), "Unexpected message: " + e.getMessage());
		}

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 2, "The rejected message must not be buffered");
		Assert.assertTrue(tahuClient.getPublishBufferDepth() < tahuClient.getPublishBufferCapacity(),
				"The count bound must not be what rejected it - that would prove nothing about the byte bound");
		Assert.assertEquals(tahuClient.getPublishBufferBytes(), 800, "A rejected payload must not be accounted for");
		Assert.assertEquals(tahuClient.getPublishBufferRejectedMessageCount(), 1,
				"A byte-budget refusal is a rejection: the caller was told and can still store it");
	}

	/** One payload larger than the whole budget is refused on its own terms, not as a full buffer. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void payloadLargerThanTheWholeBudgetIsRejectedOutright() throws Exception {
		wire(0, 8);
		tahuClient.setPublishBufferByteCapacity(1000);

		try {
			tahuClient.publish("topic/huge", new byte[2000], 1, false);
			Assert.fail("A payload that can never fit must be rejected rather than queued");
		} catch (TahuException e) {
			Assert.assertTrue(e.getMessage().contains("exceeds the whole publish buffer budget"),
					"The message must name the payload, not the buffer state: " + e.getMessage());
		}

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0, "Nothing was queued");
		Assert.assertEquals(tahuClient.getPublishBufferBytes(), 0, "Nothing was accounted for");
	}

	/**
	 * The byte total tracks the buffer exactly, including across a failed send.
	 *
	 * This is the invariant the whole bound rests on. A total that drifts above the truth does not fail where it
	 * drifted - it fails later, as a client refusing every publish against a buffer that looks empty, which is the
	 * same shape as an in-flight permit drifting from Paho's window. The requeue path is where drift would start,
	 * since a failed send removes an entry from the head and puts it back.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void bufferBytesReturnToZeroAcrossAFailedSend() throws Exception {
		wire(0, 8);
		tahuClient.publish("topic/first", new byte[100], 1, false);
		tahuClient.publish("topic/second", new byte[100], 1, false);
		Assert.assertEquals(tahuClient.getPublishBufferBytes(), 200);

		fakeClient.failNextPublishes(1);
		releasePermits(8);
		awaitBufferDepth(0);

		Assert.assertEquals(tahuClient.getPublishBufferBytes(), 0,
				"An empty buffer must account for zero bytes - a failed send removes and re-adds its message");
	}

	/** The disconnect discard clears the byte total with the messages it throws away. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void discardingTheBufferClearsTheByteTotal() throws Exception {
		wire(0, 8);
		tahuClient.publish("topic/a", new byte[100], 1, false);
		tahuClient.publish("topic/b", new byte[100], 1, false);
		Assert.assertEquals(tahuClient.getPublishBufferBytes(), 200, "Precondition: both are accounted for");

		invoke(tahuClient, "shutdownPublishBufferDrainThread");

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0);
		Assert.assertEquals(tahuClient.getPublishBufferBytes(), 0,
				"A discarded buffer must release its byte budget, or the client refuses publishes forever after");
	}

	// ------------------------------------------------------------------------------------------------------------
	// Async publishes
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * A retrying async publish must occupy one buffer slot, however many attempts its retry budget allows.
	 *
	 * The retry loop had no success break, and handlePublish() could not tell it there had been one - it discarded
	 * publishOrBuffer()'s return and swallowed the exception. Under backpressure the first attempt buffered the
	 * message and every later attempt appended another copy of it, so numAttempts copies of one BIRTH or DATA
	 * message were delivered when permits returned. Identical copies, same payload, so no subscriber can tell them
	 * apart from a real rebirth.
	 *
	 * Not reachable before this branch: the first attempt blocked in semaphore.acquire() and never returned to the
	 * loop - the deadlock this work removed was hiding the missing break.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void asyncPublishWithRetryBuffersOneCopyOfTheMessage() throws Exception {
		wire(0, 8);

		tahuClient.asyncPublish("topic/async", "x".getBytes(), 1, false, true, 20, 3);
		Thread.sleep(400);

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1,
				"One logical message must occupy one buffer slot, however many retry attempts it was given");
	}

	/**
	 * A rejected async publish is still retried, since that is the outcome retrying exists for.
	 *
	 * The buffer is full, so nothing holds the message and each attempt is a genuine new one. This is what stops the
	 * success break above from turning "retry" into "try once".
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void asyncPublishRetriesWhenTheBufferRejectsIt() throws Exception {
		wire(0, 8);
		tahuClient.setPublishBufferCapacity(1);
		tahuClient.publish("topic/filler", "x".getBytes(), 1, false);
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1, "Precondition: the buffer is at capacity");

		tahuClient.asyncPublish("topic/async", "x".getBytes(), 1, false, true, 20, 3);
		Thread.sleep(400);

		Assert.assertEquals(tahuClient.getPublishBufferRejectedMessageCount(), 3,
				"Every attempt against a full buffer is a real attempt and must be counted as a rejection");
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1, "A rejected message must not be buffered");
	}

	// ------------------------------------------------------------------------------------------------------------
	// Harness
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * Puts the client into the state connect() would leave it in, but with a fake Paho client and a chosen number of
	 * free permits. The drain thread is NOT started - tests that need it call startDrain() so they control timing.
	 */
	/**
	 * Stands in for connect(): a live session, and a drain thread that owns the buffer.
	 *
	 * The drain is started here rather than left to each test because connect() starts it before the Paho client
	 * exists, so a client that reports connected always has one. bufferPublish() now refuses a message when it does
	 * not - see publishIsRefusedWhenNoDrainOwnsTheBuffer - and a harness that buffered without one was modelling a
	 * state the client cannot be in. Tests that want the buffer to sit still wire 0 permits, which keeps the drain
	 * parked on its acquire.
	 */
	private void wire(int availablePermits, int maxInflight) throws Exception {
		set(tahuClient, "client", fakeClient);
		set(tahuClient, "semaphore", new Semaphore(availablePermits, true));
		set(tahuClient, "lockedMessageSet", ConcurrentHashMap.newKeySet());
		set(tahuClient, "maxInFlightMessages", maxInflight);
		startDrain();
	}

	private static final String LWT_TOPIC = "spBv1.0/G1/NDEATH/E1";
	private static final String BIRTH_TOPIC = "spBv1.0/STATE/host-1";

	private void configureLwt(int qos) throws Exception {
		set(tahuClient, "lwtTopic", LWT_TOPIC);
		set(tahuClient, "lwtPayload", "death".getBytes());
		set(tahuClient, "lwtQoS", qos);
		set(tahuClient, "lwtRetain", false);
	}

	private void configureBirth() throws Exception {
		set(tahuClient, "birthTopic", BIRTH_TOPIC);
		set(tahuClient, "birthPayload", "birth".getBytes());
		set(tahuClient, "birthRetain", false);
		set(tahuClient, "useSparkplugStatePayload", false);
	}

	private void startDrain() throws Exception {
		invoke(tahuClient, "startPublishBufferDrainThread");
	}

	private void releasePermits(int count) throws Exception {
		semaphore().release(count);
	}

	private int availablePermits() throws Exception {
		return semaphore().availablePermits();
	}

	private Semaphore semaphore() throws Exception {
		return (Semaphore) get(tahuClient, "semaphore");
	}

	private void awaitPermits(int expected, long timeoutMs) throws Exception {
		long deadline = System.currentTimeMillis() + timeoutMs;
		while (System.currentTimeMillis() < deadline) {
			if (availablePermits() == expected) {
				return;
			}
			Thread.sleep(10);
		}
		Assert.fail("Permits never returned to " + expected + " (currently " + availablePermits() + ")");
	}

	private void awaitBufferDepth(int expected) throws Exception {
		awaitBufferDepth(expected, TIMEOUT_MS - 500);
	}

	private void awaitBufferDepth(int expected, long timeoutMs) throws Exception {
		long deadline = System.currentTimeMillis() + timeoutMs;
		while (System.currentTimeMillis() < deadline) {
			if (tahuClient.getPublishBufferDepth() == expected) {
				// Let the final in-flight send settle before asserting on what was published
				Thread.sleep(50);
				if (tahuClient.getPublishBufferDepth() == expected) {
					return;
				}
			}
			Thread.sleep(10);
		}
		Assert.fail("Buffer depth never reached " + expected + " (currently " + tahuClient.getPublishBufferDepth() + ")");
	}

	/** Full dump with monitor ownership, plus explicit deadlock detection, for when the stress test wedges. */
	private static String dumpThreads() {
		StringBuilder sb = new StringBuilder();
		ThreadMXBean mx = ManagementFactory.getThreadMXBean();

		long[] deadlocked = mx.findDeadlockedThreads();
		sb.append("=== deadlocked threads: ").append(deadlocked == null ? "none" : deadlocked.length).append("\n");

		for (ThreadInfo info : mx.dumpAllThreads(true, true)) {
			String name = info.getThreadName();
			if (!name.startsWith("stress-") && !name.startsWith("Tahu") && !name.startsWith("test-")
					&& !name.startsWith("lock-") && !name.startsWith("disconnector")) {
				continue;
			}
			sb.append("\n--- ").append(name).append(" [").append(info.getThreadState()).append("]");
			if (info.getLockName() != null) {
				sb.append("\n    waiting on ").append(info.getLockName());
			}
			if (info.getLockOwnerName() != null) {
				sb.append("\n    held by ").append(info.getLockOwnerName());
			}
			for (MonitorInfo m : info.getLockedMonitors()) {
				sb.append("\n    owns ").append(m).append(" at ").append(m.getLockedStackFrame());
			}
			StackTraceElement[] st = info.getStackTrace();
			for (int i = 0; i < Math.min(st.length, 12); i++) {
				sb.append("\n      at ").append(st[i]);
			}
		}
		return sb.toString();
	}

	private static void setInProgress(TahuClient client, boolean inProgress) throws Exception {
		Object state = get(client, "state");
		Method setter = state.getClass().getDeclaredMethod("setInProgress", boolean.class);
		setter.setAccessible(true);
		setter.invoke(state, inProgress);
	}

	private static boolean state(TahuClient client) throws Exception {
		Object state = get(client, "state");
		Method inProgress = state.getClass().getDeclaredMethod("inProgress");
		inProgress.setAccessible(true);
		return (Boolean) inProgress.invoke(state);
	}

	private static TahuClient newTahuClient(ClientCallback clientCallback) throws Exception {
		return new TahuClient(new MqttClientId("test-client", false), new MqttServerName("test-server"),
				new MqttServerUrl("tcp://localhost:1883"), null, null, true, 30, clientCallback, null, false);
	}

	/**
	 * Polls for a condition the BIRTH recovery worker satisfies asynchronously.
	 *
	 * The retry and the teardown moved off the calling thread, so publishBirthMessage() returns before either has
	 * happened. Asserting immediately after it would test the handoff rather than the outcome.
	 */
	private void awaitFalse(BooleanSupplier condition, String message) throws Exception {
		awaitTrue(() -> !condition.getAsBoolean(), message);
	}

	private void awaitTrue(BooleanSupplier condition, String message) throws Exception {
		long deadline = System.currentTimeMillis() + TIMEOUT_MS - 500;
		while (System.currentTimeMillis() < deadline) {
			if (condition.getAsBoolean()) {
				return;
			}
			Thread.sleep(10);
		}
		Assert.fail(message);
	}

	private static void set(Object target, String fieldName, Object value) throws Exception {
		Field field = findField(target.getClass(), fieldName);
		field.setAccessible(true);
		field.set(target, value);
	}

	private static Object get(Object target, String fieldName) throws Exception {
		Field field = findField(target.getClass(), fieldName);
		field.setAccessible(true);
		return field.get(target);
	}

	private static void invoke(Object target, String methodName) throws Exception {
		Method method = target.getClass().getDeclaredMethod(methodName);
		method.setAccessible(true);
		method.invoke(target);
	}

	private static Field findField(Class<?> type, String name) throws NoSuchFieldException {
		for (Class<?> c = type; c != null; c = c.getSuperclass()) {
			try {
				return c.getDeclaredField(name);
			} catch (NoSuchFieldException ignored) {
				// keep walking up
			}
		}
		throw new NoSuchFieldException(name);
	}

	/**
	 * A Paho client that records what it was asked to publish and can be told to fail on demand. Nothing touches a
	 * network.
	 */
	private static class FakeMqttClient extends TahuMqttAsyncClient {

		private final List<String> published = new ArrayList<>();
		private final AtomicInteger attempts = new AtomicInteger();
		private final AtomicInteger failuresRemaining = new AtomicInteger();
		private final AtomicBoolean connected = new AtomicBoolean(true);
		private final AtomicInteger nextMessageId = new AtomicInteger(1);
		private volatile long publishDelayMs;
		private volatile TahuClient ackTarget;
		private volatile ExecutorService ackExecutor;
		private volatile boolean disconnectSent;
		private volatile boolean disconnectForciblyCalled;
		private volatile Runnable duringDisconnectForcibly;
		private volatile boolean closeThrows;

		private FakeMqttClient() throws MqttException {
			super("tcp://localhost:1883", "test-client", null);
		}

		/**
		 * Makes every successful publish be ACKed asynchronously, mirroring the MQTT server plus Paho's callback
		 * thread. Deliberately on another thread: deliveryComplete() must take messageLock while the publisher may
		 * still hold publishOrderLock -> messageLock.
		 */
		private void ackOnSeparateThread(TahuClient target) {
			this.ackExecutor = Executors.newSingleThreadExecutor(runnable -> {
				Thread thread = new Thread(runnable, "fake-broker-ack");
				thread.setDaemon(true);
				return thread;
			});
			this.ackTarget = target;
		}

		private void shutdownAckThread() {
			ExecutorService executor = ackExecutor;
			ackTarget = null;
			if (executor != null) {
				executor.shutdownNow();
			}
		}

		/* Brings the fake back up, so a test can model a genuinely live next session. */
		private void markConnected() {
			connected.set(true);
		}

		private void markDisconnected() {
			connected.set(false);
		}

		/* Holds each send open, so a test can observe what a caller sees while the drain is mid-publish. */
		private void setPublishDelay(long millis) {
			publishDelayMs = millis;
		}

		private void failNextPublishes(int count) {
			failuresRemaining.set(count);
		}

		private synchronized List<String> publishedTopics() {
			return new ArrayList<>(published);
		}

		private int publishAttempts() {
			return attempts.get();
		}

		@Override
		public boolean isConnected() {
			return connected.get();
		}

		/*
		 * Recorded rather than performed. The real methods would throw on a client that never connected, and the
		 * sendDisconnectPacket flag is the observable for the Will escalation.
		 */
		@Override
		public void disconnectForcibly(long quiesceTimeout, long disconnectTimeout, boolean sendDisconnectPacket) {
			this.disconnectSent = sendDisconnectPacket;
			this.disconnectForciblyCalled = true;
			connected.set(false);

			/*
			 * Stands in for CommsCallback.stop(), which spin waits for Paho's callback thread whenever it is called
			 * from any other thread. Set by the test that checks clientLock is free while this runs.
			 */
			Runnable hook = duringDisconnectForcibly;
			if (hook != null) {
				hook.run();
			}
		}

		@Override
		public void close() throws MqttException {
			if (closeThrows) {
				// What Paho raises when the forced shutdown has not finished - paho.mqtt.java#850
				throw new MqttException(MqttException.REASON_CODE_CLIENT_CONNECTED);
			}
		}

		@Override
		public IMqttDeliveryToken publish(String topic, byte[] payload, int qos, boolean retained)
				throws MqttException {
			attempts.incrementAndGet();
			long delay = publishDelayMs;
			if (delay > 0) {
				try {
					Thread.sleep(delay);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new MqttException(e);
				}
			}
			if (failuresRemaining.get() > 0) {
				failuresRemaining.decrementAndGet();
				// 32202 == REASON_CODE_MAX_INFLIGHT, the realistic transient failure
				throw new MqttException(32202);
			}
			synchronized (this) {
				published.add(topic);
			}

			FakeDeliveryToken token = new FakeDeliveryToken(nextMessageId.getAndIncrement());
			if (qos == 0) {
				// There is no PUBACK for QoS 0, so Paho completes the token as soon as it is written
				token.markComplete();
			}
			TahuClient target = ackTarget;
			ExecutorService executor = ackExecutor;
			if (target != null && executor != null && qos > 0 && !executor.isShutdown()) {
				try {
					executor.submit(() -> {
						// Completed before the callback, as Paho does: the PUBACK is what both are made of
						token.markComplete();
						target.deliveryComplete(token);
					});
				} catch (RejectedExecutionException ignored) {
					// shutting down
				}
			}
			return token;
		}
	}

	/**
	 * Minimal IMqttDeliveryToken - getMessageId() and completion are what TahuClient reads.
	 *
	 * isComplete() used to answer true unconditionally, which was harmless while nothing asked. awaitLwtDelivery()
	 * now asks it, so a token that always claims completion would make any test of that wait vacuous - it would
	 * pass against a wait that never waits, which is the defect the wait was found to have. Completion mirrors
	 * Paho: a QoS 0 publish completes on write, a QoS 1 publish when its acknowledgement is dispatched.
	 */
	private static class FakeDeliveryToken implements IMqttDeliveryToken {

		private final int messageId;
		private volatile boolean complete;
		private Object userContext;
		private IMqttActionListener actionCallback;

		private FakeDeliveryToken(int messageId) {
			this.messageId = messageId;
		}

		private void markComplete() {
			this.complete = true;
		}

		@Override
		public int getMessageId() {
			return messageId;
		}

		@Override
		public MqttMessage getMessage() {
			return null;
		}

		@Override
		public void waitForCompletion() {
		}

		@Override
		public void waitForCompletion(long timeout) {
		}

		@Override
		public boolean isComplete() {
			return complete;
		}

		@Override
		public MqttException getException() {
			return null;
		}

		@Override
		public void setActionCallback(IMqttActionListener listener) {
			this.actionCallback = listener;
		}

		@Override
		public IMqttActionListener getActionCallback() {
			return actionCallback;
		}

		@Override
		public IMqttAsyncClient getClient() {
			return null;
		}

		@Override
		public String[] getTopics() {
			return new String[0];
		}

		@Override
		public void setUserContext(Object userContext) {
			this.userContext = userContext;
		}

		@Override
		public Object getUserContext() {
			return userContext;
		}

		@Override
		public int[] getGrantedQos() {
			return new int[0];
		}

		@Override
		public boolean getSessionPresent() {
			return false;
		}

		@Override
		public MqttWireMessage getResponse() {
			return null;
		}
	}

	/** Counts the connectionLost re-arm the BIRTH teardown fires, since a local disconnect produces none from Paho. */
	private static class RecordingCallback extends NoOpCallback {

		private final AtomicInteger connectionLostCount = new AtomicInteger();
		private final AtomicReference<Throwable> lastCause = new AtomicReference<>();

		@Override
		public void connectionLost(MqttServerName mqttServerName, MqttServerUrl mqttServerUrl, MqttClientId clientId,
				Throwable cause) {
			lastCause.set(cause);
			connectionLostCount.incrementAndGet();
		}
	}

	private static class NoOpCallback implements ClientCallback {

		@Override
		public void shutdown() {
		}

		@Override
		public void messageArrived(MqttServerName mqttServerName, MqttServerUrl mqttServerUrl, MqttClientId clientId,
				String rawTopic, MqttMessage message) {
		}

		@Override
		public void connectionLost(MqttServerName mqttServerName, MqttServerUrl mqttServerUrl, MqttClientId clientId,
				Throwable cause) {
		}

		@Override
		public void connectComplete(boolean reconnect, MqttServerName mqttServerName, MqttServerUrl mqttServerUrl,
				MqttClientId clientId) {
		}
	}
}
