/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.procedure;

import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.ProcedureLockState;
import org.apache.iotdb.confignode.procedure.state.ProcedureState;
import org.apache.iotdb.confignode.procedure.store.IProcedureStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstract class of all procedures.
 *
 * @param <Env>
 */
public abstract class Procedure<Env> implements Comparable<Procedure<Env>> {
  private static final Logger LOG = LoggerFactory.getLogger(Procedure.class);
  public static final long NO_PROC_ID = -1;
  public static final long NO_TIMEOUT = -1;

  private long parentProcId = NO_PROC_ID;
  private long rootProcId = NO_PROC_ID;
  private long procId = NO_PROC_ID;
  private long submittedTime;

  private ProcedureState state = ProcedureState.INITIALIZING;
  private int childrenLatch = 0;
  private ProcedureException exception;

  private volatile long timeout = NO_TIMEOUT;
  private volatile long lastUpdate;

  private final AtomicReference<byte[]> result = new AtomicReference<>();
  private volatile boolean locked = false;
  private boolean lockedWhenLoading = false;

  private int[] stackIndexes = null;

  public final boolean hasLock() {
    return locked;
  }

  // User level code, override it if necessary

  /**
   * The main code of the procedure. It must be idempotent since execute() may be called multiple
   * times in case of machine failure in the middle of the execution.
   *
   * @param env the environment passed to the ProcedureExecutor
   * @return a set of sub-procedures to run or ourselves if there is more work to do or null if the
   *     procedure is done.
   * @throws InterruptedException the procedure will be added back to the queue and retried later.
   */
  protected abstract Procedure<Env>[] execute(Env env) throws InterruptedException;

  /**
   * The code to undo what was done by the execute() code. It is called when the procedure or one of
   * the sub-procedures failed or an abort was requested. It should cleanup all the resources
   * created by the execute() call. The implementation must be idempotent since rollback() may be
   * called multiple time in case of machine failure in the middle of the execution.
   *
   * @param env the environment passed to the ProcedureExecutor
   * @throws IOException temporary failure, the rollback will retry later
   * @throws InterruptedException the procedure will be added back to the queue and retried later
   */
  protected abstract void rollback(Env env)
      throws IOException, InterruptedException, ProcedureException;

  public void serialize(DataOutputStream stream) throws IOException {
    // procid
    stream.writeLong(this.procId);
    // state
    stream.writeInt(this.state.ordinal());
    // submit time
    stream.writeLong(this.submittedTime);
    // last updated
    stream.writeLong(this.lastUpdate);
    // parent id
    stream.writeLong(this.parentProcId);
    // time out
    stream.writeLong(this.timeout);
    // stack indexes
    if (stackIndexes != null) {
      stream.writeInt(stackIndexes.length);
      for (int index : stackIndexes) {
        stream.writeInt(index);
      }
    } else {
      stream.writeInt(-1);
    }

    // exceptions
    if (hasException()) {
      // symbol of exception
      stream.write((byte) 1);
      // exception's name
      String exceptionClassName = exception.getClass().getName();
      byte[] exceptionClassNameBytes = exceptionClassName.getBytes(StandardCharsets.UTF_8);
      stream.writeInt(exceptionClassNameBytes.length);
      stream.write(exceptionClassNameBytes);
      // exception's message
      String message = this.exception.getMessage();
      if (message != null) {
        byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);
        stream.writeInt(messageBytes.length);
        stream.write(messageBytes);
      } else {
        stream.writeInt(-1);
      }
    } else {
      // symbol of no exception
      stream.write((byte) 0);
    }

    // result
    if (result.get() != null) {
      stream.writeInt(result.get().length);
      stream.write(result.get());
    } else {
      stream.writeInt(-1);
    }

    // Has lock
    stream.write(this.hasLock() ? (byte) 1 : (byte) 0);
  }

  public void deserialize(ByteBuffer byteBuffer) {
    // procid
    this.setProcId(byteBuffer.getLong());
    // state
    this.setState(ProcedureState.values()[byteBuffer.getInt()]);
    //  submit time
    this.setSubmittedTime(byteBuffer.getLong());
    //  last updated
    this.setLastUpdate(byteBuffer.getLong());
    //  parent id
    this.setParentProcId(byteBuffer.getLong());
    //  time out
    this.setTimeout(byteBuffer.getLong());
    //  stack index
    int stackIndexesLen = byteBuffer.getInt();
    if (stackIndexesLen >= 0) {
      List<Integer> indexList = new ArrayList<>(stackIndexesLen);
      for (int i = 0; i < stackIndexesLen; i++) {
        indexList.add(byteBuffer.getInt());
      }
      this.setStackIndexes(indexList);
    }

    // exception
    if (byteBuffer.get() == 1) {
      deserializeTypeInfoForCompatibility(byteBuffer);
      int messageBytesLength = byteBuffer.getInt();
      String errMsg = null;
      if (messageBytesLength > 0) {
        byte[] messageBytes = new byte[messageBytesLength];
        byteBuffer.get(messageBytes);
        errMsg = new String(messageBytes, StandardCharsets.UTF_8);
      }
      setFailure(new ProcedureException(errMsg));
    }

    // result
    int resultLen = byteBuffer.getInt();
    if (resultLen > 0) {
      byte[] resultArr = new byte[resultLen];
      byteBuffer.get(resultArr);
    }
    //  has lock
    if (byteBuffer.get() == 1) {
      this.lockedWhenLoading();
    }
  }

  /**
   * Deserialize class Name and load class
   *
   * @param byteBuffer bytebuffer
   * @return Procedure
   */
  @Deprecated
  public static void deserializeTypeInfoForCompatibility(ByteBuffer byteBuffer) {
    int classNameBytesLen = byteBuffer.getInt();
    byte[] classNameBytes = new byte[classNameBytesLen];
    byteBuffer.get(classNameBytes);
  }

  /**
   * Acquire a lock, user should override it if necessary.
   *
   * @param env environment
   * @return state of lock
   */
  protected ProcedureLockState acquireLock(Env env) {
    return ProcedureLockState.LOCK_ACQUIRED;
  }

  /**
   * Release a lock, user should override it if necessary.
   *
   * @param env env
   */
  protected void releaseLock(Env env) {
    // no op
  }

  /**
   * Used to keep procedure lock even when the procedure is yielded or suspended.
   *
   * @param env env
   * @return true if hold the lock
   */
  protected boolean holdLock(Env env) {
    return false;
  }

  /**
   * To make executor yield between each execution step to give other procedures a chance to run.
   *
   * @param env environment
   * @return return true if yield is allowed.
   */
  protected boolean isYieldAfterExecution(Env env) {
    return false;
  }

  // -------------------------Internal methods - called by the procedureExecutor------------------
  /**
   * Internal method called by the ProcedureExecutor that starts the user-level code execute().
   *
   * @param env execute environment
   * @return sub procedures
   */
  protected Procedure<Env>[] doExecute(Env env) throws InterruptedException {
    try {
      updateTimestamp();
      return execute(env);
    } finally {
      updateTimestamp();
    }
  }

  /**
   * Internal method called by the ProcedureExecutor that starts the user-level code rollback().
   *
   * @param env execute environment
   * @throws IOException ioe
   * @throws InterruptedException interrupted exception
   */
  public void doRollback(Env env) throws IOException, InterruptedException, ProcedureException {
    try {
      updateTimestamp();
      rollback(env);
    } finally {
      updateTimestamp();
    }
  }

  /**
   * Internal method called by the ProcedureExecutor that starts the user-level code acquireLock().
   *
   * @param env environment
   * @param store ProcedureStore
   * @return ProcedureLockState
   */
  public final ProcedureLockState doAcquireLock(Env env, IProcedureStore store) {
    if (lockedWhenLoading) {
      lockedWhenLoading = false;
      locked = true;
      return ProcedureLockState.LOCK_ACQUIRED;
    }
    ProcedureLockState state = acquireLock(env);
    if (state == ProcedureLockState.LOCK_ACQUIRED) {
      locked = true;
      store.update(this);
    }
    return state;
  }

  /**
   * Presist lock state of the procedure
   *
   * @param env environment
   * @param store ProcedureStore
   */
  public final void doReleaseLock(Env env, IProcedureStore store) {
    locked = false;
    if (getState() != ProcedureState.ROLLEDBACK) {
      store.update(this);
    }
    releaseLock(env);
  }

  public final void restoreLock(Env env) {
    if (!lockedWhenLoading) {
      LOG.debug("{} didn't hold the lock before restarting, skip acquiring lock", this);
      return;
    }
    if (isFinished()) {
      LOG.debug("{} is already bypassed, skip acquiring lock.", this);
      return;
    }
    if (getState() == ProcedureState.WAITING && !holdLock(env)) {
      LOG.debug("{} is in WAITING STATE, and holdLock= false , skip acquiring lock.", this);
      return;
    }
    LOG.debug("{} held the lock before restarting, call acquireLock to restore it.", this);
    acquireLock(env);
  }

  @Override
  public String toString() {
    // Return the simple String presentation of the procedure.
    return toStringSimpleSB().toString();
  }

  /**
   * Build the StringBuilder for the simple form of procedure string.
   *
   * @return the StringBuilder
   */
  protected StringBuilder toStringSimpleSB() {
    final StringBuilder sb = new StringBuilder();

    sb.append("pid=");
    sb.append(getProcId());

    if (hasParent()) {
      sb.append(", ppid=");
      sb.append(getParentProcId());
    }

    /*
     * TODO
     * Enable later when this is being used.
     * Currently owner not used.
    if (hasOwner()) {
      sb.append(", owner=");
      sb.append(getOwner());
    }*/

    sb.append(", state="); // pState for Procedure State as opposed to any other kind.
    toStringState(sb);

    // Only print out locked if actually locked. Most of the time it is not.
    if (this.locked) {
      sb.append(", locked=").append(locked);
    }
    if (hasException()) {
      sb.append(", exception=" + getException());
    }

    sb.append("; ");
    toStringClassDetails(sb);

    return sb;
  }

  /** Extend the toString() information with more procedure details */
  public String toStringDetails() {
    final StringBuilder sb = toStringSimpleSB();

    sb.append(" submittedTime=");
    sb.append(getSubmittedTime());

    sb.append(", lastUpdate=");
    sb.append(getLastUpdate());

    final int[] stackIndices = getStackIndexes();
    if (stackIndices != null) {
      sb.append("\n");
      sb.append("stackIndexes=");
      sb.append(Arrays.toString(stackIndices));
    }

    return sb.toString();
  }

  /**
   * Called from {@link #toString()} when interpolating {@link Procedure} State. Allows decorating
   * generic Procedure State with Procedure particulars.
   *
   * @param builder Append current {@link ProcedureState}
   */
  protected void toStringState(StringBuilder builder) {
    builder.append(getState());
  }

  /**
   * Extend the toString() information with the procedure details e.g. className and parameters
   *
   * @param builder the string builder to use to append the proc specific information
   */
  protected void toStringClassDetails(StringBuilder builder) {
    builder.append(getClass().getName());
  }

  // ==========================================================================
  //  Those fields are unchanged after initialization.
  //
  //  Each procedure will get created from the user or during
  //  ProcedureExecutor.start() during the load() phase and then submitted
  //  to the executor. these fields will never be changed after initialization
  // ==========================================================================
  public long getProcId() {
    return procId;
  }

  public boolean hasParent() {
    return parentProcId != NO_PROC_ID;
  }

  public long getParentProcId() {
    return parentProcId;
  }

  public long getRootProcId() {
    return rootProcId;
  }

  public String getProcType() {
    return getClass().getSimpleName();
  }

  public long getSubmittedTime() {
    return submittedTime;
  }

  /** Called by the ProcedureExecutor to assign the ID to the newly created procedure. */
  public void setProcId(long procId) {
    this.procId = procId;
  }

  public void setProcRunnable() {
    this.submittedTime = System.currentTimeMillis();
    setState(ProcedureState.RUNNABLE);
  }

  /** Called by the ProcedureExecutor to assign the parent to the newly created procedure. */
  protected void setParentProcId(long parentProcId) {
    this.parentProcId = parentProcId;
  }

  protected void setRootProcId(long rootProcId) {
    this.rootProcId = rootProcId;
  }

  /**
   * Called on store load to initialize the Procedure internals after the creation/deserialization.
   */
  protected void setSubmittedTime(long submittedTime) {
    this.submittedTime = submittedTime;
  }

  // ==========================================================================
  //  runtime state - timeout related
  // ==========================================================================
  /**
   * @param timeout timeout interval in msec
   */
  protected void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  public boolean hasTimeout() {
    return timeout != NO_TIMEOUT;
  }

  /**
   * @return the timeout in msec
   */
  public long getTimeout() {
    return timeout;
  }

  /**
   * Called on store load to initialize the Procedure internals after the creation/deserialization.
   */
  protected void setLastUpdate(long lastUpdate) {
    this.lastUpdate = lastUpdate;
  }

  /** Called by ProcedureExecutor after each time a procedure step is executed. */
  protected void updateTimestamp() {
    this.lastUpdate = System.currentTimeMillis();
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  /**
   * Timeout of the next timeout. Called by the ProcedureExecutor if the procedure has timeout set
   * and the procedure is in the waiting queue.
   *
   * @return the timestamp of the next timeout.
   */
  protected long getTimeoutTimestamp() {
    return getLastUpdate() + getTimeout();
  }

  // ==========================================================================
  //  runtime state
  // ==========================================================================
  /**
   * @return the time elapsed between the last update and the start time of the procedure.
   */
  public long elapsedTime() {
    return getLastUpdate() - getSubmittedTime();
  }

  /**
   * @return the serialized result if any, otherwise null
   */
  public byte[] getResult() {
    return result.get();
  }

  /**
   * The procedure may leave a "result" on completion.
   *
   * @param result the serialized result that will be passed to the client
   */
  protected void setResult(byte[] result) {
    this.result.set(result);
  }

  /**
   * Will only be called when loading procedures from procedure store, where we need to record
   * whether the procedure has already held a lock. Later we will call {@link #restoreLock(Object)}
   * to actually acquire the lock.
   */
  final void lockedWhenLoading() {
    this.lockedWhenLoading = true;
  }

  /**
   * Can only be called when restarting, before the procedure actually being executed, as after we
   * actually call the {@link #doAcquireLock(Object, IProcedureStore)} method, we will reset {@link
   * #lockedWhenLoading} to false.
   *
   * <p>Now it is only used in the ProcedureScheduler to determine whether we should put a Procedure
   * in front of a queue.
   */
  public boolean isLockedWhenLoading() {
    return lockedWhenLoading;
  }

  // ==============================================================================================
  //  Runtime state, updated every operation by the ProcedureExecutor
  //
  //  There is always 1 thread at the time operating on the state of the procedure.
  //  The ProcedureExecutor may check and set states, or some Procecedure may
  //  update its own state. but no concurrent updates. we use synchronized here
  //  just because the procedure can get scheduled on different executor threads on each step.
  // ==============================================================================================

  /**
   * @return true if the procedure is in a RUNNABLE state.
   */
  public synchronized boolean isRunnable() {
    return state == ProcedureState.RUNNABLE;
  }

  public synchronized boolean isInitializing() {
    return state == ProcedureState.INITIALIZING;
  }

  /**
   * @return true if the procedure has failed. It may or may not have rolled back.
   */
  public synchronized boolean isFailed() {
    return state == ProcedureState.FAILED || state == ProcedureState.ROLLEDBACK;
  }

  /**
   * @return true if the procedure is finished successfully.
   */
  public synchronized boolean isSuccess() {
    return state == ProcedureState.SUCCESS && !hasException();
  }

  /**
   * @return true if the procedure is finished. The Procedure may be completed successfully or
   *     rolledback.
   */
  public synchronized boolean isFinished() {
    return isSuccess() || state == ProcedureState.ROLLEDBACK;
  }

  /**
   * @return true if the procedure is waiting for a child to finish or for an external event.
   */
  public synchronized boolean isWaiting() {
    switch (state) {
      case WAITING:
      case WAITING_TIMEOUT:
        return true;
      default:
        break;
    }
    return false;
  }

  protected synchronized void setState(final ProcedureState state) {
    this.state = state;
    updateTimestamp();
  }

  public synchronized ProcedureState getState() {
    return state;
  }

  protected synchronized void setFailure(final String source, final Throwable cause) {
    setFailure(new ProcedureException(source, cause));
  }

  protected synchronized void setFailure(final ProcedureException exception) {
    this.exception = exception;
    if (!isFinished()) {
      setState(ProcedureState.FAILED);
    }
  }

  /**
   * Called by the ProcedureExecutor when the timeout set by setTimeout() is expired.
   *
   * @return true to let the framework handle the timeout as abort, false in case the procedure
   *     handled the timeout itself.
   */
  protected synchronized boolean setTimeoutFailure(Env env) {
    if (state == ProcedureState.WAITING_TIMEOUT) {
      long timeDiff = System.currentTimeMillis() - lastUpdate;
      setFailure(
          "ProcedureExecutor",
          new ProcedureException("Operation timed out after " + timeDiff + " ms."));
      return true;
    }
    return false;
  }

  public synchronized boolean hasException() {
    return exception != null;
  }

  public synchronized ProcedureException getException() {
    return exception;
  }

  /** Called by the ProcedureExecutor on procedure-load to restore the latch state */
  protected synchronized void setChildrenLatch(int numChildren) {
    this.childrenLatch = numChildren;
    if (LOG.isTraceEnabled()) {
      LOG.trace("CHILD LATCH INCREMENT SET " + this.childrenLatch, new Throwable(this.toString()));
    }
  }

  /** Called by the ProcedureExecutor on procedure-load to restore the latch state */
  protected synchronized void incChildrenLatch() {
    // TODO: can this be inferred from the stack? I think so...
    this.childrenLatch++;
    if (LOG.isTraceEnabled()) {
      LOG.trace("CHILD LATCH INCREMENT " + this.childrenLatch, new Throwable(this.toString()));
    }
  }

  /** Called by the ProcedureExecutor to notify that one of the sub-procedures has completed. */
  private synchronized boolean childrenCountDown() {
    assert childrenLatch > 0 : this;
    boolean b = --childrenLatch == 0;
    if (LOG.isTraceEnabled()) {
      LOG.trace("CHILD LATCH DECREMENT " + childrenLatch, new Throwable(this.toString()));
    }
    return b;
  }

  /**
   * Try to set this procedure into RUNNABLE state. Succeeds if all subprocedures/children are done.
   *
   * @return True if we were able to move procedure to RUNNABLE state.
   */
  synchronized boolean tryRunnable() {
    // Don't use isWaiting in the below; it returns true for WAITING and WAITING_TIMEOUT
    if (getState() == ProcedureState.WAITING && childrenCountDown()) {
      setState(ProcedureState.RUNNABLE);
      return true;
    } else {
      return false;
    }
  }

  protected synchronized boolean hasChildren() {
    return childrenLatch > 0;
  }

  protected synchronized int getChildrenLatch() {
    return childrenLatch;
  }

  /**
   * Called by the RootProcedureState on procedure execution. Each procedure store its stack-index
   * positions.
   */
  protected synchronized void addStackIndex(final int index) {
    if (stackIndexes == null) {
      stackIndexes = new int[] {index};
    } else {
      int count = stackIndexes.length;
      stackIndexes = Arrays.copyOf(stackIndexes, count + 1);
      stackIndexes[count] = index;
    }
  }

  protected synchronized boolean removeStackIndex() {
    if (stackIndexes != null && stackIndexes.length > 1) {
      stackIndexes = Arrays.copyOf(stackIndexes, stackIndexes.length - 1);
      return false;
    } else {
      stackIndexes = null;
      return true;
    }
  }

  /**
   * Called on store load to initialize the Procedure internals after the creation/deserialization.
   */
  protected synchronized void setStackIndexes(final List<Integer> stackIndexes) {
    this.stackIndexes = new int[stackIndexes.size()];
    for (int i = 0; i < this.stackIndexes.length; ++i) {
      this.stackIndexes[i] = stackIndexes.get(i);
    }
  }

  protected synchronized boolean wasExecuted() {
    return stackIndexes != null;
  }

  protected synchronized int[] getStackIndexes() {
    return stackIndexes;
  }

  public void setRootProcedureId(long rootProcedureId) {
    this.rootProcId = rootProcedureId;
  }

  /**
   * @param a the first procedure to be compared.
   * @param b the second procedure to be compared.
   * @return true if the two procedures have the same parent
   */
  public static boolean haveSameParent(Procedure<?> a, Procedure<?> b) {
    return a.hasParent() && b.hasParent() && (a.getParentProcId() == b.getParentProcId());
  }

  @Override
  public int compareTo(Procedure<Env> other) {
    return Long.compare(getProcId(), other.getProcId());
  }

  /**
   * This function will be called just when procedure is submitted for execution.
   *
   * @param env The environment passed to the procedure executor
   */
  protected void updateMetricsOnSubmit(Env env) {
    if (env instanceof ConfigNodeProcedureEnv) {
      ((ConfigNodeProcedureEnv) env)
          .getConfigManager()
          .getProcedureManager()
          .getProcedureMetrics()
          .updateMetricsOnSubmit(getProcType());
    }
  }

  /**
   * This function will be called just after procedure execution is finished. Override this method
   * to update metrics at the end of the procedure. The default implementation adds runtime of a
   * procedure to a time histogram for successfully completed procedures. Increments failed counter
   * for failed procedures.
   *
   * @param env The environment passed to the procedure executor
   * @param runtime Runtime of the procedure in milliseconds
   * @param success true if procedure is completed successfully
   */
  protected void updateMetricsOnFinish(Env env, long runtime, boolean success) {
    if (env instanceof ConfigNodeProcedureEnv) {
      ((ConfigNodeProcedureEnv) env)
          .getConfigManager()
          .getProcedureManager()
          .getProcedureMetrics()
          .updateMetricsOnFinish(getProcType(), runtime, success);
    }
  }
}
