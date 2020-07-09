using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// A Snapshot persists a version by making a copy for every entry of that version separate from the log. It is
    /// slower and more complex than a foldover, but more space-efficient on the log, and retains in-place
    /// update performance as it does not advance the readonly marker unnecessarily.
    /// </summary>
    internal class SnapshotCheckpointTask : HybridLogCheckpointOrchestrationTask
    {
        /// <inheritdoc />
        public override void GlobalBeforeEnteringState<Key, Value, Input, Output, Context, Functions>(SystemState next,
            FasterKV<Key, Value, Input, Output, Context, Functions> faster)
        {
            base.GlobalBeforeEnteringState(next, faster);
            switch (next.phase)
            {
                case Phase.PREPARE:
                    faster._hybridLogCheckpoint.info.flushedLogicalAddress = faster.hlog.FlushedUntilAddress;
                    faster._hybridLogCheckpoint.info.useSnapshotFile = 1;
                    break;
                case Phase.WAIT_FLUSH:
                    faster.ObtainCurrentTailAddress(ref faster._hybridLogCheckpoint.info.finalLogicalAddress);

                    faster._hybridLogCheckpoint.snapshotFileDevice =
                        faster.checkpointManager.GetSnapshotLogDevice(faster._hybridLogCheckpointToken);
                    faster._hybridLogCheckpoint.snapshotFileObjectLogDevice =
                        faster.checkpointManager.GetSnapshotObjectLogDevice(faster._hybridLogCheckpointToken);
                    faster._hybridLogCheckpoint.snapshotFileDevice.Initialize(faster.hlog.GetSegmentSize());
                    faster._hybridLogCheckpoint.snapshotFileObjectLogDevice.Initialize(-1);

                    long startPage = faster.hlog.GetPage(faster._hybridLogCheckpoint.info.flushedLogicalAddress);
                    long endPage = faster.hlog.GetPage(faster._hybridLogCheckpoint.info.finalLogicalAddress);
                    if (faster._hybridLogCheckpoint.info.finalLogicalAddress >
                        faster.hlog.GetStartLogicalAddress(endPage))
                    {
                        endPage++;
                    }

                    // This can be run on a new thread if we want to immediately parallelize 
                    // the rest of the log flush
                    faster.hlog.AsyncFlushPagesToDevice(
                        startPage,
                        endPage,
                        faster._hybridLogCheckpoint.info.finalLogicalAddress,
                        faster._hybridLogCheckpoint.snapshotFileDevice,
                        faster._hybridLogCheckpoint.snapshotFileObjectLogDevice,
                        out faster._hybridLogCheckpoint.flushedSemaphore);
                    break;
            }
        }

        /// <inheritdoc />
        public override async ValueTask OnThreadState<Key, Value, Input, Output, Context, Functions>(
            SystemState current,
            SystemState prev, FasterKV<Key, Value, Input, Output, Context, Functions> faster,
            FasterKV<Key, Value, Input, Output, Context, Functions>.FasterExecutionContext ctx,
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, bool async = true,
            CancellationToken token = default)
        {
            await base.OnThreadState(current, prev, faster, ctx, clientSession, async, token);
            if (current.phase != Phase.WAIT_FLUSH) return;

            if (ctx == null || !ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush])
            {
                var notify = faster._hybridLogCheckpoint.flushedSemaphore != null &&
                             faster._hybridLogCheckpoint.flushedSemaphore.CurrentCount > 0;

                if (async && !notify)
                {
                    Debug.Assert(faster._hybridLogCheckpoint.flushedSemaphore != null);
                    clientSession?.UnsafeSuspendThread();
                    await faster._hybridLogCheckpoint.flushedSemaphore.WaitAsync(token);
                    clientSession?.UnsafeResumeThread();
                    faster._hybridLogCheckpoint.flushedSemaphore.Release();
                    notify = true;
                }

                if (!notify) return;
                
                if (ctx != null)
                    ctx.prevCtx.markers[EpochPhaseIdx.WaitFlush] = true;
            }

            if (ctx != null)
                faster.epoch.Mark(EpochPhaseIdx.WaitFlush, current.version);

            if (faster.epoch.CheckIsComplete(EpochPhaseIdx.WaitFlush, current.version))
                faster.GlobalStateMachineStep(current);
        }
    }
}