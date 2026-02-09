import asyncio

from durabletask.worker import TaskHubGrpcWorker


def test_async_activity_executes():
    worker = TaskHubGrpcWorker()

    @worker.add_activity
    async def my_activity(ctx, input):
        await asyncio.sleep(0.01)
        return "ok"

    class DummyStub:
        def __init__(self):
            self.completed = []

        def CompleteActivityTask(self, res):
            self.completed.append(res)

    class DummyReq:
        def __init__(self):
            self.name = "my_activity"
            self.taskId = 1
            self.input = type("I", (), {"value": ""})
            self.orchestrationInstance = type("O", (), {"instanceId": "inst1"})
            # Tracing context fields are accessed by worker, so provide minimal shape
            self.parentTraceContext = type("T", (), {"traceParent": ""})

    async def run():
        stub = DummyStub()
        await worker._execute_activity_async(DummyReq(), stub, completionToken=None)
        assert len(stub.completed) == 1

    asyncio.run(run())

