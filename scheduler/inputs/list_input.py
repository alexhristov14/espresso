from typing import List, Any, AsyncIterator, Union
from collections import deque
from .base import EspressoInputAdapter
from ..models import EspressoListInputDefinition


class EspressoListInputAdapter(EspressoInputAdapter):
    def __init__(self, input_def: EspressoListInputDefinition):
        self.input_def = input_def
        self.buffer = deque()
        self.stream_iterator = None
        self.stream_exhausted = False

        if hasattr(input_def.items, "__aiter__"):
            # Infinite or async stream
            self.is_stream = True
            self.stream_iterator = input_def.items.__aiter__()
        else:
            # Static list
            self.is_stream = False
            if input_def.items:
                self.buffer.extend(input_def.items)

    async def _fill_buffer(self, target_size: int) -> None:
        if not self.is_stream or self.stream_exhausted:
            return

        try:
            while len(self.buffer) < target_size and not self.stream_exhausted:
                item = await self.stream_iterator.__anext__()
                self.buffer.append(item)
        except StopAsyncIteration:
            self.stream_exhausted = True

    async def poll(self) -> List[Any]:
        return await self.poll_batch(batch_size=1)

    async def poll_batch(self, batch_size: int) -> List[Any]:
        if self.is_stream:
            await self._fill_buffer(batch_size)

        batch = []
        for _ in range(min(batch_size, len(self.buffer))):
            if self.buffer:
                batch.append(self.buffer.popleft())

        return batch

    async def poll_all(self) -> List[Any]:
        if self.is_stream:
            await self._fill_buffer(len(self.buffer) + 100)

        all_items = list(self.buffer)
        self.buffer.clear()
        return all_items

    async def has_data(self) -> bool:
        if len(self.buffer) > 0:
            return True

        if self.is_stream and not self.stream_exhausted:
            await self._fill_buffer(1)
            return len(self.buffer) > 0

        return False

    def append_item(self, item: Any) -> None:
        self.buffer.append(item)

    def append_items(self, items: List[Any]) -> None:
        self.buffer.extend(items)
