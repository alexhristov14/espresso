from typing import List, Any
from .base import EspressoInputAdapter
from ..models import EspressoListInputDefinition


class EspressoListInputAdapter(EspressoInputAdapter):
    def __init__(self, input_def: EspressoListInputDefinition):
        self.items = list(input_def.items)
        self.cursor = 0

    def poll(self) -> List[Any]:
        return self.poll_batch(batch_size=1)

    def poll_batch(self, batch_size: int) -> List[Any]:
        start_index = int(self.cursor) if self.cursor is not None else 0
        end_index = min(start_index + batch_size, len(self.items))

        batch = self.items[start_index:end_index]
        self.cursor = end_index if end_index < len(self.items) else None

        return batch

    def poll_all(self) -> List[Any]:
        return self.items[self.cursor :]

    def has_data(self) -> bool:
        return self.cursor < len(self.items) if self.cursor is not None else False
