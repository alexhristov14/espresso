from typing import List, Any, Dict
from .models import EspressoInputDefinition
from .inputs.base import EspressoInputAdapter
from .inputs.list_input import EspressoListInputAdapter
from .inputs.rabbitmq_input import EspressoRabbitMQInputAdapter


class EspressoInputManager:
    def __init__(self, inputs: List[EspressoInputDefinition]):
        self.adapters: Dict[str, EspressoInputAdapter] = {}
        self.input_types: Dict[str, str] = {}

        for inp in inputs:
            if inp.type == "list":
                adapter = EspressoListInputAdapter(inp)
                self.adapters[inp.id] = adapter
                self.input_types[inp.id] = "list"
            elif inp.type == "rabbitmq":
                adapter = EspressoRabbitMQInputAdapter(inp)
                self.adapters[inp.id] = adapter
                self.input_types[inp.id] = "rabbitmq"
            else:
                raise ValueError(f"Unknown input type: {inp.type}")

    async def poll(self, batch_size: int = 10):
        """
        Polls for each input adapter, one item at a time or by cursor pagination.
        """
        results: Dict[str, List[Any]] = {}

        for input_id, adapter in self.adapters.items():
            items = await adapter.poll_batch(batch_size=batch_size)
            if items:
                results[input_id] = items

        return results

    async def poll_all(self) -> Dict[str, List[Any]]:
        """ "
        Polls for each input adapter, all available items.
        """
        results: Dict[str, List[Any]] = {}
        for input_id, adapter in self.adapters.items():
            items = await adapter.poll_all()
            if items:
                results[input_id] = items
        return results

    async def has_data(self, input_id: str) -> bool:
        """
        Check if an input adapter has data available.
        """
        adapter = self.adapters.get(input_id)
        if not adapter:
            return False
        return await adapter.has_data()

    async def ack_batch(self, input_id: str, items: List[Any]) -> None:
        """
        Acknowledge a batch of messages after successful processing.
        """
        adapter = self.adapters.get(input_id)
        if not adapter:
            return

        if self.input_types.get(input_id) == "rabbitmq":
            for item in items:
                await adapter.ack(item)

    async def nack_batch(self, input_id: str, items: List[Any], requeue: bool = True) -> None:
        """
        Negative-acknowledge a batch of messages after failed processing.
        """
        adapter = self.adapters.get(input_id)
        if not adapter:
            return

        if self.input_types.get(input_id) == "rabbitmq":
            for item in items:
                await adapter.nack(item, requeue=requeue)
