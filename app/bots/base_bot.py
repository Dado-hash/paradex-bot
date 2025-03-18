from abc import ABC, abstractmethod


class BaseBot(ABC):
    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    async def trading_loop(self):
        pass
