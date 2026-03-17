from .chunking import chunk_text

__all__ = ["RetrievalService", "chunk_text"]


def __getattr__(name: str):
    if name == "RetrievalService":
        from .service import RetrievalService

        return RetrievalService
    raise AttributeError(name)
